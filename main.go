package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/alphauslabs/jupiter/internal/appdata"
	"github.com/alphauslabs/jupiter/internal/cluster"
	"github.com/alphauslabs/jupiter/internal/flags"
	rl "github.com/alphauslabs/jupiter/internal/ratelimit"
	v1 "github.com/alphauslabs/jupiter/proto/v1"
	"github.com/flowerinthenight/hedge"
	"github.com/flowerinthenight/timedoff"
	"github.com/golang/glog"
	"github.com/grpc-ecosystem/go-grpc-middleware/ratelimit"
	"github.com/tidwall/redcon"
	"google.golang.org/grpc"
)

var (
	cctx = func(p context.Context) context.Context {
		return context.WithValue(p, struct{}{}, nil)
	}

	// f *os.File
)

func grpcServe(ctx context.Context, network, port string, done chan error) error {
	l, err := net.Listen(network, ":"+port)
	if err != nil {
		glog.Errorf("net.Listen failed: %v", err)
		return err
	}

	defer l.Close()
	gs := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			ratelimit.UnaryServerInterceptor(&rl.Limiter{}),
		),
		grpc.ChainStreamInterceptor(
			ratelimit.StreamServerInterceptor(&rl.Limiter{}),
		),
	)

	svc := &service{}
	v1.RegisterJupiterServer(gs, svc)

	go func() {
		<-ctx.Done()
		gs.GracefulStop()
		done <- nil
	}()

	return gs.Serve(l)
}

func main() {
	flag.Parse()
	defer glog.Flush()

	// Test:
	if *flags.Test {
		test()
		return
	}

	app := &appdata.AppData{}
	var err error
	ctx, cancel := context.WithCancel(context.Background())

	// For debug: log if no leader detected.
	app.LeaderActive = timedoff.New(time.Minute*30, &timedoff.CallbackT{
		Callback: func(args interface{}) {
			glog.Errorf("failed: no leader for the past 30mins?")
		},
	})

	app.Client, err = spanner.NewClient(cctx(ctx), *flags.Database)
	if err != nil {
		glog.Fatal(err) // essential
	}

	defer app.Client.Close()
	clusterData := cluster.ClusterData{App: app}

	// Setup our group coordinator.
	app.FleetOp = hedge.New(
		app.Client,
		":8081",
		"jupiter_lock",
		"jupiter",
		"jupiter_store",
		hedge.WithGroupSyncInterval(time.Second*10),
		hedge.WithLeaderHandler(&clusterData, cluster.LeaderHandler),
		hedge.WithBroadcastHandler(&clusterData, cluster.BroadcastHandler),
	)

	done := make(chan error)
	doneLock := make(chan error, 1)
	go app.FleetOp.Run(cctx(ctx), doneLock)

	// Ensure leader is active before proceeding.
	func() {
		var m string
		defer func(line *string, begin time.Time) {
			glog.Infof("%v %v", *line, time.Since(begin))
		}(&m, time.Now())

		glog.Infof("attempt leader wait...")
		ok, err := cluster.EnsureLeaderActive(cctx(ctx), app)
		switch {
		case !ok:
			m = fmt.Sprintf("failed: %v, no leader after", err)
		default:
			m = "confirm: leader active after"
		}
	}()

	go cluster.LeaderLiveness(cctx(ctx), app)

	// Setup our cluster of Redis nodes.
	rcluster := cluster.NewCluster()
	defer rcluster.Close()
	for _, m := range strings.Split(*flags.Members, ",") {
		rcluster.AddMember(m)
	}

	// Test random ping.
	err = rcluster.RandomPing()
	if err != nil {
		glog.Fatal(err) // so we will know
	}

	clusterData.Cluster = rcluster
	atomic.StoreInt32(&clusterData.ClusterOk, 1)

	// Setup our gRPC management API.
	go func() {
		port := "8080"
		glog.Infof("serving grpc at :%v", port)
		if err := grpcServe(ctx, "tcp", port, done); err != nil {
			glog.Fatal(err)
		}
	}()

	// Setup our Redis proxy.
	addr := ":6379"
	rclone := redcon.NewServer(addr, newProxy(app, rcluster).Handler,
		func(conn redcon.Conn) bool { return true },
		func(conn redcon.Conn, err error) {},
	)

	defer rclone.Close()

	go func() {
		glog.Infof("start redis proxy at %s", addr)
		err := rclone.ListenAndServe()
		if err != nil {
			glog.Fatal(err)
		}
	}()

	// f, err = os.Create("/tmp/cpuprofile")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// pprof.StartCPUProfile(f)

	// Interrupt handler.
	go func() {
		sigch := make(chan os.Signal)
		signal.Notify(sigch, syscall.SIGINT, syscall.SIGTERM)
		glog.Infof("signal: %v", <-sigch)
		cancel()
	}()

	<-done
	<-doneLock
}
