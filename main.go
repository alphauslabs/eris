package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"cloud.google.com/go/spanner"
	v1 "github.com/alphauslabs/jupiter/proto/v1"
	"github.com/flowerinthenight/hedge"
	"github.com/flowerinthenight/timedoff"
	"github.com/golang/glog"
	"github.com/grpc-ecosystem/go-grpc-middleware/ratelimit"
	"github.com/tidwall/redcon"
	"google.golang.org/grpc"
)

var (
	paramTest              = flag.Bool("test", false, "Scratch pad, anything")
	paramMembers           = flag.String("members", "", "Initial Redis members, comma-separated, fmt: [passwd@]host:port")
	paramPartitions        = flag.Int("partitions", 27_103, "Partition count for our consistent hashring")
	paramReplicationFactor = flag.Int("replicationfactor", 10, "Replication factor for our consistent hashring")
	paramDatabase          = flag.String("db", "", "Spanner database, fmt: projects/{v}/instances/{v}/databases/{v}")

	cctx = func(p context.Context) context.Context {
		return context.WithValue(p, struct{}{}, nil)
	}

	client       *spanner.Client    // spanner client
	op           *hedge.Op          // group coordinator
	leaderActive *timedoff.TimedOff // when active/on, we have a live leader in the group
	redisFleet   *fleet             // our main fleet of cache nodes

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
			ratelimit.UnaryServerInterceptor(&limiter{}),
		),
		grpc.ChainStreamInterceptor(
			ratelimit.StreamServerInterceptor(&limiter{}),
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
	if *paramTest {
		test()
		return
	}

	var err error
	ctx, cancel := context.WithCancel(context.Background())

	// For debug: log if no leader detected.
	leaderActive = timedoff.New(time.Minute*30, &timedoff.CallbackT{
		Callback: func(args interface{}) {
			glog.Errorf("failed: no leader for the past 30mins?")
		},
	})

	client, err = spanner.NewClient(cctx(ctx), *paramDatabase)
	if err != nil {
		glog.Fatal(err) // essential
	}

	// Setup our group coordinator.
	op = hedge.New(
		client,
		":8081",
		"jupiter_lock",
		"jupiter",
		"jupiter_store",
		hedge.WithGroupSyncInterval(time.Second*10),
		hedge.WithLeaderHandler(nil, leaderHandler),
		hedge.WithBroadcastHandler(nil, broadcastHandler),
	)

	done := make(chan error)
	doneLock := make(chan error, 1)
	go op.Run(cctx(ctx), doneLock)

	// Ensure leader is active before proceeding.
	func() {
		var m string
		defer func(line *string, begin time.Time) {
			glog.Infof("%v %v", *line, time.Since(begin))
		}(&m, time.Now())

		glog.Infof("attempt leader wait...")
		ok, err := ensureLeaderActive(cctx(ctx))
		switch {
		case !ok:
			m = fmt.Sprintf("failed: %v, no leader after", err)
		default:
			m = "confirm: leader active after"
		}
	}()

	go leaderLiveness(cctx(ctx))

	// Setup our fleet of Redis nodes.
	redisFleet = newFleet()
	defer redisFleet.close()
	for _, m := range strings.Split(*paramMembers, ",") {
		redisFleet.addMember(m)
	}

	// Test random ping.
	err = redisFleet.ping()
	if err != nil {
		glog.Fatal(err) // so we will know
	}

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
	rc := redcon.NewServer(addr, handler,
		func(conn redcon.Conn) bool { return true },
		func(conn redcon.Conn, err error) {},
	)

	go func() {
		glog.Infof("start redis proxy at %s", addr)
		err := rc.ListenAndServe()
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
	rc.Close()
	client.Close()
}
