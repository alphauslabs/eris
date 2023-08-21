package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	v1 "github.com/alphauslabs/jupiter/proto/v1"
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
)

func run(ctx context.Context, network, port string, done chan error) error {
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
	defer func(begin time.Time) {
		slog.Info("end;", "duration", time.Since(begin))
		glog.Flush()
	}(time.Now())

	if *paramTest {
		test()
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)

	go func() {
		port := "8080"
		glog.Infof("serving grpc at :%v", port)
		if err := run(ctx, "tcp", port, done); err != nil {
			glog.Fatal(err)
		}
	}()

	addr := ":6379"
	rc := redcon.NewServer(addr, handler,
		func(conn redcon.Conn) bool {
			glog.Infof("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			glog.Infof("closed: %s, err: %v", conn.RemoteAddr(), err)
		},
	)

	go func() {
		glog.Infof("start redis proxy at %s", addr)
		err := rc.ListenAndServe()
		if err != nil {
			glog.Fatal(err)
		}
	}()

	// Interrupt handler.
	go func() {
		sigch := make(chan os.Signal)
		signal.Notify(sigch, syscall.SIGINT, syscall.SIGTERM)
		glog.Infof("signal: %v", <-sigch)
		cancel()
	}()

	<-done
	rc.Close()
}
