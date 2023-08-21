package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	v1 "github.com/alphauslabs/jupiter/proto/v1"
	"github.com/buraksezer/consistent"
	"github.com/golang/glog"
	"github.com/grpc-ecosystem/go-grpc-middleware/ratelimit"
	"github.com/tidwall/redcon"
	"golang.org/x/oauth2/google"
	"google.golang.org/grpc"
)

var (
	paramTest = flag.Bool("test", false, "Scratch pad, anything")
)

func test() {
	members := []consistent.Member{}
	for i := 0; i < 3; i++ {
		member := Member(fmt.Sprintf("node%d", i))
		members = append(members, member)
	}

	// Modify PartitionCount, ReplicationFactor and Load to increase or decrease
	// relocation ratio.
	cfg := consistent.Config{
		PartitionCount:    27_103,
		ReplicationFactor: 10,
		Hasher:            hasher{},
	}

	c := consistent.New(members, cfg)

	addMember(c, cfg)
	checkLoad(c, cfg)
	delMember(c, cfg)
	checkLoad(c, cfg)
	testKey(c)
}

func testKey(c *consistent.Consistent) {
	defer func(begin time.Time) {
		slog.Info("testKey;", "duration", time.Since(begin))
	}(time.Now())

	m := c.LocateKey([]byte("sample/key"))
	slog.Info("locate;", "member", m.String())

	mn, err := c.GetClosestN([]byte("sample/key"), 1)
	if err != nil {
		slog.Error("GetClosestN failed;", "err", err)
	} else {
		slog.Info("closest;", "member", mn)
	}
}

func addMember(c *consistent.Consistent, cfg consistent.Config) {
	defer func(begin time.Time) {
		slog.Info("addMember;", "duration", time.Since(begin))
	}(time.Now())

	// Store current layout of partitions
	owners := make(map[int]string)
	for part := 0; part < cfg.PartitionCount; part++ {
		owners[part] = c.GetPartitionOwner(part).String()
	}

	// Add a new member
	c.Add(Member(fmt.Sprintf("node%d", 3)))

	// Get the new layout and compare with the previous
	var changed int
	for part, member := range owners {
		owner := c.GetPartitionOwner(part)
		if member != owner.String() {
			changed++
			msg := fmt.Sprintf("part: %6d moved to %s from %s", part, owner.String(), member)
			slog.Info(msg)
		}
	}

	slog.Info("partitions are relocated;", "percent", (100*changed)/cfg.PartitionCount)
}

func delMember(c *consistent.Consistent, cfg consistent.Config) {
	defer func(begin time.Time) {
		slog.Info("delMember;", "duration", time.Since(begin))
	}(time.Now())

	// Store current layout of partitions
	owners := make(map[int]string)
	for part := 0; part < cfg.PartitionCount; part++ {
		owners[part] = c.GetPartitionOwner(part).String()
	}

	c.Remove(fmt.Sprintf("node%d", 0))

	// Get the new layout and compare with the previous
	var changed int
	for part, member := range owners {
		owner := c.GetPartitionOwner(part)
		if member != owner.String() {
			changed++
			msg := fmt.Sprintf("part: %6d moved to %s from %s", part, owner.String(), member)
			slog.Info(msg)
		}
	}

	slog.Info("partitions are relocated;", "percent", (100*changed)/cfg.PartitionCount)
}

func checkLoad(c *consistent.Consistent, cfg consistent.Config) {
	defer func(begin time.Time) {
		slog.Info("checkLoad;", "duration", time.Since(begin))
	}(time.Now())

	// load distribution
	keyCount := 100_000_000
	load := (c.AverageLoad() * float64(keyCount)) / float64(cfg.PartitionCount)
	slog.Info("max key for a member should be around this;", "count", math.Ceil(load))
	distribution := make(map[string]int)
	// key := make([]byte, 4)

	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("key%v", i)
		member := c.LocateKey([]byte(key))
		distribution[member.String()]++
	}
	for member, count := range distribution {
		slog.Info("keys distribution;", "member", member, "keyCount", count)
	}

	for k, v := range c.LoadDistribution() {
		slog.Info("load distribution;", "member", k, "load", v)
	}
}

func checkCreds(ctx context.Context) {
	creds, err := google.FindDefaultCredentials(ctx)
	if err != nil {
		glog.Errorf("FindDefaultCredentials failed: %v", err)
		return
	}

	b, _ := json.Marshal(creds)
	glog.Infof("[dbg] creds: %v", string(b))
}

func run(ctx context.Context, network, port string, done chan error) error {
	checkCreds(ctx)
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
	defer func(begin time.Time) {
		slog.Info("end;", "duration", time.Since(begin))
	}(time.Now())

	flag.Parse()
	defer glog.Flush()

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
