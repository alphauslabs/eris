package main

import (
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/alphauslabs/jupiter/internal/cluster"
	"github.com/alphauslabs/jupiter/internal/flags"
	"github.com/buraksezer/consistent"
)

type lmember string

func (m lmember) String() string { return string(m) }

func test() {
	if true {
		var n int
		n = 5801 / 5
		slog.Info("assigned:", "n", n)
		slog.Info("extra:", "mod", 5801%5)
		return
	}

	members := []consistent.Member{}
	for i := 0; i < 3; i++ {
		m := lmember(fmt.Sprintf("node%d", i))
		members = append(members, m)
	}

	// Modify PartitionCount, ReplicationFactor and Load to increase or decrease
	// relocation ratio.
	cfg := consistent.Config{
		PartitionCount:    *flags.Partitions,
		ReplicationFactor: *flags.ReplicationFactor,
		Hasher:            cluster.Hasher{},
	}

	c := consistent.New(members, cfg)

	addMember(c, cfg)
	// checkLoad(c, cfg)
	delMember(c, cfg)
	// checkLoad(c, cfg)
	testKey(c)
}

func testKey(c *consistent.Consistent) {
	defer func(begin time.Time) {
		slog.Info("testKey;", "duration", time.Since(begin))
	}(time.Now())

	for i := 0; i < 100_000_000; i++ {
		k := fmt.Sprintf("sample/key%04d", i)
		m := c.LocateKey([]byte(k))
		// pk := c.FindPartitionID([]byte(k))
		// slog.Info("locate;", "member", m.String(), "key", k, "part", pk)
		_ = m
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
	c.Add(lmember(fmt.Sprintf("node%d", 3)))

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
