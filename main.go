package main

import (
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
)

type Member string

func (m Member) String() string {
	return string(m)
}

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
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
		// rand.Read(key)
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

func main() {
	defer func(begin time.Time) {
		slog.Info("end;", "duration", time.Since(begin))
	}(time.Now())

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
