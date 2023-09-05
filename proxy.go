package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alphauslabs/jupiter/internal"
	"github.com/alphauslabs/jupiter/internal/appdata"
	"github.com/alphauslabs/jupiter/internal/cluster"
	"github.com/golang/glog"
	"github.com/gomodule/redigo/redis"
	"github.com/google/uuid"
	"github.com/tidwall/redcon"
)

var (
	mu    sync.RWMutex
	items = make(map[string][]byte)
	ps    redcon.PubSub

	cmds = map[string]func(redcon.Conn, redcon.Command, string, *proxy){
		"detach":   detachCmd,
		"ping":     pingCmd,
		"quit":     quitCmd,
		"config":   configCmd,
		"disttest": distTestCmd,
	}
)

type proxy struct {
	app     *appdata.AppData
	cluster *cluster.Cluster
}

// Special: optional last args fmt: hash={key}|index={num}
// where:
//
//	{key} = string combination (chars not allowed: ,=)
//	{num} = 0-based index in args to use as hash key
//
// If this custom args is not provided, args[1] will be used.
func (p *proxy) Handler(conn redcon.Conn, cmd redcon.Command) {
	ncmd := cmd
	var key string
	if len(ncmd.Args) >= 2 {
		var custom bool
		last := string(ncmd.Args[len(ncmd.Args)-1])
		switch {
		case strings.HasPrefix(last, "hash="):
			key = strings.Split(last, "=")[1]
			custom = true
		case strings.HasPrefix(last, "index="):
			i, err := strconv.Atoi(strings.Split(last, "=")[1])
			if err != nil {
				conn.WriteError("ERR [" + err.Error() + "]")
				return
			}

			if i == 0 || i >= (len(ncmd.Args)-1) {
				conn.WriteError("ERR " + fmt.Sprintf("[invalid index %d]", i))
				return
			}

			key = string(ncmd.Args[i])
			custom = true
		}

		if custom {
			ncmd = redcon.Command{
				Raw:  cmd.Raw,
				Args: cmd.Args[:len(cmd.Args)-1],
			}
		}
	}

	cmdtl := strings.ToLower(string(ncmd.Args[0]))
	if _, found := cmds[cmdtl]; found {
		cmds[cmdtl](conn, ncmd, key, p)
		return
	}

	if len(ncmd.Args) >= 2 && key == "" {
		key = string(ncmd.Args[1])
	}

	if key == "" {
		key = uuid.NewString()
	}

	v, err := p.cluster.Do(key, ncmd.Args)
	if err != nil {
		conn.WriteError(err.Error())
	} else {
		conn.WriteAny(v)
	}
}

func newProxy(app *appdata.AppData, c *cluster.Cluster) *proxy {
	return &proxy{app: app, cluster: c}
}

func detachCmd(conn redcon.Conn, cmd redcon.Command, key string, p *proxy) {
	hconn := conn.Detach()
	glog.Info("connection has been detached")
	go func() {
		defer hconn.Close()
		hconn.WriteString("OK")
		hconn.Flush()
	}()
}

func pingCmd(conn redcon.Conn, cmd redcon.Command, key string, p *proxy) {
	switch {
	case key != "":
		v, err := p.cluster.Do(key, cmd.Args)
		if err != nil {
			conn.WriteError(err.Error())
		} else {
			conn.WriteAny(v)
		}
	default:
		conn.WriteString("PONG")
	}

	// pprof
	// pprof.StopCPUProfile()
}

func quitCmd(conn redcon.Conn, cmd redcon.Command, key string, p *proxy) {
	conn.WriteString("OK")
	conn.Close()
}

func configCmd(conn redcon.Conn, cmd redcon.Command, key string, p *proxy) {
	// This simple (blank) response is only here to allow for the
	// redis-benchmark command to work with this clone.
	conn.WriteArray(2)
	conn.WriteBulk(cmd.Args[2])
	conn.WriteBulkString("")
}

func distTestCmd(conn redcon.Conn, cmd redcon.Command, key string, p *proxy) {
	defer func(begin time.Time) {
		glog.Infof("distTestCmd took %v", time.Since(begin))
	}(time.Now())

	ctx := context.Background()
	key = "proto/len"
	n, err := redis.Int(p.cluster.Do(key, [][]byte{[]byte("GET"), []byte("proto/len")}))
	glog.Infof("%v, %v", n, err)

	// TODO: Expose member list in hedge.
	// Get all members in the cluster.
	members := make(map[string]string)
	b, _ := json.Marshal(internal.NewEvent(
		cluster.TrialDistInput{Assign: map[int]string{0: "node1"}}, // dummy
		"jupiter/internal",
		cluster.CtrlBroadcastTrialDist,
	))

	outs := p.app.FleetOp.Broadcast(ctx, b)
	for _, out := range outs {
		members[out.Id] = out.Id
		if out.Error != nil {
			glog.Errorf("%v failed: %v", out.Id, out.Error)
		}
	}

	glog.Infof("members=%v", members)
	var nodes []string
	for k := range members {
		nodes = append(nodes, k)
	}

	// Assign query indeces to all members.
	assign := make(map[int]string)
	idx := 0
	for i := 0; i < n; i++ {
		assign[i] = nodes[idx]
		idx++
		if idx >= len(members) {
			idx = 0
		}
	}

	var total int
	b, _ = json.Marshal(internal.NewEvent(
		cluster.TrialDistInput{Assign: assign},
		"jupiter/internal",
		cluster.CtrlBroadcastTrialDist,
	))

	outs = p.app.FleetOp.Broadcast(ctx, b)
	for _, out := range outs {
		members[out.Id] = out.Id
		if out.Error != nil {
			glog.Errorf("%v failed: %v", out.Id, out.Error)
		} else {
			total += len(out.Reply)
		}
	}

	glog.Infof("bytes=%v", total)
	conn.WriteString("OK")
}
