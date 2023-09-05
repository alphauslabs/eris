package main

import (
	"bytes"
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
	"github.com/flowerinthenight/hedge"
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
		"ping":    pingCmd,
		"distget": distGetCmd,
		"detach":  detachCmd,
		"quit":    quitCmd,
		"config":  configCmd,
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

func distGetCmd(conn redcon.Conn, cmd redcon.Command, key string, p *proxy) {
	defer func(begin time.Time) {
		glog.Infof("distGetCmd took %v", time.Since(begin))
	}(time.Now())

	glog.Infof("initiator=%v, args[0]=%v, args[1]=%v",
		p.app.FleetOp.Name(),
		string(cmd.Args[0]),
		string(cmd.Args[1]),
	)

	ctx := context.Background()
	key = string(cmd.Args[1])
	keyLen := fmt.Sprintf("%v/len", key)
	n, err := redis.Int(p.cluster.Do(key, [][]byte{[]byte("GET"), []byte(keyLen)}))
	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}

	// TODO: Expose member list in hedge.
	// For now, get all members in the cluster via empty broadcast.
	members := make(map[string]string)
	b, _ := json.Marshal(internal.NewEvent(
		hedge.KeyValue{}, // dummy
		cluster.EventSource,
		cluster.CtrlBroadcastEmpty,
	))

	outs := p.app.FleetOp.Broadcast(ctx, b)
	for _, out := range outs {
		members[out.Id] = out.Id
	}

	glog.Infof("len(chunks)=%v, members=%v", n, members)

	var nodes []string
	for k := range members {
		nodes = append(nodes, k)
	}

	// Assign query indeces to all members.
	loc := 0
	assign := make(map[int]string)
	for i := 0; i < n; i++ {
		assign[i] = nodes[loc]
		loc++
		if loc >= len(members) {
			loc = 0
		}
	}

	mb := make(map[int][]byte)
	errs := []error{}
	b, _ = json.Marshal(internal.NewEvent(
		cluster.DistributedGetInput{Name: key, Assign: assign},
		cluster.EventSource,
		cluster.CtrlBroadcastDistributedGet,
	))

	// Send out GET assignments to all members; wait for reply.
	// TODO: How to handle any member failing? For now, fail all.
	outs = p.app.FleetOp.Broadcast(ctx, b)
	for _, out := range outs {
		members[out.Id] = out.Id
		if out.Error != nil {
			errs = append(errs, out.Error)
		} else {
			var o cluster.DistributedGetOutput
			err := json.Unmarshal(out.Reply, &o)
			if err != nil {
				errs = append(errs, out.Error)
			} else {
				for k, v := range o.Data {
					mb[k] = v
				}
			}
		}
	}

	if len(errs) > 0 {
		glog.Errorf("failed: %v", errs)
		conn.WriteError("ERR no cache")
		return
	}

	var out bytes.Buffer
	for i := 0; i < n; i++ {
		if _, ok := mb[i]; !ok {
			m := fmt.Sprintf("index %v not found", i)
			glog.Errorf("failed: %v", m)
			conn.WriteError("ERR " + m)
			return
		}

		out.Write(mb[i])
	}

	glog.Infof("len=%v, cap=%v", out.Len(), out.Cap())
	conn.WriteAny(out.Bytes())
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
