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
	"github.com/golang/glog"
	"github.com/google/uuid"
	"github.com/tidwall/redcon"
)

var (
	mu    sync.RWMutex
	items = make(map[string][]byte)
	ps    redcon.PubSub

	cmds = map[string]func(redcon.Conn, redcon.Command, metaT){
		"ping":    pingCmd,
		"distget": distGetCmd,
		"detach":  detachCmd,
		"quit":    quitCmd,
		"config":  configCmd,
	}
)

type metaT struct {
	this   *proxy // required
	key    string // optional
	chunks int    // optional, used in DISTGET
}

type proxy struct {
	app     *appdata.AppData
	cluster *cluster.Cluster
}

// Special: optional last args fmt: hash={key}[,len=n]|index={num}
// where:
//
//	{key} = string combination (chars not allowed: ,=)
//	n     = if provided, use as len instead of querying 'key/len'
//	        (for now, used in DISTGET)
//	{num} = 0-based index in args to use as hash key
//
// If this custom args is not provided, args[1] will be used.
func (p *proxy) Handler(conn redcon.Conn, cmd redcon.Command) {
	ncmd := cmd
	var key string
	var chunks int
	if len(ncmd.Args) >= 2 {
		var custom bool
		last := string(ncmd.Args[len(ncmd.Args)-1])
		switch {
		case strings.HasPrefix(last, "hash="):
			ll := strings.Split(last, ",")
			if len(ll) > 1 { // see if len=n is provided
				if strings.HasPrefix(ll[1], "len=") {
					chunks, _ = strconv.Atoi(strings.Split(ll[1], "=")[1])
				}
			}

			key = strings.Split(ll[0], "=")[1]
			custom = true
		case strings.HasPrefix(last, "index="):
			i, err := strconv.Atoi(strings.Split(last, "=")[1])
			if err != nil {
				conn.WriteError("ERR " + err.Error())
				return
			}

			if i == 0 || i >= (len(ncmd.Args)-1) {
				conn.WriteError("ERR " + fmt.Sprintf("invalid index [%d]", i))
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
		meta := metaT{this: p, key: key, chunks: chunks}
		cmds[cmdtl](conn, ncmd, meta)
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
		// Already have the 'ERR ' prefix.
		conn.WriteError(err.Error())
		return
	}

	// Write as is.
	conn.WriteAny(v)
}

func newProxy(app *appdata.AppData, c *cluster.Cluster) *proxy {
	return &proxy{app: app, cluster: c}
}

func pingCmd(conn redcon.Conn, cmd redcon.Command, meta metaT) {
	switch {
	case meta.key != "":
		v, err := meta.this.cluster.Do(meta.key, cmd.Args)
		if err != nil {
			conn.WriteError("ERR " + err.Error())
		} else {
			conn.WriteAny(v)
		}
	default:
		conn.WriteString("PONG")
	}

	// pprof
	// pprof.StopCPUProfile()
}

// distGetCmd is a very naive implementation of a distributed GET. It tries to get
// the current list of active nodes (not Redis) in the cluster, distribute the GET
// load to all of them (including the node that received the call), and wait for
// all responses. Trouble is, this is deployed as a Deployment in k8s, where pods
// are quite volatile, so chances of pods not being available at call time, or
// disappearing mid processing is quite high. Probably need to look at deploying
// this as MIGs in GCP, that could be a slightly more stable environment for this
// kind of load distribution. At the moment, the HPA for this deployment is set
// with min = max, so at least the scale up/down is relatively fixed.
func distGetCmd(conn redcon.Conn, cmd redcon.Command, meta metaT) {
	var line string
	defer func(begin time.Time, m *string) {
		if *m != "" {
			glog.Infof("[distGetCmd] %v, took %v", *m, time.Since(begin))
		}
	}(time.Now(), &line)

	if len(cmd.Args) != 2 {
		conn.WriteError("ERR invalid args")
		return
	}

	ctx := context.Background()
	nkey := string(cmd.Args[1]) // 'key' arg not used here
	chunks := meta.chunks
	if chunks == 0 { // try getting it ourselves
		keyLen := fmt.Sprintf("%v/len", nkey) // no hash={key} used for '/len'
		r, err := meta.this.cluster.Do(keyLen, [][]byte{[]byte("GET"), []byte(keyLen)})
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}

		switch r := r.(type) {
		case string:
			i, err := strconv.ParseInt(r, 10, 0)
			if err != nil {
				conn.WriteError("ERR " + err.Error())
				return
			}
			chunks = int(i)
		default:
			e := fmt.Errorf("unknown type %T for /len", r)
			conn.WriteError("ERR " + e.Error())
			return
		}
	}

	if chunks == 0 { // if still none
		conn.WriteError("ERR no chunks found")
		return
	}

	members := make(map[string]string)
	for _, m := range meta.this.app.FleetOp.Members() {
		members[m] = m
	}

	var nodes []string
	for k := range members {
		nodes = append(nodes, k)
	}

	// Assign query indeces to all members.
	loc := 0
	assign := make(map[int]string)
	for i := 0; i < chunks; i++ {
		assign[i] = nodes[loc]
		loc++
		if loc >= len(members) {
			loc = 0
		}
	}

	mb := make(map[int][]byte)
	errs := []error{}
	b, _ := json.Marshal(internal.NewEvent(
		cluster.DistributedGetInput{Name: nkey, Assign: assign},
		cluster.EventSource,
		cluster.CtrlBroadcastDistributedGet,
	))

	// Send out GET assignments to all members; wait for reply.
	// TODO: How to handle any member failing? For now, fail all.
	outs := meta.this.app.FleetOp.Broadcast(ctx, b)
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
		conn.WriteError("ERR incomplete cache data")
		return
	}

	var out bytes.Buffer
	for i := 0; i < chunks; i++ {
		if _, ok := mb[i]; !ok {
			m := fmt.Sprintf("index [%v] not found", i)
			conn.WriteError("ERR " + m)
			return
		}

		out.Write(mb[i])
	}

	line = fmt.Sprintf("key=%v, chunks=%v, len=%v, cap=%v",
		nkey, chunks, out.Len(), out.Cap())

	conn.WriteAny(out.Bytes())
}

func detachCmd(conn redcon.Conn, cmd redcon.Command, meta metaT) {
	hconn := conn.Detach()
	glog.Info("connection has been detached")
	go func() {
		defer hconn.Close()
		hconn.WriteString("OK")
		hconn.Flush()
	}()
}

func quitCmd(conn redcon.Conn, cmd redcon.Command, meta metaT) {
	conn.WriteString("OK")
	conn.Close()
}

func configCmd(conn redcon.Conn, cmd redcon.Command, meta metaT) {
	// This simple (blank) response is only here to allow for the
	// redis-benchmark command to work with this clone.
	conn.WriteArray(2)
	conn.WriteBulk(cmd.Args[2])
	conn.WriteBulkString("")
}
