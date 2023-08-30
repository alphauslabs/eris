package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

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

	cmds = map[string]func(redcon.Conn, redcon.Command, string, *proxy){
		"detach": detachCmd,
		"ping":   pingCmd,
		"quit":   quitCmd,
		"config": configCmd,
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
	c := conn.PeekPipeline()
	for _, v := range c {
		for i, v := range v.Args {
			glog.Infof("peek[%v]: %v", i, string(v))
		}
	}

	for i, v := range cmd.Args {
		glog.Infof("cmd[%v]: %v", i, string(v))
	}
	glog.Infof("dbg ---")

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
		c := conn.PeekPipeline()
		glog.Infof("%+v", c)
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
