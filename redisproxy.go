package main

import (
	"log"
	"strings"
	"sync"

	"github.com/tidwall/redcon"
)

var (
	mu    sync.RWMutex
	items = make(map[string][]byte)
	ps    redcon.PubSub

	cmds = map[string]func(redcon.Conn, redcon.Command){
		"detach": detachCmd,
		"ping":   pingCmd,
		"quit":   quitCmd,
		"config": configCmd,
		"set":    setCmd,
		"get":    getCmd,
		"del":    delCmd,
	}
)

func detachCmd(conn redcon.Conn, cmd redcon.Command) {
	hconn := conn.Detach()
	log.Printf("connection has been detached")
	go func() {
		defer hconn.Close()
		hconn.WriteString("OK")
		hconn.Flush()
	}()
}

func pingCmd(conn redcon.Conn, cmd redcon.Command) { conn.WriteString("PONG") }

func quitCmd(conn redcon.Conn, cmd redcon.Command) {
	conn.WriteString("OK")
	conn.Close()
}

func configCmd(conn redcon.Conn, cmd redcon.Command) {
	// This simple (blank) response is only here to allow for the
	// redis-benchmark command to work with this example.
	conn.WriteArray(2)
	conn.WriteBulk(cmd.Args[2])
	conn.WriteBulkString("")
}

func setCmd(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	mu.Lock()
	items[string(cmd.Args[1])] = cmd.Args[2]
	mu.Unlock()
	conn.WriteString("OK")
}

func getCmd(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	mu.RLock()
	val, ok := items[string(cmd.Args[1])]
	mu.RUnlock()
	if !ok {
		conn.WriteNull()
	} else {
		conn.WriteBulk(val)
	}
}

func delCmd(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	mu.Lock()
	_, ok := items[string(cmd.Args[1])]
	delete(items, string(cmd.Args[1]))
	mu.Unlock()
	if !ok {
		conn.WriteInt(0)
	} else {
		conn.WriteInt(1)
	}
}

func handler(conn redcon.Conn, cmd redcon.Command) {
	cmdtl := strings.ToLower(string(cmd.Args[0]))
	if _, ok := cmds[cmdtl]; !ok {
		conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
		return
	}

	cmds[cmdtl](conn, cmd)
}
