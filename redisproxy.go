package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"

	"github.com/golang/glog"
	"github.com/google/uuid"
	"github.com/tidwall/redcon"
)

var (
	mu    sync.RWMutex
	items = make(map[string][]byte)
	ps    redcon.PubSub

	cmds = map[string]func(redcon.Conn, redcon.Command, string){
		"detach":  detachCmd,
		"ping":    pingCmd,
		"quit":    quitCmd,
		"config":  configCmd,
		"command": commandCmd,
	}
)

// Special: optional last args fmt: hash={key}|index={num}
// where:
//
//	{key} = string combination (chars not allowed: ,=)
//	{num} = 0-based index in args to use as hash key
//
// If this custom args is not provided, args[1] will be used
func handler(conn redcon.Conn, cmd redcon.Command) {
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
		cmds[cmdtl](conn, ncmd, key)
		return
	}

	if len(ncmd.Args) >= 2 && key == "" {
		key = string(ncmd.Args[1])
	}

	v, err := redisFleet.do(key, ncmd.Args)
	if err != nil {
		conn.WriteError(err.Error())
	} else {
		conn.WriteAny(v)
	}
}

func detachCmd(conn redcon.Conn, cmd redcon.Command, key string) {
	hconn := conn.Detach()
	log.Printf("connection has been detached")
	go func() {
		defer hconn.Close()
		hconn.WriteString("OK")
		hconn.Flush()
	}()
}

func pingCmd(conn redcon.Conn, cmd redcon.Command, key string) {
	switch {
	case key != "":
		v, err := redisFleet.do(key, cmd.Args)
		if err != nil {
			conn.WriteError(err.Error())
		} else {
			conn.WriteAny(v)
		}
	default:
		glog.Infof("[pingCmd] direct reply")
		conn.WriteString("PONG")
	}
}

func quitCmd(conn redcon.Conn, cmd redcon.Command, key string) {
	conn.WriteString("OK")
	conn.Close()
}

func configCmd(conn redcon.Conn, cmd redcon.Command, key string) {
	// This simple (blank) response is only here to allow for the
	// redis-benchmark command to work with this example.
	conn.WriteArray(2)
	conn.WriteBulk(cmd.Args[2])
	conn.WriteBulkString("")
}

func commandCmd(conn redcon.Conn, cmd redcon.Command, key string) {
	v, err := redisFleet.do(uuid.NewString(), cmd.Args)
	if err != nil {
		conn.WriteError(err.Error())
	} else {
		conn.WriteAny(v)
	}
}

// func setCmd(conn redcon.Conn, cmd redcon.Command, key string) {
// 	if len(cmd.Args) != 3 {
// 		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
// 		return
// 	}

// 	mu.Lock()
// 	items[string(cmd.Args[1])] = cmd.Args[2]
// 	mu.Unlock()
// 	conn.WriteString("OK")
// }

// func getCmd(conn redcon.Conn, cmd redcon.Command, key string) {
// 	if len(cmd.Args) != 2 {
// 		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
// 		return
// 	}

// 	mu.RLock()
// 	val, ok := items[string(cmd.Args[1])]
// 	mu.RUnlock()
// 	if !ok {
// 		conn.WriteNull()
// 	} else {
// 		conn.WriteBulk(val)
// 	}
// }

// func delCmd(conn redcon.Conn, cmd redcon.Command, key string) {
// 	if len(cmd.Args) != 2 {
// 		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
// 		return
// 	}

// 	mu.Lock()
// 	_, ok := items[string(cmd.Args[1])]
// 	delete(items, string(cmd.Args[1]))
// 	mu.Unlock()
// 	if !ok {
// 		conn.WriteInt(0)
// 	} else {
// 		conn.WriteInt(1)
// 	}
// }
