package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/golang/glog"
	"github.com/gomodule/redigo/redis"
)

const (
	maxIdle   = 3
	maxActive = 20
)

type cmember string

func (m cmember) String() string { return string(m) }

type rcmd struct {
	cmd    string
	args   []interface{}
	runner string
	done   chan error
	reply  interface{}
}

func (rc *rcmd) String() string { return fmt.Sprintf("%v %v", rc.cmd, rc.args) }

type member struct {
	host  string // fmt: host:port
	pool  *redis.Pool
	queue chan *rcmd
	done  sync.WaitGroup
}

type fleet struct {
	mtx        sync.Mutex
	members    map[string]*member
	consistent *consistent.Consistent
}

func (m *fleet) getMembers() map[string]struct{} {
	m.mtx.Lock()
	copy := make(map[string]struct{})
	for k := range m.members {
		copy[k] = struct{}{}
	}

	m.mtx.Unlock()
	return copy
}

func (m *fleet) encodeMembers() string {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	b, _ := json.Marshal(m.members)
	return base64.StdEncoding.EncodeToString(b)
}

// func (m *membersT) setMembers(v map[string]string) {
// 	m.mtx.Lock()
// 	defer m.mtx.Unlock()
// 	m.members = v
// }

func (m *fleet) addMember(host string) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if _, found := m.members[host]; !found {
		m.members[host] = &member{
			host: host,
			pool: &redis.Pool{
				MaxIdle:     maxIdle,
				MaxActive:   maxActive,
				IdleTimeout: 240 * time.Second,
				Dial: func() (redis.Conn, error) {
					return redis.Dial("tcp", host)
				},
			},
			queue: make(chan *rcmd, maxActive),
		}

		for i := 0; i < maxActive; i++ {
			id := fmt.Sprintf("%v/%04d", host, i)
			m.members[host].done.Add(1)
			go m.worker(
				id,
				m.members[host].pool,
				m.members[host].queue,
				&m.members[host].done,
			)
		}
	}

	if m.consistent == nil {
		glog.Infof("init hashring with %v", host)
		cm := cmember(host)
		m.consistent = consistent.New(
			[]consistent.Member{cm},
			consistent.Config{
				PartitionCount:    *paramPartitions,
				ReplicationFactor: *paramReplicationFactor,
				Hasher:            hasher{},
			},
		)
	} else {
		glog.Infof("add %v to hashring", host)
		m.consistent.Add(cmember(host))
	}
}

func (m *fleet) ping() error {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	key := "locate/ping"
	node := m.consistent.LocateKey([]byte(key))
	c := &rcmd{
		cmd:  "PING",
		args: []interface{}{},
		done: make(chan error, 1),
	}

	m.members[node.String()].queue <- c
	err := <-c.done // wait for reply
	if err != nil {
		return err
	}

	if c.reply.(string) != "PONG" {
		return fmt.Errorf("PING: something is wrong!")
	}

	glog.Infof("reply=%v", c.reply)
	return nil
}

func (m *fleet) worker(id string, pool *redis.Pool, queue chan *rcmd, done *sync.WaitGroup) {
	defer func() { done.Done() }()
	glog.Infof("runner %v started", id)
	con := pool.Get()
	defer con.Close()
	for j := range queue {
		j.runner = id
		glog.Infof("[%v] do: %v", id, j)
		out, err := con.Do(j.cmd, j.args...)
		j.reply = out
		j.done <- err
	}
}

func (m *fleet) close() {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	for k, v := range m.members {
		glog.Infof("closing %v...", k)
		close(v.queue)
		v.done.Wait()
		v.pool.Close()
	}
}

// func (m *membersT) delMember(id string) {
// 	m.mtx.Lock()
// 	defer m.mtx.Unlock()
// 	delete(m.members, id)
// }

func newFleet() *fleet { return &fleet{members: map[string]*member{}} }
