package cluster

import (
	"fmt"
	"sync"
	"time"

	"github.com/alphauslabs/jupiter/internal/flags"
	"github.com/buraksezer/consistent"
	"github.com/golang/glog"
	"github.com/gomodule/redigo/redis"
	"github.com/google/uuid"
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

type Cluster struct {
	mtx        sync.RWMutex
	members    map[string]*member
	consistent *consistent.Consistent
}

func (m *Cluster) AddMember(host string) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if _, found := m.members[host]; !found {
		m.members[host] = &member{
			host: host,
			pool: &redis.Pool{
				MaxIdle:     *flags.MaxIdle,
				MaxActive:   *flags.MaxActive,
				IdleTimeout: 240 * time.Second,
				Dial: func() (redis.Conn, error) {
					return redis.Dial("tcp", host)
				},
			},
			queue: make(chan *rcmd, 10_000),
		}

		for i := 0; i < *flags.MaxActive; i++ {
			id := fmt.Sprintf("%v/%04d", host, i)
			m.members[host].done.Add(1)
			go m.runner(
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
				PartitionCount:    *flags.Partitions,
				ReplicationFactor: *flags.ReplicationFactor,
				Hasher:            Hasher{},
			},
		)
	} else {
		glog.Infof("add %v to hashring", host)
		m.consistent.Add(cmember(host))
	}
}

func (m *Cluster) runner(id string, pool *redis.Pool, queue chan *rcmd, done *sync.WaitGroup) {
	defer func() { done.Done() }()
	glog.Infof("runner %v started", id)
	for j := range queue {
		con := pool.Get()
		j.runner = id
		out, err := con.Do(j.cmd, j.args...)
		j.reply = out
		j.done <- err
		con.Close()
	}
}

func (m *Cluster) Do(key string, args [][]byte) (interface{}, error) {
	var node string
	node = m.consistent.LocateKey([]byte(key)).String()
	nargs := []interface{}{}
	if len(args) > 1 {
		for i := 1; i < len(args); i++ {
			nargs = append(nargs, args[i])
		}
	}

	c := &rcmd{
		cmd:  string(args[0]),
		args: nargs,
		done: make(chan error, 1),
	}

	m.mtx.RLock()
	m.members[node].queue <- c
	m.mtx.RUnlock()
	err := <-c.done
	return c.reply, err
}

func (m *Cluster) RandomPing() error {
	_, err := m.Do(uuid.NewString(), [][]byte{[]byte("PING")})
	return err
}

func (m *Cluster) Close() {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	for k, v := range m.members {
		glog.Infof("closing %v...", k)
		close(v.queue)
		v.done.Wait()
		v.pool.Close()
	}
}

func NewCluster() *Cluster { return &Cluster{members: map[string]*member{}} }
