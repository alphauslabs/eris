package cluster

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alphauslabs/jupiter/internal/flags"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/golang/glog"
	"github.com/gomodule/redigo/redis"
)

const (
	EventSource = "jupiter/internal"
)

type DistributedGetInput struct {
	Name   string         `json:"name"`
	Assign map[int]string `json:"assign"`
}

type DistributedGetOutput struct {
	Data map[int][]byte `json:"data"`
}

var (
	ErrClusterOffline = fmt.Errorf("failed: cluster not running")

	CtrlBroadcastLeaderLiveness = "CTRL_BROADCAST_LEADER_LIVENESS"
	CtrlBroadcastEmpty          = "CTRL_BROADCAST_EMPTY"
	CtrlBroadcastDistributedGet = "CTRL_BROADCAST_DISTRIBUTED_GET"

	fnBroadcast = map[string]func(*ClusterData, *cloudevents.Event) ([]byte, error){
		CtrlBroadcastLeaderLiveness: doBroadcastLeaderLiveness,
		CtrlBroadcastEmpty:          doBroadcastEmpty,
		CtrlBroadcastDistributedGet: doDistributedGet,
	}
)

func BroadcastHandler(data interface{}, msg []byte) ([]byte, error) {
	cd := data.(*ClusterData)
	if atomic.LoadInt32(&cd.ClusterOk) == 0 {
		return nil, ErrClusterOffline
	}

	var e cloudevents.Event
	err := json.Unmarshal(msg, &e)
	if err != nil {
		glog.Errorf("Unmarshal failed: %v", err)
		return nil, err
	}

	if _, ok := fnBroadcast[e.Type()]; !ok {
		return nil, fmt.Errorf("failed: unsupported type: %v", e.Type())
	}

	return fnBroadcast[e.Type()](cd, &e)
}

func doBroadcastLeaderLiveness(cd *ClusterData, e *cloudevents.Event) ([]byte, error) {
	cd.App.LeaderActive.On()
	return nil, nil
}

// doBroadcastEmpty does nothing, actually. At the moment, used to gather all member info.
// TODO: It's better if hedge exposes the member list function instead of this.
func doBroadcastEmpty(cd *ClusterData, e *cloudevents.Event) ([]byte, error) {
	return nil, nil
}

func doDistributedGet(cd *ClusterData, e *cloudevents.Event) ([]byte, error) {
	var line string
	defer func(begin time.Time, m *string) {
		if *m != "" {
			glog.Infof("[doDistributedGet] %v, took %v", *m, time.Since(begin))
		}
	}(time.Now(), &line)

	var in DistributedGetInput
	err := json.Unmarshal(e.Data(), &in)
	if err != nil {
		glog.Errorf("Unmarshal failed: %v", err)
		return nil, err
	}

	var assigned int32
	var w sync.WaitGroup
	var m sync.Mutex
	mb := make(map[int][]byte)
	me := make(map[int]error)
	concurrent := make(chan struct{}, *flags.MaxActive) // concurrent read limit
	for k, v := range in.Assign {
		if v == cd.App.FleetOp.Name() {
			atomic.AddInt32(&assigned, 1)
			w.Add(1)
			go func(idx int) {
				concurrent <- struct{}{}
				defer func() {
					<-concurrent
					w.Done()
				}()

				key := fmt.Sprintf("%v/%v", in.Name, idx)
				v, err := redis.Bytes(cd.Cluster.Do(key, [][]byte{[]byte("GET"), []byte(key)}))
				if err != nil {
					m.Lock()
					me[idx] = fmt.Errorf("GET [%v] failed: %v", key, err)
					m.Unlock()
					return
				}

				m.Lock()
				mb[idx] = v
				m.Unlock()
			}(k)
		}
	}

	if atomic.LoadInt32(&assigned) > 0 {
		w.Wait()
		for _, v := range me {
			if v != nil {
				return nil, v
			}
		}

		ids := []int{}
		for k := range mb {
			ids = append(ids, k)
		}

		sort.Ints(ids)
		line = fmt.Sprintf("%v:%v", in.Name, ids)
		out := DistributedGetOutput{Data: mb}
		b, _ := json.Marshal(out)
		return b, nil
	}

	return nil, nil
}
