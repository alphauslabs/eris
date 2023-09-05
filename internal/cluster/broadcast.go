package cluster

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alphauslabs/jupiter/internal/flags"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/golang/glog"
	"github.com/gomodule/redigo/redis"
)

type TrialDistInput struct {
	Assign map[int]string `json:"assign"`
}

type TrialDistOutput struct {
	Data map[int][]byte `json:"data"`
}

var (
	ctrlBroadcastLeaderLiveness = "CTRL_BROADCAST_LEADER_LIVENESS"
	CtrlBroadcastTrialDist      = "CTRL_BROADCAST_TRIAL_DIST"

	fnBroadcast = map[string]func(*ClusterData, *cloudevents.Event) ([]byte, error){
		ctrlBroadcastLeaderLiveness: doBroadcastLeaderLiveness,
		CtrlBroadcastTrialDist:      doTrialDist,
	}
)

func BroadcastHandler(data interface{}, msg []byte) ([]byte, error) {
	cd := data.(*ClusterData)
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

func doTrialDist(cd *ClusterData, e *cloudevents.Event) ([]byte, error) {
	defer func(begin time.Time) {
		glog.Infof("doTrialDist took %v", time.Since(begin))
	}(time.Now())

	var in TrialDistInput
	err := json.Unmarshal(e.Data(), &in)
	if err != nil {
		glog.Errorf("Unmarshal failed: %v", err)
		return nil, err
	}

	var on int32
	var w sync.WaitGroup
	var m sync.Mutex
	mb := make(map[int][]byte)
	me := make(map[int]error)
	concurrent := make(chan struct{}, *flags.MaxActive) // concurrent read limit

	for k, v := range in.Assign {
		if v == cd.App.FleetOp.Name() {
			atomic.AddInt32(&on, 1)
			glog.Infof("%d is assigned to me (%v)", k, cd.App.FleetOp.Name())
			w.Add(1)
			go func(idx int) {
				concurrent <- struct{}{}
				defer func() {
					<-concurrent
					w.Done()
				}()

				key := fmt.Sprintf("proto/%v", idx)
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

	if atomic.LoadInt32(&on) > 0 {
		w.Wait()
		out := TrialDistOutput{Data: mb}
		b, _ := json.Marshal(out)
		return b, nil
	}

	return nil, nil
}
