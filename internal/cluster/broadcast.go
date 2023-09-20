package cluster

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/golang/glog"
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

	mgetIds := []int{}
	mgets := [][]byte{[]byte("MGET")}
	mb := make(map[int][]byte)
	for k, v := range in.Assign {
		if v == cd.App.FleetOp.Name() {
			mgetIds = append(mgetIds, k)
			mgets = append(mgets, []byte(fmt.Sprintf("%v/%v", in.Name, k)))
		}
	}

	mgets = append(mgets, []byte(fmt.Sprintf("hash=%v", in.Name)))
	v, err := cd.Cluster.Do(in.Name, mgets)
	if err != nil {
		return nil, err
	}

	ids := []string{}
	for _, id := range mgetIds {
		ids = append(ids, fmt.Sprintf("%v", id))
	}

	switch v.(type) {
	case []interface{}:
		for i, d := range v.([]interface{}) {
			if _, ok := d.(string); !ok {
				e := fmt.Errorf("unexpected non-string value for [%v:%v], type=%T, val=%v",
					in.Name, mgetIds[i], d, d)
				glog.Error(e)
				return nil, e
			} else {
				mb[mgetIds[i]] = []byte(d.(string))
			}
		}
	default:
		e := fmt.Errorf("unknown type for [%v:%v]: %T", in.Name, strings.Join(ids, ","), v)
		glog.Error(e)
		return nil, e
	}

	line = fmt.Sprintf("%v:assigned=%v:[%v]", in.Name, len(ids), strings.Join(ids, ","))
	out := DistributedGetOutput{Data: mb}
	b, _ := json.Marshal(out)
	return b, nil
}
