package main

import (
	"encoding/json"
	"fmt"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/golang/glog"
)

var (
	ctrlBroadcastLeaderLiveness = "CTRL_BROADCAST_LEADER_LIVENESS"

	fnBroadcast = map[string]func(e *cloudevents.Event) ([]byte, error){
		ctrlBroadcastLeaderLiveness: doBroadcastLeaderLiveness,
	}
)

func broadcastHandler(data interface{}, msg []byte) ([]byte, error) {
	var e cloudevents.Event
	err := json.Unmarshal(msg, &e)
	if err != nil {
		glog.Errorf("Unmarshal failed: %v", err)
		return nil, err
	}

	if _, ok := fnBroadcast[e.Type()]; !ok {
		return nil, fmt.Errorf("failed: unsupported type: %v", e.Type())
	}

	return fnBroadcast[e.Type()](&e)
}

func doBroadcastLeaderLiveness(e *cloudevents.Event) ([]byte, error) {
	leaderActive.On()
	return nil, nil
}
