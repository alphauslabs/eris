package cluster

import (
	"encoding/json"
	"fmt"

	"github.com/alphauslabs/jupiter/internal/appdata"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/golang/glog"
)

var (
	ctrlBroadcastLeaderLiveness = "CTRL_BROADCAST_LEADER_LIVENESS"

	fnBroadcast = map[string]func(*appdata.AppData, *cloudevents.Event) ([]byte, error){
		ctrlBroadcastLeaderLiveness: doBroadcastLeaderLiveness,
	}
)

func BroadcastHandler(data interface{}, msg []byte) ([]byte, error) {
	app := data.(*appdata.AppData)
	var e cloudevents.Event
	err := json.Unmarshal(msg, &e)
	if err != nil {
		glog.Errorf("Unmarshal failed: %v", err)
		return nil, err
	}

	if _, ok := fnBroadcast[e.Type()]; !ok {
		return nil, fmt.Errorf("failed: unsupported type: %v", e.Type())
	}

	return fnBroadcast[e.Type()](app, &e)
}

func doBroadcastLeaderLiveness(app *appdata.AppData, e *cloudevents.Event) ([]byte, error) {
	app.LeaderActive.On()
	return nil, nil
}
