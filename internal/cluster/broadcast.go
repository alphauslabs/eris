package cluster

import (
	"encoding/json"
	"fmt"

	"github.com/alphauslabs/jupiter/internal/appdata"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/golang/glog"
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

	fnBroadcast = map[string]func(*appdata.AppData, *cloudevents.Event) ([]byte, error){
		ctrlBroadcastLeaderLiveness: doBroadcastLeaderLiveness,
		CtrlBroadcastTrialDist:      doTrialDist,
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

func doTrialDist(app *appdata.AppData, e *cloudevents.Event) ([]byte, error) {
	var in TrialDistInput
	err := json.Unmarshal(e.Data(), &in)
	if err != nil {
		glog.Errorf("Unmarshal failed: %v", err)
		return nil, err
	}

	for k, v := range in.Assign {
		if v == app.FleetOp.Name() {
			glog.Infof("%d is assigned to me (%v)", k, app.FleetOp.Name())
		}
	}

	return nil, nil
}
