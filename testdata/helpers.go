package testdata

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/goccy/go-json"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/client"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/settings"
	"github.com/stretchr/testify/require"
)

func GetConnection(url string, nameExtra string) *client.Client {
	author := events.PluginEntity{Name: "restapi-test" + nameExtra, Version: "1", Category: "plugin"}
	c := client.NewClient(url, url, author, "restapi-test-key")
	err := c.PublishPlugin()
	if err != nil {
		bedSet.Logger.Fatal().Err(err).Msg("Could not obtain client connection to dispatcher")
	}
	return c
}

// compare two structures by first dumping to json
// this removes problems with time, and other non-normalised data
func MarshalEqual(t *testing.T, in1, in2 any) {
	// non pointer marshalling can prevent custom MarshalJSON from running properly
	if reflect.ValueOf(in1).Kind() != reflect.Ptr {
		panic(fmt.Errorf("provided in1 was not a pointer, was %s", reflect.TypeOf(in1)))
	}
	if reflect.ValueOf(in2).Kind() != reflect.Ptr {
		panic(fmt.Errorf("provided in2 was not a pointer, was %s", reflect.TypeOf(in2)))
	}
	raw1, err := json.Marshal(in1)
	require.Nil(t, err)
	raw2, err := json.Marshal(in2)
	require.Nil(t, err)
	require.JSONEq(t, string(raw1), string(raw2))
}

var GlobalTestCtx context.Context
var GlobalTestCancelCtx context.CancelFunc

func InitGlobalContext() context.CancelFunc {
	GlobalTestCtx, GlobalTestCancelCtx = context.WithCancel(context.Background())
	return GlobalTestCancelCtx
}

func GetGlobalTestContext() context.Context {
	return GlobalTestCtx
}

func CloseGlobalContext() {
	GlobalTestCancelCtx()
}
