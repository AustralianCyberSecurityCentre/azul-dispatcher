package pipeline_produce

import (
	"os"
	"testing"

	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	testdata "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
)

func TestMain(m *testing.M) {
	// Needed for task tracker to immediately delete dequeuedIds
	st.Events.LostTasksBulkCreateLimit = 0
	// Global Context setup.
	cancelFunc := testdata.InitGlobalContext()
	defer cancelFunc()
	exitVal := m.Run()
	os.Exit(exitVal)
}
