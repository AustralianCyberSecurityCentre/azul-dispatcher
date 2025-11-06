package pipeline_produce

import (
	"strings"
	"testing"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	testdata "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/sjson"
)

func TestKeyDescriptor(t *testing.T) {
	tables := []struct {
		model      events.Model
		test       string
		inputFile  string
		expectedID string
		err        bool
	}{
		// valid key
		{events.ModelBinary, "simple", "simple.json", "virustotal..2021-02-06T16:43:23.interface.web.submitter_city.kharkiv.submitter_country.UA.submitter_id.0a9e3094.submitter_region.63.vtdownload.downloaded.b9debe8afbdc6d0f552ca4d41fc8a5760778f2724d1560f6b4b0fc28bd837a82.MimeDecoder.extracted.ee303d3c6d7cfa24d42e6348bdd1103a26de77a887e9dbee3dd1fe6304414f69.", false},
		// should still match, same as simple with small alterations
		{events.ModelBinary, "whitespace", "simple.txt", "virustotal..2021-02-06T16:43:23.interface.web.submitter_city.kharkiv.submitter_country.UA.submitter_id.0a9e3094.submitter_region.63.vtdownload.downloaded.b9debe8afbdc6d0f552ca4d41fc8a5760778f2724d1560f6b4b0fc28bd837a82.MimeDecoder.extracted.ee303d3c6d7cfa24d42e6348bdd1103a26de77a887e9dbee3dd1fe6304414f69.", false},
		{events.ModelBinary, "reordered", "reordered.json", "virustotal..2021-02-06T16:43:23.interface.web.submitter_city.kharkiv.submitter_country.UA.submitter_id.0a9e3094.submitter_region.63.vtdownload.downloaded.b9debe8afbdc6d0f552ca4d41fc8a5760778f2724d1560f6b4b0fc28bd837a82.MimeDecoder.extracted.ee303d3c6d7cfa24d42e6348bdd1103a26de77a887e9dbee3dd1fe6304414f69.", false},
		{events.ModelBinary, "existing_key", "existing_key.json", "virustotal..2021-02-06T16:43:23.interface.web.submitter_city.kharkiv.submitter_country.UA.submitter_id.0a9e3094.submitter_region.63.vtdownload.downloaded.b9debe8afbdc6d0f552ca4d41fc8a5760778f2724d1560f6b4b0fc28bd837a82.MimeDecoder.extracted.ee303d3c6d7cfa24d42e6348bdd1103a26de77a887e9dbee3dd1fe6304414f69.", false},
		// should produce different key
		{events.ModelBinary, "source_security_change", "source_security_change.json", "virustotal.RESTRICTED.2021-02-06T16:43:23.interface.web.submitter_city.kharkiv.submitter_country.UA.submitter_id.0a9e3094.submitter_region.63.vtdownload.downloaded.b9debe8afbdc6d0f552ca4d41fc8a5760778f2724d1560f6b4b0fc28bd837a82.MimeDecoder.extracted.ee303d3c6d7cfa24d42e6348bdd1103a26de77a887e9dbee3dd1fe6304414f69.", false},
		{events.ModelBinary, "path_security_change", "path_security_change.json", "virustotal..2021-02-06T16:43:23.interface.web.submitter_city.kharkiv.submitter_country.UA.submitter_id.0a9e3094.submitter_region.63.PROVIDED.RESTRICTED.vtdownload.downloaded.b9debe8afbdc6d0f552ca4d41fc8a5760778f2724d1560f6b4b0fc28bd837a82.MimeDecoder.extracted.ee303d3c6d7cfa24d42e6348bdd1103a26de77a887e9dbee3dd1fe6304414f69.", false},
		{events.ModelBinary, "source_ref_change", "source_ref_change.json", "virustotal..2021-02-06T16:43:23.interface.web.submitter_city.kharkiv.submitter_country.UA.submitter_id.xxxxxxxx.submitter_region.63.vtdownload.downloaded.b9debe8afbdc6d0f552ca4d41fc8a5760778f2724d1560f6b4b0fc28bd837a82.MimeDecoder.extracted.ee303d3c6d7cfa24d42e6348bdd1103a26de77a887e9dbee3dd1fe6304414f69.", false},
		{events.ModelBinary, "author_change", "author_change.json", "virustotal..2021-02-06T16:43:23.interface.web.submitter_city.kharkiv.submitter_country.UA.submitter_id.0a9e3094.submitter_region.63.vtdownload.downloaded.b9debe8afbdc6d0f552ca4d41fc8a5760778f2724d1560f6b4b0fc28bd837a82.SlimeDecoder.extracted.ee303d3c6d7cfa24d42e6348bdd1103a26de77a887e9dbee3dd1fe6304414f69.", false},
		// should use different method to produce key
		{events.ModelStatus, "status", "status_key.json", "dac804f3662b2228e43af80f6e0769614bf53d6c8ea16241c80d779de1308c20-pluigin-2023.01.01.MimeDecoder", false},
		{events.ModelPlugin, "plugin", "plugin.json", "RatConfigs-JRat.2020.06.02", false},
	}
	for _, table := range tables {
		raw := testdata.GetEventBytes("events/pipelines/key_generator/" + table.inputFile)
		inFlight, err := pipeline.NewMsgInFlightFromJson(raw, table.model)
		require.Nil(t, err, table.test)

		key, err := keyDescriptor(inFlight)
		if table.err {
			require.NotNil(t, err)
			continue
		}
		require.Nil(t, err)
		assert.Equal(t, table.expectedID, string(key), table.test)
	}
}

func TestKey(t *testing.T) {
	tables := []struct {
		model      events.Model
		test       string
		inputFile  string
		expectedID string
		err        bool
	}{
		// valid key
		{events.ModelBinary, "simple", "simple.json", "b77549952b9354551975192f6c09e3c7", false},
		// should still match, same as simple with small alterations
		{events.ModelBinary, "whitespace", "simple.txt", "b77549952b9354551975192f6c09e3c7", false},
		{events.ModelBinary, "reordered", "reordered.json", "b77549952b9354551975192f6c09e3c7", false},
		{events.ModelBinary, "existing_key", "existing_key.json", "b77549952b9354551975192f6c09e3c7", false},
		// should produce different key
		{events.ModelBinary, "source_security_change", "source_security_change.json", "6f15f707b81e3b611c63f65f3bed84a3", false},
		{events.ModelBinary, "path_security_change", "path_security_change.json", "4643bc0ea3ca0e97d4f9f820611cced6", false},
		{events.ModelBinary, "source_ref_change", "source_ref_change.json", "fb5d549a5b78adfbd5b606bf0814be32", false},
		{events.ModelBinary, "author_change", "author_change.json", "b261b7b235b49105eb7632e6afaac8da", false},
		// should use different method to produce key
		{events.ModelStatus, "status", "status_key.json", "52bfc3a94719718fabb7d50f39ce52f5", false},
		{events.ModelPlugin, "plugin", "plugin.json", "68e3d23d6ab9facfbd4e3d5f7543c636", false},
		{events.ModelDelete, "delete", "delete.json", "any", false},
	}
	for _, table := range tables {
		raw := testdata.GetEventBytes("events/pipelines/key_generator/" + table.inputFile)
		inFlight, err := pipeline.NewMsgInFlightFromJson(raw, table.model)
		require.Nil(t, err, table.test)
		key, err := key(inFlight)
		if table.err {
			require.NotNil(t, err, table.test)
			continue
		}
		require.Nil(t, err, table.test)
		if table.expectedID == "any" {
			assert.Greater(t, len(string(key)), 16, table.test)
		} else {
			assert.Equal(t, table.expectedID, string(key), table.test)
		}
	}
}

// Test that binary docs deduplicate when they share only parent and first binary.
func TestKeyAncestorDiffs(t *testing.T) {
	raw := testdata.GetEvent("events/pipelines/key_generator/" + "ancestor_diffs.jsonlines")
	for i, line := range strings.Split(raw, "\n") {
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		updated, err := sjson.Set(line, "entity.data", []map[string]any{{"label": "content"}})
		require.Nil(t, err)
		inFlight, err := pipeline.NewMsgInFlightFromJson([]byte(updated), events.ModelBinary)
		require.Nil(t, err, updated)

		keyDescriptor, err := keyDescriptor(inFlight)
		require.Nil(t, err)
		assert.Equal(t, "samples.OFFICIAL.2024-07-04T15:27:40.description.File uploaded by Azul Stress tester..user.bob@email.com.OFFICIAL.DotnetDeob.extracted.731fcd36832ec3885bb535640fb7f704cb130168a7c1bdb7cada92b04e3c3a14.LiefPE.extracted.f852600ceec04601167cf9862af09bc6da99a70ec77b8296af3144e46a50ad4a.", string(keyDescriptor), i+1)

		key, err := key(inFlight)
		require.Nil(t, err)
		assert.Equal(t, "fc7a850c00afabef06eb986dbb3cf184", string(key), i+1)
	}
}

func BenchmarkKeys(b *testing.B) {
	raw := testdata.GetEventBytes("events/pipelines/key_generator/" + "simple.json")
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		inFlight, err := pipeline.NewMsgInFlightFromJson(raw, events.ModelBinary)
		require.Nil(b, err)
		_, err = key(inFlight)
		require.Nil(b, err)
	}
}
