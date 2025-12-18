package pipeline_dual

import (
	"fmt"
	"testing"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/producer"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	"github.com/stretchr/testify/require"
)

func TestFPDNormal(t *testing.T) {
	prov, err := provider.NewMemoryProvider()
	if err != nil {
		panic(err)
	}

	p, err := producer.NewProducer(prov)
	if err != nil {
		panic(err)
	}
	defer p.Stop()

	pipe, err := NewFilterDeleted(prov, p)
	require.Nil(t, err)

	// test registration of all delete types
	// id delete
	for i := range 5 {
		ids := []string{}
		for y := range 2 {
			id := fmt.Sprintf("%d.%d", i, y)
			ids = append(ids, id)
		}
		ev := events.DeleteEvent{
			Action: events.DeleteActionIDs,
			Entity: events.DeleteEntity{
				IDs: events.DeleteEntityIDs{IDs: ids},
			},
		}
		pipe.registerDelete(&ev)
	}
	// submission delete
	for i := range 10 {
		ev := events.DeleteEvent{
			Action: events.DeleteActionSubmission,
			Entity: events.DeleteEntity{
				Submission: events.DeleteEntitySubmission{
					TrackSourceReferences: fmt.Sprintf("source.key1.key2.hash.%d", i),
				},
			},
		}
		pipe.registerDelete(&ev)
	}
	// link delete
	for i := range 8 {
		ev := events.DeleteEvent{
			Action: events.DeleteActionLink,
			Entity: events.DeleteEntity{
				Link: events.DeleteEntityLink{
					TrackLink: fmt.Sprintf("parent.child.%d", i),
				},
			},
		}
		pipe.registerDelete(&ev)
	}
	// author delete (24 hours after event was generated)
	authorDeleteTime := testdata.GenericTime().Add(time.Hour * 24)
	for i := range 5 {
		ev := events.DeleteEvent{
			Action: events.DeleteActionAuthor,
			Entity: events.DeleteEntity{
				Author: events.DeleteEntityAuthor{
					TrackAuthor: fmt.Sprintf("cat.name.version.%d", i),
					Timestamp:   authorDeleteTime,
				},
			},
		}
		pipe.registerDelete(&ev)
	}
	require.Equal(t, 5*2, len(pipe.purgedIDs))
	require.Equal(t, 10, len(pipe.purgedSourceReferences))
	require.Equal(t, 8, len(pipe.purgedLinks))
	require.Equal(t, 5, len(pipe.purgedAuthors))

	// test that things get deleted
	var ispurge bool
	bse1 := testdata.GenEventBinary(&testdata.BC{ID: "b1"})

	// default to not purge
	ispurge, err = pipe.judgePurged(bse1)
	require.Nil(t, err)
	require.Equal(t, false, ispurge)

	// matching submission
	bse1.TrackSourceReferences = "source.key1.key2.hash.2"
	ispurge, err = pipe.judgePurged(bse1)
	require.Nil(t, err)
	require.Equal(t, true, ispurge)

	// no match submission
	bse1.TrackSourceReferences = "source.key1.key2.hash.999"
	ispurge, err = pipe.judgePurged(bse1)
	require.Nil(t, err)
	require.Equal(t, false, ispurge)

	// reset and test id
	bse1 = testdata.GenEventBinary(&testdata.BC{ID: "b1"})
	bse1.KafkaKey = "1.1"
	ispurge, err = pipe.judgePurged(bse1)
	require.Nil(t, err)
	require.Equal(t, true, ispurge)

	bse1.KafkaKey = "1bacds"
	ispurge, err = pipe.judgePurged(bse1)
	require.Nil(t, err)
	require.Equal(t, false, ispurge)

	// reset and test link
	bse1 = testdata.GenEventBinary(&testdata.BC{ID: "b1"})
	bse1.TrackLinks = []string{"link1", "link2", "parent.child.1", "link4"}
	ispurge, err = pipe.judgePurged(bse1)
	require.Nil(t, err)
	require.Equal(t, true, ispurge)

	bse1.TrackLinks = []string{"link1", "link2", "link3", "link4"}
	ispurge, err = pipe.judgePurged(bse1)
	require.Nil(t, err)
	require.Equal(t, false, ispurge)

	bse1.TrackLinks = []string{"parent.child.4"}
	ispurge, err = pipe.judgePurged(bse1)
	require.Nil(t, err)
	require.Equal(t, true, ispurge)

	// reset and test plugin
	bse1 = testdata.GenEventBinary(&testdata.BC{ID: "b1"})
	bse1.TrackAuthors = []string{"extract.2022", "extract.2022", "cat.name.version.2", "unxor"}
	ispurge, err = pipe.judgePurged(bse1)
	require.Nil(t, err)
	require.Equal(t, true, ispurge)

	bse1.TrackAuthors = []string{"extract.2022", "extract.2022", "decrypt.8", "unxor"}
	ispurge, err = pipe.judgePurged(bse1)
	require.Nil(t, err)
	require.Equal(t, false, ispurge)

	bse1.TrackAuthors = []string{"cat.name.version.2"}
	ispurge, err = pipe.judgePurged(bse1)
	require.Nil(t, err)
	require.Equal(t, true, ispurge)

	bse1.TrackAuthors = []string{"cat.name.version.999"}
	ispurge, err = pipe.judgePurged(bse1)
	require.Nil(t, err)
	require.Equal(t, false, ispurge)
}

func TestDontDeleteNewPluginEvents(t *testing.T) {
	/*Test that events aren't deleted if the author(plugin) was deleted before the event was generated.*/
	prov, err := provider.NewMemoryProvider()
	if err != nil {
		panic(err)
	}

	p, err := producer.NewProducer(prov)
	if err != nil {
		panic(err)
	}
	defer p.Stop()

	pipe, err := NewFilterDeleted(prov, p)
	require.Nil(t, err)

	// author delete (24 hours before event was generated)
	authorDeleteTime := testdata.GenericTime().Add(-time.Hour * 24)
	for i := range 5 {
		ev := events.DeleteEvent{
			Action: events.DeleteActionAuthor,
			Entity: events.DeleteEntity{
				Author: events.DeleteEntityAuthor{
					TrackAuthor: fmt.Sprintf("cat.name.version.%d", i),
					Timestamp:   authorDeleteTime,
				},
			},
		}
		pipe.registerDelete(&ev)
	}
	require.Equal(t, 5, len(pipe.purgedAuthors))

	// test that things get deleted
	var ispurge bool
	bse1 := testdata.GenEventBinary(&testdata.BC{ID: "b1"})

	// Verify plugin deletion fails when the events are newer than the plugin deletion.
	bse1.TrackAuthors = []string{"extract.2022", "extract.2022", "cat.name.version.2", "unxor"}
	ispurge, err = pipe.judgePurged(bse1)
	require.Nil(t, err)
	require.Equal(t, false, ispurge)

	bse1.TrackAuthors = []string{"cat.name.version.2"}
	ispurge, err = pipe.judgePurged(bse1)
	require.Nil(t, err)
	require.Equal(t, false, ispurge)

}
