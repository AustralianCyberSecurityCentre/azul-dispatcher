package restapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/client"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/testutils"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/testdata"
	"github.com/stretchr/testify/require"
)

type Benchmarker struct {
	server      *httptest.Server
	dp          *Dispatcher
	fileManager *testutils.FileManager
	cancel      context.CancelFunc
}

func benchmarkGetEvents(t *testing.B, s *Benchmarker) {
	perChunk := 100
	var err error

	// reuse same connection otherwise lots of time is spent generating a new consumer
	conn2 := GetConnection(s.server.URL, fmt.Sprintf("p1-%d", t.N))
	// start up consumer
	for {
		_, info, err := conn2.GetBinaryEvents(&client.FetchEventsStruct{
			Count: 1, Deadline: 1, IsTask: false, AvroFormat: true,
			RequireExpedite: true, RequireLive: true, RequireHistoric: true,
		})
		require.Nil(t, err)
		if info.Ready == true {
			break
		}
	}

	conn1 := GetConnection(s.server.URL, "original")
	// create needed evs in advance
	evs := []*events.BinaryEvent{}
	for n := range t.N + 1 {
		ev := testdata.GenEventBinary(&testdata.BC{ID: fmt.Sprintf("1-%d", n), Action: events.ActionExtracted, PresetFeatures: 20})
		evs = append(evs, ev)
	}
	// publish event
	bulk := events.BulkBinaryEvent{Events: evs}
	resp, err := conn1.PostEvents(&bulk, &client.PublishEventsOptions{Sync: true, AvroFormat: true})
	require.Nil(t, err)
	require.Equal(t, resp.TotalOk, t.N+1)

	chunkSizes := []int{}
	for range t.N / perChunk {
		chunkSizes = append(chunkSizes, perChunk)
	}
	chunkSizes = append(chunkSizes, t.N%perChunk)
	// begin actual benchmarking
	t.ResetTimer()
	totalRead := 0
	for _, size := range chunkSizes {
		if size == 0 {
			continue
		}
		// consume event
		_, info, err := conn2.GetBinaryEvents(&client.FetchEventsStruct{
			Count: size, Deadline: 10, IsTask: false, AvroFormat: true,
			RequireExpedite: true, RequireLive: true, RequireHistoric: true,
		})
		require.Nil(t, err)
		totalRead += info.Fetched
	}
	require.Equal(t, t.N, totalRead) // expect event
	t.StopTimer()
	t.ReportMetric(float64(t.N)/float64(t.Elapsed().Seconds()), "events/s")
}

func benchmarkPostEvents(t *testing.B, s *Benchmarker) {
	perChunk := 100
	conn1 := GetConnection(s.server.URL, "original")
	chunkSizes := []int{}
	for range t.N / perChunk {
		chunkSizes = append(chunkSizes, perChunk)
	}
	chunkSizes = append(chunkSizes, t.N%perChunk)

	evs := [][]*events.BinaryEvent{}
	for _, size := range chunkSizes {
		if size == 0 {
			continue
		}
		subevents := []*events.BinaryEvent{}
		for n := range size {
			ev := testdata.GenEventBinary(&testdata.BC{Action: events.ActionExtracted, PresetFeatures: 20})
			ev.KafkaKey = fmt.Sprintf("1-%d", n)
			subevents = append(subevents, ev)
		}
		evs = append(evs, subevents)
	}

	// begin actual benchmarking
	t.ResetTimer()
	for _, subevents := range evs {
		bulk := events.BulkBinaryEvent{Events: subevents}
		resp, err := conn1.PostEvents(&bulk, &client.PublishEventsOptions{Sync: true, AvroFormat: true})
		require.Nil(t, err)
		require.Equal(t, resp.TotalOk, len(subevents))
	}
	t.StopTimer()
	t.ReportMetric(float64(t.N)/float64(t.Elapsed().Seconds()), "events/s")
}

func postStreamWorker(t *testing.B, s *Benchmarker, identify bool, ch chan bool, wg *sync.WaitGroup, id int) {
	var err error
	var data []byte

	// Malicious email (MIME HTML Archive), trojan, cve20120158.
	data, err = s.fileManager.DownloadFileBytes("0dc315e0b3d9f4098ea5cac977b9814e3c6e9116cf296c1bbfcb3ab95c72ca99")
	require.Nil(t, err)
	conn1 := GetConnection(s.server.URL, "original")
	counter := 0
	sha256sum := sha256.New()
	t.StopTimer()
	for {
		counter += 1
		select {
		case _, ok := <-ch:
			if !ok {
				ch = nil
			} else {
				// change the data to make sure not sending same file each time
				data2 := append(data, []byte(fmt.Sprint(id))...)
				data2 = append(data2, []byte(fmt.Sprint(counter))...)
				sha256sum.Reset()
				_, err = sha256sum.Write(data2)
				require.Nil(t, err)
				cursha256 := fmt.Sprintf("%x", sha256sum.Sum(nil))
				reader := bytes.NewReader(data2)
				t.StartTimer()

				var resp *events.BinaryEntityDatastream
				if identify {
					resp, err = conn1.PostStream("testing", events.DataLabelContent, reader, &client.PostStreamStruct{})
				} else {
					resp, err = conn1.PostStream("testing", events.DataLabelContent, reader, &client.PostStreamStruct{
						SkipIdentify:   true,
						ExpectedSha256: cursha256,
					})
				}
				if err != nil {
					panic(err)
				}
				t.StopTimer() // don't time binary downloading or setup for next test
				if resp.Size != uint64(len(data2)) {
					panic(fmt.Errorf("saved data not same length as input %v vs %v", resp.Size, len(data2)))
				}
				ret, err := conn1.DownloadBinary("testing", events.DataLabelContent, resp.Sha256)
				if err != nil {
					panic(err)
				}
				data3, err := io.ReadAll(ret)
				if err != nil {
					panic(err)
				}
				if len(data3) != len(data2) {
					panic(fmt.Errorf("returned data not same length as input %v vs %v", len(data3), len(data2)))
				}
			}
		}
		if ch == nil {
			break
		}
	}
	wg.Done()
}

func benchmarkPostStreams(t *testing.B, s *Benchmarker) {
	// Malicious email (MIME HTML Archive), trojan, cve20120158.
	data, err := s.fileManager.DownloadFileBytes("0dc315e0b3d9f4098ea5cac977b9814e3c6e9116cf296c1bbfcb3ab95c72ca99")
	require.Nil(t, err)
	var wg sync.WaitGroup
	ch := make(chan bool, 20)

	wg.Add(4)
	go postStreamWorker(t, s, true, ch, &wg, 1)
	go postStreamWorker(t, s, true, ch, &wg, 2)
	go postStreamWorker(t, s, true, ch, &wg, 3)
	go postStreamWorker(t, s, true, ch, &wg, 4)

	// begin actual benchmarking
	t.ResetTimer()
	for range t.N {
		ch <- true
	}
	close(ch)
	wg.Wait()
	t.StopTimer()
	t.ReportMetric(float64(t.N)/float64(t.Elapsed().Seconds()), "streams/s")
	t.ReportMetric((float64(t.N)*float64(len(data)))/float64(t.Elapsed().Seconds())/1024, "kb/s")
}

func benchmarkPostStreamsSkipIdentify(t *testing.B, s *Benchmarker) {
	// Malicious email (MIME HTML Archive), trojan, cve20120158.
	data, err := s.fileManager.DownloadFileBytes("0dc315e0b3d9f4098ea5cac977b9814e3c6e9116cf296c1bbfcb3ab95c72ca99")
	require.Nil(t, err)
	var wg sync.WaitGroup
	ch := make(chan bool, 10)

	wg.Add(4)
	go postStreamWorker(t, s, false, ch, &wg, 1)
	go postStreamWorker(t, s, false, ch, &wg, 2)
	go postStreamWorker(t, s, false, ch, &wg, 3)
	go postStreamWorker(t, s, false, ch, &wg, 4)

	// begin actual benchmarking
	t.ResetTimer()
	for range t.N {
		ch <- true
	}
	close(ch)
	wg.Wait()
	t.StopTimer()
	t.ReportMetric(float64(t.N)/float64(t.Elapsed().Seconds()), "streams/s")
	t.ReportMetric((float64(t.N)*float64(len(data)))/float64(t.Elapsed().Seconds())/1024, "kb/s")
}
