package tracking

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/kvprovider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/prom"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/rs/zerolog/log"
)

// Number of seconds to wait between retry attempts when deleting items out of redis.
const secondsBeforeRetryingDeleteRedisItem = 10

// Number of seconds to wait before a bulk delete request is sent even if the buffer limit hasn't been reached.
const secondsBeforeDeletingDequeuedEvents = 30

// Number of seconds to wait before checking if a bulk delete request should be run even if buffer limit hasn't been reached.
const secondsBetweenCheckIntervalForDelete = 10

// namespace for redis keys
const NS = "tracking."

var trackerCtx = context.Background()

// All info about a dequeued event in redis
type TaskStarted struct {
	Dequeued string
	Time     int64
	Event    *events.BinaryEvent
}

// TaskTracker provides functions to inspect the currently executing jobs in the cluster.
// Intended for discovery of jobs that have failed badly at some point.
type TaskTracker struct {
	Prov kvprovider.KVInterface
	// Channel that is used to queue up tasks to delete out of redis.
	delChan chan string
}

func NewTaskTracker(kvprov kvprovider.KVInterface, ctx context.Context) (*TaskTracker, error) {
	// Make a sufficiently large buffer for the given Max channel size
	var delChannelSize int
	if st.Events.LostTasksBulkCreateLimit < 1 {
		delChannelSize = 1
	} else {
		delChannelSize = st.Events.LostTasksBulkCreateLimit
	}
	taskTracker := &TaskTracker{Prov: kvprov, delChan: make(chan string, delChannelSize)}
	taskTracker.startDequeuedDeleter(ctx)
	return taskTracker, nil
}

/*Start a goroutine that will consume from DelChan and continually delete events from redis in batches by dequeuedId.*/
func (tr *TaskTracker) startDequeuedDeleter(ctx context.Context) {
	go func(ctx context.Context) {
		dequeuedIdMap := make(map[string]any)
		var lastDeletionTime time.Time
		lastDeletionTime = time.Now()
		runDelete := false
		periodicDeleteCheck := time.NewTicker(secondsBetweenCheckIntervalForDelete * time.Second)
		// infinite run loop after setup.
		for {
			select {
			case <-ctx.Done():
				return
			case dequeuedId := <-tr.delChan:
				dequeuedIdMap[dequeuedId] = nil
				if len(dequeuedIdMap) >= st.Events.LostTasksBulkCreateLimit {
					runDelete = true
				}
			case <-periodicDeleteCheck.C:
				// Triggers the batch of delete dequeuedIds to run even if the batch isn't large enough yet.
				// Runs every time ticker triggers.
				if len(dequeuedIdMap) > 0 && time.Since(lastDeletionTime).Seconds() > secondsBeforeDeletingDequeuedEvents {
					runDelete = true
				}
			}
			// Delete a batch of dequeued Ids when the appropriate conditions have been met.
			if runDelete {
				runDelete = false
				dequeuedIdList := make([]string, len(dequeuedIdMap))
				for k := range dequeuedIdMap {
					dequeuedIdList = append(dequeuedIdList, k)
				}
				// Launch a go-routine to prevent it blocking the channel consumption.
				go deleteDequeuedWithRetry(tr.Prov, dequeuedIdList, ctx)
				lastDeletionTime = time.Now()
				dequeuedIdMap = make(map[string]any)
			}
		}
	}(ctx)
}

/*Delete dequeuedId's provided in the list of dequeuedIds and retry on any failures.*/
func deleteDequeuedWithRetry(kvProv kvprovider.KVInterface, dequeuedIdList []string, ctx context.Context) {
	var goNumDeletions int64
	var goErr error
	completions := 0.0
	// Retry deletion with 3 re-attempts waiting 30 seconds each time.
	for i := 0; i < 3; i++ {
		goNumDeletions, goErr = kvProv.Del(ctx, dequeuedIdList...)
		if goErr != nil {
			bedSet.Logger.Err(goErr).Msg("Couldn't delete a key out of redis in goroutine during delayed retry.")
		}
		completions += float64(goNumDeletions)
		// Finished with the number of retries appended.
		prom.EventsRedisTrackingControl.WithLabelValues(fmt.Sprintf("finished-%d", i)).Add(float64(goNumDeletions))

		// Exit if everything was deleted.
		if int(completions) >= len(dequeuedIdList) {
			break
		}
		time.Sleep(secondsBeforeRetryingDeleteRedisItem * time.Second)
	}

	if goErr != nil {
		numberOfErrors := float64(len(dequeuedIdList)) - completions
		if numberOfErrors > 0 {
			prom.EventsRedisTrackingControl.WithLabelValues("errored").Add(numberOfErrors)
		}
	}
}

// GenerateDequeuedID generates the dequeued id for the supplied event
func (tr *TaskTracker) GenerateDequeuedID(kafka_key, author_name, author_version string) string {
	// add time to dequeued_id so we can calculate expiry during tracking without decoding data
	cTime := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	// dequeued_id is a unique string designed to tie together the triggering event with
	// the results of processing that event
	dequeued_id := fmt.Sprintf(`%s.%s.%s.%s`, kafka_key, author_name, author_version, cTime)
	return dequeued_id
}

// GetRedisTaskStarted returns the raw tracking data
func (tr *TaskTracker) GetRedisTaskStarted(dequeuedID string) ([]byte, error) {
	msg, err := tr.Prov.GetBytes(trackerCtx, dequeuedID)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

// ParseDequeuedIDTimestamp parses the tracking id and original message into TrackItem
func (tr *TaskTracker) ParseDequeuedIDTimestamp(dequeuedID string) (int64, error) {
	// get the timestamp from the end of the string
	s := strings.Split(dequeuedID, ".")
	timestamp, err := strconv.ParseInt(s[len(s)-1], 10, 64)
	if err != nil {
		return 0, errors.New("bad timestamp in id")
	}
	return timestamp, nil
}

func (tr *TaskTracker) ParseTaskStarted(dequeuedID string, timestamp int64, msg []byte) (*TaskStarted, error) {
	record := events.BinaryEvent{}
	err := json.Unmarshal(msg, &record)
	if err != nil {
		return nil, err
	}
	return &TaskStarted{dequeuedID, timestamp, &record}, nil
}

// DoProcessingStarted registers the processing state of the event as dequeued
func (tr *TaskTracker) DoProcessingStarted(msg *events.BinaryEvent, plugin, version string) error {
	encoded, err := json.Marshal(&msg)
	if err != nil {
		return err
	}

	// expire after 2x lost task time to give time for iterating over whole key set
	expiry := time.Duration((st.Events.LostTasksAfterMinutes * 2) * int64(time.Minute))
	err = tr.Prov.Set(trackerCtx, msg.Dequeued, encoded, expiry)
	if err != nil {
		return err
	}
	prom.EventsRedisTrackingControl.WithLabelValues("started").Add(1)
	return nil
}

// DoProcessingFinished registers the processing state of the event as complete
func (tr *TaskTracker) DoProcessingFinished(dequeuedID string) {
	tr.delChan <- dequeuedID
}

// GetAllDequeuedIDs returns all tracking keys in redis. Intended for testing.
func (tr *TaskTracker) GetAllDequeuedIDs() ([]string, error) {
	// should probably bail out if too large
	var err error
	var cursor uint64 = 0
	ret := []string{}
	for {
		var keys []string
		keys, cursor, err = tr.Prov.Scan(trackerCtx, cursor, "*", 1000)
		if err != nil {
			return nil, err
		}
		ret = append(ret, keys...)
		if cursor == 0 {
			break
		}
	}
	return ret, nil
}

// FindLostTasks returns DequeuedInfo that have been lost during processing
func (tr *TaskTracker) FindLostTasks() ([]*TaskStarted, uint64, error) {
	// expiry time in seconds
	expiry := time.Now().UTC().Unix() - (st.Events.LostTasksAfterMinutes * 60)
	// should probably bail out if too large
	var err error
	var cursor uint64 = 0
	var ret []*TaskStarted
	var keys []string
	keys, cursor, err = tr.Prov.Scan(trackerCtx, cursor, "*", 1000)
	if err != nil {
		log.Warn().Err(err).Msg("Scan failed")
		return nil, 0, err
	}
	// check timestamp for expiry
	for _, k := range keys {
		timestamp, err := tr.ParseDequeuedIDTimestamp(k)
		if err != nil {
			log.Warn().Err(err).Str("key", k).Msg("ParseTrackItem key does not contain valid timestamp")
			continue
		}
		if timestamp < expiry {
			// get actual message and add to return list
			msg, err := tr.Prov.GetBytes(trackerCtx, k)
			if err != nil {
				log.Warn().Err(err).Str("key", k).Msg("GetTracking key not found in redis")
				continue
			}

			trackRow, err := tr.ParseTaskStarted(k, timestamp, msg)
			if err != nil {
				log.Warn().Err(err).Str("key", k).Msg("GetTracking key not found in redis")
				continue
			}
			ret = append(ret, trackRow)
		}
	}
	return ret, cursor, nil
}
