package saramago

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pauser"
	"github.com/IBM/sarama"
)

type Message struct {
	Key       []byte
	Value     []byte
	Topic     string
	Partition int32
	Offset    int64
}

// Similar to the base Sarama consumer but tracks when all partitions have been fully consumed.
type TrackingConsumer struct {
	DataChan chan *Message
	// Holds topic name followed by assigned partitions.
	Claims        map[string][]int32
	Name, Group   string
	TopicRegex    *regexp.Regexp
	Brokers       []string
	claimsTracker []int32
	trackerMutex  sync.Mutex
	// True if all partitions are caught up to live.
	IsCaughtUpToLive bool
	// Skip ahead to latest if it hasn't been too long
	skippedClaimsLock sync.Mutex
	lastPauseTime     time.Time
	skippedClaims     map[string][]int32
}

/*
Create a Sarama consumer and start it consuming events from kafka.
ctx: context which will stop to consumer when cancelled.
brokers: kafka brokers to connect to.
name: Name of the thing consuming from Kafka used for stats only (e.g plugin name.)
group: Consumer Group name
topicRegex: Regex to use when locating topics to subscribe to.
offset: 'earliest' or 'latest'
*/
func NewTrackingSaramaConsumer(ctx context.Context, brokers []string, kafkaVersion sarama.KafkaVersion, name, group, topicRegex, offset string, lastPauseTime time.Time) (*TrackingConsumer, error) {
	topicRegexCompiled, err := regexp.Compile(topicRegex)
	if err != nil {
		bedSet.Logger.Error().Str("topicRegex", topicRegex).Msgf("Failed to compile topic regex")
		return nil, fmt.Errorf("the provided regex '%s' could not be compiled: %w", topicRegex, err)
	}
	consumer := &TrackingConsumer{
		Brokers:          brokers,
		Name:             name,
		Group:            group,
		TopicRegex:       topicRegexCompiled,
		DataChan:         make(chan *Message, 1),
		Claims:           map[string][]int32{},
		claimsTracker:    []int32{},
		IsCaughtUpToLive: false,
		lastPauseTime:    lastPauseTime,
		skippedClaims:    map[string][]int32{},
	}

	// Force offset to latest if pause recently ended.
	if time.Since(consumer.lastPauseTime) < pauser.MAX_TIME_TO_SKIP_PLUGIN_TO_LATEST {
		offset = "latest"
	}

	err = startSaramaConsumer(ctx, kafkaVersion, offset, consumer)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}

func (tc *TrackingConsumer) GetName() string {
	return tc.Name
}

func (tc *TrackingConsumer) GetGroup() string {
	return tc.Group
}

func (tc *TrackingConsumer) GetBrokers() []string {
	return tc.Brokers
}

func (tc *TrackingConsumer) GetTopics() []string {
	topics := GetTopics(tc.Brokers, tc.TopicRegex)
	if len(topics) == 0 {
		bedSet.Logger.Info().Msgf("No topics can be found for topic regex %s and group %s.", tc.TopicRegex, tc.Group)
	}
	return topics
}

func (sc *TrackingConsumer) AreAllPartitionsCaughtUp() bool {
	return sc.IsCaughtUpToLive
}

// Below here is all to be compliant with a Sarama Consumer interface that is required.

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *TrackingConsumer) Setup(session sarama.ConsumerGroupSession) error {
	consumer.Claims = session.Claims()
	// Track if end of topic has been reached.
	claimTrackers := []int32{}
	topics := consumer.GetTopics()
	if len(topics) == 0 {
		return fmt.Errorf("no topics for %s", consumer.TopicRegex)
	}
	for _, topic := range topics {
		claimTrackers = append(claimTrackers, consumer.Claims[topic]...)
	}
	consumer.claimsTracker = claimTrackers
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *TrackingConsumer) Cleanup(session sarama.ConsumerGroupSession) error {
	consumer.Claims = map[string][]int32{}
	return nil
}

func (consumer *TrackingConsumer) markPartitionAsCaughtUp(partition int32) {
	if consumer.IsCaughtUpToLive {
		return
	}
	consumer.trackerMutex.Lock()
	defer consumer.trackerMutex.Unlock()
	// Remove the partition if its in the claim tracker list.
	for i := range consumer.claimsTracker {
		if consumer.claimsTracker[i] == partition {
			// Remove the partition.
			consumer.claimsTracker[i] = consumer.claimsTracker[len(consumer.claimsTracker)-1]
			consumer.claimsTracker = consumer.claimsTracker[:len(consumer.claimsTracker)-1]
			break
		}
	}
	// If all partitions are out of the claim tracker we've caught up to live.
	if len(consumer.claimsTracker) == 0 {
		consumer.IsCaughtUpToLive = true
	}
}

// Skip this topic ahead to latest if it hasn't been done before and there is a need to skip.
// Note - setup could be called multiple times on this consumer if a rebalance occurs.
// So we don't reset the skippedClaims here for that reason.
func (consumer *TrackingConsumer) SkipClaimToLiveIfRequired(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) bool {
	// No need to skip to latest
	if time.Since(consumer.lastPauseTime) > pauser.MAX_TIME_TO_SKIP_PLUGIN_TO_LATEST {
		return false
	}
	topicName := claim.Topic()
	partitionToSkip := claim.Partition()
	consumer.skippedClaimsLock.Lock()
	defer consumer.skippedClaimsLock.Unlock()

	alreadySkippedPartitions, ok := consumer.skippedClaims[topicName]
	shouldSkipPartition := true
	if !ok {
		// No claim has been skipped for topic yet, so create a list of skipped claims.
		consumer.skippedClaims[topicName] = []int32{}
	} else {
		// If the partition is in the already skipped partitions we shouldn't skip it again.
		shouldSkipPartition = !slices.Contains(alreadySkippedPartitions, partitionToSkip)
	}
	if shouldSkipPartition {
		bedSet.Logger.Info().Msgf("Skipping to latest for the consumer group %s, topic: %s, and partition %d", consumer.Group, topicName, partitionToSkip)
		session.MarkOffset(topicName, partitionToSkip, claim.HighWaterMarkOffset(), "")
		session.Commit()
		consumer.skippedClaims[topicName] = append(consumer.skippedClaims[topicName], partitionToSkip)
		return true
	}
	return false
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (consumer *TrackingConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	if consumer.SkipClaimToLiveIfRequired(session, claim) {
		return nil
	}
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/IBM/sarama/blob/main/consumer_group.go#L27-L29
	highWatermark := claim.HighWaterMarkOffset()
	// If the high water mark is 0 no messages are consumed so immediately mark as ready.
	if !consumer.IsCaughtUpToLive && (claim.InitialOffset() >= (highWatermark-1) || (highWatermark == 0 && claim.InitialOffset() < 0)) {
		consumer.markPartitionAsCaughtUp(claim.Partition())
	}
	for {
		select {
		// Must return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			// make copy of ConsumerMessage as sarama seems to reuse the buffers
			// and we process way after we mark this message as complete
			copyKey := make([]byte, len(message.Key))
			copy(copyKey, message.Key)
			copyValue := make([]byte, len(message.Value))
			copy(copyValue, message.Value)
			copied := Message{
				Key:       copyKey,
				Value:     copyValue,
				Topic:     strings.Clone(message.Topic),
				Partition: message.Partition,
				Offset:    message.Offset,
			}
			select {
			case consumer.DataChan <- &copied:
				// Mark partition as caught up if its offset is equal to the high watermark.
				if !consumer.IsCaughtUpToLive && message.Offset >= (highWatermark-1) {
					highWatermark = claim.HighWaterMarkOffset()
					if message.Offset >= (highWatermark - 1) {
						consumer.markPartitionAsCaughtUp(message.Partition)
					}
				}
				session.MarkMessage(message, "")
			case <-session.Context().Done():
				return nil
			}
		}
	}
}
