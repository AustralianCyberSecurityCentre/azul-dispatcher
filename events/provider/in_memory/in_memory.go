package in_memory

import (
	"fmt"
	"log"
	"regexp"
	"sync"

	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/settings"
	sarama_internals "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider/sarama_internals"
	"github.com/IBM/sarama"
)

var mutex = &sync.Mutex{}

type InMemory struct {
	Topics map[string]map[int32][][]byte
	// map consumer names to positions in each topic
	Consumers     map[string]map[string]map[int32]int
	MatchedTopics map[*regexp.Regexp][]string
}

func NewInMemory() InMemory {
	return InMemory{
		// TopicsList: []string{},
		Topics:        make(map[string]map[int32][][]byte),
		Consumers:     make(map[string]map[string]map[int32]int),
		MatchedTopics: make(map[*regexp.Regexp][]string),
	}
}

func (mem *InMemory) CreateTopic(topic string, partitionCount int) {
	if partitionCount < 1 {
		partitionCount = 1
	}
	mutex.Lock()
	defer mutex.Unlock()
	mem.createTopic(topic, partitionCount)
}

func (mem *InMemory) IncreasePartitionCount(topic string, partitionCount int) {
	mutex.Lock()
	defer mutex.Unlock()
	log.Printf("kafka increase partitions %v %v", topic, partitionCount)

	var ok bool
	// Topic must exist
	_, ok = mem.Topics[topic]
	if !ok {
		log.Fatalf("Attempted to update topic %v partition count to %v but the topic does not exist", topic, partitionCount)
	}
	// topic partition count can't be decreasing or equal to the current size
	_, ok = mem.Topics[topic][int32(partitionCount-1)]
	if ok {
		log.Fatalf("Attempting to decrease the size of topic %v to %v partitions which is not allowed.", topic, partitionCount)
	}
	// Increase the topic partition count.
	mem.createTopic(topic, partitionCount)
}

func (mem *InMemory) createTopic(topic string, partitionCount int) {
	var ok bool
	_, ok = mem.Topics[topic]
	if !ok {
		mem.Topics[topic] = make(map[int32][][]byte)
	}
	for cur_par := int32(0); cur_par < int32(partitionCount); cur_par++ {
		_, ok = mem.Topics[topic][cur_par]
		if !ok {
			mem.Topics[topic][cur_par] = make([][]byte, 0)
		}
	}
	// reset cached regex matches
	mem.MatchedTopics = map[*regexp.Regexp][]string{}
}

func (mem *InMemory) checkTopic(topic string, partition int32) bool {
	var ok bool
	_, ok = mem.Topics[topic]
	if !ok {
		return false
	}
	_, ok = mem.Topics[topic][partition]
	return ok
}

func (mem *InMemory) ensureConsumers(consumer, topic string, partition int32, offset string) error {
	if !mem.checkTopic(topic, partition) {
		return fmt.Errorf("topic:partition %s:%v does not exist, for consumer %s", topic, partition, consumer)
	}

	var ok bool
	_, ok = mem.Consumers[consumer]
	if !ok {
		mem.Consumers[consumer] = make(map[string]map[int32]int)
	}
	_, ok = mem.Consumers[consumer][topic]
	if !ok {
		mem.Consumers[consumer][topic] = make(map[int32]int)
	}
	_, ok = mem.Consumers[consumer][topic][partition]
	if !ok {
		if offset == "earliest" {
			mem.Consumers[consumer][topic][partition] = 0
		} else if offset == "latest" {
			// be over the end
			mem.Consumers[consumer][topic][partition] = len(mem.Topics[topic][partition])
		} else {
			return fmt.Errorf("bad consumer %s offset %s", consumer, offset)
		}
	}
	return nil
}

func (mem *InMemory) MatchPattern(re *regexp.Regexp) []string {
	mutex.Lock()
	defer mutex.Unlock()

	matches, ok := mem.MatchedTopics[re]
	if !ok {
		for topic := range mem.Topics {
			if re.MatchString(topic) {
				matches = append(matches, topic)
			}
		}
		mem.MatchedTopics[re] = matches
	}

	return matches
}

func (mem *InMemory) Push(msg *sarama.ProducerMessage) error {
	mutex.Lock()
	defer mutex.Unlock()
	if msg.Partition < 0 {
		topicData, ok := mem.Topics[msg.Topic]
		if ok {
			partitioner := sarama.NewHashPartitioner(msg.Topic)
			selectedPartition, err := partitioner.Partition(msg, int32(len(topicData)))
			if err != nil {
				bedSet.Logger.Warn().Err(err).Msg("failed to find a valid partition using partitioner!")
				return err
			}
			msg.Partition = selectedPartition
		}
	}

	if !mem.checkTopic(msg.Topic, msg.Partition) {
		return fmt.Errorf("bad push with unregistered topic '%s' partition '%v'", msg.Topic, msg.Partition)
	}
	var msgVal []byte
	var err error
	if msg.Value != nil {
		msgVal, err = msg.Value.Encode()
		if err != nil {
			bedSet.Logger.Error().Err(err).Msg("unexpected failed to encode message value during push.")
			return err
		}
	} else {
		msgVal = nil
	}
	mem.Topics[msg.Topic][msg.Partition] = append(mem.Topics[msg.Topic][msg.Partition], msgVal)

	return nil
}

func (mem *InMemory) Pop(consumer string, topic string, partition int32, offset string) (*sarama_internals.Message, error) {
	if partition < 0 {
		partition = 0
	}
	mutex.Lock()
	defer mutex.Unlock()
	err := mem.ensureConsumers(consumer, topic, partition, offset)
	if err != nil {
		return nil, err
	}
	pos := mem.Consumers[consumer][topic][partition]
	events := mem.Topics[topic][partition]
	if (len(events) - 1) < pos {
		return nil, nil
	}
	event := mem.Topics[topic][partition][pos]
	mem.Consumers[consumer][topic][partition] += 1
	bedSet.Logger.Debug().Msgf("kafka mem pop topic:%v partition:%v consumer:%v bytes:%v", topic, partition, consumer, len(event))
	return &sarama_internals.Message{Value: event, Topic: topic}, nil
}

func (mem *InMemory) IsCaughtUpToLive(consumer, topic, offset string) bool {
	mutex.Lock()
	defer mutex.Unlock()
	for partition := range mem.Topics[topic] {
		pos := mem.Consumers[consumer][topic][partition]
		events := mem.Topics[topic][partition]
		if pos < (len(events)) {
			return false
		}
	}
	return true
}
