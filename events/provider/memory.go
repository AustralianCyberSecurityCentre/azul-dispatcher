package provider

import (
	"context"
	"log"
	"regexp"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pauser"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider/in_memory"
	sarama_internals "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider/sarama_internals"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/IBM/sarama"
)

type MemoryProvider struct {
	Mem *in_memory.InMemory
}

func NewMemoryProvider() (*MemoryProvider, error) {
	st.Logger.Info().Msg("new consumer manager")
	mem := in_memory.NewInMemory()
	prov := MemoryProvider{Mem: &mem}
	return &prov, nil
}

func (prov *MemoryProvider) CreateProducer() (ProducerInterface, error) {
	kp := MemoryProducer{Mem: prov.Mem}
	return &kp, nil
}

func (prov *MemoryProvider) CreateConsumer(consumerName, group, offset, pattern string, consumerOptions CreateConsumerOptions) (ConsumerInterface, error) {
	c := newMemoryConsumer(prov.Mem, group, offset, pattern, consumerOptions.PollWait, consumerOptions.LastPauseTime)
	return c, nil
}

func (prov *MemoryProvider) CreateAdmin() (AdminInterface, error) {
	c := MemoryAdmin{Mem: prov.Mem}
	return &c, nil
}

type MemoryConsumer struct {
	Mem      *in_memory.InMemory
	group    string
	offset   string
	pattern  string
	re       *regexp.Regexp
	pollWait time.Duration
	channel  chan *sarama_internals.Message
	cancel   context.CancelFunc
}

func newMemoryConsumer(mem *in_memory.InMemory, group, offset, pattern string, pollWait time.Duration, lastPauseTime time.Time) *MemoryConsumer {
	// Force to latest is lastPauseTime is within the last 30minutes.
	if time.Since(lastPauseTime) < pauser.MAX_TIME_TO_SKIP_PLUGIN_TO_LATEST {
		offset = "latest"
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		panic("bad regex")
	}
	ctx, cancel := context.WithCancel(context.Background())

	ch := make(chan *sarama_internals.Message, 100)

	topics := mem.MatchPattern(re)
	go func() {
		for {
			gotNothing := true
			select {
			case <-ctx.Done():
				return
			default:
				for _, topic := range topics {
					msg, err := mem.Pop(group, topic, 0, offset)
					if err != nil {
						log.Printf("bad error memory consume: %s", err.Error())
						continue
					}
					if msg != nil {
						gotNothing = false
						ch <- msg
					}
				}
			}
			if gotNothing {
				// try not to busy wait
				time.Sleep(time.Microsecond)
			}
		}
	}()
	return &MemoryConsumer{
		Mem:      mem,
		group:    group,
		offset:   offset,
		pattern:  pattern,
		re:       re,
		cancel:   cancel,
		pollWait: pollWait,
		channel:  ch,
	}
}

func (c *MemoryConsumer) Assignment() map[string][]int32 {
	ret := map[string][]int32{}
	topics := c.Mem.MatchPattern(c.re)
	for _, topic := range topics {
		ret[topic] = []int32{0}
	}
	return ret
}

func (c *MemoryConsumer) Ready() bool {
	return len(c.Assignment()) > 0
}

func (c *MemoryConsumer) Poll() *sarama_internals.Message {
	ticker := time.NewTicker(c.pollWait)
	defer ticker.Stop()
	select {
	case kafkaMessage, ok := <-c.channel:
		if !ok {
			st.Logger.Error().Msgf("Unexpected err")
		}
		return kafkaMessage
	case <-ticker.C:
		return nil
	}
}

func (c *MemoryConsumer) Chan() chan *sarama_internals.Message {
	return c.channel
}

func (c *MemoryConsumer) AreAllPartitionsCaughtUp() bool {
	return c.Mem.IsCaughtUpToLive(c.group, c.pattern, c.offset)
}

func (c *MemoryConsumer) Close() error {
	c.cancel()
	return nil
}

type MemoryProducer struct {
	Mem *in_memory.InMemory
}

func (p *MemoryProducer) Produce(msg *sarama.ProducerMessage, confirm bool) error {
	err := p.Mem.Push(msg)
	return err
}

func (p *MemoryProducer) Close() error {
	return nil
}

type MemoryAdmin struct {
	Mem *in_memory.InMemory
}

func (p *MemoryAdmin) GetMetadata(topic *string, allTopics bool, timeoutMs int) ([]*sarama.TopicMetadata, error) {
	meta := []*sarama.TopicMetadata{}
	if topic != nil {
		meta = append(meta,
			&sarama.TopicMetadata{
				Version: 10,
				Err:     sarama.ErrNoError,
				Name:    *topic,
				Partitions: []*sarama.PartitionMetadata{{
					Version: 10,
					ID:      0,
				},
				},
			},
		)
	} else {
		for atopic := range p.Mem.Topics {
			parts := []*sarama.PartitionMetadata{}
			for x := range len(p.Mem.Topics[atopic]) {
				parts = append(parts, &sarama.PartitionMetadata{ID: int32(x)})
			}
			meta = append(meta,
				&sarama.TopicMetadata{
					Version:    10,
					Err:        sarama.ErrNoError,
					Name:       atopic,
					Partitions: parts,
				},
			)
		}
	}
	return meta, nil
}

func (p *MemoryAdmin) CreateTopics(topics []st.GenericKafkaTopicSpecification) error {
	for _, topic := range topics {
		p.Mem.CreateTopic(topic.Topic, topic.NumPartitions)
	}
	return nil
}

func (p *MemoryAdmin) UpdateTopicPartitions(topicToNewPartitionCount map[string]int) {
	for topicName, newPartitionCount := range topicToNewPartitionCount {
		p.Mem.IncreasePartitionCount(topicName, newPartitionCount)
	}
}
func (p *MemoryAdmin) UpdateTopicsConfig(topics []st.GenericKafkaTopicSpecification) {
	log.Printf("In memory can't update topic configurations %+v", topics)
}

func (p *MemoryAdmin) DescribeTopicConfig(topics []st.GenericKafkaTopicSpecification) []*sarama.ResourceResponse {
	// Try and be close to what the api actually returns.
	var result []*sarama.ResourceResponse
	for _, topic := range topics {
		var topicConfigs []*sarama.ConfigEntry
		for optionName, optionValue := range topic.Config {
			topicConfigs = append(topicConfigs, &sarama.ConfigEntry{
				Name:      optionName,
				Value:     optionValue,
				ReadOnly:  false,
				Default:   false,
				Source:    sarama.SourceTopic,
				Sensitive: false,
			})
		}

		resourceResponse := sarama.ResourceResponse{
			ErrorCode: int16(sarama.ErrNoError),
			ErrorMsg:  "",
			Type:      sarama.TopicResource,
			Name:      topic.Topic,
			Configs:   topicConfigs,
		}
		result = append(result, &resourceResponse)
	}
	return result
}

func (p *MemoryAdmin) DeleteTopics(topicNames []string) error {
	for _, topic := range topicNames {
		delete(p.Mem.Topics, topic)
	}
	for cg := range p.Mem.Consumers {
		for _, topic := range topicNames {
			delete(p.Mem.Consumers[cg], topic)
		}
	}
	return nil
}

func (p *MemoryAdmin) Close() {
}
