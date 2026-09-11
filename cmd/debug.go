package cmd

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/events"
	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v13/gosrc/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/pipeline"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	sarama_internals "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider/sarama_internals"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/spf13/cobra"
)

// debugCmd represents the serve command
var debugCmd = &cobra.Command{
	Use:   "debug",
	Short: "Query Kafka for topic",
	Long:  `Query kafka for events about a specific topic`,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()

		// Loading input params
		historicBool, err := cmd.Flags().GetBool("historic")
		if err != nil {
			bedSet.Logger.Fatal().Err(err).Msg("couldn't load offset flag.")
		}
		offset := "latest"
		if historicBool {
			offset = "earliest"
		}

		consumerGroupName, err := cmd.Flags().GetString("consumer-group")
		if err != nil {
			bedSet.Logger.Fatal().Err(err).Msg("couldn't load consumer group name flag.")
		}
		consumerName, err := cmd.Flags().GetString("consumer-name")
		if err != nil {
			bedSet.Logger.Fatal().Err(err).Msg("couldn't load consumer name flag.")
		}

		topicPattern, err := cmd.Flags().GetString("pattern")
		if err != nil {
			bedSet.Logger.Fatal().Err(err).Msg("couldn't load topic pattern flag.")
		}

		eventsToCollect, err := cmd.Flags().GetInt("count")
		if err != nil {
			bedSet.Logger.Fatal().Err(err).Msg("couldn't load eventsToCollect flag.")
		}

		// Main work
		qprov, err := provider.NewSaramaProvider(st.Events.Kafka.Endpoint, ctx)
		if err != nil {
			bedSet.Logger.Fatal().Err(err).Msg("could not initialise sarama provider")
		}

		consumer, err := qprov.CreateConsumer(consumerName, consumerGroupName, offset, topicPattern, provider.NewConsumerOptions(30*time.Second))
		if err != nil {
			bedSet.Logger.Fatal().Err(err).Msg("could not initialise kafka client")
		}

		// Poll multiple times for kafka events
		var message *sarama_internals.Message

		fmt.Println("Messages:")
		// Continually print messages until the count is reached
		for range eventsToCollect {
			// Retry finding a message up to 3 times before giving up.
			for range 3 {
				message = consumer.Poll()
				if message != (*sarama_internals.Message)(nil) {
					break
				}
			}
			if message == (*sarama_internals.Message)(nil) {
				fmt.Println("Consumer could not find any events!")
				break
			}
			msgs, failedConversions, err := pipeline.AvroToMsgInFlights(message.Value, events.ModelBinary)
			if err != nil {
				bedSet.Logger.Fatal().Err(err).Msg("Could not get any messages from avro format")
			}
			if failedConversions.TotalFailures > 0 {
				fmt.Println("Failed Messages:")
				fmt.Printf("%+v\n", failedConversions)
			}
			// Non json print method
			// for _, m := range msgs {
			// 	event, ok := m.GetBinary()
			// 	if ok {
			// 		fmt.Printf("%+v\n", *event)
			// 	} else {
			// 		bedSet.Logger.Warn().Msg("could not print event as GetBinary failed!")
			// 	}
			// }
			for _, m := range msgs {
				rawJson, err := m.MarshalJSON()
				if err == nil {
					fmt.Printf("%s\n", rawJson)
				} else {
					bedSet.Logger.Warn().Msgf("could not print event with error %v!", err)
				}
			}
		}

	},
}

// debugCmd represents the serve command
var listTopicsCmd = &cobra.Command{
	Use:   "list-topics",
	Short: "List Kafka topics",
	Long:  `List kafka topics`,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()
		// Main work
		qprov, err := provider.NewSaramaProvider(st.Events.Kafka.Endpoint, ctx)
		if err != nil {
			bedSet.Logger.Fatal().Err(err).Msg("could not initialise sarama provider")
		}
		aClient, err := qprov.CreateAdmin()
		if err != nil {
			bedSet.Logger.Fatal().Err(err).Msg("could not initialise admin kafka client")
		}
		topics, err := aClient.GetMetadata(nil, true, 5000)
		if err != nil {
			bedSet.Logger.Fatal().Err(err).Msg("could not list topic metadata")
		}
		fmt.Println("All topics:")

		disableFilter, err := cmd.Flags().GetBool("disable-filter")
		if err != nil {
			bedSet.Logger.Fatal().Err(err).Msg("couldn't load disable-filter flag.")
		}

		for _, t := range topics {
			if disableFilter {
				fmt.Printf("%v\n", t.Name)
			} else if strings.HasPrefix(t.Name, st.Events.Kafka.TopicPrefix) {
				fmt.Printf("%v\n", t.Name)
			}

		}
	},
}

func init() {
	debugCmd.Flags().Bool("historic", false, "Start the consumer at historic as compared to live.")
	debugCmd.Flags().String("consumer-group", "test-group-1", "Name of the consumer group to use for tracking kafka groups")
	debugCmd.Flags().String("consumer-name", "test-consumer-1", "Name of the consumer to use for talking to kafka groups.")
	debugCmd.Flags().String("pattern", ".*", "Regex for matching specific topics")
	debugCmd.Flags().Int("count", 1, "Number of events to consume.")
	rootCmd.AddCommand(debugCmd)
	listTopicsCmd.Flags().Bool("disable-filter", false, "Disable the dispatcher topicPrefix filtering.")
	rootCmd.AddCommand(listTopicsCmd)
}
