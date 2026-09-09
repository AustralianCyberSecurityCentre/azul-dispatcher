package cmd

import (
	"context"
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
			offset = "historic"
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

		// Main work
		qprov, err := provider.NewSaramaProvider(st.Events.Kafka.Endpoint, ctx)
		if err != nil {
			bedSet.Logger.Fatal().Err(err).Msg("could not initialise sarama provider")
		}

		consumer, err := qprov.CreateConsumer(consumerName, consumerGroupName, offset, topicPattern, provider.NewConsumerOptions(10*time.Second))
		if err != nil {
			bedSet.Logger.Fatal().Err(err).Msg("could not initialise kafka client")
		}

		// Poll multiple times for kafka events
		var message *sarama_internals.Message
		for range 3 {
			message = consumer.Poll()
			if message != (*sarama_internals.Message)(nil) {
				break
			}
		}
		if message == (*sarama_internals.Message)(nil) {
			bedSet.Logger.Info().Msg("Consumer could not find any events!")
			return
		}

		msgs, failedConversions, err := pipeline.AvroToMsgInFlights(message.Value, events.ModelBinary)
		if err != nil {
			bedSet.Logger.Fatal().Err(err).Msg("Could not get any messages from avro format")
		}

		bedSet.Logger.Info().Msg("Messages:")
		bedSet.Logger.Info().Msgf("%+v", msgs)

		bedSet.Logger.Info().Msg("Failed Messages:")
		bedSet.Logger.Info().Msgf("%+v", failedConversions)

	},
}

func init() {
	debugCmd.Flags().Bool("historic", false, "Start the consumer at historic as compared to live.")
	debugCmd.Flags().String("consumer-group", "test-group-1", "Name of the consumer group to use for tracking kafka groups")
	debugCmd.Flags().String("consumer-name", "test-consumer-1", "Name of the consumer to use for talking to kafka groups.")
	debugCmd.Flags().String("pattern", ".*", "Regex for matching specific topics")
	rootCmd.AddCommand(debugCmd)
}
