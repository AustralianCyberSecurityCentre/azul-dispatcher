package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/reprocessor"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/kvprovider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/prom"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/streams"
	"github.com/spf13/cobra"
)

var (
	skipSleep bool
)

// reprocessCmd represents the reprocess command
var reprocessCmd = &cobra.Command{
	Use:   "reprocess",
	Short: "Upgrades events in Kafka",
	Long: `The reprocessor allows upgrading events in Kafka. This is used in Azul for
major version upgrades without needing data resets.

Arguments are specified via environmental variables.

Profiles are mutually exclusive and exactly one must be selected.

'DP.EVENTS.REPROCESS.LEGACY_SOURCES_TO_PREFIX' reprocesses legacy sources.

'DP.EVENTS.REPROCESS.LEGACY_SYSTEM_TO_PREFIX' reprocesses legacy system topics.

Additional settings:

'DP.EVENTS.REPROCESS.DENY_TOPICS' is a comma separated list of topics to *not* process,
even if they match the regex.

'DP.EVENTS.REPROCESS.CONSUMER_GROUP' is the consumer group to use. Changing this can
be useful if you have multiple migrations or are recovering from a failed
migration.

	`,
	Example: `DP.EVENTS.KAFKA.ENDPOINT=localhost:9092
DP.EVENTS.REPROCESS.DENY_TOPICS=azul.donttransfer
DP.EVENTS.REPROCESS.CONSUMER_GROUP=reprocess-01
DP.EVENTS.KAFKA.TOPIC_PREFIX=newprefix01

DP.EVENTS.REPROCESS.LEGACY_SOURCES_TO_PREFIX=true
# or
DP.EVENTS.REPROCESS.LEGACY_SYSTEM_TO_PREFIX=true"`,
	Args: cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		go prom.StartStandalonePromServer()
		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()
		prov, err := provider.NewSaramaProvider(st.Events.Kafka.Endpoint, ctx)
		if err != nil {
			fmt.Println("Error initialising kafka:", err)
			os.Exit(1)

		}
		kvprov, err := kvprovider.NewRedisProviders()
		if err != nil {
			fmt.Println("Error creating redis client:", err)
			os.Exit(1)
		}
		stream := streams.NewStreams()
		defer stream.Close()
		err = reprocessor.Start(prov, kvprov, skipSleep, false)
		if err != nil {
			fmt.Println("Error performing reprocess:", err)
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(reprocessCmd)

	reprocessCmd.Flags().BoolVar(&skipSleep, "ignore-warning", false, "Skip the 30 second cooldown before performing writes")
}
