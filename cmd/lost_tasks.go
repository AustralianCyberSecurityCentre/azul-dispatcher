package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/lost_tasks"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/kvprovider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/prom"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/spf13/cobra"
)

// reprocessCmd represents the reprocess command
var lostTasksCmd = &cobra.Command{
	Use:   "lost-tasks",
	Short: "Find and record lost Azul tasks",
	Long:  `Find and record lost Azul tasks`,
	Args:  cobra.NoArgs,
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

		ltp, err := lost_tasks.NewLostTaskProcessor(prov, kvprov, ctx)
		if err != nil {
			fmt.Println("Error creating lost task processor:", err)
			os.Exit(1)
		}
		err = ltp.Start()
		if err != nil {
			fmt.Println("Error performing lost task processing:", err)
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(lostTasksCmd)
}
