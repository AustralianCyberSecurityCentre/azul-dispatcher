package cmd

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/events/provider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/kvprovider"
	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/restapi"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	prometheusmetrics "github.com/deathowl/go-metrics-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rcrowley/go-metrics"
	"github.com/spf13/cobra"
)

// serveCmd represents the serve command
var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Launch the dispatcher server",
	Long:  `Starts the main HTTP server and binds to Kafka.`,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()

		// Disable or enable extendedKafkaMetrics
		if st.Events.EnableExtendedKafkaMetrics {
			prometheusClient := prometheusmetrics.NewPrometheusProvider(
				metrics.DefaultRegistry, "dispatcher", "sarama", prometheus.DefaultRegisterer, 1*time.Second)
			go prometheusClient.UpdatePrometheusMetrics()
		} else {
			metrics.UseNilMetrics = true
		}

		qprov, err := provider.NewSaramaProvider(st.Events.Kafka.Endpoint, ctx)
		if err != nil {
			panic(err)
		}
		kvprov, err := kvprovider.NewRedisProviders()
		if err != nil {
			fmt.Println("Error creating redis client:", err)
			os.Exit(1)
		}

		dp := restapi.NewDispatcher(qprov, kvprov, ctx)
		// FUTURE use viper in cobra to allow overriding of ListenAddr/other settings
		log.Fatal(http.ListenAndServe(st.Settings.ListenAddr, dp.Router))
	},
}

func init() {
	rootCmd.AddCommand(serveCmd)
}
