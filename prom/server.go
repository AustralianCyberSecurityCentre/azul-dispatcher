package prom

import (
	"net/http"

	bedSet "github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/settings"
	st "github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/settings"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Starts a HTTP server just for Prometheus - we don't want to setup the full dispatcher here
func StartStandalonePromServer() {
	http.Handle("/metrics", promhttp.Handler())

	bedSet.Logger.Info().Str("addr", st.Settings.ListenAddr).Msg("launching metrics server")

	err := http.ListenAndServe(st.Settings.ListenAddr, nil)
	if err != nil {
		bedSet.Logger.Fatal().Err(err).Msg("failed to listen for prometheus metrics")
	}
}
