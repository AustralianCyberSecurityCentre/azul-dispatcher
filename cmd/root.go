package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "azul-dispatcher",
	Short: "Azul event handling and dispatching",
	Long: `The dispatcher is a core component of Azul which acts as the "messenger"
between various internal services.

Specifically, the dispatcher provides an mechanism for interacting with Kafka via a
HTTP interface. This is used by other components of Azul to submit events.

Other commands are also available for testing event handling on the command line, as
well as performing upgrades of pre-existing events in Kafka.
`,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
