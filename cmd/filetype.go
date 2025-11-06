package cmd

import (
	"fmt"

	"os"
	"path/filepath"

	"github.com/AustralianCyberSecurityCentre/azul-dispatcher.git/streams/identify"
	"github.com/spf13/cobra"
)

// PrintMeta prints metadata for the named filepath.
func printMeta(name string, identifier identify.Identifier) {
	m, err := identifier.HashAndIdentify(name)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s\t\t%s\t%s\n", name, m.FileFormat, m.Mime)
}

// runFiletype walks the directory tree, printing metadata for each file encountered.
func runFiletype(args []string) {
	identifier, err := identify.NewIdentifier()
	if err != nil {
		panic(err)
	}
	defer identifier.Close()

	for _, name := range args {
		file, err := os.Open(name)
		if err != nil {
			fmt.Printf("*Unable to open: %s (%v)\n", name, err)
			file.Close()
			continue
		}
		fi, err := file.Stat()
		if err != nil {
			fmt.Printf("*Unable to stat: %s (%v)\n", name, err)
			file.Close()
			continue
		}
		if !fi.IsDir() {
			printMeta(name, identifier)
		} else {
			// walk it
			err := filepath.Walk(name,
				func(path string, info os.FileInfo, err error) error {
					if err != nil {
						return err
					}
					if info.Mode().IsRegular() {
						printMeta(path, identifier)
					}
					return nil
				})
			if err != nil {
				fmt.Printf("*Unable to walk dir: %s (%v)\n", name, err)
			}
		}
		file.Close()
	}
}

// filetypeCmd represents the filetype command
var filetypeCmd = &cobra.Command{
	Use:   "filetype <dir/path>...",
	Short: "Evaluate file identification data on specified files/directories",
	Long: `Scans one or more files or directories and prints internal
metadata on how this file is identified in the system.`,
	Args: cobra.MatchAll(cobra.MinimumNArgs(1), cobra.OnlyValidArgs),
	Run: func(cmd *cobra.Command, args []string) {
		runFiletype(args)
	},
}

func init() {
	rootCmd.AddCommand(filetypeCmd)
}
