package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.PersistentFlags().BoolP("help", "", false, "help for this command")
}

const VERSION = "v0.1-SNAPSHOT"

var rootCmd = &cobra.Command{
	Use:   "tkpd",
	Short: "Making engineer life's easier in Tokopedia",
	Long:  `A collection of scripts for common tasks as an engineer in Tokopedia`,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
