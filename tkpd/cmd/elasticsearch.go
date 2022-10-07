package cmd

import (
	"github.com/spf13/cobra"
)

func init() {
	esCmd.PersistentFlags().StringVarP(&esParam.host, "host", "h", "127.0.0.1", "The host for elastic search")
	esCmd.PersistentFlags().StringVarP(&esParam.port, "port", "p", "9200", "The port for the elastic search")
	esCmd.PersistentFlags().StringVarP(&esParam.password, "password", "a", "", "The authentication password for the elastic search")

	esCmd.AddCommand(esSearch2DocsCmd)
	esCmd.AddCommand(esIDs2DocsCmd)
	esCmd.AddCommand(esDocs2IndexCmd)

	rootCmd.AddCommand(esCmd)
}

type esParameter struct {
	host     string
	port     string
	password string
}

var esParam esParameter

var esCmd = &cobra.Command{
	Use:   "es",
	Short: "A collection of tools for elastic search database",
	Long:  `A collection of tools for elastic search database.`,
}

var esSearch2DocsCmd = &cobra.Command{
	Use:  "search2docs",
	Long: "Search data in ES, output them all as line separated JSON (JSONL)",
}

var esIDs2DocsCmd = &cobra.Command{
	Use:  "ids2docs",
	Long: "Read line separated product IDs from file or STDIN, output them as line separated JSON (JSONL)",
}

var esDocs2IndexCmd = &cobra.Command{
	Use:   "docs2index",
	Short: "Read line separated JSON (JSONL) files from file or STDIN, insert them to an ES index",
}
