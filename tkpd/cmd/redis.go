package cmd

import (
	"github.com/spf13/cobra"
)

func init() {
	redisDumpCmd.AddCommand(redisDumpSortedSetCmd)
	redisDumpCmd.AddCommand(redisDumpListCmd)
	redisDumpCmd.AddCommand(redisDumpStringCmd)

	redisCmd.PersistentFlags().StringVarP(&redisParam.host, "host", "h", "127.0.0.1", "The address of a single redis instance")
	redisCmd.PersistentFlags().StringVarP(&redisParam.port, "port", "p", "6379", "The port of a single redis instance")
	redisCmd.PersistentFlags().StringVarP(&redisParam.password, "password", "a", "", "The authentication password for the redis")
	redisCmd.AddCommand(redisDumpCmd)

	redisCmd.AddCommand(redisPopulateCmd)

	rootCmd.AddCommand(redisCmd)
}

type redisParameter struct {
	host     string
	port     string
	password string
}

var redisParam redisParameter

var redisCmd = &cobra.Command{
	Use:   "redis",
	Short: "A collection of tools for redis database",
	Long: `A collection of tools for redis database. 
	This tool is intended to be used against a single node redis instance, not redis cluster or proxy.
	If you have a redis cluster, it is better to execute this script on each individual node.
	
	[WARN] Doing scripting in redis proxy is usually dangerous since the load will only be centralized on the proxy.
	`,
}

var redisDumpCmd = &cobra.Command{
	Use:  "dump",
	Long: "Read data from Redis and put the data to CSV",
}

var redisPopulateCmd = &cobra.Command{
	Use:  "populate",
	Long: "Read data from CSV and put the data to Redis",
}

var redisDumpSortedSetCmd = &cobra.Command{
	Use:   "sorted-set",
	Short: "Dump redis sorted set datastructure (ZRANGE)",
}

var redisDumpListCmd = &cobra.Command{
	Use:   "list",
	Short: "Dump redis list (LRANGE)",
}

var redisDumpStringCmd = &cobra.Command{
	Use:   "string",
	Short: "Dump redis simple string value (GET)",
}
