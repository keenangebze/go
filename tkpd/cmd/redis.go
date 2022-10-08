package cmd

import (
	"strconv"

	"github.com/spf13/cobra"

	"github.com/keenangebze/go/internal/pkg/redis"
)

func init() {
	redisDumpCmd.AddCommand(redisDumpSortedSetCmd)
	redisDumpCmd.AddCommand(redisDumpListCmd)
	redisDumpCmd.AddCommand(redisDumpStringCmd)
	redisDumpCmd.PersistentFlags().StringVarP(&redisScanParam.matchPattern, "match", "m", "*", "Redis key scan pattern")
	redisDumpCmd.PersistentFlags().StringVarP(&redisScanParam.exactKeys, "keys", "k", "", "Exact keys to dump (will ignore match flag)")
	redisDumpCmd.PersistentFlags().Int64VarP(&redisScanParam.scanSize, "scan-size", "ss", 10000, "Exact keys to dump (will ignore match flag)")

	redisCmd.PersistentFlags().StringVarP(&redisParam.host, "host", "h", "127.0.0.1", "The address of a single redis instance")
	redisCmd.PersistentFlags().IntVarP(&redisParam.port, "port", "p", 6379, "The port of a single redis instance")
	redisCmd.PersistentFlags().StringVarP(&redisParam.password, "password", "a", "", "The authentication password for the redis")
	redisCmd.AddCommand(redisDumpCmd)

	redisCmd.AddCommand(redisPopulateCmd)

	rootCmd.AddCommand(redisCmd)
}

type redisScanParameter struct {
	matchPattern string
	exactKeys    string
	scanSize     int64
}
type redisParameter struct {
	host     string
	port     int
	password string
}

var redisParam redisParameter
var redisScanParam redisScanParameter

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
	Run: func(cmd *cobra.Command, args []string) {
		redis.ScanSortedSet(redisParam.host+":"+strconv.Itoa(redisParam.port), redisParam.password, redisScanParam.matchPattern, redisScanParam.exactKeys, redisScanParam.scanSize, -1)
	},
}

var redisDumpListCmd = &cobra.Command{
	Use:   "list",
	Short: "Dump redis list (LRANGE)",
	Run: func(cmd *cobra.Command, args []string) {
		redis.ScanList(redisParam.host+":"+strconv.Itoa(redisParam.port), redisParam.password, redisScanParam.matchPattern, redisScanParam.exactKeys, redisScanParam.scanSize)
	},
}

var redisDumpStringCmd = &cobra.Command{
	Use:   "string",
	Short: "Dump redis simple string value (GET)",
	Run: func(cmd *cobra.Command, args []string) {
		redis.ScanString(redisParam.host+":"+strconv.Itoa(redisParam.port), redisParam.password, redisScanParam.matchPattern, redisScanParam.exactKeys, redisScanParam.scanSize)
	},
}
