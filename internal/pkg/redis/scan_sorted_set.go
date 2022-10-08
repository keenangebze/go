package redis

import (
	"encoding/csv"
	"log"
	"os"
	"strings"

	redis "github.com/go-redis/redis"
)

// ScanSortedSet scan redis key using matchPattern and return their value
func ScanSortedSet(address, password, matchPattern, exactKeys string, scanSize, zRangeSize int64) {
	// Initialize dependencies
	csvWriter := csv.NewWriter(os.Stdout)
	redisClient := initRedis(address, password)
	pipe := redisClient.Pipeline()

	// Local variables for iterating the keys
	var cursor uint64
	visitedCursor := make(map[uint64]bool)
	nScan := int64(1)
	for {
		// that's mean all cursor already visited
		if visitedCursor[cursor] {
			break
		}
		// to handle starting cursor not 0
		if visitedCursor[0] == false {
			visitedCursor[0] = true
		}
		// get all keys from each scan

		var keys []string
		var nextCursor uint64
		var err error

		if exactKeys == "" {
			keys, nextCursor, err = redisClient.Scan(cursor, matchPattern, scanSize).Result()
			if err != nil {
				log.Println("ERR", err)
				break
			}
		} else {
			keys = strings.Split(exactKeys, ",")
		}
		m := map[string]*redis.StringSliceCmd{}
		// append keys
		for _, key := range keys {
			m[key] = pipe.ZRange(key, 0, zRangeSize)
		}
		// exec
		pipe.Exec()

		// print the result
		iterateResultStringSlice(m, csvWriter)

		// iterate for next
		nScan++
		visitedCursor[cursor] = true
		cursor = nextCursor
	}
}
