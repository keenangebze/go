package redis

import (
	"encoding/csv"
	"log"
	"os"
	"strings"

	redis "github.com/go-redis/redis"
)

// ScanList scans (using matchPattern) redis keys and dump the value using LRANGE.
//
// it also support getting a list of value using a comma separated keys in exactKeys (will not scan).
// redisAddress should contain the <HOST>:<PORT> information.
func ScanList(redisAddress, password, matchPattern, exactKeys string, scanSize int64) {
	// Initialize dependencies
	csvWriter := csv.NewWriter(os.Stdout)
	redisClient := initRedis(redisAddress, password)
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

		var keys []string
		var nextCursor uint64
		var err error

		if exactKeys == "" {
			// get all keys from each scan
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
			m[key] = pipe.LRange(key, 0, -1)
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
