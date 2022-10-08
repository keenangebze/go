package redis

import (
	"fmt"
	"log"
)

// Scan scan redis key using matchPattern and return their value
func Scan(address, password, matchPattern string, scanSize int64) {
	// Initialize dependencies
	redisClient := initRedis(address, password)

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

		keys, nextCursor, err = redisClient.Scan(cursor, matchPattern, scanSize).Result()
		if err != nil {
			log.Println("ERR", err)
			break
		}

		for _, key := range keys {
			fmt.Println(key)
		}

		// iterate for next
		nScan++
		visitedCursor[cursor] = true
		cursor = nextCursor
	}
}
