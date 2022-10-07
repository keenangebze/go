// Scan for redis list, obtain its value.
// format them as CSV with keys as first column, and comma separated value as second column.
package redis

import (
	"encoding/csv"
	"flag"
	"log"
	"os"
	"strings"

	redis "github.com/go-redis/redis"
)

var address string
var matchPattern string
var scanSize int64
var exactKeys string

func main() {
	// Initialize input
	flag.StringVar(&address, "addr", "", "Redis address with format <Redis IP>:<PORT>")
	flag.StringVar(&matchPattern, "match", "", "Redis scan match pattern")
	flag.Int64Var(&scanSize, "ss", 10000, "How many keys to scan at a time")
	flag.StringVar(&exactKeys, "keys", "", "Using exact comma separated key, not scanning for keys. `Match` parameter will be ignored.")

	flag.Parse()

	// Log the input
	log.Println(address, matchPattern, scanSize)

	// Initialize dependencies
	csvWriter := csv.NewWriter(os.Stdout)
	redisClient := initRedis(address)
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
			log.Println(key)
			m[key] = pipe.LRange(key, 0, -1)
		}
		// exec
		pipe.Exec()

		// print the result
		iterateResult(m, csvWriter)

		// iterate for next
		nScan++
		visitedCursor[cursor] = true
		cursor = nextCursor
	}
}

// initRedis initialize redis client
func initRedis(address string) *redis.Client {
	redisClient := redis.NewClient(&redis.Options{
		Addr:       address,
		Password:   "",
		DB:         0,
		MaxRetries: 5,
	})
	return redisClient
}

// iterateResult iterate redis pipeline result and write them as csv rows
func iterateResult(m map[string]*redis.StringSliceCmd, csvWriter *csv.Writer) {
	for key, value := range m {
		results, err := value.Result()
		if err != nil {
			log.Println("[WARN] Cannot obtain result", err)
			continue
		}

		csvWriter.Write([]string{key, strings.Join(results, ",")})
		csvWriter.Flush()
	}
}
