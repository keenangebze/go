package redis

import (
	"encoding/csv"
	"log"
	"strings"

	redis "github.com/go-redis/redis"
)

// initRedis initialize redis client
func initRedis(address, password string) *redis.Client {
	redisClient := redis.NewClient(&redis.Options{
		Addr:       address,
		Password:   password,
		DB:         0,
		MaxRetries: 5,
	})
	return redisClient
}

// iterateResultStringSlice iterate redis pipeline result and write them as csv rows
func iterateResultStringSlice(m map[string]*redis.StringSliceCmd, csvWriter *csv.Writer) {
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
