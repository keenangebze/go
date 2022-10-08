package config

var DEFAULT Config

type Config struct {
	// RateLimit how many request we will send to server
	RateLimit int
}
