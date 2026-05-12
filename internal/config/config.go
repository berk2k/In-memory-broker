package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	GRPCPort          string
	MetricsPort       string
	MaxRetries        int
	MaxDLQSize        int
	VisibilityTimeout time.Duration
	DrainTimeout      time.Duration
	DefaultPrefetch   int
	WALPath           string
}

func Load() Config {
	return Config{
		GRPCPort:          getEnv("GRPC_PORT", ":50051"),
		MetricsPort:       getEnv("METRICS_PORT", ":8080"),
		MaxRetries:        getEnvInt("MAX_RETRIES", 3),
		MaxDLQSize:        getEnvInt("MAX_DLQ_SIZE", 100),
		VisibilityTimeout: getEnvDuration("VISIBILITY_TIMEOUT_SEC", 5),
		DrainTimeout:      getEnvDuration("DRAIN_TIMEOUT_SEC", 10),
		DefaultPrefetch:   getEnvInt("DEFAULT_PREFETCH", 1),
		WALPath:           getEnv("WAL_PATH", ""),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}

func getEnvDuration(key string, fallbackSec int) time.Duration {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return time.Duration(i) * time.Second
		}
	}
	return time.Duration(fallbackSec) * time.Second
}
