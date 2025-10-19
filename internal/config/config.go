package config

import (
	"errors"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
)

// Config holds runtime configuration derived from environment variables.
type Config struct {
	MongoURI               string
	ListenAddr             string
	CheckInterval          time.Duration
	SlowMS                 int64
	EnableProfiler         bool
	TargetDB               string
	CollectCollStats       bool
	IncludeCollections     *regexp.Regexp
	ExcludeCollections     *regexp.Regexp
	MaxCollectionsPerCycle int
	ServerSelectionTimeout time.Duration
	ConnectTimeout         time.Duration
	SocketTimeout          time.Duration
	MaxPoolSize            uint64
}

// Load reads configuration from the environment, applying defaults where needed.
func Load() (Config, error) {
	cfg := Config{
		ListenAddr:             ":9216",
		CheckInterval:          5 * time.Second,
		SlowMS:                 100,
		CollectCollStats:       true,
		ServerSelectionTimeout: 15 * time.Second,
		ConnectTimeout:         10 * time.Second,
		SocketTimeout:          30 * time.Second,
		MaxPoolSize:            50,
	}

	cfg.MongoURI = strings.TrimSpace(os.Getenv("MONGODB_URI"))
	if cfg.MongoURI == "" {
		return Config{}, errors.New("MONGODB_URI não definido")
	}

	if v := strings.TrimSpace(os.Getenv("CHECK_INTERVAL")); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.CheckInterval = d
		} else {
			return Config{}, err
		}
	}

	if v := strings.TrimSpace(os.Getenv("SLOW_MS")); v != "" {
		if slow, ok := parseSlowMS(v); ok {
			cfg.SlowMS = slow
		}
	}

	if strings.EqualFold(strings.TrimSpace(os.Getenv("ENABLE_PROFILER")), "true") {
		cfg.EnableProfiler = true
	}

	if v := strings.TrimSpace(os.Getenv("LISTEN_ADDR")); v != "" {
		cfg.ListenAddr = v
	}

	if v := strings.TrimSpace(os.Getenv("COLLECT_COLLSTATS")); v != "" {
		if v == "false" || v == "0" {
			cfg.CollectCollStats = false
		}
	}

	if v := strings.TrimSpace(os.Getenv("INCLUDE_COLLECTIONS")); v != "" {
		if re, err := regexp.Compile(v); err == nil {
			cfg.IncludeCollections = re
		}
	}

	if v := strings.TrimSpace(os.Getenv("EXCLUDE_COLLECTIONS")); v != "" {
		if re, err := regexp.Compile(v); err == nil {
			cfg.ExcludeCollections = re
		}
	}

	if v := strings.TrimSpace(os.Getenv("MAX_COLLECTIONS_PER_CYCLE")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			cfg.MaxCollectionsPerCycle = n
		}
	}

	if v := strings.TrimSpace(os.Getenv("SERVER_SELECTION_TIMEOUT")); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.ServerSelectionTimeout = d
		}
	}

	if v := strings.TrimSpace(os.Getenv("CONNECT_TIMEOUT")); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.ConnectTimeout = d
		}
	}

	if v := strings.TrimSpace(os.Getenv("SOCKET_TIMEOUT")); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.SocketTimeout = d
		}
	}

	if v := strings.TrimSpace(os.Getenv("MAX_POOL_SIZE")); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			cfg.MaxPoolSize = n
		}
	}

	cfg.TargetDB = strings.TrimSpace(os.Getenv("MONGODB_DB"))
	if cfg.TargetDB == "" {
		if cs, err := connstring.ParseAndValidate(cfg.MongoURI); err == nil {
			cfg.TargetDB = cs.Database
		}
	}

	return cfg, nil
}

func parseSlowMS(v string) (int64, bool) {
	if d, err := time.ParseDuration(v + "ms"); err == nil {
		return d.Milliseconds(), true
	}
	if d, err := time.ParseDuration(v); err == nil {
		return d.Milliseconds(), true
	}
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		return n, true
	}
	return 0, false
}
