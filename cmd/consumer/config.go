package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Config holds everything tunable at runtime. The visibility timeout and
// heartbeat interval are the operational levers you actually reach for: if
// jobqueue_stale_results_discarded_total ever leaves zero, raise VisTimeout or
// lower HeartbeatTick without touching code.
type Config struct {
	RedisURL        string
	MetricsAddr     string
	NumWorkers      int
	ShutdownTimeout time.Duration

	VisTimeout     time.Duration
	HeartbeatTick  time.Duration
	SweepTick      time.Duration
	BlockTimeout   time.Duration
	DepthPollTick  time.Duration
	MaxAttempts    int
	BaseRetryDelay time.Duration
	MaxRetryDelay  time.Duration
	PromoteTick    time.Duration
}

func loadConfig() Config {
	cfg := Config{
		RedisURL:        envString("REDIS_URL", "redis://localhost:6379/0"),
		MetricsAddr:     envString("METRICS_ADDR", ":2112"),
		NumWorkers:      envInt("WORKER_COUNT", 3),
		ShutdownTimeout: envDuration("SHUTDOWN_TIMEOUT", 30*time.Second),

		VisTimeout:     envDuration("VISIBILITY_TIMEOUT", 15*time.Second),
		HeartbeatTick:  envDuration("HEARTBEAT_INTERVAL", 5*time.Second),
		SweepTick:      envDuration("SWEEP_INTERVAL", 3*time.Second),
		BlockTimeout:   envDuration("BLOCK_TIMEOUT", 15*time.Second),
		DepthPollTick:  envDuration("DEPTH_POLL_INTERVAL", 5*time.Second),
		MaxAttempts:    envInt("MAX_ATTEMPTS", 3),
		BaseRetryDelay: envDuration("BASE_RETRY_DELAY", 1*time.Second),
		MaxRetryDelay:  envDuration("MAX_RETRY_DELAY", 5*time.Minute),
		PromoteTick:    envDuration("PROMOTE_INTERVAL", 1*time.Second),
	}

	for _, problem := range cfg.validate() {
		log.Printf("\033[1;33m[Config] %s\033[0m", problem)
	}

	return cfg
}

// validate reports settings that are legal but dangerous. These are warnings
// rather than fatal errors: an operator tuning timeouts under load should not
// be blocked, but should be told.
func (c Config) validate() []string {
	var problems []string

	if c.HeartbeatTick >= c.VisTimeout {
		problems = append(problems, fmt.Sprintf(
			"HEARTBEAT_INTERVAL (%v) >= VISIBILITY_TIMEOUT (%v): a healthy worker cannot refresh its claim in time and the sweeper will steal live jobs",
			c.HeartbeatTick, c.VisTimeout))
	} else if c.VisTimeout < 3*c.HeartbeatTick {
		problems = append(problems, fmt.Sprintf(
			"VISIBILITY_TIMEOUT (%v) is less than 3x HEARTBEAT_INTERVAL (%v): a single dropped heartbeat can trigger a false reclaim",
			c.VisTimeout, c.HeartbeatTick))
	}

	if c.NumWorkers < 1 {
		problems = append(problems, fmt.Sprintf("WORKER_COUNT (%d) is below 1; no jobs will be processed", c.NumWorkers))
	}
	if c.MaxAttempts < 1 {
		problems = append(problems, fmt.Sprintf("MAX_ATTEMPTS (%d) is below 1; every job goes straight to the DLQ", c.MaxAttempts))
	}
	if c.BaseRetryDelay > c.MaxRetryDelay {
		problems = append(problems, fmt.Sprintf(
			"BASE_RETRY_DELAY (%v) exceeds MAX_RETRY_DELAY (%v); every retry will be clamped to the max",
			c.BaseRetryDelay, c.MaxRetryDelay))
	}

	return problems
}

// apply pushes the config into the package-level values the rest of the
// consumer reads. They stay as vars so tests can compress them.
func (c Config) apply() {
	visTimeout = c.VisTimeout
	heartbeatTick = c.HeartbeatTick
	sweepTick = c.SweepTick
	blockTimeout = c.BlockTimeout
	depthPollInterval = c.DepthPollTick
	maxAttempts = c.MaxAttempts
	baseRetryDelay = c.BaseRetryDelay
	maxRetryDelay = c.MaxRetryDelay
	promoteTick = c.PromoteTick
}

func envString(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Printf("\033[1;33m[Config] %s=%q is not an integer, using %d\033[0m", key, v, fallback)
		return fallback
	}
	return n
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Printf("\033[1;33m[Config] %s=%q is not a duration (try \"15s\"), using %v\033[0m", key, v, fallback)
		return fallback
	}
	return d
}
