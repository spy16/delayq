package redis

import (
	"time"
)

// Options represents queue-level configurations for the delay queue.
type Options struct {
	// ReadShards represents the number of splits to read from. This will
	// always be >= WriteShards.
	ReadShards int

	// WriteShards represents the number of splits made to the delay & un-ack
	// sets. This value must always be <= ReadShards. Defaults to 1.
	WriteShards int

	// Workers represents the number of threads to launch for fetching batch
	// and processing. A single worker fetches a batch and processes items in
	// the batch sequentially.
	Workers int

	// PollInterval decides the interval between each fetch from Redis.
	PollInterval time.Duration

	// PreFetchCount decides the number of items to fetch in a single fetch
	// call. Once fetched, entire batch is processed in-memory. Entire batch
	// must complete within ReclaimTTL to ensure exactly-once semantics. If
	// this guarantee is breached, at-least-once semantics apply.
	PreFetchCount int

	// ReclaimTTL decides the lifetime of items in the unack set before they
	// are re-claimed by recovery. Setting this lower than the time required
	// for processing one entire batch may cause more-than-once delivery to
	// workers.
	ReclaimTTL time.Duration
}

func (opt *Options) setDefaults() {
	if opt.Workers == 0 {
		opt.Workers = 10
	}
	if opt.PollInterval == 0 {
		opt.PollInterval = 500 * time.Millisecond
	}
	if opt.PreFetchCount == 0 {
		opt.PreFetchCount = 20
	}
	if opt.ReclaimTTL == 0 {
		opt.ReclaimTTL = 3 * time.Minute
	}
	if opt.WriteShards == 0 {
		opt.WriteShards = 1
	}
	if opt.ReadShards < opt.WriteShards {
		opt.ReadShards = opt.WriteShards
	}
}

type loggerFunc func(level, format string, args ...interface{})
