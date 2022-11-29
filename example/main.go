package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"

	goredis "github.com/go-redis/redis/v8"
	"github.com/paulbellamy/ratecounter"

	"github.com/spy16/delayq"
	"github.com/spy16/delayq/inmem"
	"github.com/spy16/delayq/redis"
	"github.com/spy16/delayq/sqldb"
)

var (
	config = flag.String("config", "config.json", "Queue config file")

	// enqueuing parameters.
	count      = flag.Int("count", 0, "number of reminders to set")
	distribute = flag.Bool("dist", false, "distribute randomly over the in period")
	delay      = flag.Duration("in", 0, "remind in after this duration")

	// worker parameters.
	workers = flag.Int("workers", 10, "number of workers")
	pollInt = flag.Duration("poll", 300*time.Millisecond, "polling interval")

	rate    = ratecounter.NewRateCounter(1 * time.Second)
	counter = int64(0)
)

type exampleConfig struct {
	QueueType string `json:"queue_type"`
	QueueName string `json:"queue_name"`

	Redis struct {
		Addr          string `json:"addr"`
		ReadShards    int    `json:"read_shards"`
		WriteShards   int    `json:"write_shards"`
		PreFetchCount int    `json:"pre_fetch_count"`
		ReclaimTTL    int    `json:"reclaim_ttl"`
	} `json:"redis"`

	SQLDb struct {
		Driver string `json:"driver"`
		Spec   string `json:"spec"`
	} `json:"sql_db"`
}

func main() {
	flag.Parse()

	dq := setupQ()

	if *count > 0 {
		log.Printf("enqueing %d reminder(s) in %s", *count, *delay)
		if err := enqueue(dq, *count, *delay, *distribute); err != nil {
			log.Fatalf("failed to enqueue: %v", err)
		}
		return
	}

	go reportRate()

	w := delayq.Worker{
		Invoke:       remind,
		Queue:        dq,
		Workers:      *workers,
		PollInterval: *pollInt,
	}

	if err := w.Run(context.Background()); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("delay-queue worker exited: %v", err)
	}
	log.Printf("delay-queue worker exited normally")
}

func remind(_ context.Context, item delayq.Item) error {
	atomic.AddInt64(&counter, 1)
	rate.Incr(1)
	return nil
}

func setupQ() delayq.DelayQ {
	f, err := os.Open(*config)
	if err != nil {
		log.Fatalf("config file not found")
	}
	defer f.Close()

	var cfg exampleConfig
	if err := json.NewDecoder(f).Decode(&cfg); err != nil {
		log.Fatalf("config file is not valid json: %v", err)
	}

	switch cfg.QueueType {
	case "in_memory":
		return &inmem.DelayQ{}

	case "redis":
		var client goredis.UniversalClient
		if strings.Contains(cfg.Redis.Addr, ",") {
			client = goredis.NewClusterClient(&goredis.ClusterOptions{
				Addrs: strings.Split(cfg.Redis.Addr, ","),
			})
		} else {
			client = goredis.NewClient(&goredis.Options{
				Addr: cfg.Redis.Addr,
			})
		}

		return redis.New(cfg.QueueName, client, redis.Options{
			PreFetchCount: cfg.Redis.PreFetchCount,
			ReclaimTTL:    time.Duration(cfg.Redis.ReclaimTTL) * time.Second,
			ReadShards:    cfg.Redis.ReadShards,
			WriteShards:   cfg.Redis.WriteShards,
		})

	case "sql_db":
		db, err := sql.Open(cfg.SQLDb.Driver, cfg.SQLDb.Spec)
		if err != nil {
			log.Fatalf("failed to open sql db: %v", err)
		}

		q, err := sqldb.New(db, cfg.QueueName)
		if err != nil {
			log.Fatalf("failed to init sql_db queue: %v", err)
		}
		return q

	default:
		log.Fatalf("queue_type='%s' is not valid", cfg.QueueType)
		return nil
	}
}

func enqueue(dq delayq.DelayQ, count int, in time.Duration, distribute bool) error {
	now := time.Now()

	batchNum := 1
	maxBatchSz := 20000
	items := make([]delayq.Item, maxBatchSz, maxBatchSz)
	for i := 0; i < count; i++ {
		idx := i % maxBatchSz

		items[idx] = delayq.Item{
			At: getT(now, in, distribute),
			Value: fmt.Sprintf("Hello - %d! (set at %d with %s delay)",
				i, time.Now().Unix(), in),
		}
		if idx == len(items)-1 || i == count-1 {
			batch := items[0 : idx+1]
			log.Printf("enqueing batch %d (size=%d)", batchNum, len(batch))
			if err := dq.Enqueue(context.Background(), batch...); err != nil {
				log.Printf("failed to enqueue: %v", err)
			}
			batchNum++
		}
	}

	return nil
}

func reportRate() {
	lastCount := int64(0)
	for range time.NewTicker(1 * time.Second).C {
		if lastCount != counter {
			log.Printf("current rate: %d (total=%d)", rate.Rate(), counter)
			lastCount = counter
		}
	}
}

func getT(ref time.Time, in time.Duration, distribute bool) time.Time {
	var at int64
	if distribute {
		at = ref.Unix() + rand.Int63n(int64(in.Seconds()))
	} else {
		at = ref.Unix() + int64(in.Seconds())
	}
	return time.Unix(at, 0).UTC()
}
