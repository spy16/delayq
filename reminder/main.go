package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/paulbellamy/ratecounter"

	"github.com/spy16/delayq"
)

var (
	addr       = flag.String("redis", "localhost:6379", "Redis host address")
	count      = flag.Int("count", 0, "number of reminders to set")
	delay      = flag.Duration("in", 0, "remind in after this duration")
	distribute = flag.Bool("dist", false, "distribute randomly over the in period")

	workers  = flag.Int("workers", 10, "number of workers")
	pollInt  = flag.Duration("poll", 300*time.Millisecond, "polling interval")
	reclaim  = flag.Duration("reclaim", 1*time.Minute, "reclaim interval for unAck set")
	prefetch = flag.Int("prefetch", 100, "pre-fetch batch size")

	rate    = ratecounter.NewRateCounter(1 * time.Second)
	counter = int64(0)
)

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

	if err := dq.Run(context.Background(), remind); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("delay-queue worker exited: %v", err)
	}
	log.Printf("delay-queue worker exited normally")
}

func remind(_ context.Context, value []byte) error {
	atomic.AddInt64(&counter, 1)
	rate.Incr(1)
	return nil
}

func setupQ() *delayq.DelayQ {
	var client redis.UniversalClient
	if strings.Contains(*addr, ",") {
		client = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: strings.Split(*addr, ","),
		})
	} else {
		client = redis.NewClient(&redis.Options{
			Addr: *addr,
		})
	}

	return delayq.New("example", client, delayq.Options{
		Workers:       *workers,
		PollInterval:  *pollInt,
		PreFetchCount: *prefetch,
		ReclaimTTL:    *reclaim,
	})
}

func enqueue(dq *delayq.DelayQ, count int, in time.Duration, distribute bool) error {
	now := time.Now()

	batchNum := 1
	maxBatchSz := 20000
	items := make([]delayq.Item, maxBatchSz, maxBatchSz)
	for i := 0; i < count; i++ {
		idx := i % maxBatchSz

		items[idx] = delayq.Item{
			At: getT(now, in, distribute),
			Value: fmt.Sprintf("Hello - %d! (set at %d with %s delay)",
				i, time.Now().UnixNano(), in),
		}
		if idx == len(items)-1 || i == count-1 {
			batch := items[0 : idx+1]
			log.Printf("enqueing batch %d (size=%d)", batchNum, len(batch))
			if err := dq.Delay(context.Background(), batch...); err != nil {
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
