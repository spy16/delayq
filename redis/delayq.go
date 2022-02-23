package redis

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/spy16/delayq"
)

// New returns a new delay-queue instance with given queue name.
func New(queueName string, client redis.UniversalClient, opts ...Options) *DelayQ {
	var opt Options
	if len(opts) > 0 {
		opt = opts[0]
	}
	opt.setDefaults()

	return &DelayQ{
		client:      client,
		workers:     opt.Workers,
		pollInt:     opt.PollInterval,
		prefetch:    opt.PreFetchCount,
		queueName:   queueName,
		reclaimTTL:  opt.ReclaimTTL,
		readShards:  opt.ReadShards,
		writeShards: opt.WriteShards,
		Logger: func(level, format string, args ...interface{}) {
			level = strings.TrimSpace(strings.ToUpper(level))
			log.Printf("[%s] %s", level, fmt.Sprintf(format, args...))
		},
	}
}

// DelayQ represents a distributed, reliable delay-queue backed by Redis.
type DelayQ struct {
	Logger      loggerFunc
	client      redis.UniversalClient
	pollInt     time.Duration
	workers     int
	prefetch    int
	queueName   string
	reclaimTTL  time.Duration
	readShards  int
	writeShards int
}

// Delay pushes an item onto the delay-queue with the given delay. Value must
// be unique. Duplicate values will be ignored.
func (dq *DelayQ) Delay(ctx context.Context, items ...delayq.Item) error {
	_, err := dq.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, item := range items {
			pipe.ZAdd(ctx, dq.getSetName(true), &redis.Z{
				Score:  float64(item.At.Unix()),
				Member: item.Value,
			})
		}
		return nil
	})
	return err
}

// Run runs the worker loop that invoke fn whenever a delayed value is ready.
// It blocks until all worker goroutines exit due to some critical error or
// until context is cancelled, whichever happens first.
func (dq *DelayQ) Run(ctx context.Context, fn delayq.Process) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg := &sync.WaitGroup{}
	for i := 0; i < dq.workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if err := dq.reaper(ctx, id, fn); err != nil && !errors.Is(err, context.Canceled) {
				dq.log("warn", "reaper-%d died: %v", id, err)
			} else {
				dq.log("info", "reaper-%d exited gracefully", id)
			}
		}(i)
	}
	wg.Wait() // wait for workers to exit.

	dq.log("info", "all workers returned, delayq shutting down")
	return nil
}

func (dq *DelayQ) reaper(ctx context.Context, id int, fn delayq.Process) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf("panic: %v", v)
		}
	}()

	pollTimer := time.NewTimer(0)
	defer pollTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-pollTimer.C:
			pollTimer.Reset(dq.pollInt)

			if err := dq.reap(ctx, id, fn); err != nil {
				dq.log("warn", "reap error: %v", err)
				continue
			}
		}
	}
}

func (dq *DelayQ) reap(ctx context.Context, reaperID int, fn delayq.Process) error {
	// add 30 seconds as a safety measure to prevent reclaiming too soon.
	reclaimTTL := (dq.reclaimTTL + (30 * time.Second)).Seconds()

	setName := dq.getSetName(false)
	dq.log("debug", "reaper-%d picking set '%s'", reaperID, setName)
	list, err := dq.zfetch(ctx, setName, reclaimTTL, dq.prefetch)
	if err != nil {
		return err
	} else if len(list) == 0 {
		return nil
	}
	dq.log("debug", "reaper-%d got %d items", reaperID, len(list))

	// entire batch of items is subjected to the same TTL. So we create
	// a single context with timeout.
	ctx, cancel := context.WithTimeout(ctx, dq.reclaimTTL)
	defer cancel()

	start := time.Now()
	for _, v := range list {
		select {
		case <-ctx.Done():
			// TODO: nAck items that we have not pushed to the stream.
			return ctx.Err()

		default:
			item, isStr := v.(string)
			if !isStr {
				panic(fmt.Errorf("expecting string, got %s", reflect.TypeOf(v)))
			}

			failure := false
			fnErr := fn(ctx, []byte(item))
			if fnErr != nil {
				failure = true
				dq.log("warn", "fn failed to process item, will requeue: %v", err)
			}

			// ack/nAck the item. ignoring the error is okay since if the ack failed,
			// the item remains in the un-ack set and will be claimed by the reclaimer.
			_ = dq.ack(ctx, setName, item, failure)
		}
	}

	dq.log("debug", "finished %d items in %s", len(list), time.Since(start))
	return nil
}

func (dq *DelayQ) ack(ctx context.Context, queueName, item string, negative bool) error {
	if negative {
		// reduce the score set for the item when it was fetched for
		// execution so that it is picked up again for immediate retry.
		_, err := dq.client.ZIncrBy(ctx, queueName, -1*dq.reclaimTTL.Seconds(), item).Result()
		return err
	}

	_, err := dq.client.ZRem(ctx, queueName, item).Result()
	return err
}

func (dq *DelayQ) zfetch(ctx context.Context, fromSet string, scoreDelta float64, batchSz int) ([]interface{}, error) {
	now := time.Now().Unix()
	newScore := float64(now) + scoreDelta

	// atomically fetch & increment score for ready items from the queue.
	keys := []string{fromSet}
	args := []interface{}{now, newScore, batchSz}
	items, err := zfetchLua.Run(ctx, dq.client, keys, args...).Result()
	if err != nil {
		dq.log("error", "lua script execution failed: %v", err)
		return nil, err
	}

	list, ok := items.([]interface{})
	if !ok {
		panic(fmt.Errorf("expecting list, got %s", reflect.TypeOf(list)))
	}

	dq.log("debug", "moved %d items", len(list))
	return list, nil
}

func (dq *DelayQ) getSetName(forWrite bool) string {
	var shardID int

	if forWrite {
		shardID = rand.Intn(dq.writeShards)
	} else {
		shardID = rand.Intn(dq.readShards)
	}

	// Refer https://redis.io/topics/cluster-spec for understanding reason for using '{}'
	// in the following keys.
	if shardID == 0 {
		return fmt.Sprintf("delayq:{%s}", dq.queueName)
	}
	return fmt.Sprintf("delayq:{%s-%d}", dq.queueName, shardID)
}
func (dq *DelayQ) log(level, format string, args ...interface{}) {
	if dq.Logger == nil {
		return
	}
	dq.Logger(level, format, args...)
}

var zfetchLua = redis.NewScript(`
local from_set = KEYS[1]
local max_priority, target_priority, limit = ARGV[1], ARGV[2], ARGV[3]

local items
if limit then
	items = redis.call('ZRANGE', from_set, '-inf', max_priority, 'BYSCORE', 'LIMIT', 0, limit)
else
	items = redis.call('ZRANGE', from_set, '-inf', max_priority, 'BYSCORE')
end

for i, value in ipairs(items) do
	redis.call('ZINCRBY', from_set, target_priority or 0.0, value)
end

return items
`)
