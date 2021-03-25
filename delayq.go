package delayq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

// New returns a new delay-queue instance with given queue name. If a Redis cluster
// is being used, delay/unack sets of this queue are ensured to go into same slot.
func New(queueName string, client redis.UniversalClient, opts ...Options) *DelayQ {
	var opt Options
	if len(opts) > 0 {
		opt = opts[0]
	}
	opt.setDefaults()

	return &DelayQ{
		client:     client,
		workers:    opt.Workers,
		pollInt:    opt.PollInterval,
		prefetch:   opt.PreFetchCount,
		delaySet:   fmt.Sprintf("delay_set:{%s}", queueName),
		unAckSet:   fmt.Sprintf("unack_set:{%s}", queueName),
		reclaimTTL: opt.ReclaimTTL,
	}
}

// Process function is invoked for every item that becomes ready. An item
// remains on the queue until this function returns no error.
type Process func(ctx context.Context, value []byte) error

// DelayQ represents a distributed, reliable delay-queue backed by Redis.
type DelayQ struct {
	client     redis.UniversalClient
	pollInt    time.Duration
	workers    int
	prefetch   int
	delaySet   string
	unAckSet   string
	reclaimTTL time.Duration
}

// Item represents an item to be pushed to the queue.
type Item struct {
	At    time.Time `json:"at"`
	Value string    `json:"value"`
}

// Delay pushes an item onto the delay-queue with the given delay. Value must
// be unique. Duplicate values will be ignored.
func (dq *DelayQ) Delay(ctx context.Context, items ...Item) error {
	_, err := dq.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, item := range items {
			pipe.ZAdd(ctx, dq.delaySet, &redis.Z{
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
func (dq *DelayQ) Run(ctx context.Context, fn Process) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg := &sync.WaitGroup{}
	for i := 0; i < dq.workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if err := dq.reaper(ctx, fn); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("[WARN] reaper-%d died: %v", id, err)
			} else {
				log.Printf("[INFO] reaper-%d exited gracefully", id)
			}
		}(i)
	}

	if err := dq.reclaimer(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("[ERR] reclaimer exited due to error: %v", err)
	} else {
		log.Printf("[INFO] reclaimer exited gracefully")
	}
	wg.Wait() // wait for workers to exit.
	log.Printf("[INFO] all workers returned, delayq shutting down")
	return nil
}

// reclaimer moves all items that have expired from the un-ack set to the delay
// set with 0 delay to requeue for immediate execution.
func (dq *DelayQ) reclaimer(ctx context.Context) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf("panic: %v", v)
		}
	}()

	recoveryTimer := time.NewTimer(0)
	defer recoveryTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case now := <-recoveryTimer.C:
			recoveryTimer.Reset(dq.reclaimTTL)

			log.Printf("[INFO] running reclaimer @ %d", now.Unix())
			if items, err := dq.zmove(ctx, dq.unAckSet, dq.delaySet, 0, 1000); err != nil {
				log.Printf("[ERR] failed to reclaim (run=%d): %v", now.Unix(), err)
			} else {
				log.Printf("[INFO] reclaimed %d items (run=%d)", len(items), now.Unix())
			}
		}
	}
}

func (dq *DelayQ) reaper(ctx context.Context, fn Process) (err error) {
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

			if err := dq.reap(ctx, fn); err != nil {
				log.Printf("[WARN] reap error: %v", err)
				continue
			}
		}
	}
}

func (dq *DelayQ) reap(ctx context.Context, fn Process) error {
	// add 30 seconds as a safety measure to prevent reclaiming too soon.
	reclaimTTL := (dq.reclaimTTL + (30 * time.Second)).Seconds()

	list, err := dq.zmove(ctx, dq.delaySet, dq.unAckSet, reclaimTTL, dq.prefetch)
	if err != nil {
		return err
	}

	// entire batch of items is subjected to the same TTL. So we create
	// a single context with timeout.
	ctx, cancel := context.WithTimeout(ctx, dq.reclaimTTL)
	defer cancel()

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
				log.Printf("[WARN] fn failed to process item, will requeue: %v", err)
			}

			// ack/nAck the item. ignoring the error is okay since if the ack failed,
			// the item remains in the un-ack set and will be claimed by the reclaimer.
			_ = dq.ack(ctx, item, failure)
		}
	}

	return nil
}

func (dq *DelayQ) ack(ctx context.Context, item string, negative bool) error {
	if negative {
		// add the item to the delay set with 0 score to queue it for
		// immediate execution. we don't need to zrem in this case
		// since de-duplication will be done when reaper moves item
		// from delay to unAck set.
		_, err := dq.client.ZAdd(ctx, dq.delaySet, &redis.Z{
			Score:  0,
			Member: item,
		}).Result()
		return err
	}

	_, err := dq.client.ZRem(ctx, dq.unAckSet, item).Result()
	return err
}

func (dq *DelayQ) zmove(ctx context.Context, fromSet, toSet string, scoreDelta float64, limit int) ([]interface{}, error) {
	now := time.Now().Unix()
	newScore := float64(now) + scoreDelta

	// atomically move ready items from delay-set to unack-set and return.
	// once moved it is guaranteed that no other worker will pick up the
	// same items.
	keys := []string{fromSet, toSet}
	args := []interface{}{now, newScore, limit}
	items, err := zmoveLua.Run(ctx, dq.client, keys, args...).Result()
	if err != nil {
		log.Printf("[ERR] lua script execution failed: %v", err)
		return nil, err
	}

	list, ok := items.([]interface{})
	if !ok {
		panic(fmt.Errorf("expecting list, got %s", reflect.TypeOf(list)))
	}

	log.Printf("[INFO] moved %d items", len(list))
	return list, nil
}

// zmove moves items having scores in the given range from the source
// set to target set with a new fixed score for all items.
var zmoveLua = redis.NewScript(`
local source_set, target_set  = KEYS[1], KEYS[2]
local max_priority, score, limit = ARGV[1], ARGV[2], ARGV[3]

local items
if limit then
	items = redis.call('ZRANGEBYSCORE', source_set, '-inf', max_priority, 'LIMIT', 0, limit)
else
	items = redis.call('ZRANGEBYSCORE', source_set, '-inf', max_priority)
end

for i, value in ipairs(items) do
	redis.call('ZADD', target_set, score or 0.0, value)
	redis.call('ZREM', source_set, value)
end

return items
`)
