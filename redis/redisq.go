package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/spy16/delayq"
)

const reclaimTTLBuffer = 5 * time.Second

// New returns a new delay-queue instance with given queue name.
func New(queueName string, client redis.UniversalClient, opts ...Options) *DelayQ {
	var opt Options
	if len(opts) > 0 {
		opt = opts[0]
	}
	opt.setDefaults()

	return &DelayQ{
		client:      client,
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

type DelayQ struct {
	Logger      loggerFunc
	client      redis.UniversalClient
	prefetch    int
	queueName   string
	reclaimTTL  time.Duration
	readShards  int
	writeShards int
}

type loggerFunc func(level, format string, args ...interface{})

func (dq *DelayQ) Enqueue(ctx context.Context, items ...delayq.Item) error {
	members := make([]*redis.Z, len(items), len(items))
	for i, item := range items {
		members[i] = &redis.Z{
			Score:  float64(item.At.Unix()),
			Member: item.JSON(),
		}
	}

	delaySet := dq.getSetName(true)
	_, err := dq.client.ZAdd(ctx, delaySet, members...).Result()
	return err
}

func (dq *DelayQ) Dequeue(ctx context.Context, relativeTo time.Time, fn delayq.Process) error {
	if err := dq.reap(ctx, relativeTo, fn); err != nil {
		return err
	}
	return nil
}

func (dq *DelayQ) Delete(ctx context.Context, items ...delayq.Item) error {
	members := make([]interface{}, len(items), len(items))
	for i, item := range items {
		members[i] = item.JSON()
	}

	_, err := dq.client.ZRem(ctx, dq.getSetName(true), members...).Result()
	return err
}

func (dq *DelayQ) reap(ctx context.Context, relativeTo time.Time, fn delayq.Process) error {
	delaySet := dq.getSetName(false)

	// add some buffer as a safety measure to prevent reclaiming too soon.
	reclaimTTL := (dq.reclaimTTL + reclaimTTLBuffer).Seconds()

	list, err := dq.zFetch(ctx, relativeTo, delaySet, reclaimTTL, dq.prefetch)
	if err != nil {
		return err
	}

	// entire batch of items is subjected to the same TTL. So we create
	// a single context with timeout. Note that the context timeout does
	// not include the reclaim-ttl buffer.
	ctx, cancel := context.WithTimeout(ctx, dq.reclaimTTL)
	defer cancel()

	if err := dq.processAll(ctx, delaySet, fn, list); err != nil {
		return err
	}

	return nil
}

func (dq *DelayQ) processAll(ctx context.Context, delaySet string, fn delayq.Process, items []interface{}) error {
	for _, v := range items {
		select {
		case <-ctx.Done():
			return ctx.Err()

		default:
			var item delayq.Item
			itemJSON, isStr := v.(string)
			if !isStr {
				return fmt.Errorf("expecting string, got %s", reflect.TypeOf(v))
			} else if err := json.Unmarshal([]byte(itemJSON), &item); err != nil {
				return fmt.Errorf("invalid item json: %w", err)
			}

			var fnFailure bool
			fnErr := fn(ctx, item)
			if fnErr != nil {
				fnFailure = true
				dq.log("error", "fn() failed: %v", fnErr)
			}

			// ack/nAck the item. ignoring the error is okay since if the ack failed,
			// the item remains in the un-ack set and will be claimed by the reclaimer.
			if err := dq.ack(ctx, delaySet, itemJSON, fnFailure); err != nil {
				dq.log("error", "ack() failed: %v", err)
			}
		}
	}

	return nil
}

func (dq *DelayQ) ack(ctx context.Context, queueName, itemJSON string, negative bool) error {
	if negative {
		member := &redis.Z{
			Score:  0,
			Member: itemJSON,
		}

		// add the item to the delay set with 0 score to queue it for
		// immediate execution (Redis will de-duplicate and update the
		// score of the already existing member in this case).
		_, err := dq.client.ZAdd(ctx, queueName, member).Result()
		return err
	}

	_, err := dq.client.ZRem(ctx, queueName, itemJSON).Result()
	return err
}

func (dq *DelayQ) zFetch(ctx context.Context, t time.Time, fromSet string, scoreDelta float64, batchSz int) ([]interface{}, error) {
	now := t.Unix()
	newScore := float64(now) + scoreDelta

	// atomically fetch & increment score for ready items from the queue.
	// Any item that has score in range [-inf, now] is considered ready.
	// The score of ready-items is set to the newScore to move them out
	// of the above range so that other workers do not pick the same item
	// for some time.
	keys := []string{fromSet}
	args := []interface{}{now, newScore, batchSz}
	items, err := zFetchLua.Run(ctx, dq.client, keys, args...).Result()
	if err != nil {
		dq.log("error", "lua script execution failed: %v", err)
		return nil, err
	}

	list, ok := items.([]interface{})
	if !ok {
		return nil, fmt.Errorf("expecting list, got %s", reflect.TypeOf(list))
	}

	dq.log("debug", "moved %d items", len(list))
	return list, nil
}

// getSetName returns the sharded set-name for reading-from or writing-to.
func (dq *DelayQ) getSetName(forEnqueue bool) string {
	var shardID int

	if forEnqueue {
		shardID = rand.Intn(dq.writeShards)
	} else {
		shardID = rand.Intn(dq.readShards)
	}

	// Refer https://redis.io/topics/cluster-spec for understanding reason
	// for using '{}' in the following keys.
	if shardID == 0 {
		// This ensures the polling is backward compatible with versions of
		// DelayQ that did not have sharding support.
		return fmt.Sprintf("delay_set:{%s}", dq.queueName)
	} else {
		return fmt.Sprintf("delay_set:{%s-%d}", dq.queueName, shardID)
	}
}

func (dq *DelayQ) log(level, format string, args ...interface{}) {
	if dq.Logger == nil {
		return
	}
	dq.Logger(level, format, args...)
}

var zFetchLua = redis.NewScript(`
local from_set = KEYS[1]
local max_priority, target_priority, limit = ARGV[1], ARGV[2], ARGV[3]

local items
if limit then
	items = redis.call('ZRANGE', from_set, '-inf', max_priority, 'BYSCORE', 'LIMIT', 0, limit)
else
	items = redis.call('ZRANGE', from_set, '-inf', max_priority, 'BYSCORE')
end

for i, value in ipairs(items) do
	redis.call('ZADD', from_set, 'XX', target_priority or 0.0, value)
end

return items
`)
