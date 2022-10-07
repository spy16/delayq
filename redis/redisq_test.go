package redis

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/spy16/delayq"
)

var client redis.UniversalClient

func TestDelayQ_Delay(t *testing.T) {
	frozenTime := time.Now().Add(1 * time.Hour)

	item := delayq.Item{
		At:    frozenTime,
		Value: "foo",
	}

	dq := New(t.Name(), client)
	err := dq.Enqueue(context.Background(), item)
	if err != nil {
		t.Fatalf("failed to enqueue item: %v", err)
	}

	items, err := client.ZRange(context.Background(), dq.getSetName(false), 0, 1000).Result()
	if err != nil {
		t.Fatalf("failed to zrange: %v", err)
	}

	if len(items) != 1 {
		t.Fatalf("expected 1 item in set, got %d", len(items))
	}

	var readItem delayq.Item
	if err := json.Unmarshal([]byte(items[0]), &readItem); err != nil {
		t.Fatalf("invalid item json (%v): %s", err, items[0])
	} else if reflect.DeepEqual(readItem, item) {
		t.Fatalf("expected item to be '%s', got '%s'", item.Value, items[0])
	}
}

func TestDelayQ_Dequeue(t *testing.T) {
	dq := New(t.Name(), client)

	now := time.Now()
	readyT := now.Add(-1 * time.Hour)
	nonReadyT := now.Add(1 * time.Hour)

	items := []delayq.Item{
		{
			At:    readyT,
			Value: "this-is-ready-since-in-past",
		},
		{
			At:    nonReadyT,
			Value: "this-is-not-ready-since-in-future",
		},
	}

	err := dq.Enqueue(context.Background(), items...)
	if err != nil {
		t.Errorf("failed to delay item: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	counter := int64(0)

	if err := dq.Dequeue(ctx, now, func(ctx context.Context, item delayq.Item) error {
		atomic.AddInt64(&counter, 1)
		return nil
	}); err != nil {
		t.Errorf("failed to run: %v", err)
	}

	if counter != 1 {
		t.Errorf("expected 1 item to have been processed, counter is %d", counter)
	}
}

func TestMain(m *testing.M) {
	redisHost := strings.TrimSpace(os.Getenv("DELAYQ_REDIS_HOST"))
	if redisHost == "" {
		redisHost = "localhost:6379"
	}

	if strings.Contains(redisHost, ",") {
		client = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: strings.Split(redisHost, ","),
		})
	} else {
		client = redis.NewClient(&redis.Options{
			Addr: redisHost,
		})
	}

	exitCode := m.Run()
	_, _ = client.FlushDB(context.Background()).Result()

	os.Exit(exitCode)
}
