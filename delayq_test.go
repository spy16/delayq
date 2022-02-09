package delayq

import (
	"context"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

var client redis.UniversalClient

func TestDelayQ_Delay(t *testing.T) {
	frozenTime := time.Now().Add(1 * time.Hour)

	item := Item{
		At:    frozenTime,
		Value: "foo",
	}

	dq := New(t.Name(), client)
	err := dq.Delay(context.Background(), item)
	if err != nil {
		t.Fatalf("failed to delay item: %v", err)
	}

	items, err := client.ZRange(context.Background(), dq.getSetName(false), 0, 1000).Result()
	if err != nil {
		t.Fatalf("failed to zrange: %v", err)
	}

	if len(items) != 1 {
		t.Fatalf("expected 1 item in set, got %d", len(items))
	}

	if items[0] != item.Value {
		t.Fatalf("expected item to be '%s', got '%s'", item.Value, items[0])
	}
}

func TestDelayQ_Run(t *testing.T) {
	dq := New(t.Name(), client)

	deliverAt := time.Now().Add(-1 * time.Hour)
	items := []Item{
		{
			At:    deliverAt,
			Value: "foo",
		},
		{
			At:    deliverAt,
			Value: "bar",
		},
	}

	err := dq.Delay(context.Background(), items...)
	if err != nil {
		t.Errorf("failed to delay item: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	counter := int64(0)
	go func() {
		err := dq.Run(ctx, func(ctx context.Context, value []byte) error {
			atomic.AddInt64(&counter, 1)
			return nil
		})
		if err != nil {
			t.Errorf("failed to run: %v", err)
		}
	}()

	<-ctx.Done()
	if counter != 2 {
		t.Errorf("expected 2 items to have been processed, counter is %d", counter)
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

	os.Exit(m.Run())
}
