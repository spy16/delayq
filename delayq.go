package delayq

import (
	"context"
	"time"
)

// DelayQ represents a queue that ensures delivery of items at specified
// points in time. Items are delivered to the function value registered
// during Run().
type DelayQ interface {
	Delay(ctx context.Context, items ...Item) error
	Run(ctx context.Context, fn Process) error
}

// Process function is invoked for every item that becomes ready. An item
// remains on the queue until this function returns no error.
type Process func(ctx context.Context, value []byte) error

// Item represents an item to be pushed to the queue.
type Item struct {
	At    time.Time `json:"at"`
	Value string    `json:"value"`
}
