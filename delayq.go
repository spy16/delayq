package delayq

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var ErrNoItem = errors.New("no ready items")

// DelayQ is a queue with a time constraint on each item. Items in
// the queue should be ready for dequeue only at/after the At timestamp
// set for each item.
type DelayQ interface {
	// Enqueue should enqueue all items atomically into the queue storage.
	// If any item fails, all items should be dequeued.
	Enqueue(ctx context.Context, items ...Item) error

	// Dequeue should dequeue available items relative to the given time
	// and invoke 'fn' for each item safely. Dequeue must ensure that in
	// case of failures or context cancellation, the items being dequeued
	// are released back to the queue storage.
	Dequeue(ctx context.Context, relativeTo time.Time, fn Process) error
}

// Process function is invoked for every item that becomes ready. An item
// remains on the queue until this function returns without error.
type Process func(ctx context.Context, item Item) error

// Item represents an item to be pushed to the queue.
type Item struct {
	At    time.Time `json:"at"`
	Value string    `json:"value"`
}

func (itm *Item) JSON() string {
	b, err := json.Marshal(itm)
	if err != nil {
		// this should never happen since Item is meant for JSON.
		panic(err)
	}
	return string(b)
}

func (itm *Item) MarshalJSON() ([]byte, error) {
	return json.Marshal(itemJSONModel{
		At:    itm.At.Unix(),
		Value: itm.Value,
	})
}

func (itm *Item) UnmarshalJSON(bytes []byte) error {
	var model itemJSONModel
	if err := json.Unmarshal(bytes, &model); err != nil {
		return err
	}
	itm.At = time.Unix(model.At, 0).UTC()
	itm.Value = model.Value
	return nil
}

type itemJSONModel struct {
	At    int64  `json:"at"`
	Value string `json:"value"`
}
