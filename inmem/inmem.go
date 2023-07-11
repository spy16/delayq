package inmem

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/spy16/delayq"
)

var _ delayq.DelayQ = (*DelayQ)(nil)

// DelayQ implements DelayQ using an in-memory heap.
type DelayQ struct {
	Tick    time.Duration
	Retry   bool
	Backoff time.Duration

	qu requestQ
}

func (mem *DelayQ) Enqueue(_ context.Context, items ...delayq.Item) error {
	mem.qu.Enqueue(items...)
	return nil
}

func (mem *DelayQ) Dequeue(ctx context.Context, relativeTo time.Time, fn delayq.Process) error {
	readyReq := mem.qu.Dequeue(relativeTo)
	if readyReq == nil {
		return delayq.ErrNoItem
	}

	fnErr := fn(ctx, *readyReq)
	if fnErr != nil {
		log.Printf("apply failed: %v", fnErr)
		if mem.Retry {
			readyReq.At = time.Now().Add(mem.Backoff)
			mem.qu.Enqueue(*readyReq)
		}
		return fnErr
	}
	return nil
}

func (mem *DelayQ) Delete(ctx context.Context, items ...delayq.Item) error {
	mem.qu.Delete(items...)
	return nil
}

func (mem *DelayQ) Run(ctx context.Context, fn delayq.Process) error {
	ticker := time.NewTicker(mem.Tick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case t := <-ticker.C:
			if err := mem.Dequeue(ctx, t, fn); err != nil {
				if !errors.Is(err, delayq.ErrNoItem) {
					log.Printf("dequeue failed: %v", err)
				}
				continue
			}
		}
	}
}

type requestQ struct {
	mu    sync.Mutex
	items []delayq.Item
}

// Enqueue adds the given item to the queue.
func (q *requestQ) Enqueue(reqs ...delayq.Item) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, req := range reqs {
		q.items = append(q.items, req)
		q.heapifyUp(q.Size() - 1)
	}
}

// Dequeue pops the next item from the queue and returns it. If the
// queue is empty, nil is returned.
func (q *requestQ) Dequeue(t time.Time) *delayq.Item {
	if q.Size() == 0 {
		return nil
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	item := q.items[0]
	if item.At.After(t) {
		return nil
	}

	q.swap(0, q.Size()-1)          // swap with last element
	q.items = q.items[:q.Size()-1] // remove last element
	q.heapifyDown(0)               // bubble down

	return &item
}

func (q *requestQ) Delete(reqs ...delayq.Item) {
	if q.Size() == 0 {
		return
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	for _, req := range reqs {
		for i, item := range q.items {
			if item.At == req.At && item.Value == req.Value {
				q.swap(i, q.Size()-1)          // swap with last element
				q.items = q.items[:q.Size()-1] // remove last element
				q.heapifyDown(i)               // bubble down
			}
		}
	}
}

// Size returns the number of items in the queue.
func (q *requestQ) Size() int { return len(q.items) }

func (q *requestQ) heapifyUp(index int) {
	parentAt := parent(index)
	if index > 0 {
		child := q.items[index]
		parent := q.items[parent(index)]
		if child.At.Before(parent.At) {
			q.swap(index, parentAt)
		}

		q.heapifyUp(parentAt)
	}
}

func (q *requestQ) heapifyDown(index int) {
	rightChildAt := rightChild(index)
	leftChildAt := leftChild(index)

	if index < q.Size() && leftChildAt < q.Size() && rightChildAt < q.Size() {
		parent := q.items[index]
		if parent.At.After(q.items[rightChildAt].At) {
			q.swap(rightChildAt, index)
			q.heapifyDown(rightChildAt)
		} else if parent.At.After(q.items[leftChildAt].At) {
			q.swap(leftChildAt, index)
			q.heapifyDown(leftChildAt)
		}
	}
}

func (q *requestQ) swap(i, j int) {
	tmp := q.items[i]
	q.items[i] = q.items[j]
	q.items[j] = tmp
}

func parent(index int) int {
	if index == 0 {
		return 0
	}

	return (index - 1) / 2
}

func leftChild(index int) int  { return 2*index + 1 }
func rightChild(index int) int { return 2*index + 2 }
