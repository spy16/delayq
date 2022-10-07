package delayq

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
)

// Worker is a wrapper around DelayQ that provides functionality
// to run polling worker threads that continuously dequeue items and
// process.
type Worker struct {
	Queue DelayQ

	// Invoke is the function to be invoked for each ready item.
	Invoke Process

	// Workers represents the number of threads to launch for polling the queue.
	Workers int

	// PollInterval decides the interval between each Dequeue() invocation.
	PollInterval time.Duration
}

func (w *Worker) Run(ctx context.Context) error {
	if w.Queue == nil {
		return errors.New("queue is not set, nothing to do")
	}

	if w.Workers <= 0 {
		w.Workers = 1
	}

	if w.PollInterval <= 0 {
		w.PollInterval = 500 * time.Millisecond
	}

	wg := &sync.WaitGroup{}
	for i := 0; i < w.Workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			if err := w.work(ctx, id); err != nil {
				log.Printf("[ERROR] worker-%d exited with error: %v", id, err)
			}
		}(i)
	}
	wg.Wait()

	log.Printf("[INFO] all worker-threads exited")
	return nil
}

func (w *Worker) work(ctx context.Context, id int) error {
	tick := time.NewTimer(w.PollInterval)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case t := <-tick.C:
			tick.Reset(w.PollInterval)

			if err := w.Queue.Dequeue(ctx, t, w.Invoke); err != nil {
				log.Printf("[ERROR] [worker-%d] dequeue failed: %v", id, err)
				continue
			}
		}
	}
}
