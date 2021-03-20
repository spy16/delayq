# DelayQ

DelayQ is a `Go` library that provides a distributed, performant, reliable delay-queue mechanism to build job-queues,
schedulers etc.

## Features

* Simple - More like a queue primitive to build job-queue instead of being one.
* Distributed - Multiple producer/consumer processes connect to a Redis node or cluster.
* Reliable - `at-least-once` semantics (exactly-once in most cases except for when there are worker crashes)
* Good performance (See Benchmarks).

## Usage

For enqueueing items on the delay queue:

```go
package foo

func enqueue() error {
	dq := delayq.New("my_queue", redisClient)

	// arrange the message to be delivered to a worker
	// after 1 hour.
	return dq.Delay(context.Background(), delayq.Item{
		At:    time.Now().Add(1 * time.Hour),
		Value: "Hello!",
	})
}
```

Creating a worker to process:

```go
package foo

func worker() {
	dq := delayq.New("my_queue", redisClient)

	// run worker threads that invoke myFunc for every item.
	if err := dq.Run(context.Background(), myFunc); err != nil {
		log.Fatalf("run exited: %v", err)
	}
}

func myFunc(ctx context.Context, value string) error {
	log.Printf("got message: %v", value)
	return nil
}
```

## Benchmarks

Benchmarks can be re-produced using the example application in `./reminder`.

Following are few actual benchmark results that I  got:

> Both Redis and Reminder tool are running on single node with following specs:
> * CPU   : `2.8 GHz Quad-Core Intel Core i7`
> * Memory: `16 GB 2133 MHz LPDDR3`

1. With 3 worker processes (30 goroutines, prefetch count 100, poll interval 5ms):
    * 10k jobs are delivered on the exact requested second (~3300 req/second each.)
    * 100k jobs are delivered within 3 seconds. (upto 12000 req/second each.)
2. With 2 worker processes (20 goroutines, prefetch count 1000, poll interval 5ms):
    * 10k jobs are delivered on the exact requested second (~5000 req/second each.)
    * 100k jobs are delivered within 3 seconds. (upto 20000 req/second each.)
