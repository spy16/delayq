# DelayQ

[![GoDoc](https://godoc.org/github.com/spy16/delayq?status.svg)](https://godoc.org/github.com/spy16/delayq) [![Go Report Card](https://goreportcard.com/badge/github.com/spy16/delayq)](https://goreportcard.com/report/github.com/spy16/delayq) ![Go](https://github.com/spy16/delayq/actions/workflows/code_change.yml/badge.svg)

DelayQ is a Go library that provides job-queue (time-constrained) primitives.

Currently following implementations are supported:

1. `inmem.DelayQ`: An in-memory queue using modified min-heap for time-constrained dequeue.
2. `redis.DelayQ`: A production-tested queue using Redis (supports both single-node & cluster) that uses sorted-sets.
3. `sql.DelayQ`: An implementation backed by any SQL database.

## Features

* Simple - More like a queue primitive to build job-queue instead of being a feature-rich job-queue system.
* Distributed (with `redis` or `sqldb`) - Multiple producer/consumer processes connect to a Redis node or cluster.
* Reliable - `at-least-once` semantics (exactly-once in most cases except for when there are worker crashes)
* Good performance (See Benchmarks).

## Usage

For enqueueing items on the delay queue:

```go
package main

func main() {
   err := dq.Enqueue(context.Background(), []delayq.Item{
      {
         At:    time.Now().Add(1 * time.Hour),
         Value: "Item 1!",
      },
      {
         At:    time.Now().Add(2 * time.Hour),
         Value: "Item 2!",
      },
   }...)
}
```

Creating a worker to process:

```go
package main

import "github.com/spy16/delayq"

func worker(dq delayq.DelayQ) {
   w := &delayq.Worker{
      Queue:  dq,
      Invoke: onReady,
   }

   // run worker threads that invoke myFunc for every item.
   if err := w.Run(context.Background()); err != nil {
      log.Fatalf("run exited: %v", err)
   }
}

func onReady(ctx context.Context, item delayq.Item) error {
   log.Printf("got message: %v", item)
   return nil
}
```

## Benchmarks

Benchmarks can be re-produced using the example application in `./example`.

Following are few actual benchmark results that I got:

> Both Redis and worker are running on single node with following specs:
> * CPU   : `2.8 GHz Quad-Core Intel Core i7`
> * Memory: `16 GB 2133 MHz LPDDR3`

1. With 3 worker processes (30 goroutines, prefetch count 100, poll interval 5ms):
    * 10k jobs are delivered on the exact requested second (~3300 req/second each.)
    * 100k jobs are delivered within 3 seconds. (upto 12000 req/second each.)
2. With 2 worker processes (20 goroutines, prefetch count 1000, poll interval 5ms):
    * 10k jobs are delivered on the exact requested second (~5000 req/second each.)
    * 100k jobs are delivered within 3 seconds. (upto 20000 req/second each.)
