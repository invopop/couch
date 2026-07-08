package changes

import (
	"context"
	"hash/maphash"
	"sync"
	"time"

	"github.com/jpillora/backoff"
	"github.com/rs/zerolog/log"
)

// Fetcher materialises a typed document from a doc ID emitted by a
// change feed. WorkerPool retries the call with backoff on error.
type Fetcher[T any] func(ctx context.Context, id string) (T, error)

// KeyFunc extracts a partition key from a fetched item. Used by
// hashed-mode pools to keep same-key items on the same worker.
type KeyFunc[T any] func(T) string

// WorkerPool dispatches change-feed items across a pool of workers. It
// pulls IDs from one or more Feeds, calls Fetcher to load the typed
// document, and routes the result to a worker channel. Workers consume
// via Next.
//
// In flat mode every worker reads from the same shared channel — any
// worker can pull any item. In hashed mode each worker has its own
// channel and items are routed by hash(KeyFunc(item)) % workers; items
// sharing a key always land on the same worker, preserving arrival
// order within the partition.
//
// WorkerPool owns the feed lifecycle: do not call Feed.Start before
// passing the feed in, and do not call Feed.Stop independently. Use
// WorkerPool.Start and WorkerPool.Stop instead.
type WorkerPool[T any] struct {
	feeds      map[string]Feed
	fetcher    Fetcher[T]
	workers    int
	channels   []chan T
	hashed     bool
	keyFn      KeyFunc[T]
	seed       maphash.Seed
	fatal      chan error
	fatalOnce  sync.Once
	procWG     sync.WaitGroup
	stopOnce   sync.Once
	closedOnce sync.Once
}

// NewFlatWorkerPool constructs a pool that distributes items across a
// single shared channel. Suitable for non-partitioned models (and for
// partitioned ones where downstream processing is order-tolerant).
//
// `workers` controls the pool size; the shared channel is buffered to
// `workers` slots so a single slow worker doesn't immediately stall
// the fetcher.
func NewFlatWorkerPool[T any](feeds map[string]Feed, fetcher Fetcher[T], workers int) *WorkerPool[T] {
	if workers < 1 {
		workers = 1
	}
	return &WorkerPool[T]{
		feeds:    feeds,
		fetcher:  fetcher,
		workers:  workers,
		channels: []chan T{make(chan T, workers)},
		fatal:    make(chan error, 1),
	}
}

// NewHashedWorkerPool constructs a pool with per-worker channels.
// Items are routed by hash(keyFn(item)) % workers so all items sharing
// a key always land on the same worker. Use for partitioned models
// when you need to preserve arrival order within a partition.
//
// Per-worker channels are buffered to `workers` slots each.
func NewHashedWorkerPool[T any](feeds map[string]Feed, fetcher Fetcher[T], workers int, keyFn KeyFunc[T]) *WorkerPool[T] {
	if workers < 1 {
		workers = 1
	}
	chs := make([]chan T, workers)
	for i := range chs {
		chs[i] = make(chan T, workers)
	}
	return &WorkerPool[T]{
		feeds:    feeds,
		fetcher:  fetcher,
		workers:  workers,
		channels: chs,
		hashed:   true,
		keyFn:    keyFn,
		seed:     maphash.MakeSeed(),
		fatal:    make(chan error, 1),
	}
}

// Start opens every feed and spawns a fetcher goroutine per shard.
// Each fetcher pulls IDs, calls Fetcher, and dispatches the resulting
// item to a worker channel. The call returns immediately; workers
// should be started by the caller and pull via Next.
func (p *WorkerPool[T]) Start(ctx context.Context) {
	for shard, f := range p.feeds {
		f.Start(ctx)
		p.procWG.Add(1)
		go p.runFetcher(ctx, shard, f)
		go p.watchFatal(shard, f)
	}
}

// Next returns the next item routed to the given worker, blocking
// until one is available. Returns the zero value and false once the
// pool has been stopped and the worker's channel is fully drained.
func (p *WorkerPool[T]) Next(workerID int) (T, bool) {
	v, ok := <-p.recv(workerID)
	return v, ok
}

// Workers reports the configured pool size.
func (p *WorkerPool[T]) Workers() int {
	return p.workers
}

// Fatal returns a channel that fires when one of the underlying feeds
// signals an unrecoverable condition (most notably a persistent
// conflict on the sequence document, indicating another process is
// consuming the same feed). The channel is closed when the pool stops
// without a fatal condition, so a closed-channel receive doubles as a
// shutdown signal.
func (p *WorkerPool[T]) Fatal() <-chan error {
	return p.fatal
}

// Stop halts every feed, waits for the fetcher goroutines to exit,
// and closes the worker channels. Safe to call multiple times.
func (p *WorkerPool[T]) Stop() {
	p.stopOnce.Do(func() {
		for _, f := range p.feeds {
			f.Stop()
		}
		p.procWG.Wait()
		p.closedOnce.Do(func() {
			for _, ch := range p.channels {
				close(ch)
			}
		})
		// Fatal is closed by Stop iff no fatal signal fired first.
		p.fatalOnce.Do(func() { close(p.fatal) })
	})
}

// dispatch routes a fetched item to the appropriate worker channel.
func (p *WorkerPool[T]) dispatch(item T) {
	if p.hashed {
		sum := maphash.String(p.seed, p.keyFn(item))
		idx := int(sum % uint64(p.workers)) //nolint:gosec // bounded by modulo workers
		p.channels[idx] <- item
		return
	}
	p.channels[0] <- item
}

// recv returns the channel a given worker should consume from. In flat
// mode every workerID maps to the same shared channel.
func (p *WorkerPool[T]) recv(workerID int) <-chan T {
	if p.hashed {
		return p.channels[workerID]
	}
	return p.channels[0]
}

// runFetcher pulls IDs from one shard's feed, fetches the typed doc
// for each, and dispatches it. Both feed reads and fetch errors are
// retried with backoff.
func (p *WorkerPool[T]) runFetcher(ctx context.Context, shard string, f Feed) {
	defer p.procWG.Done()
	bo := &backoff.Backoff{
		Min:    2 * time.Second,
		Max:    5 * time.Minute,
		Factor: 2,
	}
	for {
		id, err := f.Next(ctx)
		if err != nil {
			dur := bo.Duration()
			log.Error().Err(err).Str("shard", shard).Dur("wait", dur).Msg("change feed read error, will retry after wait")
			time.Sleep(dur)
			continue
		}
		if id == "" {
			log.Info().Str("shard", shard).Msg("change feed closed")
			return
		}

		var item T
		for {
			item, err = p.fetcher(ctx, id)
			if err == nil {
				break
			}
			dur := bo.Duration()
			log.Error().Err(err).Str("shard", shard).Str("id", id).Dur("wait", dur).Msg("fetch error, will retry after wait")
			time.Sleep(dur)
		}
		bo.Reset()
		p.dispatch(item)
	}
}

// watchFatal forwards a per-shard feed's first fatal error onto the
// pool's combined fatal channel.
func (p *WorkerPool[T]) watchFatal(shard string, f Feed) {
	err, ok := <-f.Fatal()
	if !ok {
		return
	}
	log.Error().Err(err).Str("shard", shard).Msg("change feed fatal")
	p.fatalOnce.Do(func() {
		select {
		case p.fatal <- err:
		default:
		}
		close(p.fatal)
	})
}
