package changes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeFeed is a Feed stub for testing WorkerPool in isolation. It
// returns IDs from a fixed slice, then blocks until Stop is called.
type fakeFeed struct {
	id        string
	ids       []string
	cur       int
	stopped   atomic.Bool
	stopCh    chan struct{}
	fatal     chan error
	fatalOnce sync.Once
	startCnt  atomic.Int32
}

func newFakeFeed(id string, ids ...string) *fakeFeed {
	return &fakeFeed{
		id:     id,
		ids:    ids,
		stopCh: make(chan struct{}),
		fatal:  make(chan error, 1),
	}
}

func (f *fakeFeed) GetID() string                          { return f.id }
func (f *fakeFeed) GetRev() string                         { return "" }
func (f *fakeFeed) Seed(_ context.Context, _ string) error { return nil }
func (f *fakeFeed) Start(_ context.Context) {
	f.startCnt.Add(1)
}
func (f *fakeFeed) Next(ctx context.Context) (string, error) {
	if f.stopped.Load() {
		return "", nil
	}
	if f.cur < len(f.ids) {
		id := f.ids[f.cur]
		f.cur++
		return id, nil
	}
	select {
	case <-f.stopCh:
		return "", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}
func (f *fakeFeed) NextDoc(ctx context.Context) (string, json.RawMessage, error) {
	id, err := f.Next(ctx)
	return id, nil, err
}
func (f *fakeFeed) Stop() {
	if f.stopped.CompareAndSwap(false, true) {
		close(f.stopCh)
		f.fatalOnce.Do(func() { close(f.fatal) })
	}
}
func (f *fakeFeed) Fatal() <-chan error { return f.fatal }

// signalFatal injects a fatal condition. Mirrors the real feed's
// fatal/close semantics: send then close, idempotent.
func (f *fakeFeed) signalFatal(err error) {
	f.fatalOnce.Do(func() {
		f.fatal <- err
		close(f.fatal)
	})
}

func waitUntil(t *testing.T, deadline time.Duration, cond func() bool, msg string) {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", msg)
}

func TestFlatWorkerPool_AllItemsDelivered(t *testing.T) {
	feed := newFakeFeed("test", "a", "b", "c", "d")
	feeds := map[string]Feed{"test": feed}
	pool := NewFlatWorkerPool(feeds, func(_ context.Context, id string) (string, error) {
		return id, nil
	}, 2)
	pool.Start(context.Background())
	defer pool.Stop()

	var got []string
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := range pool.Workers() {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				v, ok := pool.Next(id)
				if !ok {
					return
				}
				mu.Lock()
				got = append(got, v)
				mu.Unlock()
			}
		}(i)
	}

	waitUntil(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) == 4
	}, "all 4 items delivered")

	pool.Stop()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 4 {
		t.Fatalf("expected 4 items, got %d: %v", len(got), got)
	}
}

func TestHashedWorkerPool_SameKeyAlwaysSameWorker(t *testing.T) {
	// Many copies of the same key — all must arrive on a single worker.
	ids := []string{}
	for i := range 50 {
		ids = append(ids, fmt.Sprintf("entry-a-rev-%d", i))
	}
	feed := newFakeFeed("test", ids...)
	feeds := map[string]Feed{"test": feed}
	pool := NewHashedWorkerPool(feeds, func(_ context.Context, id string) (string, error) {
		return id, nil
	}, 4, func(s string) string { return "entry-a" })
	pool.Start(context.Background())
	defer pool.Stop()

	hits := make([]int, pool.Workers())
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := range pool.Workers() {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				_, ok := pool.Next(id)
				if !ok {
					return
				}
				mu.Lock()
				hits[id]++
				mu.Unlock()
			}
		}(i)
	}

	waitUntil(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		sum := 0
		for _, h := range hits {
			sum += h
		}
		return sum == 50
	}, "all 50 items processed")

	pool.Stop()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	used := 0
	for _, h := range hits {
		if h > 0 {
			used++
		}
	}
	if used != 1 {
		t.Fatalf("expected all items on one worker; hits=%v", hits)
	}
}

func TestWorkerPool_FetcherErrorIsRetried(t *testing.T) {
	feed := newFakeFeed("test", "doc-1")
	feeds := map[string]Feed{"test": feed}
	var attempts atomic.Int32
	pool := NewFlatWorkerPool(feeds, func(_ context.Context, id string) (string, error) {
		if attempts.Add(1) < 3 {
			return "", errors.New("transient")
		}
		return id, nil
	}, 1)
	// Tighten the backoff for the test so the retry happens quickly.
	pool.Start(context.Background())
	defer pool.Stop()

	v, ok := pool.Next(0)
	if !ok {
		t.Fatal("expected item, got closed channel")
	}
	if v != "doc-1" {
		t.Fatalf("got %q, want doc-1", v)
	}
	if got := attempts.Load(); got < 3 {
		t.Fatalf("expected at least 3 fetch attempts, got %d", got)
	}
}

func TestWorkerPool_FatalFromFeedSurfaces(t *testing.T) {
	feed := newFakeFeed("test")
	feeds := map[string]Feed{"test": feed}
	pool := NewFlatWorkerPool(feeds, func(_ context.Context, id string) (string, error) {
		return id, nil
	}, 1)
	pool.Start(context.Background())
	defer pool.Stop()

	want := errors.New("boom")
	feed.signalFatal(want)

	select {
	case err, ok := <-pool.Fatal():
		if !ok {
			t.Fatal("Fatal channel closed without delivering the error")
		}
		if !errors.Is(err, want) {
			t.Fatalf("got err=%v, want=%v", err, want)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for fatal signal")
	}
}

func TestWorkerPool_StopClosesWorkerChannelsCleanly(t *testing.T) {
	feed := newFakeFeed("test", "a")
	feeds := map[string]Feed{"test": feed}
	pool := NewFlatWorkerPool(feeds, func(_ context.Context, id string) (string, error) {
		return id, nil
	}, 2)
	pool.Start(context.Background())

	// Drain the first item from any worker.
	_, ok := pool.Next(0)
	if !ok {
		t.Fatal("expected the buffered item")
	}

	pool.Stop()

	// After Stop, Next on any worker must observe a closed channel.
	for i := range pool.Workers() {
		if _, ok := pool.Next(i); ok {
			t.Fatalf("worker %d: expected closed channel after Stop", i)
		}
	}

	// Stop is idempotent.
	pool.Stop()
}

func TestWorkerPool_StopWithoutFatalClosesFatalChannel(t *testing.T) {
	feed := newFakeFeed("test")
	feeds := map[string]Feed{"test": feed}
	pool := NewFlatWorkerPool(feeds, func(_ context.Context, id string) (string, error) {
		return id, nil
	}, 1)
	pool.Start(context.Background())
	pool.Stop()

	select {
	case _, ok := <-pool.Fatal():
		if ok {
			t.Fatal("expected Fatal to close on Stop, but got an error")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Fatal close")
	}
}

func TestWorkerPool_MultipleFeedsAllRun(t *testing.T) {
	feedA := newFakeFeed("a", "doc-a-1", "doc-a-2")
	feedB := newFakeFeed("b", "doc-b-1", "doc-b-2", "doc-b-3")
	feeds := map[string]Feed{"a": feedA, "b": feedB}
	pool := NewFlatWorkerPool(feeds, func(_ context.Context, id string) (string, error) {
		return id, nil
	}, 2)
	pool.Start(context.Background())
	defer pool.Stop()

	got := map[string]bool{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := range pool.Workers() {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				v, ok := pool.Next(id)
				if !ok {
					return
				}
				mu.Lock()
				got[v] = true
				mu.Unlock()
			}
		}(i)
	}

	waitUntil(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) == 5
	}, "all 5 items across both feeds")

	if feedA.startCnt.Load() != 1 || feedB.startCnt.Load() != 1 {
		t.Fatalf("expected each feed Started once; a=%d b=%d", feedA.startCnt.Load(), feedB.startCnt.Load())
	}

	pool.Stop()
	wg.Wait()
}

func TestWorkerPool_NonPositiveWorkersNormalised(t *testing.T) {
	feeds := map[string]Feed{"test": newFakeFeed("test")}
	fetcher := func(_ context.Context, id string) (string, error) { return id, nil }

	if p := NewFlatWorkerPool(feeds, fetcher, 0); p.Workers() != 1 {
		t.Fatalf("flat: workers normalised to 1, got %d", p.Workers())
	}
	if p := NewFlatWorkerPool(feeds, fetcher, -3); p.Workers() != 1 {
		t.Fatalf("flat: negative workers normalised to 1, got %d", p.Workers())
	}
	if p := NewHashedWorkerPool(feeds, fetcher, 0, func(s string) string { return s }); p.Workers() != 1 {
		t.Fatalf("hashed: workers normalised to 1, got %d", p.Workers())
	}
}
