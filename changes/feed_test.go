package changes

import (
	"errors"
	"testing"
	"time"
)

// newBareFeed produces a feed instance for unit tests of in-memory state.
// It does NOT call New (which sets log fields off the kivik DB), so it is
// only safe for tests that don't touch the DB or run the connect loop.
func newBareFeed(opts ...Option) *feed {
	f := &feed{
		Seq:         "0",
		saveTimeout: make(chan bool, 1),
		opts:        newOptions(),
		fatal:       make(chan error, 1),
	}
	for _, opt := range opts {
		opt(f.opts)
	}
	return f
}

func TestWithFatalOnConflict_DefaultOff(t *testing.T) {
	o := newOptions()
	if o.fatalOnConflict {
		t.Fatalf("fatalOnConflict should default to false")
	}
}

func TestWithFatalOnConflict_Sets(t *testing.T) {
	o := newOptions()
	WithFatalOnConflict()(o)
	if !o.fatalOnConflict {
		t.Fatalf("WithFatalOnConflict should enable fatalOnConflict")
	}
}

func TestWithIncludeDocs_DefaultOff(t *testing.T) {
	o := newOptions()
	if o.includeDocs {
		t.Fatalf("includeDocs should default to false")
	}
}

func TestWithIncludeDocs_Sets(t *testing.T) {
	o := newOptions()
	WithIncludeDocs()(o)
	if !o.includeDocs {
		t.Fatalf("WithIncludeDocs should enable includeDocs")
	}
}

func TestWithStoreTimeout_Default(t *testing.T) {
	o := newOptions()
	if o.storeTimeout != DefaultStoreTimeout {
		t.Fatalf("storeTimeout default = %v, want %v", o.storeTimeout, DefaultStoreTimeout)
	}
}

func TestWithStoreTimeout_Sets(t *testing.T) {
	o := newOptions()
	WithStoreTimeout(5 * time.Minute)(o)
	if o.storeTimeout != 5*time.Minute {
		t.Fatalf("WithStoreTimeout did not apply: got %v", o.storeTimeout)
	}
}

func TestWithStoreTimeout_IgnoresNonPositive(t *testing.T) {
	o := newOptions()
	WithStoreTimeout(0)(o)
	WithStoreTimeout(-1 * time.Second)(o)
	if o.storeTimeout != DefaultStoreTimeout {
		t.Fatalf("WithStoreTimeout should ignore non-positive values; got %v", o.storeTimeout)
	}
}

func TestWithStoreLimit_Default(t *testing.T) {
	o := newOptions()
	if o.storeLimit != DefaultStoreLimit {
		t.Fatalf("storeLimit default = %d, want %d", o.storeLimit, DefaultStoreLimit)
	}
}

func TestWithStoreLimit_Sets(t *testing.T) {
	o := newOptions()
	WithStoreLimit(500)(o)
	if o.storeLimit != 500 {
		t.Fatalf("WithStoreLimit did not apply: got %d", o.storeLimit)
	}
}

func TestWithStoreLimit_IgnoresNonPositive(t *testing.T) {
	o := newOptions()
	WithStoreLimit(0)(o)
	WithStoreLimit(-7)(o)
	if o.storeLimit != DefaultStoreLimit {
		t.Fatalf("WithStoreLimit should ignore non-positive values; got %d", o.storeLimit)
	}
}

func TestSetSeq_RecordsPairedID(t *testing.T) {
	f := newBareFeed()
	f.setSeq("doc-A", "100")
	if f.Seq != "100" || f.seqID != "doc-A" {
		t.Fatalf("setSeq did not record paired id/seq: got seqID=%q Seq=%q", f.seqID, f.Seq)
	}

	// A subsequent setSeq with an empty id (defensive, shouldn't happen in
	// practice) must not clobber the last known id.
	f.setSeq("", "101")
	if f.Seq != "101" {
		t.Fatalf("Seq should advance even with empty id: got %q", f.Seq)
	}
	if f.seqID != "doc-A" {
		t.Fatalf("empty id must not clobber seqID: got %q", f.seqID)
	}

	f.setSeq("doc-B", "102")
	if f.seqID != "doc-B" {
		t.Fatalf("seqID should track the latest non-empty id: got %q", f.seqID)
	}
}

func TestShouldSave_HonoursStoreLimit(t *testing.T) {
	// The count > limit branch only matters while the delay timer holds
	// (delay != nil). Install a dummy long-running timer so the timer
	// branch doesn't short-circuit shouldSave.
	armTimer := func(f *feed) {
		f.delay = time.AfterFunc(time.Hour, func() {})
	}

	f := newBareFeed(WithStoreLimit(3))
	armTimer(f)
	defer f.delay.Stop()
	f.count = 4
	if !f.shouldSave() {
		t.Fatalf("shouldSave should be true once count exceeds configured storeLimit")
	}

	f = newBareFeed(WithStoreLimit(10))
	armTimer(f)
	defer f.delay.Stop()
	f.count = 4
	if f.shouldSave() {
		t.Fatalf("shouldSave should be false while count is below storeLimit and delay timer holds")
	}
}

func TestSignalFatal_DeliversAndCloses(t *testing.T) {
	f := newBareFeed()
	want := errors.New("boom")
	f.signalFatal(want)

	select {
	case err, ok := <-f.Fatal():
		if !ok {
			t.Fatalf("Fatal channel closed without delivering the error")
		}
		if !errors.Is(err, want) {
			t.Fatalf("got err=%v, want=%v", err, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for fatal signal")
	}

	// Subsequent receive must observe channel closure.
	select {
	case _, ok := <-f.Fatal():
		if ok {
			t.Fatalf("expected Fatal channel to be closed after delivery")
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for fatal channel close")
	}
}

func TestSignalFatal_OnlyFiresOnce(t *testing.T) {
	f := newBareFeed()
	f.signalFatal(errors.New("first"))
	// Second call must not panic (it would if it tried to send/close again).
	f.signalFatal(errors.New("second"))
	// Drain the buffered first error.
	if err := <-f.Fatal(); err == nil || err.Error() != "first" {
		t.Fatalf("expected to receive the first error, got %v", err)
	}
}

func TestStop_ClosesFatalEvenWithoutFatal(t *testing.T) {
	f := newBareFeed()
	// Mark started so Stop runs the full path and exercises stopOnce; we
	// don't care about the source/outgoing teardown since they're nil
	// only when never started — set started=false to take the shortcut
	// branch which still closes the fatal channel.
	f.Stop()

	select {
	case _, ok := <-f.Fatal():
		if ok {
			t.Fatalf("Fatal should be closed (no signal), but got an error")
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for Fatal channel to close on Stop")
	}

	// Stop is idempotent.
	f.Stop()

	if !f.stopped {
		t.Fatalf("expected feed.stopped to be true after Stop")
	}
}

func TestLeadingSeq(t *testing.T) {
	cases := []struct {
		name string
		seq  string
		want int64
		ok   bool
	}{
		{"clustered", "12345-g1AAAABXeJ", 12345, true},
		{"bare integer", "678", 678, true},
		{"zero", "0", 0, true},
		{"initial now", "now", 0, false},
		{"empty", "", 0, false},
		{"non-numeric prefix", "abc-def", 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := leadingSeq(tc.seq)
			if ok != tc.ok {
				t.Fatalf("leadingSeq(%q) ok = %v, want %v", tc.seq, ok, tc.ok)
			}
			if ok && got != tc.want {
				t.Fatalf("leadingSeq(%q) = %d, want %d", tc.seq, got, tc.want)
			}
		})
	}
}
