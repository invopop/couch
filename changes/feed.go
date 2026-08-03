// Package changes makes it easier to listen to CouchDB change feeds.
package changes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kivik/kivik/v4"
	"github.com/invopop/couch"
	"github.com/jpillora/backoff"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

const (
	// conflictFatalThreshold is the number of consecutive 409 conflicts on
	// the per-feed sequence document that will trigger a fatal signal when
	// WithFatalOnConflict is enabled. Three is high enough to absorb a
	// transient race during a rolling restart but low enough to surface a
	// genuine duplicate-consumer situation quickly.
	conflictFatalThreshold = 3
)

// Feed controls the lifecycle of a connection to a couchdb changes
// feed and ensures that synchronisation can continue by maintaining
// a reference to the latest sequence ID.
//
// To use, instantiate with New and run the Start method for a
// connection to be established in the background.
//
// Call the Next method to get each of the changed document IDs
// from the source feed. Next acts as an ack method, and is designed
// so that every subsequent call to Next can potentially save the
// current state of the feed.
//
// Feeds are persisted to the changes database either when the configured
// store limit is reached, or when the configured store timeout is met.
// Defaults are DefaultStoreLimit and DefaultStoreTimeout; both are
// overridable per-feed via WithStoreLimit and WithStoreTimeout.
type Feed interface {
	// GetID provides the underlying ID of the feed.
	GetID() string

	// GetRev provides the revision ID of the underlying document if available.
	GetRev() string

	// Start makes a request to the changes feed to start receiving updates.
	Start(ctx context.Context)

	// Seed overwrites the feed's persisted sequence position so a subsequent
	// Start resumes from seq instead of any previously stored or initial
	// position. It is intended for administrative recovery — for example
	// seeding the source's current update_seq to skip a large backlog. It must
	// be called before Start, with no other consumer writing the same feed
	// document.
	Seed(ctx context.Context, seq string) error

	// Next grabs the next change from the feed, describing it rather than just
	// naming it so a consumer can tell a deletion from an update. If there is
	// an error, it'll be provided. If the feed is stopped, the Change will be
	// zero (an empty ID) and there will not be an error.
	//
	// Deletions are reported like any other change, with Deleted set. A
	// consumer that fetches each ID must handle that case: the document is a
	// tombstone by then and fetching it will fail.
	Next(ctx context.Context) (Change, error)

	// NextDoc returns the next changed document's ID and body, skipping
	// deletions — there is no body to return for a tombstone. Acknowledgement
	// semantics are identical to Next. The body is nil when the feed was not
	// created WithIncludeDocs, or when it could not be read for this change (in
	// which case the consumer should fetch the document itself).
	//
	// Deprecated: use Next, which reports deletions instead of hiding them.
	// Silently dropping them is how downstream copies of deleted documents end
	// up living forever.
	NextDoc(ctx context.Context) (id string, doc json.RawMessage, err error)

	// Stop requests that we stop listening for new changes. The current call to
	// Next should then return a zero Change.
	Stop()

	// Fatal returns a channel that fires when the feed has detected an
	// unrecoverable condition such as a persistent conflict on the
	// sequence document (only when WithFatalOnConflict is set). When
	// fatal is signalled the feed self-stops, so a follow-up Next will
	// return a zero Change as for a normal close. The channel is closed
	// when the feed is stopped.
	Fatal() <-chan error
}

// FeedCallback defines a callback to be used as an alternative to using
// the channel. This allows more messages to be processed at the same time
// and will wait for all currently executing callbacks to be processed before
// storing the current sequence. Any errors return from the callback will cause
// the Feed to be closed, so should only be used for major issues.
//
// Deletions are delivered like any other change, so the callback has to check
// Change.Deleted before treating the ID as a document it can load.
type FeedCallback func(c Change) error

// Change describes a single entry from the feed.
type Change struct {
	// ID of the document that changed.
	ID string

	// Deleted is true when the change is a deletion: the document is now a
	// tombstone, so there is nothing left to fetch and consumers mirroring the
	// data should remove their copy.
	Deleted bool

	// Doc holds the document body when the feed was created with
	// WithIncludeDocs, and is nil otherwise or when the body could not be
	// read. For a deletion CouchDB sends the tombstone, which carries the id,
	// the rev and `_deleted`, and none of the document's own fields.
	Doc json.RawMessage
}

type feedItem struct {
	id      string
	seq     string
	deleted bool
	doc     json.RawMessage // populated only when include_docs is enabled
}

type feed struct {
	couch.Model
	sync.Mutex
	Seq string `json:"seq"`

	db          *kivik.DB
	changesDB   *kivik.DB
	source      *kivik.Changes
	outgoing    chan feedItem
	saveTimeout chan bool
	started     bool
	lastSeq     string
	lastID      string // doc ID paired with lastSeq (pending ack, Next mode only)
	seqID       string // doc ID paired with Seq, surfaced in the "persisted feed" log

	opts *options

	delay *time.Timer
	count int // number of sequence updates since last save

	consecConflicts int        // 409s in a row on the seq doc; reset on success
	fatal           chan error // signals unrecoverable conditions
	fatalOnce       sync.Once  // guards send/close on fatal
	stopOnce        sync.Once  // makes Stop idempotent
	stopped         bool       // true once Stop has run; checked in Next

	log zerolog.Logger
}

// New instantiates a new Feed object ready to be started.
func New(changesDB, srcDB *kivik.DB, opts ...Option) Feed {
	f := &feed{
		Seq:         "0",
		db:          srcDB,
		changesDB:   changesDB,
		saveTimeout: make(chan bool, 1),
		count:       0,
		started:     false,
		opts:        newOptions(),
		fatal:       make(chan error, 1),
	}

	for _, opt := range opts {
		opt(f.opts)
	}
	if f.opts.initialSeq != "" {
		f.Seq = f.opts.initialSeq
	}
	f.prepareID()
	f.log = log.With().Str("id", f.GetID()).Logger()
	return f
}

func (f *feed) prepareID() {
	id := f.db.Name()
	for _, s := range f.opts.suffix {
		if s != "" {
			id = id + "_" + s
		}
	}
	f.SetID(id)
}

// Seed overwrites the feed's persisted sequence position with seq (see the
// Feed interface). It loads any existing document first so the write updates
// it in place, then stores the new sequence.
func (f *feed) Seed(ctx context.Context, seq string) error {
	f.Lock()
	defer f.Unlock()
	if f.started {
		return errors.New("cannot seed a started feed")
	}
	// Load the current document (if any) so we update in place and keep its
	// revision; a missing document is fine and will be created by store.
	if err := f.fetch(ctx); err != nil {
		return err
	}
	f.Seq = seq
	// store only writes when shouldSave is satisfied during normal operation;
	// here we persist directly since this is an explicit, one-off seed.
	return f.store(ctx)
}

// Start establishes connection to fetch current feed state and
// establish change feed. If a callback is provided, this method
// will block until the connection is stopped, or an error occurs.
func (f *feed) Start(ctx context.Context) {
	if f.started {
		return
	}
	f.outgoing = make(chan feedItem)
	f.started = true
	go f.connect(ctx)

	if f.opts.callback != nil {
		// this will block
		f.startWithCallbacks()
	}
}

// Stop stops the connection as gracefully as possible.
func (f *feed) Stop() {
	f.stopOnce.Do(func() {
		f.stopped = true
		// Always close fatal so any waiting consumer is released, even
		// if Start was never called or no fatal condition occurred.
		f.fatalOnce.Do(func() { close(f.fatal) })
		if !f.started {
			return
		}
		f.stopSaveTimer()
		if f.source != nil {
			// Kivik won't allow feed closure while a next call is
			// blocking.
			go func() {
				_ = f.source.Close()
				f.source = nil
			}()
		}
		close(f.outgoing)
		f.started = false
		f.log.Info().Msg("stopped")
	})
}

// Fatal returns the channel that fires (and is then closed) when the
// feed encounters an unrecoverable condition. See the Feed interface
// for details. The channel is also closed on a normal Stop, so
// consumers can use a closed receive to detect shutdown.
func (f *feed) Fatal() <-chan error {
	return f.fatal
}

// signalFatal publishes err on the fatal channel and triggers a
// self-stop so consumers waiting on Next see end-of-feed. fatalOnce
// guards the send and the close together so we never send on a closed
// channel even if Stop and signalFatal race.
func (f *feed) signalFatal(err error) {
	sent := false
	f.fatalOnce.Do(func() {
		// Buffered(1), this will not block.
		f.fatal <- err
		close(f.fatal)
		sent = true
	})
	if sent {
		// Stop in a separate goroutine — we may be holding f.Mutex via
		// Next, which Stop's path could compete with.
		go f.Stop()
	}
}

// Next provides the next change, deletions included. This acts as an Ack as
// the current sequence state will not be saved until next is called again.
// Any errors that happen while trying to save the feed state or read from the
// source will be returned here.
func (f *feed) Next(ctx context.Context) (Change, error) {
	item, err := f.nextItem(ctx)
	return Change{ID: item.id, Deleted: item.deleted, Doc: item.doc}, err
}

// NextDoc returns the next changed document's ID and body, skipping deletions
// (see the Feed interface).
//
// Deprecated: use Next.
func (f *feed) NextDoc(ctx context.Context) (string, json.RawMessage, error) {
	for {
		item, err := f.nextItem(ctx)
		if err != nil || item.id == "" {
			return item.id, item.doc, err
		}
		if item.deleted {
			// A tombstone has no body to hand back. Acking it and moving on
			// keeps the old contract — at the cost of the consumer never
			// learning the document is gone, which is why this is deprecated.
			continue
		}
		return item.id, item.doc, nil
	}
}

// nextItem is the shared implementation behind Next and NextDoc. It acts
// as an Ack: the current sequence state is not saved until the following
// call. Any errors saving the feed state or reading from the source are
// returned here. A zero feedItem with a nil error signals end-of-feed.
func (f *feed) nextItem(ctx context.Context) (feedItem, error) {
	if f.opts.callback != nil {
		return feedItem{}, errors.New("callback mode enabled, do not use Next")
	}
	f.Lock() // Only support requesting one next at a time
	defer f.Unlock()

	// If Stop has already run (including via a fatal self-stop), report
	// a clean end-of-feed so iterators don't spin in a retry loop.
	if f.stopped {
		return feedItem{}, nil
	}
	if !f.started {
		return feedItem{}, errors.New("not started")
	}

	if f.lastSeq != "" {
		f.setSeq(f.lastID, f.lastSeq)
		f.lastSeq = ""
		f.lastID = ""
	}

	for {
		// Check if we need to save
		if err := f.save(ctx); err != nil {
			return feedItem{}, err
		}
		select {
		case <-ctx.Done():
			return feedItem{}, ctx.Err()
		case item, more := <-f.outgoing:
			if !more {
				return feedItem{}, nil // the end
			}
			f.lastSeq = item.seq
			f.lastID = item.id
			return item, nil
		case <-f.saveTimeout:
			// Timer fired: clear it so shouldSave() triggers on the next
			// loop, then loop around to save.
			f.stopSaveTimer()
			continue
		}
	}
}

// startWithCallbacks is the alternative approach to using the Next iterator.
func (f *feed) startWithCallbacks() {
	closed := false
	for !closed {
		g := new(errgroup.Group)
	maxInFlightLoop:
		for i := 0; i < f.opts.maxInFlight; i++ {
			select {
			case item, more := <-f.outgoing:
				if !more {
					closed = true
					break maxInFlightLoop
				}
				f.setSeq(item.id, item.seq)
				i := item
				g.Go(func() error {
					return f.opts.callback(Change{ID: i.id, Deleted: i.deleted, Doc: i.doc})
				})
			case <-f.saveTimeout:
				// Timer fired: clear it so the save below runs.
				f.stopSaveTimer()
				break maxInFlightLoop
			}
		}
		if err := g.Wait(); err != nil {
			f.log.Error().Err(err).Msg("closing due to errors")
			return
		}
		ctx := context.Background() // independent context
		if err := f.save(ctx); err != nil {
			f.log.Error().Err(err).Msg("failed to save sequence position, ignoring")
		}
	}
}

// setSeq records that the provided seq (and the doc ID it belongs to) is
// the latest acknowledged position. It increments the internal counter
// and arms the save timer. The id is retained only for observability —
// it surfaces in the "persisted feed" log when the seq doc is written.
func (f *feed) setSeq(id, seq string) {
	f.Seq = seq
	if id != "" {
		f.seqID = id
	}
	f.count++
	f.startSaveTimer()
}

func (f *feed) startSaveTimer() {
	if f.delay != nil {
		return
	}
	// The callback only nudges the consumer; it must not touch f.delay (that
	// would race with the consumer goroutine). The consumer clears the timer
	// when it observes the notification. The send is non-blocking against a
	// buffered channel, so a fired timer never blocks or leaks its goroutine
	// even if no consumer is currently selecting.
	f.delay = time.AfterFunc(f.opts.storeTimeout, func() {
		select {
		case f.saveTimeout <- true:
		default:
		}
	})
}

func (f *feed) stopSaveTimer() {
	if f.delay != nil {
		f.delay.Stop()
		f.delay = nil
	}
}

// shouldSave returns true if the delay timer is nil or
// if the count is over the configured store limit.
// When the count is zero, then obviously don't want to save.
func (f *feed) shouldSave() bool {
	return f.count != 0 && (f.delay == nil || f.count > f.opts.storeLimit)
}

func (f *feed) save(ctx context.Context) error {
	if !f.shouldSave() {
		return nil
	}
	f.stopSaveTimer()

	err := f.store(ctx)
	if err == nil {
		f.consecConflicts = 0
		ev := f.log.Info().Str("seq", f.Seq).Str("last_id", f.seqID)
		if n, ok := f.pending(ctx); ok {
			ev = ev.Int64("pending", n)
		}
		ev.Msg("persisted feed")
		f.count = 0
		return nil
	}
	if kivik.HTTPStatus(err) != http.StatusConflict {
		f.log.Error().Err(err).Msg("storing feed")
		return err
	}

	// 409: another writer beat us to it. Our local _rev is stale and
	// every subsequent PUT will keep losing. Refresh _rev (and seq) so
	// that the next save cycle has a chance, and surface the issue if
	// it persists past conflictFatalThreshold consecutive conflicts.
	f.consecConflicts++
	f.log.Warn().
		Err(err).
		Int("consecutive", f.consecConflicts).
		Msg("conflict on feed save, another instance is writing the same feed")

	if f.opts.fatalOnConflict && f.consecConflicts >= conflictFatalThreshold {
		f.log.Error().
			Int("consecutive", f.consecConflicts).
			Msg("persistent feed conflict — signalling fatal")
		f.signalFatal(fmt.Errorf("persistent change-feed conflict on %s after %d attempts: %w",
			f.GetID(), f.consecConflicts, err))
		return err
	}

	if ferr := f.fetch(ctx); ferr != nil {
		f.log.Error().Err(ferr).Msg("refetching feed doc after conflict")
		return ferr
	}
	// Refetch synchronised us with whatever the conflicting writer
	// committed. Reset the unsaved-count and re-arm the save timer so
	// the next batch will save on its own cadence rather than spinning
	// here.
	f.count = 0
	f.startSaveTimer()
	return nil
}

// pending estimates how many source changes remain unprocessed by comparing
// the source database's current update_seq high-water mark against the feed's
// last stored position. It is called only when the sequence is persisted, so
// it adds at most one cheap stats request per save cycle.
//
// CouchDB sequence strings have the form "<n>-<hash>", where the leading
// integer is a monotonic per-database change counter; their difference is the
// estimate. It is exact on a single node and approximate on a clustered
// database (the encoded tail packs per-shard sequences). ok is false when the
// stats request fails or either sequence has no parseable prefix (for example
// the initial "now"), in which case the caller should omit the estimate.
func (f *feed) pending(ctx context.Context) (n int64, ok bool) {
	stats, err := f.db.Stats(ctx)
	if err != nil {
		f.log.Debug().Err(err).Msg("db stats unavailable for pending estimate")
		return 0, false
	}
	head, ok1 := leadingSeq(stats.UpdateSeq)
	cur, ok2 := leadingSeq(f.Seq)
	if !ok1 || !ok2 {
		return 0, false
	}
	if n = head - cur; n < 0 {
		n = 0
	}
	return n, true
}

// leadingSeq parses the integer prefix of a CouchDB sequence string, i.e. the
// "<n>" in "<n>-<hash>" (or a bare "<n>"). It reports ok=false when the prefix
// is not an integer.
func leadingSeq(seq string) (int64, bool) {
	if i := strings.IndexByte(seq, '-'); i >= 0 {
		seq = seq[:i]
	}
	n, err := strconv.ParseInt(seq, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// connect to source. This will enter a loop that attempts
// to maintain the connection active.
func (f *feed) connect(ctx context.Context) {
	bo := &backoff.Backoff{
		Min: 2 * time.Second,
		Max: 2 * time.Minute,
	}

	for { // connection loop
		err := f.fetchAndConnect(ctx)
		if err == nil {
			err = f.processNext()
			if err == nil {
				return // implies channel closed
			}
		}
		d := bo.Duration()
		f.log.Error().Dur("retry_in", d).Err(err).Msg("reconnecting...")
		time.Sleep(d)
	}
}

func (f *feed) processNext() error {
	for { // result loop
		if !f.source.Next() {
			return f.source.Err()
		}

		docID := f.source.ID()
		if docID == "" || strings.HasPrefix(docID, "_") {
			continue // ignore things we can't deal with
		}
		item := feedItem{id: docID, seq: f.source.Seq(), deleted: f.source.Deleted()}
		if f.opts.includeDocs {
			var doc json.RawMessage
			if err := f.source.ScanDoc(&doc); err != nil {
				// Don't drop the change: deliver it without a body so the
				// consumer falls back to fetching the document itself.
				f.log.Warn().Err(err).Str("id", docID).Msg("scanning change doc, consumer will fetch")
			} else {
				item.doc = doc
			}
		}
		f.outgoing <- item
	}
}

func (f *feed) fetchAndConnect(ctx context.Context) error {
	// attempt to clean up
	if f.source != nil {
		_ = f.source.Close()
		f.source = nil
	}

	var err error
	if err = f.fetch(ctx); err != nil {
		return err
	}
	params := map[string]any{
		"include_docs": f.opts.includeDocs,
		"heartbeat":    f.opts.heartbeat,
		"since":        f.Seq,
		"feed":         "continuous",
	}
	if f.opts.seqInterval > 1 {
		params["seq_interval"] = f.opts.seqInterval
	}
	if f.opts.filter != "" {
		params["filter"] = f.opts.filter
		for k, v := range f.opts.query {
			params[k] = v
		}
	}
	f.source = f.db.Changes(ctx, kivik.Params(params))
	f.log.Info().Str("seq", f.Seq).Msg("connected to changes feed")
	return nil
}

func (f *feed) fetch(ctx context.Context) error {
	if err := couch.FetchModel(ctx, f.changesDB, f); err != nil {
		if errors.Is(err, couch.ErrNotFound) {
			return nil
		}
		return fmt.Errorf("failed to fetch change feed document: %w", err)
	}
	return nil
}

func (f *feed) store(ctx context.Context) error {
	if err := couch.StoreModel(ctx, f.changesDB, f); err != nil {
		return err
	}
	return nil
}
