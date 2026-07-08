package changes

import (
	"fmt"
	"time"
)

// Now is the CouchDB-supported initial sequence value meaning "start from
// the current update_seq". Pass it to WithInitialSeq to skip historical
// changes and only receive updates from the moment the feed connects.
const Now = "now"

const (
	// DefaultStoreTimeout is the default maximum delay between sequence
	// document writes. Overridable per-feed via WithStoreTimeout.
	DefaultStoreTimeout = 1 * time.Minute

	// DefaultStoreLimit is the default number of acked sequence updates
	// the feed will buffer before forcing a save. Overridable per-feed
	// via WithStoreLimit.
	DefaultStoreLimit = 100
)

type options struct {
	suffix          []string
	query           map[string]string
	filter          string
	heartbeat       int // default 15000
	seqInterval     int
	callback        FeedCallback
	maxInFlight     int
	fatalOnConflict bool
	initialSeq      string
	storeTimeout    time.Duration
	storeLimit      int
	includeDocs     bool
}

// newOptions instantiates a new options object with defaults.
func newOptions() *options {
	return &options{
		suffix:       []string{},
		filter:       "",
		heartbeat:    15000,
		seqInterval:  1,
		maxInFlight:  10,
		storeTimeout: DefaultStoreTimeout,
		storeLimit:   DefaultStoreLimit,
	}
}

// Option defines a callback to be issued for each configuration option.
type Option func(opts *options)

// WithCallback defines a method to use for callbacks as an alternative to the
// next iterator.
func WithCallback(cb FeedCallback) Option {
	return func(opts *options) {
		opts.callback = cb
	}
}

// WithMaxInflight defines how many callbacks should be allowed to be processing
// at the same time. This is only relevant when using callbacks. Must be bigger
// than zero.
func WithMaxInFlight(size int) Option {
	return func(opts *options) {
		if size > 0 {
			opts.maxInFlight = size
		}
	}
}

// WithSeqInterval utilizes the CouchDB `seq_interval` option to define how many changes
// should be loaded inside a batch. By default, this is 1, which is suitable for low-volume
// processes. This should be higher for greater volumes.
func WithSeqInterval(val int) Option {
	return func(opts *options) {
		opts.seqInterval = val
	}
}

// WithSuffix appends the provided strings to the change feed name
func WithSuffix(s ...string) Option {
	return func(opts *options) {
		opts.suffix = s
	}
}

// WithFilter defines a CouchDB filter to use before processing the incoming
// changes from the feed.
func WithFilter(design, name string, query map[string]string) Option {
	return func(opts *options) {
		opts.filter = fmt.Sprintf("%s/%s", design, name)
		opts.query = query
	}
}

// WithHeartbeat sets the feed heartbeat interval. If none set, the default
// is 15000 (15s).
func WithHeartbeat(v int) Option {
	return func(opts *options) {
		opts.heartbeat = v
	}
}

// WithInitialSeq overrides the initial sequence used the very first time
// a feed is started (when no persisted change_feeds document exists yet).
// Defaults to "0", meaning "replay from the start". Pass "now" to start
// from the current point in the changes feed.
//
// Once the feed has persisted a sequence document, that stored value
// takes precedence on subsequent restarts and this option is a no-op.
func WithInitialSeq(seq string) Option {
	return func(opts *options) {
		opts.initialSeq = seq
	}
}

// WithFatalOnConflict enables fatal-error signalling via the Feed's Fatal
// channel when the per-feed sequence document keeps conflicting after a
// few save attempts (a strong signal that another process is consuming
// the same feed). The feed already attempts to recover from a single
// conflict transparently by refetching the document's revision; this
// option only controls whether persistent conflicts surface as fatal.
func WithFatalOnConflict() Option {
	return func(opts *options) {
		opts.fatalOnConflict = true
	}
}

// WithStoreTimeout overrides the maximum delay between sequence document
// writes. After this duration has elapsed since the last save, the feed
// forces a save on the next Next/callback cycle. A non-positive value is
// ignored and the default (DefaultStoreTimeout) is kept. Raising this
// reduces write load on the change_feeds database at the cost of more
// replay work on crash recovery.
func WithStoreTimeout(d time.Duration) Option {
	return func(opts *options) {
		if d > 0 {
			opts.storeTimeout = d
		}
	}
}

// WithStoreLimit overrides the number of acked sequence updates the feed
// will buffer before forcing a save, irrespective of the timeout. A
// non-positive value is ignored and the default (DefaultStoreLimit) is
// kept. Raising this batches more updates per save at the cost of more
// replay work on crash recovery.
func WithStoreLimit(n int) Option {
	return func(opts *options) {
		if n > 0 {
			opts.storeLimit = n
		}
	}
}

// WithIncludeDocs requests that the changes feed include each changed
// document's body (CouchDB's include_docs=true). The body is delivered
// via NextDoc as a json.RawMessage, letting consumers unmarshal it
// directly instead of issuing a separate fetch per change. Bodies are
// not delivered through Next or the callback runner. As with any
// include_docs feed, the body reflects the document's current winning
// revision, which may be newer than the revision that produced the
// change.
func WithIncludeDocs() Option {
	return func(opts *options) {
		opts.includeDocs = true
	}
}
