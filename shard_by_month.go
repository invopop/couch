package couch

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-kivik/kivik/v4"
)

// MonthShards manages one database per calendar month for data that is only
// kept for a limited time: each month's database is created the first time a
// document is written to it, and whole months are removed with Drop once they
// are old enough. Dropping a database is far cheaper than deleting its
// documents, which would leave tombstones behind.
//
// Unlike Shards, the list of databases is not fixed when the service starts,
// so nothing needs to be restarted or migrated as the months go by.
//
// The month is taken from the model's ShardValue, which must be a time.Time
// or a time-based UUID (versions 1, 6 and 7) as a string. Months are in UTC.
type MonthShards struct {
	client  *Client
	name    string
	create  []kivik.Option
	designs []*Design

	mu    sync.Mutex
	ready map[string]*kivik.DB
}

// MonthShardsOption configures a MonthShards.
type MonthShardsOption func(*MonthShards)

// WithMonthCreateOptions sets the options used when creating each month's
// database, for example kivik.Param("partitioned", true).
func WithMonthCreateOptions(opts ...kivik.Option) MonthShardsOption {
	return func(s *MonthShards) {
		s.create = append(s.create, opts...)
	}
}

// WithMonthDesigns sets design documents to sync to each month's database
// when this instance first uses it.
func WithMonthDesigns(designs ...*Design) MonthShardsOption {
	return func(s *MonthShards) {
		s.designs = append(s.designs, designs...)
	}
}

// NewMonthShards prepares databases named "<prefix>_<name>_<YYYY>_<MM>".
func NewMonthShards(client *Client, name string, opts ...MonthShardsOption) *MonthShards {
	s := &MonthShards{
		client: client,
		name:   name,
		ready:  make(map[string]*kivik.DB),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Key provides the month a shard value belongs to, as "YYYY_MM".
func (s *MonthShards) Key(v any) (string, error) {
	t, err := shardTime(v)
	if err != nil {
		return "", err
	}
	return monthKey(t), nil
}

// For provides the database for the model's month, creating it if this
// instance has not used it yet. Use it for writes.
func (s *MonthShards) For(ctx context.Context, m Shardable) (*kivik.DB, error) {
	key, err := s.Key(m.ShardValue())
	if err != nil {
		return nil, fmt.Errorf("invalid shard key: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if db, ok := s.ready[key]; ok {
		return db, nil
	}
	db := s.db(key)
	if err := s.client.Create(ctx, db, s.create...); err != nil {
		// Another instance may have created it at the same moment.
		if kivik.HTTPStatus(err) != http.StatusPreconditionFailed {
			return nil, fmt.Errorf("creating %s: %w", db.Name(), err)
		}
	}
	if err := s.syncDesigns(ctx, db); err != nil {
		return nil, fmt.Errorf("syncing designs to %s: %w", db.Name(), err)
	}
	s.ready[key] = db
	return db, nil
}

// syncDesigns stores the design documents in a month's database. Another
// instance using the month for the first time may store the same design at
// the same moment, in which case ours conflicts: syncing again then finds
// theirs in place and has nothing to do.
func (s *MonthShards) syncDesigns(ctx context.Context, db *kivik.DB) error {
	err := s.client.SyncDesigns(ctx, db, s.designs)
	if errors.Is(err, ErrAlreadyExists) || kivik.HTTPStatus(err) == http.StatusConflict {
		err = s.client.SyncDesigns(ctx, db, s.designs)
	}
	return err
}

// Lookup provides the database for the model's month without creating it.
// Use it for reads: if the month was never written to, or has been dropped,
// requests fail with a not found error, and the dropped database is not
// brought back empty.
func (s *MonthShards) Lookup(m Shardable) (*kivik.DB, error) {
	key, err := s.Key(m.ShardValue())
	if err != nil {
		return nil, fmt.Errorf("invalid shard key: %w", err)
	}
	return s.db(key), nil
}

// Names provides the names of the month databases that exist, oldest first.
func (s *MonthShards) Names(ctx context.Context) ([]string, error) {
	all, err := s.client.client.AllDBs(ctx)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, name := range all {
		if _, ok := s.month(name); ok {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names, nil
}

// Drop destroys every month database whose month ended at or before the given
// time, and provides the names of those it removed. To keep documents for at
// least 30 days, call it with the time 30 days ago: a month goes once its
// last day is that old.
//
// Other instances are not told: one that has already used a dropped month
// will find it missing on its next write. Only drop months that nothing
// writes to any more.
func (s *MonthShards) Drop(ctx context.Context, before time.Time) ([]string, error) {
	names, err := s.Names(ctx)
	if err != nil {
		return nil, err
	}
	var dropped []string
	for _, name := range names {
		start, _ := s.month(name)
		if start.AddDate(0, 1, 0).After(before) {
			continue
		}
		if err := s.destroy(ctx, name, monthKey(start)); err != nil {
			return dropped, fmt.Errorf("dropping %s: %w", name, err)
		}
		dropped = append(dropped, name)
	}
	return dropped, nil
}

// destroy removes a month's database and forgets it, holding the lock so that
// a For on this instance cannot recreate the month in between.
func (s *MonthShards) destroy(ctx context.Context, name, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.client.client.DestroyDB(ctx, name)
	if err != nil && kivik.HTTPStatus(err) != http.StatusNotFound {
		return err
	}
	delete(s.ready, key)
	return nil
}

func (s *MonthShards) db(key string) *kivik.DB {
	return s.client.DB(s.name + s.client.conf.separator() + key)
}

// month provides the start of the month a full database name stands for, if
// it is one of ours.
func (s *MonthShards) month(dbName string) (time.Time, bool) {
	prefix := s.client.conf.db(s.name) + s.client.conf.separator()
	key, ok := strings.CutPrefix(dbName, prefix)
	if !ok {
		return time.Time{}, false
	}
	t, err := time.Parse("2006_01", key)
	if err != nil || monthKey(t) != key {
		return time.Time{}, false
	}
	return t, true
}

func monthKey(t time.Time) string {
	return t.UTC().Format("2006_01")
}

// shardTime provides the time held by a shard value: a time.Time, or a
// time-based UUID as a string.
func shardTime(v any) (time.Time, error) {
	switch v := v.(type) {
	case time.Time:
		if v.IsZero() {
			return time.Time{}, errors.New("empty shard value")
		}
		return v, nil
	case string:
		b, ok := parseUUID(v)
		if !ok || b == ([16]byte{}) {
			return time.Time{}, errors.New("shard value is not a uuid")
		}
		t, ok := uuidTime(b)
		if !ok {
			return time.Time{}, errors.New("invalid shard uuid version")
		}
		return t, nil
	default:
		return time.Time{}, errors.New("unexpected shard value")
	}
}
