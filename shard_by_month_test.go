package couch_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kivik/kivik/v4"
	"github.com/invopop/couch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeServer keeps a set of database names and answers the requests needed
// to check for, create, list and destroy them.
type fakeServer struct {
	mu      sync.Mutex
	dbs     map[string]url.Values // name → query it was created with
	created int
}

func newFakeServer(t *testing.T, names ...string) (*fakeServer, *couch.Client) {
	t.Helper()
	fs := &fakeServer{dbs: make(map[string]url.Values)}
	for _, n := range names {
		fs.dbs[n] = nil
	}
	srv := httptest.NewServer(fs)
	t.Cleanup(srv.Close)

	u, err := url.Parse(srv.URL)
	require.NoError(t, err)
	conf := couch.NewConfig("test")
	conf.Host = u.Hostname()
	conf.Port = u.Port()
	client, err := couch.New(conf)
	require.NoError(t, err)
	return fs, client
}

func (fs *fakeServer) names() []string {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	out := make([]string, 0, len(fs.dbs))
	for n := range fs.dbs {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func (fs *fakeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	name := strings.Trim(r.URL.Path, "/")
	_, exists := fs.dbs[name]
	status, body := http.StatusOK, any(map[string]bool{"ok": true})
	switch {
	case name == "_all_dbs":
		list := make([]string, 0, len(fs.dbs))
		for n := range fs.dbs {
			list = append(list, n)
		}
		body = list
	case strings.Contains(name, "/"):
		status, body = http.StatusNotFound, map[string]string{"error": "not_found"}
	case r.Method == http.MethodPut && exists:
		status, body = http.StatusPreconditionFailed, map[string]string{"error": "file_exists"}
	case r.Method == http.MethodPut:
		fs.dbs[name] = r.URL.Query()
		fs.created++
		status = http.StatusCreated
	case !exists:
		status, body = http.StatusNotFound, map[string]string{"error": "not_found"}
	case r.Method == http.MethodDelete:
		delete(fs.dbs, name)
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body) //nolint:errcheck
}

type timedDoc struct {
	at time.Time
}

func (d *timedDoc) ShardValue() any { return d.at }

func TestMonthShardsKey(t *testing.T) {
	s := couch.NewMonthShards(nil, "messages")

	key, err := s.Key(uuidV7Year2022)
	require.NoError(t, err)
	assert.Equal(t, "2022_02", key)

	key, err = s.Key(uuidV1Year2022)
	require.NoError(t, err)
	assert.Equal(t, "2022_02", key)

	// Months are in UTC whatever the zone of the value.
	east := time.FixedZone("east", 5*3600)
	key, err = s.Key(time.Date(2026, 12, 1, 2, 0, 0, 0, east))
	require.NoError(t, err)
	assert.Equal(t, "2026_11", key)

	for _, v := range []any{uuidV4, uuidZero, "2022_02", "", 42, time.Time{}} {
		_, err = s.Key(v)
		assert.Error(t, err, "%v", v)
	}
}

func TestMonthShardsFor(t *testing.T) {
	ctx := context.Background()
	fs, client := newFakeServer(t)
	s := couch.NewMonthShards(client, "messages",
		couch.WithMonthCreateOptions(kivik.Param("partitioned", true)))

	doc := &shardableDoc{id: uuidV7Year2022}
	db, err := s.For(ctx, doc)
	require.NoError(t, err)
	assert.Equal(t, "test_messages_2022_02", db.Name())
	assert.Equal(t, []string{"test_messages_2022_02"}, fs.names())
	assert.Equal(t, "true", fs.dbs["test_messages_2022_02"].Get("partitioned"))

	// Used again, the database is not created or checked again.
	_, err = s.For(ctx, doc)
	require.NoError(t, err)
	_, err = s.For(ctx, &timedDoc{at: time.Date(2022, 2, 28, 23, 0, 0, 0, time.UTC)})
	require.NoError(t, err)
	assert.Equal(t, 1, fs.created)

	_, err = s.For(ctx, &shardableDoc{id: uuidV4})
	assert.ErrorContains(t, err, "invalid shard key")
}

func TestMonthShardsLookupDoesNotCreate(t *testing.T) {
	fs, client := newFakeServer(t)
	s := couch.NewMonthShards(client, "messages")

	db, err := s.Lookup(&shardableDoc{id: uuidV7Year2022})
	require.NoError(t, err)
	assert.Equal(t, "test_messages_2022_02", db.Name())
	assert.Empty(t, fs.names())
}

func TestMonthShardsDrop(t *testing.T) {
	ctx := context.Background()
	fs, client := newFakeServer(t,
		"test_messages_2026_08",
		"test_messages_2026_09",
		"test_messages_2026_10",
		"test_messages_2026_11",
		"test_messages",            // not a month
		"test_messages_2026",       // a year shard of something else
		"test_messages_2026_13",    // not a month
		"test_messages_2026_9",     // not ours
		"test_messages_v2_2026_08", // another set of shards
		"other_messages_2026_08",   // another prefix
	)
	s := couch.NewMonthShards(client, "messages")

	names, err := s.Names(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{
		"test_messages_2026_08", "test_messages_2026_09",
		"test_messages_2026_10", "test_messages_2026_11",
	}, names)

	// 30 days before 19 November is 20 October: September has ended by
	// then, October has not.
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	dropped, err := s.Drop(ctx, now.AddDate(0, 0, -30))
	require.NoError(t, err)
	assert.Equal(t, []string{"test_messages_2026_08", "test_messages_2026_09"}, dropped)
	assert.Len(t, fs.names(), 8)
	assert.NotContains(t, fs.names(), "test_messages_2026_09")
	assert.Contains(t, fs.names(), "test_messages_2026_10")

	// A month goes at the instant it ends, not before.
	dropped, err = s.Drop(ctx, time.Date(2026, 10, 31, 23, 59, 59, 0, time.UTC))
	require.NoError(t, err)
	assert.Empty(t, dropped)
	dropped, err = s.Drop(ctx, time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	assert.Equal(t, []string{"test_messages_2026_10"}, dropped)
}

func TestMonthShardsRecreatedAfterDrop(t *testing.T) {
	ctx := context.Background()
	fs, client := newFakeServer(t)
	s := couch.NewMonthShards(client, "messages")
	doc := &shardableDoc{id: uuidV7Year2022}

	_, err := s.For(ctx, doc)
	require.NoError(t, err)
	_, err = s.Drop(ctx, time.Date(2022, 4, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	assert.Empty(t, fs.names())

	_, err = s.For(ctx, doc)
	require.NoError(t, err)
	assert.Equal(t, []string{"test_messages_2022_02"}, fs.names())
}
