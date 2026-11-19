package changes

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kivik/kivik/v4"
	_ "github.com/go-kivik/kivik/v4/couchdb" // CouchDB driver
)

// fakeCouch serves just enough of CouchDB for a feed to connect: a missing
// feed document, and a continuous changes response that sends the given
// number of changes and then stays open, as the real one does.
type fakeCouch struct {
	*httptest.Server
	connections atomic.Int32
}

func newFakeCouch(t *testing.T, changes int) *fakeCouch {
	t.Helper()
	fc := new(fakeCouch)
	fc.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/_changes"):
			fc.connections.Add(1)
			w.WriteHeader(http.StatusOK)
			for i := 1; i <= changes; i++ {
				_, _ = fmt.Fprintf(w, `{"seq":"%d-x","id":"doc-%d","changes":[{"rev":"1-a"}]}`+"\n", i, i)
			}
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		case r.Method == http.MethodPut:
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"ok":true,"id":"feed","rev":"1-a"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"not_found","reason":"missing"}`))
		}
	}))
	t.Cleanup(fc.Close)
	return fc
}

func (fc *fakeCouch) feed(t *testing.T, opts ...Option) *feed {
	t.Helper()
	client, err := kivik.New("couch", fc.URL)
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}
	return New(client.DB("feeds"), client.DB("source"), opts...).(*feed)
}

// waitClosed fails the test unless the connection loop exits, which it
// signals by closing the outgoing channel.
func waitClosed(t *testing.T, f *feed) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case _, more := <-f.outgoing:
			if !more {
				return
			}
		case <-deadline:
			t.Fatal("connection loop still running")
		}
	}
}

// Stop used to close the source and the outgoing channel while the connection
// loop was still using both. Run with -race.
func TestStop_WhileReading(t *testing.T) {
	fc := newFakeCouch(t, 1)
	f := fc.feed(t)
	ctx := context.Background()
	f.Start(ctx)

	c, err := f.Next(ctx)
	if err != nil || c.ID != "doc-1" {
		t.Fatalf("expected doc-1, got %+v, %v", c, err)
	}

	// The consumer waits in Next and the loop waits on the source.
	next := make(chan Change, 1)
	go func() {
		c, _ := f.Next(ctx)
		next <- c
	}()
	time.Sleep(50 * time.Millisecond)
	f.Stop()

	select {
	case c := <-next:
		if c.ID != "" {
			t.Fatalf("expected a zero change after Stop, got %+v", c)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Next did not return after Stop")
	}
	waitClosed(t, f)

	if c, err := f.Next(ctx); err != nil || c.ID != "" {
		t.Fatalf("expected end of feed after Stop, got %+v, %v", c, err)
	}
}

// With nobody calling Next the loop is blocked handing over a change. Stop
// must release it instead of closing the channel under it.
func TestStop_WhileDelivering(t *testing.T) {
	fc := newFakeCouch(t, 3)
	f := fc.feed(t)
	f.Start(context.Background())

	for fc.connections.Load() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	f.Stop()
	waitClosed(t, f)
}

// Ending the caller's context ends the loop, rather than leaving it
// reconnecting forever.
func TestStart_ContextCancelled(t *testing.T) {
	fc := newFakeCouch(t, 0)
	f := fc.feed(t)
	ctx, cancel := context.WithCancel(context.Background())
	f.Start(ctx)

	for fc.connections.Load() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	waitClosed(t, f)
	if n := fc.connections.Load(); n != 1 {
		t.Fatalf("expected a single connection, got %d", n)
	}
}

func TestStart_AfterStop(t *testing.T) {
	fc := newFakeCouch(t, 0)
	f := fc.feed(t)
	f.Stop()
	f.Start(context.Background())

	if started, _ := f.status(); started {
		t.Fatal("a stopped feed must not start")
	}
	if c, err := f.Next(context.Background()); err != nil || c.ID != "" {
		t.Fatalf("expected end of feed, got %+v, %v", c, err)
	}
}
