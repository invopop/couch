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
	// endFirst makes the first changes response end once its changes are
	// sent, the way a proxy or a restarting node would cut it short.
	endFirst bool
}

func newFakeCouch(t *testing.T, changes int) *fakeCouch {
	t.Helper()
	fc := new(fakeCouch)
	fc.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/_changes"):
			n := fc.connections.Add(1)
			w.WriteHeader(http.StatusOK)
			if n == 1 || !fc.endFirst {
				for i := 1; i <= changes; i++ {
					_, _ = fmt.Fprintf(w, `{"seq":"%d-x","id":"doc-%d","changes":[{"rev":"1-a"}]}`+"\n", i, i)
				}
			}
			w.(http.Flusher).Flush()
			if n == 1 && fc.endFirst {
				return
			}
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

	// The feed is now stopped, as it would be after Stop: anyone waiting on
	// Fatal is released and Next reports the end.
	select {
	case _, ok := <-f.Fatal():
		if ok {
			t.Fatal("expected Fatal to be closed without an error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Fatal still open after the context ended")
	}
	if _, stopped := f.status(); !stopped {
		t.Fatal("expected the feed to be stopped")
	}
	if c, err := f.Next(context.Background()); err != nil || c.ID != "" {
		t.Fatalf("expected end of feed, got %+v, %v", c, err)
	}
}

// When the source ends the response the loop reconnects, reloading the stored
// position while the consumer may be advancing it. Run with -race. This waits
// out one reconnect delay.
func TestReconnect_WhileConsuming(t *testing.T) {
	fc := newFakeCouch(t, 2)
	fc.endFirst = true
	f := fc.feed(t)
	ctx := context.Background()
	f.Start(ctx)

	for _, want := range []string{"doc-1", "doc-2"} {
		c, err := f.Next(ctx)
		if err != nil || c.ID != want {
			t.Fatalf("expected %s, got %+v, %v", want, c, err)
		}
	}
	// Acknowledge doc-2 and wait for more, across the reconnect.
	next := make(chan Change, 1)
	go func() {
		c, _ := f.Next(ctx)
		next <- c
	}()

	deadline := time.After(10 * time.Second)
	for fc.connections.Load() < 2 {
		select {
		case <-deadline:
			t.Fatal("feed did not reconnect")
		case <-time.After(20 * time.Millisecond):
		}
	}
	f.Stop()
	if c := <-next; c.ID != "" {
		t.Fatalf("expected a zero change after Stop, got %+v", c)
	}
	waitClosed(t, f)
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
