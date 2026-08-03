# couch

A small, dependency-light wrapper around the [kivik](https://github.com/go-kivik/kivik)
CouchDB driver that makes connections, models, and design documents easier to
work with.

```bash
go get github.com/invopop/couch
```

## What it gives you

- **`couch.Config` / `couch.Client`** — configure a connection from parts and
  namespace every database behind a prefix.
- **`couch.Model`** — an embeddable base document that manages `_id`, `_rev`,
  attachments, and `created_at` / `updated_at` timestamps. `couch.Document` is
  the same without timestamps.
- **`couch.Store` / `couch.Fetch` / `couch.Delete`** — persistence helpers that
  stamp timestamps, track the revision, and map errors to `couch.ErrNotFound` /
  `couch.ErrAlreadyExists`.
- **`couch.Design` / `couch.View`** — declare design documents and sync them
  idempotently (only rewritten when their views/filters change).
- **`couch/changes`** — consume CouchDB `_changes` feeds with a resumable,
  persisted cursor and a worker pool.
- **[`invopop/at`](https://github.com/invopop/at)** — millisecond-precision
  timestamps used by `couch.Model`. It lived here as `couch/at` until it was
  moved out, so anything can use the type without depending on this library.

## Usage

```go
package main

import (
	"context"
	"errors"
	"log"

	"github.com/invopop/couch"
)

// Embed couch.Model to get _id/_rev + created_at/updated_at for free.
type Widget struct {
	couch.Model
	Name string `json:"name"`
}

func main() {
	ctx := context.Background()

	conf := couch.NewConfig("myapp") // databases are namespaced as myapp_<name>
	conf.Host = "localhost"
	conf.Username, conf.Password = "admin", "secret"

	client, err := couch.New(conf)
	if err != nil {
		log.Fatal(err)
	}
	if err := client.Ping(ctx); err != nil {
		log.Fatal(err)
	}

	db := client.DB("widgets") // resolves to "myapp_widgets"
	if err := client.Create(ctx, db); err != nil {
		log.Fatal(err)
	}

	w := &Widget{Name: "gadget"}
	w.SetID("widget-1")
	if err := couch.Store(ctx, db, w); err != nil { // sets timestamps + _rev
		log.Fatal(err)
	}

	got := &Widget{}
	got.SetID("widget-1")
	switch err := couch.Fetch(ctx, db, got); {
	case errors.Is(err, couch.ErrNotFound):
		log.Println("not found")
	case err != nil:
		log.Fatal(err)
	}
}
```

### Design documents

```go
d := couch.NewDesign("widgets")
d.SetView("by_name", &couch.View{
	Map: `function(doc) { if (doc.name) { emit(doc.name, null); } }`,
})
if err := client.SyncDesigns(ctx, db, []*couch.Design{d}); err != nil {
	log.Fatal(err)
}
```

### Change feeds

See [`changes`](./changes) for consuming a database's `_changes` feed with a
resumable cursor. Sharding helpers (`ShardByYear`, …) live in the root package.

`Next` describes each change rather than just naming it, so deletions are
reported instead of dropped:

```go
for {
    c, err := feed.Next(ctx)
    if err != nil { /* retry */ }
    if c.ID == "" { break } // feed stopped

    if c.Deleted {
        // A tombstone: nothing left to fetch, and anything mirroring this
        // document downstream should drop its copy.
        continue
    }
    // load and process c.ID
}
```

That covers documents removed by hand in the database as much as those the
application deleted. `Model.Deleted` carries the same `_deleted` marker, so a
tombstone read from a feed, a view or a fetch arrives as a model rather than a
bare ID — and `Store` refuses to persist one, since writing `_deleted` back is
how a document gets deleted.

## License

Apache 2.0 — see [LICENSE](./LICENSE).
