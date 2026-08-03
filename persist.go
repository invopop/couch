package couch

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/go-kivik/kivik/v4"
)

// Persistable defines what is expected from a model for it to be
// persisted to the database.
type Persistable interface {
	UpdateTimestamps()
	GetID() string
	GetRev() string
	SetID(string)
	SetRev(string)
}

// Fetch wraps around the kivik persistence methods to update the model
// with the data from the database or raise an error if it does not exist.
func Fetch(ctx context.Context, db *kivik.DB, d Persistable) error {
	if d.GetID() == "" {
		return errors.New("cannot fetch model without ID")
	}
	err := db.Get(ctx, d.GetID()).ScanDoc(d)
	if err != nil {
		return fmt.Errorf("fetch %s/%s: %w", db.Name(), d.GetID(), mapKivikError(err))
	}
	return nil
}

// FetchModel is the now deprecated way of Fetching a model from the database.
func FetchModel(ctx context.Context, db *kivik.DB, m Persistable) error {
	return Fetch(ctx, db, m)
}

// Store attempts to persist the provided persistable object to the database.
//
// A model carrying the `_deleted` marker (see Model.Deleted) is refused:
// putting it back is how CouchDB deletes a document, and a save that silently
// deletes instead would be a nasty way to find that out. Deletions read from a
// change feed are meant to be reacted to, not written; use Delete to remove a
// document.
func Store(ctx context.Context, db *kivik.DB, m Persistable) error {
	if m.GetID() == "" {
		return errors.New("cannot store model without ID")
	}
	if d, ok := m.(interface{ GetDeleted() bool }); ok && d.GetDeleted() {
		return fmt.Errorf("cannot store %s: model is marked as deleted", m.GetID())
	}
	m.UpdateTimestamps()
	rev, err := db.Put(ctx, m.GetID(), m)
	if err != nil {
		return fmt.Errorf("put %s/%s: %w", db.Name(), m.GetID(), mapKivikError(err))
	}
	m.SetRev(rev)
	return nil
}

// StoreModel is the deprecated way of persisting updates to the database
// and simply wraps around the Store method.
func StoreModel(ctx context.Context, db *kivik.DB, m Persistable) error {
	return Store(ctx, db, m)
}

// Delete removes the provided persistable object from the database.
func Delete(ctx context.Context, db *kivik.DB, m Persistable) error {
	if m.GetID() == "" {
		return errors.New("cannot delete model without ID")
	}
	if m.GetRev() == "" {
		return errors.New("cannot delete model without rev")
	}
	rev, err := db.Delete(ctx, m.GetID(), m.GetRev())
	if err != nil {
		return fmt.Errorf("delete %s/%s: %w", db.Name(), m.GetID(), mapKivikError(err))
	}
	m.SetRev(rev)
	return nil
}

// Errors returned by the persistence helpers, wrapping the underlying
// kivik/CouchDB failure. Match them with errors.Is.
var (
	// ErrNotFound is returned when a document does not exist.
	ErrNotFound = errors.New("not found")
	// ErrAlreadyExists is returned on a document revision conflict.
	ErrAlreadyExists = errors.New("already exists")
)

func mapKivikError(err error) error {
	switch kivik.HTTPStatus(err) {
	case http.StatusNotFound:
		return fmt.Errorf("%w: %v", ErrNotFound, err)
	case http.StatusConflict:
		return fmt.Errorf("%w: %v", ErrAlreadyExists, err)
	default:
		// no alternative mapping yet
		return err
	}
}
