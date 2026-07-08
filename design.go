package couch

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/go-kivik/kivik/v4"
)

const (
	designDocPrefix          = "_design/"
	designDocDefaultLanguage = "javascript"
)

// Design represents the special design documents used to query documents using
// pre-defined indexes.
// Only designs that have changed will be synchronised with the database using a simple
// SHA256 comparison algorithm that checks for changes in the views.
type Design struct {
	Model
	Language string `json:"language"`

	// Options stores additional options for the design document.
	Options map[string]any `json:"options,omitempty"`

	Filters map[string]string `json:"filters,omitempty"`
	Views   map[string]*View  `json:"views,omitempty"`
}

// View is a basic definition of a CouchDB view.
type View struct {
	Map    string `json:"map"`
	Reduce string `json:"reduce,omitempty"`
}

// NewDesign builds a new design document instance using the provided name.
func NewDesign(name string) *Design {
	d := new(Design)
	d.SetID(designDocPrefix + name)
	d.Language = designDocDefaultLanguage
	d.Options = make(map[string]any)
	d.Filters = make(map[string]string)
	d.Views = make(map[string]*View)
	return d
}

// SetView adds the provided view with the given name.
func (d *Design) SetView(name string, view *View) {
	d.Views[name] = view
}

// SetFilter adds the provided filter to the design document.
// Existing filters with the same name will be replaced.
func (d *Design) SetFilter(name string, filter string) {
	d.Filters[name] = filter
}

// Checksum generates a SHA256 sum by joining all the filters and views
// together to form a single string and running the result through
// the digest algorithm. The result is a Hexadecimal string.
func (d *Design) Checksum() string {
	text := d.filterConcat() + d.viewConcat()
	sum := sha256.Sum256([]byte(text))
	return fmt.Sprintf("%x", sum)
}

func (d *Design) filterConcat() string {
	var keys, items []string
	for k := range d.Filters {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		items = append(items, k, d.Filters[k])
	}
	return strings.Join(items, ":")
}

func (d *Design) viewConcat() string {
	var keys, items []string
	for k := range d.Views {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		view := d.Views[k]
		if view == nil {
			continue
		}
		items = append(items, k, "map", view.Map, "reduce", view.Reduce)
	}
	return strings.Join(items, ":")
}

// Sync compares the checksums of the current design and the previous
func (d *Design) Sync(ctx context.Context, db *kivik.DB) error {
	prev := new(Design)
	if err := db.Get(ctx, d.ID).ScanDoc(prev); err != nil {
		if kivik.HTTPStatus(err) != http.StatusNotFound {
			return fmt.Errorf("couch: %w", err)
		}
	}
	if prev.Persisted() {
		// Copy a few key details from existing design
		d.SetRev(prev.Rev)
		d.CreatedAt = prev.CreatedAt
		d.UpdatedAt = prev.UpdatedAt
		if d.Checksum() == prev.Checksum() {
			return nil // no changes
		}
	} else {
		d.Reset() // ensures revision and timestamp data is not copied
	}
	return StoreModel(ctx, db, d)
}
