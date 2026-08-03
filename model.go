package couch

import (
	"strconv"
	"strings"

	"github.com/go-kivik/kivik/v4"
	"github.com/invopop/at"
)

// Model is a standard representation of a model to be stored in CouchDB
// that takes care of the ID, Revision, and adds timestamps.
type Model struct {
	ID  string `json:"_id,omitempty"`
	Rev string `json:"_rev,omitempty"`

	// Attachments keeps together the special list of attachments that belong to
	// model. Without this placeholder, they'll get deleted after an update.
	Attachments kivik.Attachments `json:"_attachments,omitempty"`

	// Deleted reflects CouchDB's `_deleted` marker: true means this is a
	// tombstone rather than a document. It is set when reading a deletion —
	// from a change feed started WithDeletions, or anywhere else a tombstone
	// surfaces — so consumers can react to a document being removed, including
	// one deleted by hand in the database.
	//
	// It is read-only in practice: Store refuses to persist a model carrying
	// it, since writing `_deleted` back is how a document gets deleted and
	// having that happen as a side effect of a save would be surprising. Use
	// Delete instead.
	Deleted bool `json:"_deleted,omitempty"`

	CreatedAt at.Timestamp `json:"created_at"`
	UpdatedAt at.Timestamp `json:"updated_at"`
}

// GetDeleted reports whether the model represents a deleted document, so
// persistence layers can check the marker through an interface rather than
// depending on the concrete type.
func (m *Model) GetDeleted() bool {
	return m.Deleted
}

// UpdateTimestamps ensures the model's created and update at stamps are set.
func (m *Model) UpdateTimestamps() {
	if m.CreatedAt.IsZero() {
		m.CreatedAt = at.Now()
	}
	m.UpdatedAt = at.Now()
}

// Reset sets the rev, created and update at timestamps to zero usually
// so that the same model can be persisted to multiple database without having
// the revision and timestamps copied between instances.
func (m *Model) Reset() {
	m.Rev = ""
	m.CreatedAt = at.Timestamp{}
	m.UpdatedAt = at.Timestamp{}
}

// GetID provides the document's ID
func (m *Model) GetID() string {
	return m.ID
}

// GetRev provides the document's Revision
func (m *Model) GetRev() string {
	return m.Rev
}

// SetID sets the model's ID
func (m *Model) SetID(id string) {
	m.ID = id
}

// SetRev update's the model's revision ID. The should mainly be
// used by persistence layers.
func (m *Model) SetRev(rev string) {
	m.Rev = rev
}

// Persisted returns true if the revision has been set, a value that should
// always be provided by the database server.
func (m *Model) Persisted() bool {
	return m.Rev != ""
}

// GetCreatedAt provides the model's CreatedAt timestamp in situations where
// the model is being treated as an interface.
// May be zero if the model has not been prepared for persistence.
func (m *Model) GetCreatedAt() at.Timestamp {
	return m.CreatedAt
}

// GetUpdatedAt provides the model's UpdatedAt timestamp in situations where
// the model is being treated as an interface.
// May be zero if the model has not been prepared for persistence.
func (m *Model) GetUpdatedAt() at.Timestamp {
	return m.UpdatedAt
}

// RevAfter returns true if CouchDB revision a is newer than revision b.
// Revisions have the format "<seq>-<hash>", e.g. "13-a9e7c9c1...".
// This must be used instead of direct string comparison (a > b) because
// lexicographic ordering breaks when sequence numbers cross digit boundaries
// (e.g. "9-xxx" > "13-xxx" is true lexicographically but incorrect).
func RevAfter(a, b string) bool {
	aNum, _, _ := strings.Cut(a, "-")
	bNum, _, _ := strings.Cut(b, "-")
	an, aErr := strconv.Atoi(aNum)
	bn, bErr := strconv.Atoi(bNum)
	if aErr != nil || bErr != nil {
		return a > b
	}
	return an > bn
}
