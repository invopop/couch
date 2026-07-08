package couch

// Document is a simplified object that conforms to the Persistable
// implementation. Unlike the Model implementation, it doesn't include
// any timestamping.
type Document struct {
	ID  string `json:"_id,omitempty"`
	Rev string `json:"_rev,omitempty"`
}

// UpdateTimestamps in the context of a simple CouchDB document does
// nothing.
func (d *Document) UpdateTimestamps() {
	// nothing to do
}

// GetID provides the current document ID.
func (d *Document) GetID() string {
	return d.ID
}

// GetRev provides the document's revision.
func (d *Document) GetRev() string {
	return d.Rev
}

// SetID sets the model's ID
func (d *Document) SetID(id string) {
	d.ID = id
}

// SetRev update's the documents's revision ID. The should mainly be
// used by persistence layers.
func (d *Document) SetRev(rev string) {
	d.Rev = rev
}

// Persisted returns true if the revision has been set, a value that should
// always be provided by the database server.
func (d *Document) Persisted() bool {
	return d.Rev != ""
}
