package couch_test

import (
	"testing"

	"github.com/invopop/couch"
	"github.com/stretchr/testify/assert"
)

func TestDocument(t *testing.T) {
	d := &couch.Document{}
	assert.Empty(t, d.GetID())
	assert.False(t, d.Persisted())

	d.SetID("doc-1")
	d.SetRev("1-abc")
	assert.Equal(t, "doc-1", d.GetID())
	assert.Equal(t, "1-abc", d.GetRev())
	assert.True(t, d.Persisted())

	d.UpdateTimestamps() // no-op for a plain Document; must not panic
}
