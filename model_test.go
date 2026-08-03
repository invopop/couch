package couch_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/invopop/couch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestModel(t *testing.T) {
	m := &couch.Model{}
	assert.Empty(t, m.GetID())
	assert.False(t, m.Persisted())
	assert.True(t, m.GetCreatedAt().IsZero())

	m.SetID("m-1")
	m.SetRev("1-abc")
	assert.Equal(t, "m-1", m.GetID())
	assert.Equal(t, "1-abc", m.GetRev())
	assert.True(t, m.Persisted())

	m.UpdateTimestamps()
	created := m.GetCreatedAt()
	assert.False(t, created.IsZero(), "created stamped")
	assert.False(t, m.GetUpdatedAt().IsZero(), "updated stamped")

	m.UpdateTimestamps()
	assert.Equal(t, created, m.GetCreatedAt(), "created is not overwritten on re-stamp")

	m.Reset()
	assert.Empty(t, m.GetRev())
	assert.True(t, m.GetCreatedAt().IsZero())
	assert.True(t, m.GetUpdatedAt().IsZero())
}

func TestModelDeleted(t *testing.T) {
	t.Run("absent from a normal document", func(t *testing.T) {
		m := &couch.Model{ID: "m-1"}
		data, err := json.Marshal(m)
		require.NoError(t, err)
		assert.NotContains(t, string(data), "_deleted", "false must not be written")
		assert.False(t, m.GetDeleted())
	})

	t.Run("read back from a tombstone", func(t *testing.T) {
		m := new(couch.Model)
		require.NoError(t, json.Unmarshal([]byte(`{"_id":"m-1","_rev":"2-abc","_deleted":true}`), m))
		assert.True(t, m.Deleted)
		assert.True(t, m.GetDeleted())
		assert.Equal(t, "m-1", m.GetID())
	})

	t.Run("survives a round trip", func(t *testing.T) {
		data, err := json.Marshal(&couch.Model{ID: "m-1", Deleted: true})
		require.NoError(t, err)
		assert.Contains(t, string(data), `"_deleted":true`)
	})
}

func TestStoreRefusesDeletedModel(t *testing.T) {
	// The DB is never reached: the guard runs before any request, which is the
	// point — putting a `_deleted` model back is how a document gets deleted.
	err := couch.Store(context.Background(), nil, &couch.Model{ID: "m-1", Deleted: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "marked as deleted")
}

func TestRevAfter(t *testing.T) {
	tests := []struct {
		name string
		a    string
		b    string
		want bool
	}{
		{
			name: "single digit greater",
			a:    "5-abc123",
			b:    "3-def456",
			want: true,
		},
		{
			name: "single digit less",
			a:    "3-abc123",
			b:    "5-def456",
			want: false,
		},
		{
			name: "double digit greater than single digit",
			a:    "13-abc123",
			b:    "9-def456",
			want: true,
		},
		{
			name: "single digit less than double digit",
			a:    "9-abc123",
			b:    "13-def456",
			want: false,
		},
		{
			name: "equal sequence numbers",
			a:    "5-abc123",
			b:    "5-def456",
			want: false,
		},
		{
			name: "large revision numbers",
			a:    "100-abc123",
			b:    "99-def456",
			want: true,
		},
		{
			name: "rev 1 vs rev 2",
			a:    "2-abc123",
			b:    "1-def456",
			want: true,
		},
		{
			name: "empty strings",
			a:    "",
			b:    "",
			want: false,
		},
		{
			name: "malformed falls back to string comparison",
			a:    "abc",
			b:    "def",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := couch.RevAfter(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("RevAfter(%q, %q) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}
