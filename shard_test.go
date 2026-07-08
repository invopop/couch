package couch_test

import (
	"testing"

	"github.com/invopop/couch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type shardableDoc struct {
	id string
}

func (d *shardableDoc) ShardValue() interface{} {
	return d.id
}

func TestNewShards(t *testing.T) {
	c, _ := couch.New(couch.NewConfig("test")) // nolint:errcheck
	sr := couch.NewShardByYear("foo", 2020)
	s := couch.NewShards(c, sr)

	assert.Len(t, s.Map(), len(sr.List()))
	assert.NotNil(t, s.Map()["2020"])

	doc := &shardableDoc{id: uuidV1Year2021}
	db, err := s.For(doc)
	require.NoError(t, err)
	assert.Equal(t, "test_foo_2021", db.Name())

	doc = &shardableDoc{id: uuidV4} // random UUID, no year
	db, err = s.For(doc)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid shard")
	assert.Nil(t, db)

	doc = &shardableDoc{id: uuidV1Old} // year out of range
	db, err = s.For(doc)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "out of range")
	assert.Nil(t, db)
}
