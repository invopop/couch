package couch_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/invopop/couch"
	"github.com/stretchr/testify/assert"
)

// Fixed UUID vectors with known versions and encoded timestamps. The RFC 9562
// examples (v1/v6/v7) all encode 2022-02-22T19:22:22Z.
const (
	uuidV1Year2021 = "67cae486-0c2c-11ec-b15b-0242ac130002" // v1 → 2021
	uuidV1Old      = "a8098c1a-f86e-11da-bd1a-00112444be1e" // v1 → 2006 (out of range)
	uuidV1Year2022 = "c232ab00-9414-11ec-b3c8-9e6bdeced846" // v1 → 2022 (RFC 9562)
	uuidV6Year2022 = "1ec9414c-232a-6b00-b3c8-9e6bdeced846" // v6 → 2022 (RFC 9562)
	uuidV7Year2022 = "017f22e2-79b0-7cc3-98c4-dc0c0c07398f" // v7 → 2022 (RFC 9562)
	uuidV3         = "c2119440-3957-3584-8db9-058632002d07" // v3 (no timestamp)
	uuidV4         = "7483ccea-b672-4583-99a7-3797e0505083" // v4 (no timestamp)
	uuidV5         = "eed3f9a6-ddb2-5fd1-bd7c-56f357a01984" // v5 (no timestamp)
	uuidZero       = "00000000-0000-0000-0000-000000000000"
)

func TestNewShardingByYear(t *testing.T) {
	sr := couch.NewShardByYear("test", 2020)
	yn := time.Now().Year()
	l := (yn + 2) - 2020
	ls := sr.List()
	assert.Len(t, ls, l)
	assert.Equal(t, fmt.Sprintf("%d", yn+1), ls[0], "first entry")
	assert.Equal(t, "2021", ls[len(ls)-2], "last entry")
	assert.Equal(t, "test_%s", sr.Template())
}

func TestNewShardingByYearWithStatic(t *testing.T) {
	sr := couch.NewShardByYearWithStatic("test", 2020, "static")
	yn := time.Now().Year()
	l := (yn + 2) - 2020
	ls := sr.List()
	assert.Len(t, ls, l+1)
	assert.Equal(t, "static", ls[0], "static entry")
	assert.Equal(t, fmt.Sprintf("%d", yn+1), ls[1], "first entry")
	assert.Equal(t, "2021", ls[len(ls)-2], "last entry")
	assert.Equal(t, "test_%s", sr.Template())
}

func TestShardByYearFutureStart(t *testing.T) {
	// A start year beyond next year must not panic; it yields no year shards.
	assert.Empty(t, couch.NewShardByYear("test", 9999).List())
	assert.Equal(t, []string{"static"}, couch.NewShardByYearWithStatic("test", 9999, "static").List())
}

func TestShardingByYearKey(t *testing.T) {
	sr := couch.NewShardByYear("test", 2020)

	_, err := sr.Key(1234)
	assert.ErrorContains(t, err, "unexpected shard value")

	str, err := sr.Key("2021") // already-prepared shard name
	assert.NoError(t, err)
	assert.Equal(t, "2021", str)

	_, err = sr.Key(uuidZero)
	assert.ErrorContains(t, err, "empty shard value")

	_, err = sr.Key("")
	assert.ErrorContains(t, err, "empty shard value")

	// Random UUID with no static shard configured.
	_, err = sr.Key(uuidV4)
	assert.ErrorContains(t, err, "invalid shard uuid version")

	// Time-based UUIDs resolve to their encoded year.
	str, err = sr.Key(uuidV1Year2021)
	assert.NoError(t, err)
	assert.Equal(t, "2021", str)

	for _, id := range []string{uuidV1Year2022, uuidV6Year2022, uuidV7Year2022} {
		str, err = sr.Key(id)
		assert.NoError(t, err)
		assert.Equal(t, "2022", str, id)
	}

	_, err = sr.Key(uuidV1Old)
	assert.ErrorContains(t, err, "out of range")
}

func TestShardingByYearWithStaticKey(t *testing.T) {
	sr := couch.NewShardByYearWithStatic("test", 2020, "static")

	str, err := sr.Key("2021")
	assert.NoError(t, err)
	assert.Equal(t, "2021", str)

	_, err = sr.Key(uuidZero)
	assert.ErrorContains(t, err, "empty shard value")

	// Non-time-based UUIDs land on the static shard.
	for _, id := range []string{uuidV3, uuidV4, uuidV5} {
		str, err = sr.Key(id)
		assert.NoError(t, err)
		assert.Equal(t, "static", str, id)
	}

	// Time-based UUIDs still resolve to their year.
	for _, id := range []string{uuidV1Year2022, uuidV6Year2022, uuidV7Year2022} {
		str, err = sr.Key(id)
		assert.NoError(t, err)
		assert.Equal(t, "2022", str, id)
	}
}
