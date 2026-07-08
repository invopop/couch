package couch

import (
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"
)

// NewShardByYear provides a common implementation of sharding databases
// according to the year encoded in a time-based UUID (versions 1, 6 and 7).
// For this to work, the model must implement a ShardValue method that returns
// its UUID as a string:
//
// ```
//
//	func (m *Model) ShardValue() interface{} {
//	  return m.ID
//	}
//
// ```
//
// Shards are always ordered by reverse chronological order, so the newest shards
// are listed first.
//
// This slightly naive implementation assumes that the service will be restarted
// and migrated at least once per year so that the following year's shard is prepared.
//
// If the ShardValue is a string that is not a UUID, it is assumed to be the name
// of the shard and used directly.
func NewShardByYear(name string, start int) *ShardByYear {
	s := new(ShardByYear)
	s.name = name
	s.start = start
	s.list = s.generateList()
	return s
}

// NewShardByYearWithStatic creates a new shard rule that supports time-based
// UUIDs **and** random/name-based ones (versions 3, 4 and 5).
//
// The "static" parameter enables support for non-time-based IDs that will
// be persisted to a fixed shard instead of by year. This is useful to being able
// to distinguish between data that is always relevant (static) and data that
// becomes less useful over time.
func NewShardByYearWithStatic(name string, start int, static string) *ShardByYear {
	s := new(ShardByYear)
	s.name = name
	s.start = start
	s.static = static
	s.list = s.generateList()
	return s
}

// this will fail if the interface is not implemented.
var _ ShardRules = (*ShardByYear)(nil)

// ShardByYear implements database sharding by year.
type ShardByYear struct {
	name   string // base database name
	static string // static suffix for non-time-based IDs
	list   []string
	start  int
}

// Template provides the base name into which the shard will be inserted.
func (s *ShardByYear) Template() string {
	// just add the year to the end of the base name
	return fmt.Sprintf("%v_%%s", s.name)
}

// List provides an array of acceptable shards
func (s *ShardByYear) List() []string {
	return s.list
}

func (s *ShardByYear) generateList() []string {
	ym := time.Now().Year() + 1
	// Iterate newest-first down to start. A start beyond next year simply
	// yields no year shards rather than a negative-length panic.
	ys := make([]string, 0)
	for y := ym; y >= s.start; y-- {
		ys = append(ys, strconv.Itoa(y))
	}
	if s.static != "" {
		ys = append([]string{s.static}, ys...)
	}
	return ys
}

// Key converts the shardable key's value into a usable shard. The value must be
// a string: either a UUID (whose timestamp determines the year) or an
// already-prepared shard name.
func (s *ShardByYear) Key(v any) (string, error) {
	str, ok := v.(string)
	if !ok {
		return "", errors.New("unexpected shard value")
	}
	b, ok := parseUUID(str)
	if !ok {
		// Not a UUID — assume the value is already a prepared shard name.
		if str == "" {
			return "", errors.New("empty shard value")
		}
		return str, nil
	}
	if b == ([16]byte{}) {
		return "", errors.New("empty shard value")
	}
	ts, ok := uuidTime(b)
	if !ok {
		// Random or name-based UUID (v3/v4/v5): carries no timestamp.
		if s.static != "" {
			return s.static, nil
		}
		return "", errors.New("invalid shard uuid version")
	}
	year := strconv.Itoa(ts.Year())
	if slices.Contains(s.List(), year) {
		return year, nil
	}
	return "", fmt.Errorf("shard year %s out of range", year)
}

// parseUUID decodes the canonical 8-4-4-4-12 hyphenated UUID form into its 16
// bytes. ok is false when s is not a well-formed UUID.
func parseUUID(s string) (b [16]byte, ok bool) {
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return b, false
	}
	clean := s[0:8] + s[9:13] + s[14:18] + s[19:23] + s[24:36]
	if _, err := hex.Decode(b[:], []byte(clean)); err != nil {
		return b, false
	}
	return b, true
}

// gregorianToUnix100ns is the count of 100-nanosecond intervals between the UUID
// (Gregorian) epoch 1582-10-15 and the Unix epoch 1970-01-01.
const gregorianToUnix100ns = 122192928000000000

// uuidTime extracts the timestamp encoded in a time-based UUID (versions 1, 6
// and 7). ok is false for UUIDs that carry no timestamp (versions 3, 4 and 5).
func uuidTime(b [16]byte) (t time.Time, ok bool) {
	switch b[6] >> 4 { // version nibble
	case 1:
		timeLow := uint64(b[0])<<24 | uint64(b[1])<<16 | uint64(b[2])<<8 | uint64(b[3])
		timeMid := uint64(b[4])<<8 | uint64(b[5])
		timeHigh := uint64(b[6]&0x0f)<<8 | uint64(b[7])
		return gregorian(timeHigh<<48 | timeMid<<32 | timeLow), true
	case 6:
		timeHigh := uint64(b[0])<<24 | uint64(b[1])<<16 | uint64(b[2])<<8 | uint64(b[3])
		timeMid := uint64(b[4])<<8 | uint64(b[5])
		timeLow := uint64(b[6]&0x0f)<<8 | uint64(b[7])
		return gregorian(timeHigh<<28 | timeMid<<12 | timeLow), true
	case 7:
		ms := int64(b[0])<<40 | int64(b[1])<<32 | int64(b[2])<<24 |
			int64(b[3])<<16 | int64(b[4])<<8 | int64(b[5])
		return time.UnixMilli(ms).UTC(), true
	default:
		return time.Time{}, false
	}
}

// gregorian converts a 60-bit UUID timestamp (100-ns intervals since the
// Gregorian epoch) into a UTC time.
func gregorian(ts uint64) time.Time {
	return time.Unix(0, (int64(ts)-gregorianToUnix100ns)*100).UTC()
}
