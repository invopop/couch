// Package at provides timestamp handling with millisecond precision.
package at

import (
	"fmt"
	"time"
)

// Millisecond time formats to comply with W3C datetime format that
// contains rules for local timezones so that they always include a ":".
const (
	RFC3339Milli         string = "2006-01-02T15:04:05.000Z"
	RFC3339MilliWithZone string = "2006-01-02T15:04:05.000-07:00"
)

const (
	nullString = "null"
)

// Timestamp represents the basic time wrapper to be used in timestamps
// with millisecond precision.
type Timestamp struct {
	time.Time
}

// LocalTime ensures the local time information is included in the timestamp.
type LocalTime struct {
	time.Time
}

// Now provides a timestamp for the current UTC system Time
func Now() Timestamp {
	return Timestamp{time.Now().UTC()}
}

// LocalTimeNow is used to provide the current time in the provided location.
func LocalTimeNow(loc *time.Location) LocalTime {
	return LocalTime{time.Now().In(loc)}
}

// ParseTimestamp attempts to parse the timestamp string.
func ParseTimestamp(str string) (Timestamp, error) {
	// parse with generic RFC3339 precision, which supports milliseconds
	// and helps us get around issues around timestamps that don't
	// include the milliseconds for whatever reason.
	o, err := time.Parse(time.RFC3339, str)
	if err != nil {
		return Timestamp{}, fmt.Errorf("at: unable to parse timestamp: %w", err)
	}
	return Timestamp{o.UTC()}, nil
}

// ParseLocalTime attempts to read in the provided time data which hopefully includes
// a zone, but is not necessarily guaranteed.
func ParseLocalTime(str string) (LocalTime, error) {
	o, err := time.Parse(time.RFC3339, str)
	if err != nil {
		return LocalTime{}, fmt.Errorf("at: unable to parse localtime: %w", err)
	}
	return LocalTime{o}, nil
}

// String provides the timestamp in RFC3339 format including milliseconds.
func (t *Timestamp) String() string {
	return t.Format(RFC3339Milli)
}

// String provides the local time including milliseconds and a time zone.
func (t *LocalTime) String() string {
	return t.Format(RFC3339MilliWithZone)
}

// UnmarshalJSON uses our timestamp parser.
func (t *Timestamp) UnmarshalJSON(data []byte) error {
	s := string(data)
	if s == nullString {
		return nil
	}
	s = s[1 : len(s)-1] // no quotes
	var err error
	*t, err = ParseTimestamp(s)
	return err
}

// MarshalJSON provides the timestamp in JSON format
func (t Timestamp) MarshalJSON() ([]byte, error) {
	if t.IsZero() {
		return []byte(nullString), nil
	}
	return []byte(`"` + t.String() + `"`), nil
}

// UnmarshalJSON parses the provided local time JSON data.
func (t *LocalTime) UnmarshalJSON(data []byte) error {
	s := string(data)
	if s == nullString {
		return nil
	}
	s = s[1 : len(s)-1] // no quotes
	var err error
	*t, err = ParseLocalTime(s)
	return err
}

// MarshalJSON provides the local time in JSON format.
func (t LocalTime) MarshalJSON() ([]byte, error) {
	if t.IsZero() {
		return []byte(nullString), nil
	}
	return []byte(`"` + t.String() + `"`), nil
}
