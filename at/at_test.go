package at_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/invopop/couch/at"
)

func TestTimestampUnmarshal(t *testing.T) {
	var cases = []struct {
		Given    string
		Expected time.Time
		Error    bool
	}{
		// long form
		{`"2009-11-10T23:19:45.123Z"`, time.Date(2009, time.November, 10, 23, 19, 45, 123000000, time.UTC), false},
		// short form
		{`"2009-11-10T13:19:04Z"`, time.Date(2009, time.November, 10, 13, 19, 4, 0, time.UTC), false},
		// local form
		{`"2009-11-10T13:19:04+02:00"`, time.Date(2009, time.November, 10, 11, 19, 4, 0, time.UTC), false},
		// bad form
		{`"Z2009-11-10T13:19:04Z"`, time.Time{}, true},
		// nil string
		{`null`, time.Time{}, false},
	}

	for _, c := range cases {
		payload := []byte(c.Given)
		var output at.Timestamp
		if err := json.Unmarshal(payload, &output); err != nil && !c.Error {
			t.Error(err)
			continue
		}
		if !output.Equal(c.Expected) {
			t.Errorf("Expected: %q, Given: %q", c.Expected, output)
		}
	}

	// always convert to UTC
	var output at.Timestamp
	if err := json.Unmarshal([]byte(`"2009-11-10T13:19:04+02:00"`), &output); err != nil {
		t.Error(err)
		return
	}
	if z, _ := output.Zone(); z != "UTC" {
		t.Errorf("Expected UTC time zone, got: %q", z)
	}
}

func TestTimestampMarshal(t *testing.T) {
	var cases = []struct {
		Given    time.Time
		Expected string
	}{
		{time.Date(2009, time.November, 10, 23, 19, 45, 0, time.UTC), `"2009-11-10T23:19:45.000Z"`},
		{time.Date(2009, time.November, 10, 13, 19, 4, 0, time.UTC), `"2009-11-10T13:19:04.000Z"`},
		{time.Date(2009, time.November, 10, 23, 19, 45, 123456000, time.UTC), `"2009-11-10T23:19:45.123Z"`},
		{time.Time{}, `null`},
	}

	for _, c := range cases {
		ct := at.Timestamp{c.Given}
		output, err := json.Marshal(ct)
		if err != nil {
			t.Error(err)
		}
		if string(output) != c.Expected {
			t.Errorf("Expected: %q, Given: %q", c.Expected, output)
		}
	}
}

func TestNow(t *testing.T) {
	ct := at.Now()
	if z, _ := ct.Zone(); z != "UTC" {
		t.Errorf("Failed to get current time in UTC, got: %v", z)
	}
}

func TestLocalTimeNow(t *testing.T) {
	loc, _ := time.LoadLocation("America/Lima")
	ct := at.LocalTimeNow(loc)
	if z, _ := ct.Zone(); z != "-05" {
		t.Errorf("Failed to get expected time zone, got: %v", z)
	}
}

func TestLocalTimeUnmarshal(t *testing.T) {
	tl, _ := time.LoadLocation("America/Lima") // always -5 (no DST)
	tl2, _ := time.LoadLocation("Asia/Dubai")  // always +4 (no DST)
	var cases = []struct {
		Given    string
		Expected time.Time
		Error    bool
	}{
		// long form
		{`"2009-11-10T23:19:45.123-05:00"`, time.Date(2009, time.November, 10, 23, 19, 45, 123000000, tl), false},
		// long form 2
		{`"2009-11-10T23:19:45.123+04:00"`, time.Date(2009, time.November, 10, 23, 19, 45, 123000000, tl2), false},
		// short form
		{`"2009-11-10T23:19:45-05:00"`, time.Date(2009, time.November, 10, 23, 19, 45, 0, tl), false},
		// bad form
		{`"Z2009-11-10T13:19:04Z"`, time.Time{}, true},
		// bad long form
		{`"2009-11-10T23:19:45.123+0400"`, time.Time{}, true},
		// nil string
		{`null`, time.Time{}, false},
		// UTC form
		{`"2009-11-10T23:19:45.123Z"`, time.Date(2009, time.November, 10, 23, 19, 45, 123000000, time.UTC), false},
	}

	for _, c := range cases {
		payload := []byte(c.Given)
		var output at.LocalTime
		if err := json.Unmarshal(payload, &output); err != nil && !c.Error {
			t.Error(err)
			continue
		}
		if !output.Equal(c.Expected) {
			t.Errorf("Expected: %q, Given: %q", c.Expected, output)
		}
	}
}

func TestLocalTimeMarshal(t *testing.T) {
	tl, _ := time.LoadLocation("America/Lima") // always -5 (no DST)
	tl2, _ := time.LoadLocation("Asia/Dubai")  // always +4 (no DST)
	var cases = []struct {
		Given    time.Time
		Expected string
	}{
		{time.Date(2009, time.November, 10, 23, 19, 30, 0, tl), `"2009-11-10T23:19:30.000-05:00"`},
		{time.Date(2009, time.November, 10, 13, 19, 4, 123000000, tl), `"2009-11-10T13:19:04.123-05:00"`},
		{time.Date(2009, time.November, 10, 13, 19, 4, 123000000, tl2), `"2009-11-10T13:19:04.123+04:00"`},
		{time.Time{}, `null`},
	}

	for _, c := range cases {
		ct := at.LocalTime{c.Given}
		output, err := json.Marshal(ct)
		if err != nil {
			t.Error(err)
		}
		if string(output) != c.Expected {
			t.Errorf("Expected: %q, Given: %q", c.Expected, output)
		}
	}
}

func TestTimestampInModel(t *testing.T) {
	type tmodel struct {
		Value     string        `json:"v"`
		ExampleAt at.Timestamp  `json:"example_at"`
		EmptyAt   *at.Timestamp `json:"empty_at,omitempty"`
	}
	x := new(tmodel)
	x.Value = "bar"

	data, err := json.Marshal(x)
	if err != nil {
		t.Error(err)
		return
	}
	if !strings.Contains(string(data), `"example_at":null`) {
		t.Errorf("Expected output to contain value example_at, got: %v", string(data))
	}
	if strings.Contains(string(data), "empty_at") {
		t.Errorf("Did not expect output to contain value, got: %v", string(data))
	}
}
