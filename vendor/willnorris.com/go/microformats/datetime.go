// Copyright (c) 2015 Andy Leap, Google
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to
// deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
// sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.

package microformats

import (
	"regexp"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/html"
)

// datetime represents a microformats datetime value.  It encapsulates a
// time.Time value whose date, time, and timezone value can each be set
// independently.  Each of these values can be set only once and once sent,
// subsequent calls to set the value will be ignored.
type datetime struct {
	t time.Time

	// track whether date, time (with or without seconds), and timezone values have been set
	hasDate, hasTime, hasTZ bool
	hasSeconds              bool
}

// Set the date for d.  Has no effect if date has already been set.
func (d *datetime) setDate(year int, month time.Month, day int) {
	if d.hasDate {
		return
	}
	d.t = time.Date(year, month, day, d.t.Hour(), d.t.Minute(), d.t.Second(), 0, d.t.Location())
	d.hasDate = true
}

// Set the time for d.  Has no effect if time has already been set.
func (d *datetime) setTime(hour, min, sec int) {
	if d.hasTime {
		return
	}
	d.t = time.Date(d.t.Year(), d.t.Month(), d.t.Day(), hour, min, sec, 0, d.t.Location())
	d.hasTime = true
}

// Set the timezone for d.  Has no effect if timezone has already been set.
func (d *datetime) setTZ(loc *time.Location) {
	if d.hasTZ {
		return
	}
	d.t = time.Date(d.t.Year(), d.t.Month(), d.t.Day(), d.t.Hour(), d.t.Minute(), d.t.Second(), 0, loc)
	d.hasTZ = true
}

const (
	formatDate              = "2006-01-02"
	formatDateTime          = "2006-01-02 15:04"
	formatDateTimeSeconds   = "2006-01-02 15:04:05"
	formatDateTimeTZ        = "2006-01-02 15:04-0700"
	formatDateTimeSecondsTZ = "2006-01-02 15:04:05-0700"
)

// String returns a string representation of d, using the format of
// "YYYY-MM-DD HH:MM:SS+XXYY", but omitting certain values not specified in the
// creation of d.  For example:
//
//     - if no date was specified, d is invalid and an empty string is returned
//     - if no time was specified, time and timezone are omitted
//     - if no timezone was specified, timezone is omitted
//     - if no seconds were specified, seconds are omitted
//     - if no minutes were specified, 00 is implied
//
// Microformat docs: http://microformats.org/wiki/value-class-pattern#Date_and_time_parsing
func (d *datetime) String() string {
	if !d.hasDate {
		return ""
	}
	if !d.hasTime {
		return d.t.Format(formatDate)
	}
	if !d.hasTZ {
		if d.hasSeconds {
			return d.t.Format(formatDateTimeSeconds)
		}
		return d.t.Format(formatDateTime)
	}

	var value string
	if d.hasSeconds {
		value = d.t.Format(formatDateTimeSecondsTZ)
	} else {
		value = d.t.Format(formatDateTimeTZ)
	}

	// convert "+0000" to "Z", since time doesn't support a "Z-0700" format
	if strings.HasSuffix(value, "+0000") {
		value = strings.TrimSuffix(value, "+0000") + "Z"
	}
	return value
}

var (
	// regex to match ordinal dates of the form YYYY-DDD
	reOrdinalDate = regexp.MustCompile(`(\d{4})-(\d{3})`)

	// regex to match various permutations of am/pm indicator.  Supports
	// the forms: "AM" and "A.M.".  This assumes that the string has been
	// converted to uppercase before comparison.  Contains two capture
	// groups, one for each letter matched.
	reAMPM = regexp.MustCompile(`(A|P)\.?(M)\.?$`)
)

// various date time format strings
var (
	datetimeFormats = []struct {
		format     string
		hasSeconds bool
	}{
		{time.RFC3339, true},
		{"2006-01-02T15:04:05-07:00", true},
		{"2006-01-02T15:04:05-0700", true},
		{"2006-01-02T15:04:05-07", true},
		{"2006-01-02T15:04:05", true},
		{"2006-01-02T15:04Z07:00", false},
		{"2006-01-02T15:04-07:00", false},
		{"2006-01-02T15:04-0700", false},
		{"2006-01-02T15:04-07", false},
		{"2006-01-02T15:04", false},
	}

	timeFormats = []struct {
		format            string
		hasSeconds, hasTZ bool
	}{
		{"15:04:05", true, false},
		{"15:04", false, false},

		// with timezone
		{"15:04:05Z07:00", true, true},
		{"15:04:05-0700", true, true},
		{"15:04Z07:00", false, true},
		{"15:04-0700", false, true},

		// with am/pm indicator
		{"3:04:05PM", true, false},
		{"3:04PM", false, false},
		{"3PM", false, false},
	}

	tzFormats = []string{
		"Z07:00",
		"-0700",
		"-07",
	}
)

func (d *datetime) Parse(s string) {
	// normalize datetime value
	s = strings.ToUpper(s)
	s = strings.Replace(s, " ", "T", -1)
	s = reAMPM.ReplaceAllString(s, "$1$2")

	// datetime formats
	for _, f := range datetimeFormats {
		if t, err := time.Parse(f.format, s); err == nil {
			d.setDate(t.Year(), t.Month(), t.Day())
			d.setTime(t.Hour(), t.Minute(), t.Second())
			d.setTZ(t.Location())
			d.hasSeconds = f.hasSeconds
			return
		}
	}

	// date-only formats
	if t, err := time.Parse(formatDate, s); err == nil {
		d.setDate(t.Year(), t.Month(), t.Day())
		return
	}
	if m := reOrdinalDate.FindStringSubmatch(s); m != nil {
		year, _ := strconv.Atoi(m[1])
		days, _ := strconv.Atoi(m[2])
		t := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)
		t = t.AddDate(0, 0, days-1)
		d.setDate(t.Year(), t.Month(), t.Day())
		return
	}

	// time formats
	for _, f := range timeFormats {
		if t, err := time.Parse(f.format, s); err == nil {
			d.setTime(t.Hour(), t.Minute(), t.Second())
			d.hasSeconds = f.hasSeconds
			if f.hasTZ {
				d.setTZ(t.Location())
			}
			return
		}
	}

	// timezone only formats
	for _, format := range tzFormats {
		if t, err := time.Parse(format, s); err == nil {
			d.setTZ(t.Location())
			return
		}
	}
}

func getDateTimeValue(node *html.Node) *string {
	values := parseValueClassPattern(node, true)
	var d datetime
	for _, v := range values {
		d.Parse(strings.TrimSpace(v))
	}
	if value := d.String(); value != "" {
		return &value
	}
	return nil
}

// Process implied date for 'end' property.  This is technically part of the value class pattern
// parsing rules, and at this point, we don't know if these were specified using VCP, but we
// imply date all the same anyway.
func implyEndDate(item *Microformat) {
	var startDate time.Time
	for _, v := range item.Properties["start"] {
		if start, ok := v.(string); ok {
			var dt datetime
			dt.Parse(start)
			if dt.hasDate {
				startDate = dt.t
				break
			}
		}
	}
	if startDate.IsZero() {
		return
	}

	for i, v := range item.Properties["end"] {
		if end, ok := v.(string); ok {
			var dt datetime
			dt.Parse(end)
			if !dt.t.IsZero() && !dt.hasDate {
				dt.setDate(startDate.Year(), startDate.Month(), startDate.Day())
				item.Properties["end"][i] = dt.String()
			}
		}
	}
}
