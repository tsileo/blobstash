// Package asof "as of" implements utils for building "as of"/time travel queries
package asof // import "a4.io/blobstash/pkg/asof"
import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// IsValid returns true if the query looks valid.
// The only rule for now is not to conain any dots (to prevent issue with editor like vim who create metadata based on the filename).
func IsValid(asOf string) bool {
	return !strings.Contains(asOf, ".")
}

// ParseAsOf returns the parsed query (as a unix nano timestamp) or an error if it could not be parsed.
// Fow now it supports:
//  - negative duration (as parsed by `time.ParseDuration`, e.g. -10h, -30s
//  - few date formats from YYYY-MM-DDTHH:MM:SS to YYYY
//  - and unix timestamps (second precision)
func ParseAsOf(asOf string) (int64, error) {
	// Try to parse duration (-10h)
	dur, err := time.ParseDuration(asOf)
	if err == nil {
		// TODO(tsileo): ensure dur is negative
		return time.Now().UTC().Add(dur).UnixNano(), nil
	}
	// Try parsing formatted datetime
	for _, dfmt := range []string{
		"2006-01-02T15:04:05",
		"2006-01-02T15:04",
		"2006-01-02T15",
		"2006-01-02",
		"2006-01",
		"2006",
	} {
		t, err := time.Parse(dfmt, asOf)
		if err == nil {
			return t.UTC().UnixNano(), nil
		}
	}
	// Try parsing a timestamp
	ts, err := strconv.ParseInt(asOf, 10, 0)
	if err == nil {
		return time.Unix(ts, 0).UnixNano(), nil
	}
	return 0, fmt.Errorf("failed to parse asof \"%s\"", asOf)
}
