// Package asof "as of" implements utils for building "as of"/time travel queries
package asof // import "a4.io/blobstash/pkg/asof"
import (
	"fmt"
	"strconv"
	"time"
)

func ParseAsOf(asof string) (int64, error) {
	// Try to parse duration (-10h)
	dur, err := time.ParseDuration(asof)
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
		t, err := time.Parse(dfmt, asof)
		if err == nil {
			return t.UTC().UnixNano(), nil
		}
	}
	// Try parsing a timestamp
	ts, err := strconv.ParseInt(asof, 10, 0)
	if err == nil {
		return time.Unix(ts, 0).UnixNano(), nil
	}
	return 0, fmt.Errorf("failed to parse asof \"%s\"", asof)
}
