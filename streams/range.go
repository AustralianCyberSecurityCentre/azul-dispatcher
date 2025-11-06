package streams

// Lifted from golang lib http/fs and simplified to match the store api.
// As a consequence, range handling is only partially compliant with RFC7233.
// In particular it only supports a single byte range request and does not
// support relative offsets from end of requested object.

import (
	"errors"
	"strconv"
	"strings"
)

type httpRange struct {
	start, length int64
}

// ParseRange handles parsing a useful subset of RFC7233 HTTP Range Header.
func parseRange(s string) (httpRange, error) {
	if s == "" {
		return httpRange{0, -1}, nil
	}
	// only one supported unit type 'bytes'
	const b = "bytes="
	if !strings.HasPrefix(s, b) {
		return httpRange{}, errors.New("invalid range (no bytes= prefix)")
	}
	// don't support multiple ranges in header
	if strings.Contains(s, ",") {
		return httpRange{}, errors.New("invalid range (multiple not supported)")
	}
	ra := strings.TrimSpace(s[len(b):])
	// keep it simple, no -100-400 type ranges
	if strings.Count(ra, "-") > 1 {
		return httpRange{}, errors.New("invalid range (relative with range not supported)")
	}
	// missing any range char '-' or only range char
	i := strings.Index(ra, "-")
	if i < 0 || ra == "-" {
		return httpRange{}, errors.New("invalid range (no range)")
	}
	start, end := strings.TrimSpace(ra[:i]), strings.TrimSpace(ra[i+1:])
	// empty start means relative from end eg. -500
	if start == "" {
		en, err := strconv.ParseInt(end, 10, 64)
		if err != nil {
			return httpRange{}, errors.New("invalid range (invalid end)")
		}
		return httpRange{
			start:  -en,
			length: en,
		}, nil
	}
	st, err := strconv.ParseInt(start, 10, 64)
	if err != nil {
		return httpRange{}, errors.New("invalid range (non numeric)")
	}
	// empty end means til EOF eg. 100-
	if end == "" {
		return httpRange{
			start:  st,
			length: -1, // til eof
		}, nil
	}
	// explicitly listed range eg. 100-500
	en, err := strconv.ParseInt(end, 10, 64)
	if err != nil || st > en {
		return httpRange{}, errors.New("invalid range (invalid end)")
	}
	return httpRange{
		start:  st,
		length: en - st + 1,
	}, nil
}
