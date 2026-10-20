// Package gtidtoken parses MariaDB GTID selector tokens. The analyzer's
// selector and compare's scope check share it, so both read the same forms.
package gtidtoken

import (
	"math"
	"sort"
	"strconv"
	"strings"
)

// MariaDB is one selector token: domain-server-seq, or
// domain-server-start..end for a sequence range of one server.
type MariaDB struct {
	Domain, Server, Start, End uint64
}

// ParseMariaDB parses a token. Whitespace around the whole token is ignored;
// whitespace next to ".." is rejected, the way MySQL ranges ("1-5") take no
// inner whitespace.
func ParseMariaDB(value string) (MariaDB, bool) {
	first, last, isRange := strings.Cut(strings.TrimSpace(value), "..")
	parts := strings.Split(first, "-")
	if len(parts) != 3 {
		return MariaDB{}, false
	}
	var numbers [3]uint64
	for i, part := range parts {
		number, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			return MariaDB{}, false
		}
		numbers[i] = number
	}
	token := MariaDB{Domain: numbers[0], Server: numbers[1], Start: numbers[2], End: numbers[2]}
	if isRange {
		end, err := strconv.ParseUint(last, 10, 64)
		if err != nil || end < token.Start {
			return MariaDB{}, false
		}
		token.End = end
	}
	return token, true
}

// Identity is the canonical domain-server-start text.
func (t MariaDB) Identity() string {
	return strconv.FormatUint(t.Domain, 10) + "-" + strconv.FormatUint(t.Server, 10) + "-" + strconv.FormatUint(t.Start, 10)
}

// String is the canonical token: the identity, plus "..end" for a range.
func (t MariaDB) String() string {
	if t.End == t.Start {
		return t.Identity()
	}
	return t.Identity() + ".." + strconv.FormatUint(t.End, 10)
}

// MergeMariaDB parses every token (comma-separated values allowed) and
// returns the selection with overlapping and adjacent ranges of each
// domain-server merged, ordered by domain, server and sequence. It reports
// false when a token does not parse.
func MergeMariaDB(values []string) ([]string, bool) {
	var tokens []MariaDB
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			if strings.TrimSpace(part) == "" {
				continue
			}
			token, ok := ParseMariaDB(part)
			if !ok {
				return nil, false
			}
			tokens = append(tokens, token)
		}
	}
	sort.Slice(tokens, func(i, j int) bool {
		a, b := tokens[i], tokens[j]
		if a.Domain != b.Domain {
			return a.Domain < b.Domain
		}
		if a.Server != b.Server {
			return a.Server < b.Server
		}
		return a.Start < b.Start
	})
	out := make([]string, 0, len(tokens))
	for i := 0; i < len(tokens); {
		cur := tokens[i]
		for i++; i < len(tokens); i++ {
			next := tokens[i]
			if next.Domain != cur.Domain || next.Server != cur.Server || (cur.End != math.MaxUint64 && next.Start > cur.End+1) {
				break
			}
			if next.End > cur.End {
				cur.End = next.End
			}
		}
		out = append(out, cur.String())
	}
	return out, true
}
