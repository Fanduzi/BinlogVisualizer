// Package analyzer keeps the GTIDs a GTID selector matched as an interval set.
// input: one canonical matched transaction identity at a time, in binlog order.
// output: selection.matched_gtid_set (MySQL uuid:1-4:6-9 per server UUID, MariaDB domain-server-seq or domain-server-first..last) and the matched count.
// pos: Analyzer selection evidence (#379); memory is O(intervals), not O(matches).
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// MatchedGTIDIntervalLimit caps the intervals the matched set may hold across
// all sources. Matching GTIDs that leave more gaps than this fails analyze
// with a MatchedGTIDLimitError (exit 10) instead of growing without bound.
// 100k intervals is about 3MB of memory and a few MB of JSON. Tests lower it.
var MatchedGTIDIntervalLimit = 100_000

// MatchedGTIDLimitError is the resource ceiling for the matched GTID set.
type MatchedGTIDLimitError struct {
	Limit     int
	Intervals int
}

func (e *MatchedGTIDLimitError) Error() string {
	return fmt.Sprintf("GTID selectors matched transactions that leave more than %d separate GTID intervals; narrow --include-gtids/--exclude-gtids or split the input", e.Limit)
}

// matchedGTIDSet is an interval set per source: a MySQL server UUID, or a
// MariaDB domain-server pair. Ranges per source stay sorted and disjoint, with
// adjacent ranges merged.
type matchedGTIDSet struct {
	flavor    string
	sources   map[string][]sequenceRange
	intervals int
	count     int
}

func newMatchedGTIDSet() *matchedGTIDSet {
	return &matchedGTIDSet{sources: make(map[string][]sequenceRange)}
}

// add records one canonical identity (MySQL uuid:n or MariaDB d-s-n). It
// returns a MatchedGTIDLimitError when the set would exceed the interval cap.
// A repeated identity is counted once.
func (m *matchedGTIDSet) add(identity string) error {
	source, seq, flavor, err := splitMatchedIdentity(identity)
	if err != nil {
		return err
	}
	m.flavor = flavor
	ranges := m.sources[source]
	n := len(ranges)
	// Fast path: binlog order extends or repeats the last range.
	if n > 0 && seq >= ranges[n-1].start {
		last := &ranges[n-1]
		if seq <= last.end {
			return nil
		}
		if seq == last.end+1 {
			last.end = seq
			m.count++
			return nil
		}
	}
	i := sort.Search(n, func(i int) bool { return ranges[i].end >= seq })
	if i < n && ranges[i].start <= seq {
		return nil // already present
	}
	joinLeft := i > 0 && ranges[i-1].end+1 == seq
	joinRight := i < n && seq+1 == ranges[i].start
	switch {
	case joinLeft && joinRight:
		ranges[i-1].end = ranges[i].end
		ranges = append(ranges[:i], ranges[i+1:]...)
		m.intervals--
	case joinLeft:
		ranges[i-1].end = seq
	case joinRight:
		ranges[i].start = seq
	default:
		if m.intervals+1 > MatchedGTIDIntervalLimit {
			return &MatchedGTIDLimitError{Limit: MatchedGTIDIntervalLimit, Intervals: m.intervals + 1}
		}
		ranges = append(ranges, sequenceRange{})
		copy(ranges[i+1:], ranges[i:])
		ranges[i] = sequenceRange{start: seq, end: seq}
		m.intervals++
	}
	m.sources[source] = ranges
	m.count++
	return nil
}

// strings renders the set: one MySQL GTID set per server UUID, sorted by UUID
// (usable as --include-gtids), or one MariaDB entry per range, sorted by
// domain, server and sequence.
func (m *matchedGTIDSet) strings() []string {
	if m == nil || len(m.sources) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m.sources))
	for key := range m.sources {
		keys = append(keys, key)
	}
	if m.flavor == "mariadb" {
		sort.Slice(keys, func(i, j int) bool {
			di, si := mariaSourceParts(keys[i])
			dj, sj := mariaSourceParts(keys[j])
			return di < dj || di == dj && si < sj
		})
		out := make([]string, 0, m.intervals)
		for _, key := range keys {
			for _, r := range m.sources[key] {
				entry := key + "-" + strconv.FormatUint(r.start, 10)
				if r.end != r.start {
					entry += ".." + strconv.FormatUint(r.end, 10)
				}
				out = append(out, entry)
			}
		}
		return out
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		var b strings.Builder
		b.WriteString(key)
		for _, r := range m.sources[key] {
			b.WriteByte(':')
			b.WriteString(strconv.FormatUint(r.start, 10))
			if r.end != r.start {
				b.WriteByte('-')
				b.WriteString(strconv.FormatUint(r.end, 10))
			}
		}
		out = append(out, b.String())
	}
	return out
}

func splitMatchedIdentity(identity string) (source string, seq uint64, flavor string, err error) {
	if i := strings.LastIndexByte(identity, ':'); i >= 0 {
		seq, err = strconv.ParseUint(identity[i+1:], 10, 64)
		if err != nil {
			return "", 0, "", fmt.Errorf("invalid matched GTID %q", identity)
		}
		return identity[:i], seq, "mysql", nil
	}
	i := strings.LastIndexByte(identity, '-')
	if i < 0 {
		return "", 0, "", fmt.Errorf("invalid matched GTID %q", identity)
	}
	seq, err = strconv.ParseUint(identity[i+1:], 10, 64)
	if err != nil {
		return "", 0, "", fmt.Errorf("invalid matched GTID %q", identity)
	}
	return identity[:i], seq, "mariadb", nil
}

func mariaSourceParts(key string) (domain, server uint64) {
	parts := strings.SplitN(key, "-", 2)
	domain, _ = strconv.ParseUint(parts[0], 10, 64)
	if len(parts) == 2 {
		server, _ = strconv.ParseUint(parts[1], 10, 64)
	}
	return domain, server
}
