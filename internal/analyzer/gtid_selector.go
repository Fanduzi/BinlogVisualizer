// Package analyzer parses and matches transaction-group GTID selectors.
// input: MySQL UUID sequence/range sets or MariaDB domain-server-sequence identities.
// output: canonical single-flavor include/exclude selectors with exclude-wins matching.
// pos: selector grammar and identity boundary used by Analyzer before retaining complete transaction groups.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type sequenceRange struct {
	start uint64
	end   uint64
}

// GTIDSelector is a canonical include/exclude predicate for one GTID flavor.
type GTIDSelector struct {
	flavor       string
	include      []string
	exclude      []string
	mysqlInclude map[string][]sequenceRange
	mysqlExclude map[string][]sequenceRange
	mariaInclude map[string]struct{}
	mariaExclude map[string]struct{}
}

// ParseGTIDSelector parses selectors and rejects mixed or malformed flavors.
func ParseGTIDSelector(include, exclude []string) (*GTIDSelector, error) {
	selector := &GTIDSelector{}
	var err error
	selector.flavor, err = selectorFlavor(include, exclude)
	if err != nil {
		return nil, err
	}
	if selector.flavor == "" {
		return nil, fmt.Errorf("at least one GTID selector is required")
	}
	switch selector.flavor {
	case "mysql":
		selector.mysqlInclude, selector.include, err = parseMySQLGTIDSets(include)
		if err == nil {
			selector.mysqlExclude, selector.exclude, err = parseMySQLGTIDSets(exclude)
		}
	case "mariadb":
		selector.mariaInclude, selector.include, err = parseMariaDBGTIDs(include)
		if err == nil {
			selector.mariaExclude, selector.exclude, err = parseMariaDBGTIDs(exclude)
		}
	}
	if err != nil {
		return nil, err
	}
	return selector, nil
}

// Flavor returns mysql or mariadb.
func (s *GTIDSelector) Flavor() string {
	if s == nil {
		return ""
	}
	return s.flavor
}

// Include returns canonical include selectors.
func (s *GTIDSelector) Include() []string {
	if s == nil {
		return nil
	}
	return append([]string(nil), s.include...)
}

// Exclude returns canonical exclude selectors.
func (s *GTIDSelector) Exclude() []string {
	if s == nil {
		return nil
	}
	return append([]string(nil), s.exclude...)
}

// Match applies include first and exclude second; anonymous identities never match an include list.
func (s *GTIDSelector) Match(gtid string) (bool, error) {
	if s == nil {
		return true, nil
	}
	if strings.TrimSpace(gtid) == "" {
		return len(s.include) == 0, nil
	}
	switch s.flavor {
	case "mysql":
		sid, sequence, err := parseMySQLTransactionGTID(gtid)
		if err != nil {
			return false, err
		}
		included := len(s.mysqlInclude) == 0 || sequenceInRanges(sequence, s.mysqlInclude[sid])
		return included && !sequenceInRanges(sequence, s.mysqlExclude[sid]), nil
	case "mariadb":
		identity, err := parseMariaDBGTID(gtid)
		if err != nil {
			return false, err
		}
		_, included := s.mariaInclude[identity]
		_, excluded := s.mariaExclude[identity]
		return (len(s.mariaInclude) == 0 || included) && !excluded, nil
	default:
		return false, fmt.Errorf("unresolved GTID selector flavor")
	}
}

func (s *GTIDSelector) canonicalIdentity(gtid string) (string, error) {
	if s == nil || strings.TrimSpace(gtid) == "" {
		return "", nil
	}
	if s.flavor == "mysql" {
		sid, sequence, err := parseMySQLTransactionGTID(gtid)
		if err != nil {
			return "", err
		}
		return sid + ":" + strconv.FormatUint(sequence, 10), nil
	}
	return parseMariaDBGTID(gtid)
}

func selectorFlavor(groups ...[]string) (string, error) {
	flavor := ""
	for _, group := range groups {
		for _, token := range splitGTIDTokens(group) {
			candidate := "mariadb"
			if strings.Contains(token, ":") {
				candidate = "mysql"
			}
			if flavor != "" && flavor != candidate {
				return "", fmt.Errorf("mixed GTID selector flavors: %s and %s", flavor, candidate)
			}
			flavor = candidate
		}
	}
	return flavor, nil
}

func splitGTIDTokens(values []string) []string {
	var result []string
	for _, value := range values {
		for _, token := range strings.Split(value, ",") {
			if token = strings.TrimSpace(token); token != "" {
				result = append(result, token)
			}
		}
	}
	return result
}

func parseMySQLGTIDSets(values []string) (map[string][]sequenceRange, []string, error) {
	sets := make(map[string][]sequenceRange)
	for _, token := range splitGTIDTokens(values) {
		parts := strings.Split(token, ":")
		if len(parts) < 2 {
			return nil, nil, fmt.Errorf("invalid MySQL GTID set %q", token)
		}
		sid, err := normalizeUUID(parts[0])
		if err != nil {
			return nil, nil, err
		}
		for _, interval := range parts[1:] {
			rng, err := parseSequenceRange(interval)
			if err != nil {
				return nil, nil, fmt.Errorf("invalid MySQL GTID set %q: %w", token, err)
			}
			sets[sid] = append(sets[sid], rng)
		}
	}
	canonical := make([]string, 0, len(sets))
	for sid, ranges := range sets {
		ranges = mergeSequenceRanges(ranges)
		sets[sid] = ranges
		parts := make([]string, 1, len(ranges)+1)
		parts[0] = sid
		for _, rng := range ranges {
			if rng.start == rng.end {
				parts = append(parts, strconv.FormatUint(rng.start, 10))
			} else {
				parts = append(parts, strconv.FormatUint(rng.start, 10)+"-"+strconv.FormatUint(rng.end, 10))
			}
		}
		canonical = append(canonical, strings.Join(parts, ":"))
	}
	sort.Strings(canonical)
	return sets, canonical, nil
}

func parseMySQLTransactionGTID(value string) (string, uint64, error) {
	sets, _, err := parseMySQLGTIDSets([]string{value})
	if err != nil || len(sets) != 1 {
		return "", 0, fmt.Errorf("invalid MySQL transaction GTID %q", value)
	}
	for sid, ranges := range sets {
		if len(ranges) != 1 || ranges[0].start != ranges[0].end {
			return "", 0, fmt.Errorf("MySQL transaction GTID %q is not one exact identity", value)
		}
		return sid, ranges[0].start, nil
	}
	return "", 0, fmt.Errorf("invalid MySQL transaction GTID %q", value)
}

func transactionGTIDFlavor(value string) (string, error) {
	if strings.Contains(value, ":") {
		if _, _, err := parseMySQLTransactionGTID(value); err != nil {
			return "", err
		}
		return "mysql", nil
	}
	if _, err := parseMariaDBGTID(value); err != nil {
		return "", err
	}
	return "mariadb", nil
}

func normalizeUUID(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if len(value) != 36 {
		return "", fmt.Errorf("invalid MySQL SID %q", value)
	}
	for index, ch := range value {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			if ch != '-' {
				return "", fmt.Errorf("invalid MySQL SID %q", value)
			}
			continue
		}
		if !strings.ContainsRune("0123456789abcdef", ch) {
			return "", fmt.Errorf("invalid MySQL SID %q", value)
		}
	}
	return value, nil
}

func parseSequenceRange(value string) (sequenceRange, error) {
	parts := strings.Split(value, "-")
	if len(parts) > 2 || len(parts) == 0 {
		return sequenceRange{}, fmt.Errorf("invalid sequence range %q", value)
	}
	start, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil || start == 0 {
		return sequenceRange{}, fmt.Errorf("invalid sequence range %q", value)
	}
	end := start
	if len(parts) == 2 {
		end, err = strconv.ParseUint(parts[1], 10, 64)
		if err != nil || end < start {
			return sequenceRange{}, fmt.Errorf("invalid sequence range %q", value)
		}
	}
	return sequenceRange{start: start, end: end}, nil
}

func mergeSequenceRanges(ranges []sequenceRange) []sequenceRange {
	sort.Slice(ranges, func(i, j int) bool {
		return ranges[i].start < ranges[j].start || ranges[i].start == ranges[j].start && ranges[i].end < ranges[j].end
	})
	merged := make([]sequenceRange, 0, len(ranges))
	for _, current := range ranges {
		last := len(merged) - 1
		if last < 0 || merged[last].end != ^uint64(0) && current.start > merged[last].end+1 {
			merged = append(merged, current)
			continue
		}
		if current.end > merged[last].end {
			merged[last].end = current.end
		}
	}
	return merged
}

func sequenceInRanges(sequence uint64, ranges []sequenceRange) bool {
	for _, rng := range ranges {
		if sequence >= rng.start && sequence <= rng.end {
			return true
		}
	}
	return false
}

func parseMariaDBGTIDs(values []string) (map[string]struct{}, []string, error) {
	sets := make(map[string]struct{})
	for _, token := range splitGTIDTokens(values) {
		identity, err := parseMariaDBGTID(token)
		if err != nil {
			return nil, nil, err
		}
		sets[identity] = struct{}{}
	}
	canonical := make([]string, 0, len(sets))
	for identity := range sets {
		canonical = append(canonical, identity)
	}
	sort.Strings(canonical)
	return sets, canonical, nil
}

func parseMariaDBGTID(value string) (string, error) {
	parts := strings.Split(strings.TrimSpace(value), "-")
	if len(parts) != 3 {
		return "", fmt.Errorf("invalid MariaDB GTID %q", value)
	}
	canonical := make([]string, 3)
	for index, part := range parts {
		number, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			return "", fmt.Errorf("invalid MariaDB GTID %q", value)
		}
		canonical[index] = strconv.FormatUint(number, 10)
	}
	return strings.Join(canonical, "-"), nil
}
