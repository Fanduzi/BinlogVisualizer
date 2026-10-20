// Package analyzer keeps the GTIDs a GTID selector matched as an interval set.
// input: the canonical identity of every transaction read (observe, or read with keep for a matched one) and of each matched MySQL one (add).
// output: selection.matched_gtid_set (MySQL uuid:1-4:6-9 per server UUID, MariaDB domain-server-seq or domain-server-first..last, per server or shared by the servers of a domain) and the matched transaction count.
// pos: Analyzer selection evidence (#379, #381 QA F1); memory is O(intervals), not O(matches); limit errors count the stopping transaction (#384 QA F2).
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
// Intervals is exact or a true lower bound of what the matched transactions
// read so far need, the one that stopped the run included (#384 QA F2).
type MatchedGTIDLimitError struct {
	Limit     int
	Intervals int
	// Dropped reports that a MariaDB domain needs the per-server form after
	// it was dropped for every domain (a sequence number repeats), rather
	// than the rendered set passing Limit. The count may then be at or
	// below Limit.
	Dropped bool
}

func (e *MatchedGTIDLimitError) Error() string {
	if e.Dropped {
		return fmt.Sprintf("GTID selectors matched MariaDB transactions that need at least %d separate GTID intervals kept per server, because a sequence number repeats; that form was dropped when all domains together passed %d. Narrow --include-gtids/--exclude-gtids or split the input", e.Intervals, e.Limit)
	}
	return fmt.Sprintf("GTID selectors matched transactions that need at least %d separate GTID intervals; at most %d can be tracked. Narrow --include-gtids/--exclude-gtids or split the input", e.Intervals, e.Limit)
}

// matchedGTIDSet keeps the GTIDs of matched transactions with retained
// evidence as ranges.
//
// MySQL: ranges per server UUID.
//
// MariaDB (#381 QA F1): several servers may write one domain with one
// sequence (alternating writers), so ranges per domain-server pair can leave
// one range per transaction. Each domain keeps two forms and the set renders
// the smaller one that is exact:
//   - exact: ranges per domain-server pair.
//   - shared: ranges over the domain's sequence numbers plus the servers
//     that wrote them, rendered as the same ranges once per server
//     (0-7-1..9, 0-8-1..9). Together they cover every matched identity; the
//     extra identities they name are not in the input, because shared is
//     used only while no sequence number of the domain was seen twice among
//     all transactions read (matched or not). A repeat, even of the same
//     identity, makes the domain use exact.
//
// Memory is O(intervals): every form holds at most MatchedGTIDIntervalLimit
// ranges in total; a form that grows past that is dropped, and analyze fails
// when a domain is left with no form or the rendered entries exceed the limit.
type matchedGTIDSet struct {
	flavor string
	mysql  map[string]*rangeList
	maria  map[uint64]*mariaDomain
	// count is the number of matched transactions added, repeats included.
	count int
	// cost is the number of rendered entries (intervals) of the set;
	// mysqlCost is the MySQL part of it.
	cost      int
	mysqlCost int

	exactTotal  int
	sharedTotal int
	seenTotal   int
	exactOff    bool
	sharedOff   bool
}

type mariaDomain struct {
	exact    map[uint64]*rangeList
	exactN   int
	shared   rangeList
	servers  map[uint64]struct{}
	seen     rangeList
	conflict bool
	cost     int
	// repeat corrects the per-server count derived from shared for the
	// matched identity whose sequence number made the domain conflict while
	// the per-server form was gone: shared keeps one label per sequence
	// number, so it cannot hold that identity (see repeatDelta).
	repeat int
}

func newMatchedGTIDSet() *matchedGTIDSet {
	return &matchedGTIDSet{mysql: make(map[string]*rangeList), maria: make(map[uint64]*mariaDomain)}
}

func (m *matchedGTIDSet) limitError(intervals int) error {
	return &MatchedGTIDLimitError{Limit: MatchedGTIDIntervalLimit, Intervals: intervals}
}

// droppedError is the error of a domain left with no form.
func (m *matchedGTIDSet) droppedError() error {
	return &MatchedGTIDLimitError{Limit: MatchedGTIDIntervalLimit, Intervals: m.needed(), Dropped: true}
}

// observe records the GTID of an unmatched transaction (or one whose
// evidence is not kept) read from the input; see read.
func (m *matchedGTIDSet) observe(identity string) error {
	return m.read(identity, false)
}

// read records the GTID of a transaction read from the input, matched or
// not, so a MariaDB domain knows whether a sequence number repeats; with keep
// it also adds the identity as a matched transaction, in the same step, so a
// limit error counts it (#384 QA F2: the count was taken on the repeat,
// before the repeating transaction was added). MySQL identities only go to
// add.
func (m *matchedGTIDSet) read(identity string, keep bool) error {
	domain, server, seq, ok := mariaIdentity(identity)
	if !ok || m.sharedOff {
		if keep {
			return m.add(identity)
		}
		return nil
	}
	d := m.domain(domain)
	delta, present := d.seen.add(seq)
	m.seenTotal += delta
	repeat := present && !d.conflict
	if repeat {
		d.conflict = true
	}
	if keep {
		if repeat && m.exactOff {
			d.repeat = d.shared.repeatDelta(seq, server, d.servers)
		}
		return m.add(identity)
	}
	if m.seenTotal > MatchedGTIDIntervalLimit {
		return m.dropShared()
	}
	if repeat {
		return m.recost(d)
	}
	return nil
}

// add records one canonical identity (MySQL uuid:n or MariaDB d-s-n) of a
// matched transaction. It returns a MatchedGTIDLimitError when the set would
// need more than MatchedGTIDIntervalLimit intervals.
func (m *matchedGTIDSet) add(identity string) error {
	if domain, server, seq, ok := mariaIdentity(identity); ok {
		m.flavor = "mariadb"
		m.count++
		d := m.domain(domain)
		if !m.exactOff {
			ranges := d.exact[server]
			if ranges == nil {
				ranges = &rangeList{}
				d.exact[server] = ranges
			}
			delta, _ := ranges.add(seq)
			d.exactN += delta
			m.exactTotal += delta
		}
		if !m.sharedOff {
			delta, _ := d.shared.addLabeled(seq, server)
			d.servers[server] = struct{}{}
			m.sharedTotal += delta
		}
		if m.exactTotal > MatchedGTIDIntervalLimit {
			if err := m.dropExact(); err != nil {
				return err
			}
		}
		if m.sharedTotal > MatchedGTIDIntervalLimit || m.seenTotal > MatchedGTIDIntervalLimit {
			return m.dropShared()
		}
		return m.recost(d)
	}
	source, seq, err := splitMySQLIdentity(identity)
	if err != nil {
		return err
	}
	m.flavor = "mysql"
	m.count++
	ranges := m.mysql[source]
	if ranges == nil {
		ranges = &rangeList{}
		m.mysql[source] = ranges
	}
	delta, _ := ranges.add(seq)
	m.cost += delta
	m.mysqlCost += delta
	if m.cost > MatchedGTIDIntervalLimit {
		return m.limitError(m.cost)
	}
	return nil
}

func (m *matchedGTIDSet) domain(domain uint64) *mariaDomain {
	d := m.maria[domain]
	if d == nil {
		d = &mariaDomain{servers: make(map[uint64]struct{})}
		if !m.exactOff {
			d.exact = make(map[uint64]*rangeList)
		}
		m.maria[domain] = d
	}
	return d
}

// form reports which form a domain renders and its entry count. ok is false
// when the domain has matches but no form left.
func (m *matchedGTIDSet) form(d *mariaDomain) (useShared bool, cost int, ok bool) {
	if len(d.servers) == 0 && d.exactN == 0 {
		return false, 0, true
	}
	sharedOK := !m.sharedOff && !d.conflict
	sharedCost := d.shared.n * len(d.servers)
	switch {
	case !m.exactOff && (!sharedOK || d.exactN <= sharedCost):
		return false, d.exactN, true
	case sharedOK:
		return true, sharedCost, true
	}
	return false, 0, false
}

// perServer is the number of per-server ranges domain d needs: counted
// directly while that form is kept, otherwise derived from the shared ranges
// labeled by server (ranges plus server changes inside them), which is exact
// while no sequence number of the domain repeated, plus the bound for the
// matched identity that repeated one (d.repeat). Analyze stops on that
// repeat, so no later identity makes the derived count drift.
func (m *matchedGTIDSet) perServer(d *mariaDomain) int {
	if d.exact != nil {
		return d.exactN
	}
	return d.shared.n + d.shared.changes + d.repeat
}

// needed is the number of entries the set needs: each domain's rendered form,
// or its per-server count when it has no form left. It is what a
// MatchedGTIDLimitError reports, never a placeholder.
func (m *matchedGTIDSet) needed() int {
	total := m.mysqlCost
	for _, d := range m.maria {
		if _, cost, ok := m.form(d); ok {
			total += cost
		} else {
			total += m.perServer(d)
		}
	}
	return total
}

func (m *matchedGTIDSet) recost(d *mariaDomain) error {
	_, cost, ok := m.form(d)
	if !ok {
		return m.droppedError()
	}
	m.cost += cost - d.cost
	d.cost = cost
	if m.cost > MatchedGTIDIntervalLimit {
		return m.limitError(m.cost)
	}
	return nil
}

func (m *matchedGTIDSet) recostAll() error {
	for _, d := range m.maria {
		if err := m.recost(d); err != nil {
			return err
		}
	}
	return nil
}

// dropExact frees the per-server form of every MariaDB domain at once, when
// the per-server ranges of all domains together pass the limit. A domain
// that already needs that form (a sequence number repeated), or comes to need
// it later, then fails with exit 10 even if its own per-server ranges would
// fit: dropping per domain would be finer, but this fails safe (no wrong set)
// and keeps memory bounded. The error reports the per-server count, taken
// before the ranges are freed or derived from the shared ranges after.
func (m *matchedGTIDSet) dropExact() error {
	m.exactOff = true
	var err error
	for _, d := range m.maria {
		if _, _, ok := m.form(d); !ok {
			err = m.droppedError()
			break
		}
	}
	for _, d := range m.maria {
		d.exact, d.exactN = nil, 0
	}
	m.exactTotal = 0
	if err != nil {
		return err
	}
	return m.recostAll()
}

// dropShared frees the shared form (and the repeat check) of every domain;
// domains then need the per-server form. The error count is taken first.
func (m *matchedGTIDSet) dropShared() error {
	m.sharedOff = true
	for _, d := range m.maria {
		if _, _, ok := m.form(d); !ok {
			err := m.droppedError()
			for _, d := range m.maria {
				d.shared, d.seen = rangeList{}, rangeList{}
			}
			return err
		}
	}
	for _, d := range m.maria {
		d.shared, d.seen = rangeList{}, rangeList{}
	}
	m.sharedTotal, m.seenTotal = 0, 0
	return m.recostAll()
}

// strings renders the set: one MySQL GTID set per server UUID, sorted by UUID,
// or one MariaDB entry per range (d-s-n or d-s-first..last), sorted by domain,
// server and sequence. Every entry is valid --include-gtids input, and the
// entries select exactly the matched transactions of the same input.
func (m *matchedGTIDSet) strings() []string {
	if m == nil {
		return nil
	}
	if m.flavor == "mariadb" {
		return m.mariaStrings()
	}
	if len(m.mysql) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m.mysql))
	for key := range m.mysql {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		var b strings.Builder
		b.WriteString(key)
		m.mysql[key].each(func(r sequenceRange) {
			b.WriteByte(':')
			b.WriteString(strconv.FormatUint(r.start, 10))
			if r.end != r.start {
				b.WriteByte('-')
				b.WriteString(strconv.FormatUint(r.end, 10))
			}
		})
		out = append(out, b.String())
	}
	return out
}

func (m *matchedGTIDSet) mariaStrings() []string {
	domains := make([]uint64, 0, len(m.maria))
	for domain := range m.maria {
		domains = append(domains, domain)
	}
	sortUint64s(domains)
	var out []string
	for _, domain := range domains {
		d := m.maria[domain]
		useShared, _, _ := m.form(d)
		servers := make([]uint64, 0, len(d.servers)+len(d.exact))
		if useShared {
			for server := range d.servers {
				servers = append(servers, server)
			}
		} else {
			for server := range d.exact {
				servers = append(servers, server)
			}
		}
		sortUint64s(servers)
		for _, server := range servers {
			prefix := strconv.FormatUint(domain, 10) + "-" + strconv.FormatUint(server, 10) + "-"
			ranges := &d.shared
			if !useShared {
				ranges = d.exact[server]
			}
			ranges.each(func(r sequenceRange) {
				entry := prefix + strconv.FormatUint(r.start, 10)
				if r.end != r.start {
					entry += ".." + strconv.FormatUint(r.end, 10)
				}
				out = append(out, entry)
			})
		}
	}
	return out
}

func sortUint64s(values []uint64) {
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
}

func splitMySQLIdentity(identity string) (source string, seq uint64, err error) {
	i := strings.LastIndexByte(identity, ':')
	if i < 0 {
		return "", 0, fmt.Errorf("invalid matched GTID %q", identity)
	}
	seq, err = strconv.ParseUint(identity[i+1:], 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("invalid matched GTID %q", identity)
	}
	return identity[:i], seq, nil
}

// mariaIdentity splits a canonical MariaDB identity d-s-n.
func mariaIdentity(identity string) (domain, server, seq uint64, ok bool) {
	if strings.IndexByte(identity, ':') >= 0 {
		return 0, 0, 0, false
	}
	parts := strings.Split(identity, "-")
	if len(parts) != 3 {
		return 0, 0, 0, false
	}
	var err error
	if domain, err = strconv.ParseUint(parts[0], 10, 64); err != nil {
		return 0, 0, 0, false
	}
	if server, err = strconv.ParseUint(parts[1], 10, 64); err != nil {
		return 0, 0, 0, false
	}
	if seq, err = strconv.ParseUint(parts[2], 10, 64); err != nil {
		return 0, 0, 0, false
	}
	return domain, server, seq, true
}
