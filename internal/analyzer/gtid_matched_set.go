// Package analyzer keeps the GTIDs a GTID selector matched as an interval set.
// input: the canonical identity of every transaction read (observe, or read with keep for a matched one) and of each matched MySQL one (add).
// output: selection.matched_gtid_set (MySQL uuid:1-4:6-9 per server UUID, MariaDB domain-server-seq or domain-server-first..last, per server or shared by the servers of a domain) and the matched transaction count.
// pos: Analyzer selection evidence (#379, #381 QA F1); memory is O(intervals), not O(matches); limit errors count the stopping transaction (#384 QA F2), report the smaller form a domain can still be counted in, and name the form that was dropped (#390 QA).
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
// Intervals counts what the matched transactions read so far need, the one
// that stopped the run included (#384 QA F2): per MariaDB domain the smaller
// of its per-server and shared forms (#390 QA: not the shared cost of n
// ranges times the servers when the per-server count is smaller), each
// exact or a true lower bound. Reason says which count and message apply.
type MatchedGTIDLimitError struct {
	Limit     int
	Intervals int
	// Dropped reports that a MariaDB form was dropped, so Reason is not
	// MatchedGTIDLimitPlain; the count may then be at or below Limit.
	Dropped bool
	Reason  MatchedGTIDLimitReason
}

// MatchedGTIDLimitReason tells why a matched set stopped analyze.
type MatchedGTIDLimitReason string

const (
	// MatchedGTIDLimitPlain: the matched transactions need more than Limit
	// intervals in any form the set can render.
	MatchedGTIDLimitPlain MatchedGTIDLimitReason = ""
	// MatchedGTIDLimitRepeat: a MariaDB domain needs the per-server form,
	// because a sequence number repeats, after that form was dropped when
	// the per-server ranges of all domains passed Limit.
	MatchedGTIDLimitRepeat MatchedGTIDLimitReason = "repeat"
	// MatchedGTIDLimitPerServer: the shared form was dropped earlier (the
	// shared ranges or the sequence numbers read passed Limit), so Intervals
	// counts the per-server form only; the shared one could need fewer.
	MatchedGTIDLimitPerServer MatchedGTIDLimitReason = "per_server"
	// MatchedGTIDLimitFormDropped: Intervals fits in Limit, but the form
	// that would hold it was dropped when all domains together passed Limit.
	MatchedGTIDLimitFormDropped MatchedGTIDLimitReason = "form_dropped"
)

func (e *MatchedGTIDLimitError) Error() string {
	switch e.Reason {
	case MatchedGTIDLimitRepeat:
		return fmt.Sprintf("GTID selectors matched MariaDB transactions that need at least %d separate GTID intervals kept per server, because a sequence number repeats; that form was dropped when all domains together passed %d. Narrow --include-gtids/--exclude-gtids or split the input", e.Intervals, e.Limit)
	case MatchedGTIDLimitPerServer:
		return fmt.Sprintf("GTID selectors matched MariaDB transactions that need at least %d separate GTID intervals kept per server; at most %d can be tracked, and the shared form, which can need fewer, was dropped when all domains together passed %d. Narrow --include-gtids/--exclude-gtids or split the input", e.Intervals, e.Limit, e.Limit)
	case MatchedGTIDLimitFormDropped:
		return fmt.Sprintf("GTID selectors matched MariaDB transactions that need at least %d separate GTID intervals, within the limit of %d, but the form that holds them was dropped when all domains together passed %d. Narrow --include-gtids/--exclude-gtids or split the input", e.Intervals, e.Limit, e.Limit)
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

// stopError is the MariaDB limit error: the count from needed, taken before
// any form is freed, and the reason that matches it.
func (m *matchedGTIDSet) stopError() error {
	intervals, perServerOnly := m.needed()
	reason := MatchedGTIDLimitPlain
	switch {
	case m.repeatWithoutForm():
		reason = MatchedGTIDLimitRepeat
	case perServerOnly:
		reason = MatchedGTIDLimitPerServer
	case intervals <= MatchedGTIDIntervalLimit:
		reason = MatchedGTIDLimitFormDropped
	}
	return &MatchedGTIDLimitError{Limit: MatchedGTIDIntervalLimit, Intervals: intervals, Dropped: reason != MatchedGTIDLimitPlain, Reason: reason}
}

// repeatWithoutForm reports a domain whose sequence numbers repeat after the
// per-server form was dropped: it has no form left because of the repeat.
func (m *matchedGTIDSet) repeatWithoutForm() bool {
	if !m.exactOff {
		return false
	}
	for _, d := range m.maria {
		if d.conflict && d.matched() {
			return true
		}
	}
	return false
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
	if keep && repeat && m.exactOff {
		// Taken while the shared ranges are still there.
		d.repeat = d.shared.repeatDelta(seq, server, d.servers)
	}
	// The seen ranges are checked before the matched identity is added, as
	// for an unmatched one (#390 QA P2): adding first could drop the
	// per-server form while the shared form was still kept and then fail on
	// the shared cost.
	if m.seenTotal > MatchedGTIDIntervalLimit {
		if err := m.dropShared(); err != nil {
			return err
		}
	}
	if keep {
		return m.add(identity)
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

// matched reports that a domain holds matched transactions.
func (d *mariaDomain) matched() bool {
	return len(d.servers) != 0 || d.exactN != 0
}

// form reports which form a domain renders and its entry count. ok is false
// when the domain has matches but no form left.
func (m *matchedGTIDSet) form(d *mariaDomain) (useShared bool, cost int, ok bool) {
	if !d.matched() {
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

// needed is the number of entries the set needs, what a
// MatchedGTIDLimitError reports, never a placeholder. Callers take it before
// a form is freed. Each domain counts the smaller of the forms it can still
// be counted in, whether or not the form is still kept (#390 QA P2: once the
// per-server ranges are freed, their count is derived from the shared ones):
//   - per server: always valid; counted directly, or derived (perServer)
//     while the shared ranges are kept.
//   - shared: valid while no sequence number of the domain repeated and the
//     shared ranges are kept.
//
// perServerOnly reports a domain counted per server only because its shared
// ranges were freed earlier; its shared form could need fewer entries.
func (m *matchedGTIDSet) needed() (total int, perServerOnly bool) {
	total = m.mysqlCost
	for _, d := range m.maria {
		if !d.matched() {
			continue
		}
		per, perOK := 0, d.exact != nil || !m.sharedOff
		if perOK {
			per = m.perServer(d)
		}
		shared, sharedOK := d.shared.n*len(d.servers), !m.sharedOff && !d.conflict
		switch {
		case perOK && sharedOK:
			total += min(per, shared)
		case perOK:
			total += per
			perServerOnly = perServerOnly || !d.conflict
		case sharedOK:
			total += shared
		}
	}
	return total, perServerOnly
}

func (m *matchedGTIDSet) recost(d *mariaDomain) error {
	_, cost, ok := m.form(d)
	if !ok {
		return m.stopError()
	}
	m.cost += cost - d.cost
	d.cost = cost
	if m.cost > MatchedGTIDIntervalLimit {
		return m.stopError()
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
// before the ranges are freed or derived from the shared ranges after, or
// the shared count when it is smaller (#390 QA P2).
func (m *matchedGTIDSet) dropExact() error {
	m.exactOff = true
	var err error
	for _, d := range m.maria {
		if _, _, ok := m.form(d); !ok {
			err = m.stopError()
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
// domains then need the per-server form. The error count is taken first,
// while the shared ranges still count (#390 QA P3: a domain whose shared
// form fits reports that count, not the per-server one).
func (m *matchedGTIDSet) dropShared() error {
	for _, d := range m.maria {
		if !m.exactOff || !d.matched() {
			continue
		}
		err := m.stopError()
		m.sharedOff = true
		for _, d := range m.maria {
			d.shared, d.seen = rangeList{}, rangeList{}
		}
		return err
	}
	m.sharedOff = true
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
