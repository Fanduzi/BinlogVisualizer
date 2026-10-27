package codehealth

import "fmt"

// minDerived is the share of a new entry's significant lines that must
// appear verbatim in its origin at the base revision.
const minDerived = 0.8

// lineage checks each entry of n that p does not have. Its content, in the
// pull request, must come at least minDerived from one entry of p as that
// entry was at the base revision; that entry is its origin. Then, for each
// origin, the new pieces plus what is left of the origin, all measured now,
// may not exceed the origin's ceiling plus tolerance plus the kind's slack
// for each piece after the first. A rename is one piece and behaves like an
// edit of the original.
func lineage(k kind, lim Limits, p, n Ledger, base, pr Measurement) []string {
	var out []string
	if k.content == nil {
		for _, key := range sortedKeys(n) {
			if _, ok := p[key]; !ok {
				out = append(out, fmt.Sprintf("%s: adds %s; %s", k.name, key, k.fix))
			}
		}
		return out
	}
	vals := k.values(pr)
	pieces := map[string][]string{}
	origins := newOriginIndex(sortedKeys(p), func(key string) []string { return k.content(base, key) })
	for _, key := range sortedKeys(n) {
		if _, ok := p[key]; ok {
			continue
		}
		origin, share := origins.best(k.content(pr, key))
		if share < minDerived {
			out = append(out, fmt.Sprintf("%s: adds %s (%d %s), a new %s over the limit; at most %.0f%% of its lines come from one baseline entry (%s), %.0f%% needed", k.name, key, n[key], k.unit, k.noun, 100*share, orNone(origin), 100*minDerived))
			continue
		}
		pieces[origin] = append(pieces[origin], key)
	}
	for _, origin := range sortedKeys(pieces) {
		succ := pieces[origin]
		left, kept := vals[origin]
		total, count := left, len(succ)
		if kept {
			count++
		}
		for _, s := range succ {
			total += vals[s]
		}
		allowed := p[origin] + k.tolerance(lim) + k.slack*(count-1)
		if total > allowed {
			out = append(out, fmt.Sprintf("%s: %s became %v with %d %s in total (left in the original: %d), more than its ceiling %d + %d tolerance + %d for each piece after the first", k.name, origin, succ, total, k.unit, left, p[origin], k.tolerance(lim), k.slack))
		}
	}
	return out
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}

// originIndex finds which candidate a piece of content came from.
type originIndex struct {
	keys    []string
	content func(string) []string
	counts  map[string]map[string]int
}

func newOriginIndex(keys []string, content func(string) []string) *originIndex {
	return &originIndex{keys: keys, content: content, counts: map[string]map[string]int{}}
}

// best returns the candidate that holds the largest share of lines, each
// line of the candidate matching at most once, and that share.
func (o *originIndex) best(lines []string) (string, float64) {
	if len(lines) == 0 {
		return "", 0
	}
	bestKey, bestShare := "", 0.0
	for _, key := range o.keys {
		if share := sharedShare(lines, o.multiset(key)); share > bestShare {
			bestKey, bestShare = key, share
		}
	}
	return bestKey, bestShare
}

func (o *originIndex) multiset(key string) map[string]int {
	if c, ok := o.counts[key]; ok {
		return c
	}
	c := map[string]int{}
	for _, l := range o.content(key) {
		c[l]++
	}
	o.counts[key] = c
	return c
}

func sharedShare(lines []string, from map[string]int) float64 {
	used := map[string]int{}
	matched := 0
	for _, l := range lines {
		if used[l] < from[l] {
			used[l]++
			matched++
		}
	}
	return float64(matched) / float64(len(lines))
}
