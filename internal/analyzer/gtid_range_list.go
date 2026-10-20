// Package analyzer keeps a sorted, merged list of sequence ranges in chunks.
// input: sequence numbers in any order.
// output: disjoint ranges with adjacent numbers merged, iterated in order; optionally the label changes inside ranges.
// pos: storage for the matched GTID set (#379); an insert costs O(log n + chunk), so out-of-order input is not quadratic.
// note: if this file changes, update this header and module README.md.
package analyzer

import "sort"

// rangeChunkMax bounds a chunk; a chunk that grows past it is split in two.
const rangeChunkMax = 256

// labeledRange is one range plus the labels (MariaDB server ids) of its
// first and last numbers and how often the label changes between adjacent
// numbers inside it. Every number of a labeled list is added once (a repeat
// is reported as present and changes nothing), so these stay exact.
type labeledRange struct {
	start, end  uint64
	first, last uint64
	changes     int
}

type rangeList struct {
	chunks [][]labeledRange
	n      int
	// changes sums the label changes of all ranges. With labels, n+changes
	// is the number of ranges the list would need if it were split by label.
	changes int
}

func changed(a, b uint64) int {
	if a != b {
		return 1
	}
	return 0
}

// add inserts seq. delta is the change in the number of ranges (+1 new
// range, 0 extended, -1 two ranges joined); present reports seq was there.
func (l *rangeList) add(seq uint64) (delta int, present bool) {
	return l.addLabeled(seq, 0)
}

// addLabeled is add with a label for seq (see labeledRange).
func (l *rangeList) addLabeled(seq, label uint64) (delta int, present bool) {
	if len(l.chunks) == 0 {
		l.chunks = [][]labeledRange{{{start: seq, end: seq, first: label, last: label}}}
		l.n = 1
		return 1, false
	}
	lc := len(l.chunks) - 1
	last := &l.chunks[lc][len(l.chunks[lc])-1]
	if seq >= last.start {
		switch {
		case seq <= last.end:
			return 0, true
		case seq == last.end+1:
			last.end = seq
			c := changed(last.last, label)
			last.changes += c
			l.changes += c
			last.last = label
			return 0, false
		}
		l.insert(lc, len(l.chunks[lc]), seq, label)
		return 1, false
	}
	// seq < last.start, so some range ends at or after seq.
	c := sort.Search(len(l.chunks), func(i int) bool {
		ch := l.chunks[i]
		return ch[len(ch)-1].end >= seq
	})
	ch := l.chunks[c]
	i := sort.Search(len(ch), func(j int) bool { return ch[j].end >= seq })
	if ch[i].start <= seq {
		return 0, true
	}
	var prev *labeledRange
	if i > 0 {
		prev = &ch[i-1]
	} else if c > 0 {
		pch := l.chunks[c-1]
		prev = &pch[len(pch)-1]
	}
	joinLeft := prev != nil && prev.end+1 == seq // prev.end < seq: no overflow
	joinRight := seq+1 == ch[i].start            // seq < ch[i].start: no overflow
	switch {
	case joinLeft && joinRight:
		add := changed(prev.last, label) + changed(label, ch[i].first)
		prev.changes += add + ch[i].changes
		l.changes += add
		prev.end, prev.last = ch[i].end, ch[i].last
		l.remove(c, i)
		return -1, false
	case joinLeft:
		add := changed(prev.last, label)
		prev.changes += add
		l.changes += add
		prev.end, prev.last = seq, label
		return 0, false
	case joinRight:
		add := changed(label, ch[i].first)
		ch[i].changes += add
		l.changes += add
		ch[i].start, ch[i].first = seq, label
		return 0, false
	}
	l.insert(c, i, seq, label)
	return 1, false
}

func (l *rangeList) insert(c, i int, seq, label uint64) {
	ch := append(l.chunks[c], labeledRange{})
	copy(ch[i+1:], ch[i:])
	ch[i] = labeledRange{start: seq, end: seq, first: label, last: label}
	l.n++
	if len(ch) <= rangeChunkMax {
		l.chunks[c] = ch
		return
	}
	half := len(ch) / 2
	second := append([]labeledRange(nil), ch[half:]...)
	l.chunks[c] = ch[:half]
	l.chunks = append(l.chunks, nil)
	copy(l.chunks[c+2:], l.chunks[c+1:])
	l.chunks[c+1] = second
}

func (l *rangeList) remove(c, i int) {
	ch := l.chunks[c]
	l.chunks[c] = append(ch[:i], ch[i+1:]...)
	l.n--
	if len(l.chunks[c]) == 0 {
		l.chunks = append(l.chunks[:c], l.chunks[c+1:]...)
	}
}

func (l *rangeList) each(fn func(sequenceRange)) {
	for _, ch := range l.chunks {
		for _, r := range ch {
			fn(sequenceRange{start: r.start, end: r.end})
		}
	}
}
