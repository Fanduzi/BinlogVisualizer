// Package analyzer keeps a sorted, merged list of sequence ranges in chunks.
// input: sequence numbers in any order.
// output: disjoint ranges with adjacent numbers merged, iterated in order.
// pos: storage for the matched GTID set (#379); an insert costs O(log n + chunk), so out-of-order input is not quadratic.
// note: if this file changes, update this header and module README.md.
package analyzer

import "sort"

// rangeChunkMax bounds a chunk; a chunk that grows past it is split in two.
const rangeChunkMax = 256

type rangeList struct {
	chunks [][]sequenceRange
	n      int
}

// add inserts seq. delta is the change in the number of ranges (+1 new
// range, 0 extended, -1 two ranges joined); present reports seq was there.
func (l *rangeList) add(seq uint64) (delta int, present bool) {
	if len(l.chunks) == 0 {
		l.chunks = [][]sequenceRange{{{start: seq, end: seq}}}
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
			return 0, false
		}
		l.insert(lc, len(l.chunks[lc]), seq)
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
	var prev *sequenceRange
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
		prev.end = ch[i].end
		l.remove(c, i)
		return -1, false
	case joinLeft:
		prev.end = seq
		return 0, false
	case joinRight:
		ch[i].start = seq
		return 0, false
	}
	l.insert(c, i, seq)
	return 1, false
}

func (l *rangeList) insert(c, i int, seq uint64) {
	ch := append(l.chunks[c], sequenceRange{})
	copy(ch[i+1:], ch[i:])
	ch[i] = sequenceRange{start: seq, end: seq}
	l.n++
	if len(ch) <= rangeChunkMax {
		l.chunks[c] = ch
		return
	}
	half := len(ch) / 2
	second := append([]sequenceRange(nil), ch[half:]...)
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
			fn(r)
		}
	}
}
