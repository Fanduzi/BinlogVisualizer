// Package analyzer keeps the best N items of a stream in rank order.
// input: items in arrival order, a limit (0 = keep all) and a "better" order.
// output: the kept items, best first; equal items keep arrival order, and at capacity the newest of the worst ties is dropped first (the v0.23.34 order).
// pos: replaces linear insertion for top transactions (#252): O(log N) per add with a limit, O(1) without, one sort when read.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"container/heap"
	"sort"
)

type ranked[T any] struct {
	v   T
	seq int64
}

// topN is a bounded max-heap on rank (root = worst kept item) or, with no
// limit, an append-only list. sorted() sorts once and caches until the next add.
type topN[T any] struct {
	limit  int
	better func(left, right T) bool
	items  []ranked[T]
	seq    int64
	cache  []T
	fresh  bool
}

func newTopN[T any](limit int, better func(left, right T) bool) *topN[T] {
	return &topN[T]{limit: limit, better: better}
}

// before is the total rank order: better first, then earlier arrival.
func (t *topN[T]) before(a, b ranked[T]) bool {
	if t.better(a.v, b.v) {
		return true
	}
	if t.better(b.v, a.v) {
		return false
	}
	return a.seq < b.seq
}

func (t *topN[T]) add(v T) {
	if t == nil {
		return
	}
	item := ranked[T]{v: v, seq: t.seq}
	t.seq++
	t.fresh = false
	switch {
	case t.limit <= 0:
		t.items = append(t.items, item)
	case len(t.items) < t.limit:
		heap.Push((*worstHeap[T])(t), item)
	case t.before(item, t.items[0]):
		t.items[0] = item
		heap.Fix((*worstHeap[T])(t), 0)
	}
}

func (t *topN[T]) len() int {
	if t == nil {
		return 0
	}
	return len(t.items)
}

// sorted returns the kept items, best first. The slice is shared until the
// next add; callers copy before changing it.
func (t *topN[T]) sorted() []T {
	if t == nil || len(t.items) == 0 {
		return nil
	}
	if t.fresh {
		return t.cache
	}
	order := append([]ranked[T](nil), t.items...)
	sort.Slice(order, func(i, j int) bool { return t.before(order[i], order[j]) })
	out := make([]T, len(order))
	for i := range order {
		out[i] = order[i].v
	}
	t.cache, t.fresh = out, true
	return out
}

// worstHeap orders the root as the item ranked last.
type worstHeap[T any] topN[T]

func (h *worstHeap[T]) Len() int { return len(h.items) }
func (h *worstHeap[T]) Less(i, j int) bool {
	return (*topN[T])(h).before(h.items[j], h.items[i])
}
func (h *worstHeap[T]) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *worstHeap[T]) Push(x any)    { h.items = append(h.items, x.(ranked[T])) }
func (h *worstHeap[T]) Pop() any {
	last := h.items[len(h.items)-1]
	h.items = h.items[:len(h.items)-1]
	return last
}
