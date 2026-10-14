package analyzer

import (
	"math/rand"
	"reflect"
	"testing"

	"binlogviz/internal/model"
)

// referenceInsert is the v0.23.34 insertion: linear scan and shift. It is
// the semantic oracle for topN: a new item goes after existing equal items,
// and at capacity the last item is dropped.
func referenceInsert(current []model.Transaction, txn model.Transaction, limit int, better func(left, right model.Transaction) bool) []model.Transaction {
	if limit == 0 {
		limit = len(current) + 1
	}
	insertAt := len(current)
	for index := range current {
		if better(txn, current[index]) {
			insertAt = index
			break
		}
	}
	if insertAt == len(current) {
		if len(current) < limit {
			return append(current, txn)
		}
		return current
	}
	if len(current) < limit {
		current = append(current, model.Transaction{})
	}
	copy(current[insertAt+1:], current[insertAt:])
	current[insertAt] = txn
	if len(current) > limit {
		current = current[:limit]
	}
	return current
}

func TestTopNMatchesReferenceOrder(t *testing.T) {
	rng := rand.New(rand.NewSource(252))
	for _, limit := range []int{0, 1, 3, 5, 100} {
		var want []model.Transaction
		got := newTopN(limit, transactionRowsBetter)
		for i := 0; i < 3000; i++ {
			// Few distinct row counts, so ties are common.
			txn := model.Transaction{TxnKey: "txn-" + string(rune('a'+i%26)) + string(rune('0'+i%10)), TotalRows: rng.Intn(20)}
			txn.ThreadID = uint32(i)
			want = referenceInsert(want, txn, limit, transactionRowsBetter)
			got.add(txn)
		}
		if !reflect.DeepEqual(got.sorted(), want) {
			t.Fatalf("limit %d: order differs from v0.23.34", limit)
		}
	}
}

// #252: --top-transactions 0 must not be O(N^2).
func TestTopNUnlimitedIsNotQuadratic(t *testing.T) {
	compares := 0
	better := func(l, r model.Transaction) bool { compares++; return l.TotalRows > r.TotalRows }
	const n = 20000
	top := newTopN(0, better)
	for i := 0; i < n; i++ {
		top.add(model.Transaction{TotalRows: i % 97})
	}
	if len(top.sorted()) != n {
		t.Fatal("unlimited keeps every transaction")
	}
	// n log2 n is about 286k; quadratic insertion is about 200M.
	if compares > 2_000_000 {
		t.Fatalf("%d comparisons for %d transactions", compares, n)
	}
}
