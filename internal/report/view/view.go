// Package view is the one ordered selection of transactions for an analyze report.
// input: a finished AnalysisResult and the render limits (normalized top-N, whether JSON keeps the diagnostic lists, and whether a transaction has query text).
// output: the listed transactions, the text champions and query fallback, the HTML evidence champions, and the JSON diagnostic lists.
// pos: the selection seam under internal/report. Text, Markdown, JSON, and HTML read this value. They do not slice transaction lists themselves.
// note: if this file changes, update this header and internal/report/README.md. The caps are the ones the renderers used before this package existed (ADR 0012 step 5). Text and Markdown do not select the same rows; this package keeps both.
package view

import "binlogviz/internal/model"

// Reason is why an HTML evidence card includes a transaction.
// The renderer maps it to the existing label. The view does not own that text.
type Reason string

const (
	ReasonLargest Reason = "largest"
	ReasonLongest Reason = "longest"
	ReasonWidest  Reason = "widest"
)

// Limits is what Build needs from the renderer. TopN is the normalized
// report top. DiagnosticLists is false for --diagnostics summary and off.
// HasQuery reports whether text would print a query line. Nil means no
// query fallback.
type Limits struct {
	TopN            int
	DiagnosticLists bool
	HasQuery        func(model.Transaction) bool
}

// Evidence is one HTML transaction card and every category it wins.
type Evidence struct {
	Txn     model.Transaction
	Reasons []Reason
}

// Text is the text report's transaction selection.
type Text struct {
	Largest []model.Transaction
	Longest []model.Transaction
	Widest  []model.Transaction
	// QueryFallback is set when none of the three slices has query text.
	// Text prints only the query line.
	QueryFallback *model.Transaction
	// ByteLeaders is the summary byte ranking: min(3, TopN) entries,
	// stopped at the first non-positive byte count.
	ByteLeaders []model.Transaction
}

// View is the selection for one report. Listed is analyzer order.
// Largest, Longest, Widest, and LargestBytes are the full diagnostic
// lists, or nil when the JSON document does not keep them.
type View struct {
	Listed       []model.Transaction
	Omitted      int
	Text         Text
	Evidence     []Evidence
	Largest      []model.Transaction
	Longest      []model.Transaction
	Widest       []model.Transaction
	LargestBytes []model.Transaction
}

// Build selects every transaction list the four renderers print.
// It does not sort. The analyzer already ranked the lists.
func Build(result model.AnalysisResult, lim Limits) View {
	listed := result.Transactions
	omitted := result.Summary.TotalTransactions - len(listed)
	if omitted < 0 {
		omitted = 0
	}
	largestN := min(3, lim.TopN)
	widestN := min(1, lim.TopN)
	textLargest := take(result.Diagnostics.LargestTransactions, largestN)
	textLongest := take(result.Diagnostics.LongestTransactions, largestN)
	textWidest := take(result.Diagnostics.WidestTransactions, widestN)
	out := View{
		Listed:  listed,
		Omitted: omitted,
		Text: Text{
			Largest:     textLargest,
			Longest:     textLongest,
			Widest:      textWidest,
			ByteLeaders: byteLeaders(result.Diagnostics.LargestByteTransactions, largestN),
		},
		Evidence: evidence(
			result.Diagnostics.LargestTransactions,
			result.Diagnostics.LongestTransactions,
			result.Diagnostics.WidestTransactions,
		),
	}
	if lim.HasQuery != nil && !anyQuery(lim.HasQuery, textLargest, textLongest, textWidest) {
		for _, txn := range listed {
			if !lim.HasQuery(txn) {
				continue
			}
			chosen := txn
			out.Text.QueryFallback = &chosen
			break
		}
	}
	if lim.DiagnosticLists {
		out.Largest = result.Diagnostics.LargestTransactions
		out.Longest = result.Diagnostics.LongestTransactions
		out.Widest = result.Diagnostics.WidestTransactions
		out.LargestBytes = result.Diagnostics.LargestByteTransactions
	}
	return out
}

func anyQuery(has func(model.Transaction) bool, lists ...[]model.Transaction) bool {
	for _, list := range lists {
		for _, txn := range list {
			if has(txn) {
				return true
			}
		}
	}
	return false
}

func evidence(largest, longest, widest []model.Transaction) []Evidence {
	var out []Evidence
	index := make(map[string]int)
	add := func(list []model.Transaction, reason Reason) {
		if len(list) == 0 {
			return
		}
		txn := list[0]
		if txn.TxnKey != "" {
			if i, ok := index[txn.TxnKey]; ok {
				out[i].Reasons = append(out[i].Reasons, reason)
				return
			}
			index[txn.TxnKey] = len(out)
		}
		out = append(out, Evidence{Txn: txn, Reasons: []Reason{reason}})
	}
	add(largest, ReasonLargest)
	add(longest, ReasonLongest)
	add(widest, ReasonWidest)
	return out
}

// take matches the old limitTransactions: a non-positive limit keeps every row.
func take(txns []model.Transaction, limit int) []model.Transaction {
	if limit <= 0 || len(txns) <= limit {
		return txns
	}
	return txns[:limit]
}

func byteLeaders(txns []model.Transaction, limit int) []model.Transaction {
	var out []model.Transaction
	for i, txn := range txns {
		if i >= limit || txn.BinlogBytes <= 0 {
			break
		}
		out = append(out, txn)
	}
	return out
}
