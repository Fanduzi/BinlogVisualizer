// Package compare indexes transaction content for stable alert and diagnostic identity.
// input: compare input reports with top-level and diagnostic transaction evidence.
// output: deterministic transaction content keys used to align compare alerts.
// pos: identity helper between decoded reports and compare diff construction.
// note: if this file changes, update this header and internal/compare/README.md.
package compare

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

func indexReportTransactions(report InputReport) map[string]InputTransaction {
	out := make(map[string]InputTransaction, len(report.Transactions)+len(report.Diagnostics.LargestTransactions)+len(report.Diagnostics.LongestTransactions)+len(report.Diagnostics.WidestTransactions))
	for _, txn := range report.Transactions {
		if txn.TxnKey != "" {
			out[txn.TxnKey] = txn
		}
	}
	for _, txn := range report.Diagnostics.LargestTransactions {
		if txn.TxnKey != "" {
			out[txn.TxnKey] = txn
		}
	}
	for _, txn := range report.Diagnostics.LongestTransactions {
		if txn.TxnKey == "" {
			continue
		}
		if _, exists := out[txn.TxnKey]; !exists {
			out[txn.TxnKey] = txn
		}
	}
	for _, txn := range report.Diagnostics.WidestTransactions {
		if txn.TxnKey == "" {
			continue
		}
		if _, exists := out[txn.TxnKey]; !exists {
			out[txn.TxnKey] = txn
		}
	}
	return out
}

func largeTxnContentID(alert InputAlert, txns map[string]InputTransaction) string {
	if txn, ok := txns[alert.TxnKey]; ok {
		return txnContentID(txn)
	}
	return alertFallbackContentID(alert)
}

func txnContentID(txn InputTransaction) string {
	return fmt.Sprintf("%s|%s|%d|%s",
		dominantMapKey(txn.Tables),
		dominantMapKey(txn.Operations),
		txn.TotalRows,
		txnFileSpan(txn),
	)
}

func txnFileSpan(txn InputTransaction) string {
	start := strings.TrimSpace(txn.BinlogFileStart)
	end := strings.TrimSpace(txn.BinlogFileEnd)
	file := start
	if file == "" {
		file = end
	} else if end != "" && end != start {
		file = start + "->" + end
	}
	return fmt.Sprintf("%s:%d-%d", file, txn.PosStart, txn.PosEnd)
}

func txnIsEmpty(txn InputTransaction) bool {
	return txn.TxnKey == "" && txn.TotalRows == 0 && len(txn.Tables) == 0 && len(txn.Operations) == 0
}

func txnContentEqual(left, right InputTransaction) bool {
	if txnIsEmpty(left) && txnIsEmpty(right) {
		return true
	}
	return txnContentID(left) == txnContentID(right)
}

func dominantMapKey(values map[string]int) string {
	bestKey := ""
	bestVal := -1
	for key, value := range values {
		if value > bestVal || (value == bestVal && (bestKey == "" || key < bestKey)) {
			bestVal = value
			bestKey = key
		}
	}
	return bestKey
}

func alertFallbackContentID(alert InputAlert) string {
	tables := detailStrings(alert.Details, "tables")
	sort.Strings(tables)
	return fmt.Sprintf("%s||%d||", strings.Join(tables, ","), detailInt(alert.Details, "rows"))
}

func detailInt(details map[string]any, key string) int {
	if len(details) == 0 {
		return 0
	}
	switch value := details[key].(type) {
	case int:
		return value
	case int64:
		return int(value)
	case float64:
		return int(value)
	case string:
		n, _ := strconv.Atoi(value)
		return n
	default:
		return 0
	}
}

func detailStrings(details map[string]any, key string) []string {
	if len(details) == 0 {
		return nil
	}
	switch value := details[key].(type) {
	case []string:
		return append([]string(nil), value...)
	case []any:
		out := make([]string, 0, len(value))
		for _, item := range value {
			if text, ok := item.(string); ok && text != "" {
				out = append(out, text)
			}
		}
		return out
	default:
		return nil
	}
}
