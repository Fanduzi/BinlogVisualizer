package analyzer

import (
	"sort"

	"binlogviz/internal/model"
)

// DetectLargeTransactionAlerts scans completed transactions and generates alerts
// for transactions that exceed the configured row count or duration thresholds.
// If a transaction triggers both thresholds, a single alert is generated with
// all relevant details included.
func DetectLargeTransactionAlerts(transactions []model.Transaction, opts Options) []model.Alert {
	// Skip detection if both thresholds are disabled (zero values)
	if opts.LargeTxnRows == 0 && opts.LargeTxnDuration == 0 {
		return nil
	}

	var alerts []model.Alert

	for _, txn := range transactions {
		// Check if this transaction exceeds any threshold
		exceedsRows := opts.LargeTxnRows > 0 && txn.TotalRows > opts.LargeTxnRows
		exceedsDuration := opts.LargeTxnDuration > 0 && txn.Duration > opts.LargeTxnDuration

		if !exceedsRows && !exceedsDuration {
			continue
		}

		// Build alert with comprehensive details
		alert := model.Alert{
			Type:     "large_transaction",
			Severity: "warning",
			TxnKey:   txn.TxnKey,
			Details: map[string]any{
				"rows":        txn.TotalRows,
				"duration_ms": txn.Duration.Milliseconds(),
				"event_count": txn.EventCount,
			},
		}

		// Include threshold information
		if exceedsRows {
			alert.Details["rows_threshold"] = opts.LargeTxnRows
		}
		if exceedsDuration {
			alert.Details["duration_threshold_ms"] = opts.LargeTxnDuration.Milliseconds()
		}

		// Include affected tables (sorted alphabetically for deterministic output)
		if len(txn.Tables) > 0 {
			tables := make([]string, 0, len(txn.Tables))
			for table := range txn.Tables {
				tables = append(tables, table)
			}
			sort.Strings(tables)
			alert.Details["tables"] = tables
		}

		// Generate a clear message (renderer can override or format differently)
		alert.Message = buildLargeTransactionMessage(txn, exceedsRows, exceedsDuration, opts)

		alerts = append(alerts, alert)
	}

	return alerts
}

// buildLargeTransactionMessage creates a human-readable message for the alert.
// This is kept simple - the renderer can provide more sophisticated formatting.
func buildLargeTransactionMessage(txn model.Transaction, exceedsRows, exceedsDuration bool, opts Options) string {
	msg := "Transaction " + txn.TxnKey

	reasons := make([]string, 0, 2)
	if exceedsRows {
		reasons = append(reasons, "exceeds row threshold")
	}
	if exceedsDuration {
		reasons = append(reasons, "exceeds duration threshold")
	}

	if len(reasons) == 1 {
		msg += " " + reasons[0]
	} else {
		msg += " " + reasons[0] + " and " + reasons[1]
	}

	return msg
}
