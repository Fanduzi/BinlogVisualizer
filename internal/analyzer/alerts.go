package analyzer

import (
	"sort"

	"binlogviz/internal/i18n"
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
			Type:     i18n.T("alert.largeTransaction.type"),
			Severity: i18n.T("alert.largeTransaction.severity"),
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
	reasons := make([]string, 0, 2)
	if exceedsRows {
		reasons = append(reasons, i18n.T("alert.largeTransaction.exceedsRowThreshold"))
	}
	if exceedsDuration {
		reasons = append(reasons, i18n.T("alert.largeTransaction.exceedsDurationThreshold"))
	}

	var reasonsStr string
	if len(reasons) == 1 {
		reasonsStr = reasons[0]
	} else {
		reasonsStr = reasons[0] + " " + i18n.T("alert.largeTransaction.and") + " " + reasons[1]
	}

	return i18n.Tf("alert.largeTransaction.message", map[string]any{
		"TxnKey":  txn.TxnKey,
		"Reasons": reasonsStr,
	})
}
