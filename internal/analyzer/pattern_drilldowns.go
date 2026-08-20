// Package analyzer selects high-signal patterns for bounded drilldown treatment.
// input: finalized Patterns, Minutes, Transactions, and Alerts from the analysis pipeline.
// output: at most 2 PatternDrilldown entries with strictly bounded nested summaries.
// pos: post-pattern selection layer used during analyze Finalize, downstream of BuildPatterns.
// note: if this file changes, keep internal/analyzer/README.md synchronized.
package analyzer

import (
	"fmt"
	"sort"

	"binlogviz/internal/model"
)

const (
	maxDrilldowns      = 2
	maxPeakMinutes     = 2
	maxRepresentativeTxns = 2

	// Dominance thresholds
	dominanceRowShare  = 0.30
	dominanceTxnShare  = 0.30
	extremeRowShare    = 0.60
	extremeTxnShare    = 0.60

	// Anomaly thresholds
	highRowsPerTxnRatio = 3.0 // avg_rows_per_txn > 3x the mean of other top patterns
	concentrationShare  = 0.50 // top 1-2 minutes cover this share of total rows
)

// candidate holds a pattern plus its computed signals for drilldown selection.
type candidate struct {
	pattern    model.PatternStats
	dominance  bool
	anomaly    bool
	score      float64
}

// BuildPatternDrilldowns evaluates mixed-signal candidates from finalized analysis results
// and returns bounded drilldown summaries for at most 2 high-signal patterns.
func BuildPatternDrilldowns(
	patterns []model.PatternStats,
	minutes []model.MinuteBucket,
	txns []model.Transaction,
	alerts []model.Alert,
) []model.PatternDrilldown {
	if len(patterns) == 0 {
		return []model.PatternDrilldown{}
	}

	// Compute baseline stats for anomaly detection
	meanRowsPerTxn := computeMeanRowsPerTxn(patterns)
	totalRows := totalRowsFromMinutes(minutes)

	// Evaluate each pattern as a candidate
	candidates := make([]candidate, 0, len(patterns))
	for _, p := range patterns {
		c := evaluateCandidate(p, minutes, txns, alerts, meanRowsPerTxn, totalRows)
		if c.score > 0 {
			candidates = append(candidates, c)
		}
	}

	if len(candidates) == 0 {
		return []model.PatternDrilldown{}
	}

	// Sort by score descending, then by pattern_key for determinism
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score != candidates[j].score {
			return candidates[i].score > candidates[j].score
		}
		return candidates[i].pattern.PatternKey < candidates[j].pattern.PatternKey
	})

	// Cap at maxDrilldowns
	if len(candidates) > maxDrilldowns {
		candidates = candidates[:maxDrilldowns]
	}

	// Build drilldown summaries
	result := make([]model.PatternDrilldown, 0, len(candidates))
	for _, c := range candidates {
		d := buildDrilldown(c, minutes, txns)
		result = append(result, d)
	}
	return result
}

func evaluateCandidate(
	p model.PatternStats,
	minutes []model.MinuteBucket,
	txns []model.Transaction,
	alerts []model.Alert,
	meanRowsPerTxn float64,
	totalRows int,
) candidate {
	c := candidate{pattern: p}

	// Dominance checks
	if p.ShareOfRows >= dominanceRowShare || p.ShareOfTransactions >= dominanceTxnShare {
		c.dominance = true
	}

	// Anomaly checks
	anomaly := false

	// High rows-per-txn ratio relative to peers (pattern-local signal)
	if meanRowsPerTxn > 0 && p.AvgRowsPerTxn > meanRowsPerTxn*highRowsPerTxnRatio {
		anomaly = true
	}

	// Strong minute concentration is only relevant for dominant patterns:
	// a non-dominant pattern does not own enough of the workload to make
	// workload-level concentration a meaningful anomaly signal for it.
	if c.dominance && totalRows > 0 && len(minutes) >= 3 {
		concentration := computeMinuteConcentration(minutes, totalRows)
		if concentration >= concentrationShare {
			anomaly = true
		}
	}

	// Table-aligned alert signal (only large-transaction alerts whose txn
	// shares tables with this pattern)
	if hasAlertAlignment(p, minutes, txns, alerts) {
		anomaly = true
	}

	c.anomaly = anomaly

	// Scoring: both signals = highest, extreme single signal = medium, single signal = low
	switch {
	case c.dominance && c.anomaly:
		c.score = 3.0
	case c.dominance && (p.ShareOfRows >= extremeRowShare || p.ShareOfTransactions >= extremeTxnShare):
		c.score = 2.5
	case c.anomaly && p.AvgRowsPerTxn > meanRowsPerTxn*5:
		c.score = 2.5
	case c.dominance:
		c.score = 1.0
	case c.anomaly:
		c.score = 1.0
	default:
		c.score = 0
	}

	return c
}

func buildDrilldown(c candidate, minutes []model.MinuteBucket, txns []model.Transaction) model.PatternDrilldown {
	p := c.pattern
	return model.PatternDrilldown{
		PatternKey:   p.PatternKey,
		Label:        p.Label,
		WhySelected:  formatWhySelected(c),
		ShareOfRows:  p.ShareOfRows,
		ShareOfTxns:  p.ShareOfTransactions,
		AvgRowsPerTxn: p.AvgRowsPerTxn,
		SignalFlags: model.PatternSignalFlags{
			Dominance: c.dominance,
			Anomaly:   c.anomaly,
		},
		BusiestMinutes:             selectPeakMinutes(minutes, maxPeakMinutes),
		RepresentativeTransactions: selectRepresentativeTxns(txns, p.PatternKey, maxRepresentativeTxns),
	}
}

func formatWhySelected(c candidate) string {
	parts := make([]string, 0, 2)
	if c.dominance {
		parts = append(parts, fmt.Sprintf("dominates workload (%s rows, %s txns)",
			formatSharePercent(c.pattern.ShareOfRows), formatSharePercent(c.pattern.ShareOfTransactions)))
	}
	if c.anomaly {
		parts = append(parts, "anomalous concentration or spike alignment")
	}
	if len(parts) == 0 {
		return ""
	}
	return "high signal: " + joinWhy(parts)
}

func joinWhy(parts []string) string {
	result := parts[0]
	for i := 1; i < len(parts); i++ {
		result += " + " + parts[i]
	}
	return result
}

// formatSharePercent keeps sub-1% shares visible instead of rounding 0.3% to 0%.
func formatSharePercent(share float64) string {
	pct := share * 100
	if pct <= 0 {
		return "0%"
	}
	if pct < 1 {
		return fmt.Sprintf("%.1f%%", pct)
	}
	return fmt.Sprintf("%.0f%%", pct)
}

// computeMeanRowsPerTxn calculates the average avg_rows_per_txn across all patterns.
func computeMeanRowsPerTxn(patterns []model.PatternStats) float64 {
	if len(patterns) == 0 {
		return 0
	}
	var sum float64
	for _, p := range patterns {
		sum += p.AvgRowsPerTxn
	}
	return sum / float64(len(patterns))
}

// computeMinuteConcentration returns the fraction of total rows in the top 2 minutes.
func computeMinuteConcentration(minutes []model.MinuteBucket, totalRows int) float64 {
	if len(minutes) <= 2 || totalRows == 0 {
		if len(minutes) > 0 && totalRows > 0 {
			return float64(minutes[0].TotalRows) / float64(totalRows)
		}
		return 0
	}

	// Sort by TotalRows descending
	sorted := make([]model.MinuteBucket, len(minutes))
	copy(sorted, minutes)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].TotalRows > sorted[j].TotalRows
	})

	topRows := sorted[0].TotalRows + sorted[1].TotalRows
	return float64(topRows) / float64(totalRows)
}

// totalRowsFromMinutes sums TotalRows across all minute buckets.
func totalRowsFromMinutes(minutes []model.MinuteBucket) int {
	total := 0
	for _, m := range minutes {
		total += m.TotalRows
	}
	return total
}

// hasAlertAlignment checks if any large-transaction alert references a txn whose
// table set overlaps with this pattern's tables. This is a pattern-local signal:
// the alert must touch the same tables to be relevant.
//
// Global alert-minute overlap was intentionally removed because it falsely flags
// unrelated patterns in busy windows where spike alerts and high-activity minutes
// always overlap.
func hasAlertAlignment(p model.PatternStats, _ []model.MinuteBucket, txns []model.Transaction, alerts []model.Alert) bool {
	if len(alerts) == 0 {
		return false
	}

	// Build set of alert txn keys (large-transaction alerts carry txn_key)
	alertTxnKeys := make(map[string]bool)
	for _, a := range alerts {
		if a.TxnKey != "" {
			alertTxnKeys[a.TxnKey] = true
		}
	}

	// Check if any large-txn alert references a txn from this pattern's table set
	for _, txn := range txns {
		if alertTxnKeys[txn.TxnKey] && sharesAnyTable(p.Tables, txn.Tables) {
			return true
		}
	}

	return false
}

// sharesAnyTable returns true if the two table maps share at least one key.
func sharesAnyTable(a, b map[string]int) bool {
	for k := range a {
		if _, ok := b[k]; ok {
			return true
		}
	}
	return false
}

// selectPeakMinutes returns the top N minutes by TotalRows.
func selectPeakMinutes(minutes []model.MinuteBucket, n int) []model.PatternPeakMinute {
	if len(minutes) == 0 {
		return nil
	}

	sorted := make([]model.MinuteBucket, len(minutes))
	copy(sorted, minutes)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].TotalRows != sorted[j].TotalRows {
			return sorted[i].TotalRows > sorted[j].TotalRows
		}
		return sorted[i].Minute.Before(sorted[j].Minute)
	})

	if len(sorted) > n {
		sorted = sorted[:n]
	}

	result := make([]model.PatternPeakMinute, len(sorted))
	for i, m := range sorted {
		result[i] = model.PatternPeakMinute{
			Minute:    m.Minute,
			TotalRows: m.TotalRows,
			TxnCount:  m.TxnCount,
		}
	}
	return result
}

// selectRepresentativeTxns returns up to N transactions that belong to patternKey
// (same table set + ops + shape), sorted by TotalRows descending.
func selectRepresentativeTxns(txns []model.Transaction, patternKey string, n int) []model.PatternRepresentativeTxn {
	if len(txns) == 0 || n <= 0 || patternKey == "" {
		return nil
	}

	matched := make([]model.Transaction, 0, len(txns))
	for _, txn := range txns {
		key, _ := patternIdentity(txn)
		if key == patternKey {
			matched = append(matched, txn)
		}
	}
	if len(matched) == 0 {
		return nil
	}

	top := topTransactions(matched, n, func(left, right model.Transaction) bool {
		if left.TotalRows != right.TotalRows {
			return left.TotalRows > right.TotalRows
		}
		return left.TxnKey < right.TxnKey
	})

	result := make([]model.PatternRepresentativeTxn, len(top))
	for i, txn := range top {
		result[i] = model.PatternRepresentativeTxn{
			TxnKey:       txn.TxnKey,
			TotalRows:    txn.TotalRows,
			Duration:     txn.Duration,
			QuerySummary: txn.QuerySummary,
		}
	}
	return result
}
