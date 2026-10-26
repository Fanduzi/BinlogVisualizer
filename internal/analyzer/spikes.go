package analyzer

import (
	"sort"
	"time"

	"binlogviz/internal/model"
)

// DetectSpikeAlerts scans minute buckets and detects spikes based on a rolling median baseline.
// Detects two types of spikes:
// 1. Overall minute spike: based on TotalRows
// 2. Table-level spike: based on per-table row counts in TableRows
//
// A spike is detected when:
// - rows >= SpikeMinRows
// - rows >= baseline * SpikeFactor
//
// The baseline is computed as the median of the previous SpikeWindow minutes.
// Only detects spikes when DetectSpikes is enabled.
func DetectSpikeAlerts(buckets []model.MinuteBucket, opts Options) []model.Alert {
	return detectSpikeAlerts(buckets, opts, nil)
}

// detectSpikeAlerts is DetectSpikeAlerts. lookup supplies per-table rows
// when the buckets themselves do not carry them. It keeps only the baseline
// window of lookups, not every minute.
func detectSpikeAlerts(buckets []model.MinuteBucket, opts Options, lookup func(time.Time) map[string]int) []model.Alert {
	if !opts.DetectSpikes {
		return nil
	}

	// Sort buckets by time to ensure correct ordering
	sorted := make([]model.MinuteBucket, len(buckets))
	copy(sorted, buckets)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Minute.Before(sorted[j].Minute)
	})

	var alerts []model.Alert
	var cache map[time.Time]map[string]int
	rowsOf := func(b model.MinuteBucket) map[string]int {
		if lookup == nil {
			return b.TableRows
		}
		if cache == nil {
			cache = make(map[time.Time]map[string]int, opts.SpikeWindow+1)
		}
		if rows, ok := cache[b.Minute]; ok {
			return rows
		}
		rows := lookup(b.Minute)
		cache[b.Minute] = rows
		return rows
	}

	for i, bucket := range sorted {
		// Need at least SpikeWindow previous buckets for baseline
		if i < opts.SpikeWindow {
			continue
		}

		// === Overall minute spike detection ===
		if bucket.TotalRows >= opts.SpikeMinRows {
			baselineValues := make([]int, 0, opts.SpikeWindow)
			for j := i - opts.SpikeWindow; j < i; j++ {
				baselineValues = append(baselineValues, sorted[j].TotalRows)
			}

			baseline := median(baselineValues)
			if baseline > 0 {
				factor := float64(bucket.TotalRows) / float64(baseline)
				if factor >= opts.SpikeFactor {
					alerts = append(alerts, model.Alert{
						Type:     "spike",
						Severity: "warning",
						Minute:   bucket.Minute,
						Message:  "Write spike detected",
						Details: map[string]any{
							"rows":     bucket.TotalRows,
							"baseline": baseline,
							"factor":   factor,
							"minute":   bucket.Minute,
						},
					})
				}
			}
		}

		// === Table-level spike detection ===
		// Collect table names and sort for deterministic ordering
		tableRowsNow := rowsOf(bucket)
		tableNames := make([]string, 0, len(tableRowsNow))
		for table := range tableRowsNow {
			tableNames = append(tableNames, table)
		}
		sort.Strings(tableNames)

		for _, table := range tableNames {
			rows := tableRowsNow[table]
			if rows < opts.SpikeMinRows {
				continue
			}

			// Collect baseline values for this specific table
			tableBaselineValues := make([]int, 0, opts.SpikeWindow)
			for j := i - opts.SpikeWindow; j < i; j++ {
				if tableRows, ok := rowsOf(sorted[j])[table]; ok {
					tableBaselineValues = append(tableBaselineValues, tableRows)
				} else {
					tableBaselineValues = append(tableBaselineValues, 0)
				}
			}

			baseline := median(tableBaselineValues)
			if baseline > 0 {
				factor := float64(rows) / float64(baseline)
				if factor >= opts.SpikeFactor {
					alerts = append(alerts, model.Alert{
						Type:     "spike",
						Severity: "warning",
						Minute:   bucket.Minute,
						Message:  "Table write spike detected",
						Details: map[string]any{
							"table":    table,
							"rows":     rows,
							"baseline": baseline,
							"factor":   factor,
							"minute":   bucket.Minute,
						},
					})
				}
			}
		}
		if lookup != nil && i >= opts.SpikeWindow {
			delete(cache, sorted[i-opts.SpikeWindow].Minute)
		}
	}

	return alerts
}

// median calculates the median of a slice of integers.
// For even-length slices, returns the average of the two middle values.
func median(values []int) int {
	if len(values) == 0 {
		return 0
	}

	// Sort the values
	sorted := make([]int, len(values))
	copy(sorted, values)
	sort.Ints(sorted)

	n := len(sorted)
	if n%2 == 1 {
		// Odd length: return middle element
		return sorted[n/2]
	}
	// Even length: return average of two middle elements
	return (sorted[n/2-1] + sorted[n/2]) / 2
}
