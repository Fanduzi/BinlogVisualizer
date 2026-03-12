package analyzer

import (
	"sort"

	"binlogviz/internal/model"
)

// DetectSpikeAlerts scans minute buckets and detects spikes based on a rolling median baseline.
// A spike is detected when:
// 1. TotalRows >= SpikeMinRows
// 2. TotalRows >= baseline * SpikeFactor
//
// The baseline is computed as the median of the previous SpikeWindow minutes.
// Only detects spikes when DetectSpikes is enabled.
func DetectSpikeAlerts(buckets []model.MinuteBucket, opts Options) []model.Alert {
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

	for i, bucket := range sorted {
		// Skip if below minimum rows threshold
		if bucket.TotalRows < opts.SpikeMinRows {
			continue
		}

		// Need at least SpikeWindow previous buckets for baseline
		if i < opts.SpikeWindow {
			continue
		}

		// Collect baseline values from previous SpikeWindow buckets
		baselineValues := make([]int, 0, opts.SpikeWindow)
		for j := i - opts.SpikeWindow; j < i; j++ {
			baselineValues = append(baselineValues, sorted[j].TotalRows)
		}

		baseline := median(baselineValues)
		if baseline == 0 {
			continue // Avoid division by zero
		}

		// Check if current bucket is a spike
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
