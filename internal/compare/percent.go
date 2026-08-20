package compare

import "fmt"

func deltaPercent(current, baseline int) *float64 {
	if baseline == 0 {
		if current == 0 {
			zero := 0.0
			return &zero
		}
		return nil
	}
	value := float64(current-baseline) / float64(baseline) * 100
	return &value
}

func formatDeltaPercent(current, baseline int, pct *float64) string {
	if baseline == 0 && current > 0 {
		return "new"
	}
	if pct == nil {
		return "new"
	}
	return fmt.Sprintf("%.1f%%", *pct)
}

func percentValue(v float64) *float64 {
	return &v
}
