package analyzer

import "time"

// Options configures the analyzer behavior.
type Options struct {
	// Time window filtering (future - Task 9)
	Start *time.Time
	End   *time.Time

	// Report limits (future - CLI flags)
	TopTables       int // 0 = unlimited
	TopTransactions int // 0 = unlimited
	TopMinutes      int // 0 = unlimited

	// Alert thresholds (future - Task 10/11)
	LargeTxnRows      int // alert if transaction has more rows
	LargeTxnDuration  time.Duration // alert if transaction exceeds duration
	DetectSpikes     bool // enable spike detection
	SpikeWindow     int // minutes for rolling baseline
	SpikeFactor     float64 // multiplier for spike detection
	SpikeMinRows    int // minimum rows to consider a spike
}

// DefaultOptions returns Options with sensible defaults.
func DefaultOptions() Options {
	return Options{
		TopTables:       20,
		TopTransactions: 20,
		TopMinutes:      60, // last 60 minutes
		LargeTxnRows:    1000,
		LargeTxnDuration: 30 * time.Second,
		DetectSpikes:    false, // disabled by default
		SpikeWindow:     5,
		SpikeFactor:     5.0,
		SpikeMinRows:    100,
	}
}
