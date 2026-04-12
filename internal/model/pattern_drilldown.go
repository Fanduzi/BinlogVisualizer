package model

import "time"

// PatternDrilldown holds bounded explanatory detail for a selected high-signal pattern.
// It is an optional addition to the Top Patterns summary, not a replacement.
type PatternDrilldown struct {
	PatternKey  string
	Label       string
	WhySelected string

	// Share metrics from the parent pattern, surfaced for operator context.
	ShareOfRows   float64
	ShareOfTxns   float64
	AvgRowsPerTxn float64

	// SignalFlags describes which signals triggered this drilldown selection.
	SignalFlags PatternSignalFlags

	// BusiestMinutes holds at most 2 peak minute buckets from this pattern's activity.
	BusiestMinutes []PatternPeakMinute

	// RepresentativeTransactions holds at most 2 example transactions from this pattern.
	RepresentativeTransactions []PatternRepresentativeTxn
}

// PatternSignalFlags records which signals qualified a pattern for drilldown.
type PatternSignalFlags struct {
	Dominance bool // pattern dominates workload volume or transaction count
	Anomaly   bool // pattern is unusually concentrated, spike-aligned, or suspicious
}

// PatternPeakMinute is a bounded summary of one high-activity minute within a pattern.
type PatternPeakMinute struct {
	Minute    time.Time
	TotalRows int
	TxnCount  int
}

// PatternRepresentativeTxn is a bounded summary of one example transaction within a pattern.
type PatternRepresentativeTxn struct {
	TxnKey     string
	TotalRows  int
	Duration   time.Duration
	QuerySummary string
}
