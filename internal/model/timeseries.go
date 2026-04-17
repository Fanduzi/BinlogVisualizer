package model

import "time"

// TimeseriesPoint is a single minute-level datapoint for a metric series.
type TimeseriesPoint struct {
	Minute time.Time
	Value  float64
}

// TxnSizeBucket summarizes a transaction-size histogram bucket.
type TxnSizeBucket struct {
	Label       string
	TxnCount    int
	Rows        int
	BinlogBytes int64
}

// TxnSizeSeriesSummary carries a bounded transaction-size distribution summary.
type TxnSizeSeriesSummary struct {
	Buckets []TxnSizeBucket
}

// Timeseries groups the chart-ready metric series used by analysis outputs.
// Minutes in AnalysisResult remains the authoritative aggregated bucket source.
type Timeseries struct {
	// TPSSeries stores transactions-per-second rates for minute buckets, computed as TxnCount / 60.
	TPSSeries            []TimeseriesPoint
	RowsSeries           []TimeseriesPoint
	EventsSeries         []TimeseriesPoint
	InsertEventSeries    []TimeseriesPoint
	UpdateEventSeries    []TimeseriesPoint
	DeleteEventSeries    []TimeseriesPoint
	DDLEventSeries       []TimeseriesPoint
	BinlogBytesSeries    []TimeseriesPoint
	TxnSizeSeriesSummary TxnSizeSeriesSummary
}
