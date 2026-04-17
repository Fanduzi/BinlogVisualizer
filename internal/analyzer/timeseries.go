// Package analyzer builds chart-ready time series from analyzer minute buckets, events, and transactions.
// input: minute buckets, normalized events, and completed transactions.
// output: model.Timeseries projections suitable for HTML/JSON reporting.
// pos: analyzer-side projection layer that turns canonical aggregates into report-friendly series.
// note: if this file changes, update this header and README.md.
package analyzer

import (
	"sort"
	"time"

	"binlogviz/internal/model"
)

// TimeseriesBuildInput groups the aggregate inputs required to build model.Timeseries.
type TimeseriesBuildInput struct {
	Minutes         []model.MinuteBucket
	Events          []model.NormalizedEvent
	Transactions    []model.Transaction
	OperationCounts map[time.Time]operationMinuteStats
}

// BuildTimeseries projects chart-ready metric series from minute buckets, normalized events, and transactions.
func BuildTimeseries(input TimeseriesBuildInput) model.Timeseries {
	minutes := collectSeriesMinutes(input.Minutes, input.Events)
	if len(minutes) == 0 && len(input.Transactions) > 0 {
		minutes = append(minutes, collectTransactionMinutes(input.Transactions)...)
		sort.Slice(minutes, func(i, j int) bool { return minutes[i].Before(minutes[j]) })
	}

	minuteStats := make(map[time.Time]model.MinuteBucket, len(input.Minutes))
	for _, bucket := range input.Minutes {
		minuteStats[truncateToMinute(bucket.Minute)] = bucket
	}

	opStats := make(map[time.Time]operationMinuteStats, len(input.OperationCounts))
	for minute, stats := range input.OperationCounts {
		opStats[truncateToMinute(minute)] = stats
	}
	for _, ev := range input.Events {
		minute := truncateToMinute(ev.Timestamp)
		stats := opStats[minute]
		switch ev.Operation {
		case "INSERT":
			stats.insertEvents++
		case "UPDATE":
			stats.updateEvents++
		case "DELETE":
			stats.deleteEvents++
		}
		if ddlEvent, ok := DDLEventFromNormalizedEvent(ev); ok && ddlEvent.Operation != "" {
			stats.ddlEvents++
		}
		opStats[minute] = stats
	}

	series := model.Timeseries{
		TPSSeries:            make([]model.TimeseriesPoint, 0, len(minutes)),
		RowsSeries:           make([]model.TimeseriesPoint, 0, len(minutes)),
		EventsSeries:         make([]model.TimeseriesPoint, 0, len(minutes)),
		InsertEventSeries:    make([]model.TimeseriesPoint, 0, len(minutes)),
		UpdateEventSeries:    make([]model.TimeseriesPoint, 0, len(minutes)),
		DeleteEventSeries:    make([]model.TimeseriesPoint, 0, len(minutes)),
		DDLEventSeries:       make([]model.TimeseriesPoint, 0, len(minutes)),
		BinlogBytesSeries:    make([]model.TimeseriesPoint, 0, len(minutes)),
		TxnSizeSeriesSummary: buildTxnSizeSeriesSummary(input.Transactions),
	}

	for _, minute := range minutes {
		bucket := minuteStats[minute]
		op := opStats[minute]

		ddlCount := bucket.DDLCount
		if ddlCount == 0 {
			ddlCount = op.ddlEvents
		}

		avgTPS := float64(bucket.TxnCount) / 60.0
		series.TPSSeries = append(series.TPSSeries, model.TimeseriesPoint{Minute: minute, Value: avgTPS})
		series.RowsSeries = append(series.RowsSeries, model.TimeseriesPoint{Minute: minute, Value: float64(bucket.TotalRows)})
		series.EventsSeries = append(series.EventsSeries, model.TimeseriesPoint{Minute: minute, Value: float64(bucket.EventCount)})
		series.InsertEventSeries = append(series.InsertEventSeries, model.TimeseriesPoint{Minute: minute, Value: float64(op.insertEvents)})
		series.UpdateEventSeries = append(series.UpdateEventSeries, model.TimeseriesPoint{Minute: minute, Value: float64(op.updateEvents)})
		series.DeleteEventSeries = append(series.DeleteEventSeries, model.TimeseriesPoint{Minute: minute, Value: float64(op.deleteEvents)})
		series.DDLEventSeries = append(series.DDLEventSeries, model.TimeseriesPoint{Minute: minute, Value: float64(ddlCount)})
		series.BinlogBytesSeries = append(series.BinlogBytesSeries, model.TimeseriesPoint{Minute: minute, Value: float64(bucket.BinlogBytes)})
	}

	return series
}

type operationMinuteStats struct {
	insertEvents int
	updateEvents int
	deleteEvents int
	ddlEvents    int
}

// TimeseriesAggregator tracks operation-level event counts needed for chart projections.
type TimeseriesAggregator struct {
	operationCounts map[time.Time]operationMinuteStats
}

// NewTimeseriesAggregator creates an empty TimeseriesAggregator.
func NewTimeseriesAggregator() *TimeseriesAggregator {
	return &TimeseriesAggregator{
		operationCounts: make(map[time.Time]operationMinuteStats),
	}
}

// Consume records operation-level counts for one normalized event.
func (a *TimeseriesAggregator) Consume(ev model.NormalizedEvent) {
	if a == nil {
		return
	}

	minute := truncateToMinute(ev.Timestamp)
	stats := a.operationCounts[minute]
	switch ev.Operation {
	case "INSERT":
		stats.insertEvents++
	case "UPDATE":
		stats.updateEvents++
	case "DELETE":
		stats.deleteEvents++
	}
	if ddlEvent, ok := DDLEventFromNormalizedEvent(ev); ok && ddlEvent.Operation != "" {
		stats.ddlEvents++
	}
	a.operationCounts[minute] = stats
}

// Snapshot builds a model.Timeseries projection from canonical minutes and transactions.
func (a *TimeseriesAggregator) Snapshot(minutes []model.MinuteBucket, transactions []model.Transaction) model.Timeseries {
	input := TimeseriesBuildInput{
		Minutes:      minutes,
		Transactions: transactions,
	}
	if a != nil && len(a.operationCounts) > 0 {
		input.OperationCounts = make(map[time.Time]operationMinuteStats, len(a.operationCounts))
		for minute, stats := range a.operationCounts {
			input.OperationCounts[minute] = stats
		}
	}
	return BuildTimeseries(input)
}

func collectSeriesMinutes(buckets []model.MinuteBucket, events []model.NormalizedEvent) []time.Time {
	seen := make(map[time.Time]struct{}, len(buckets)+len(events))
	minutes := make([]time.Time, 0, len(buckets)+len(events))
	for _, bucket := range buckets {
		minute := truncateToMinute(bucket.Minute)
		if _, ok := seen[minute]; ok {
			continue
		}
		seen[minute] = struct{}{}
		minutes = append(minutes, minute)
	}
	for _, ev := range events {
		minute := truncateToMinute(ev.Timestamp)
		if _, ok := seen[minute]; ok {
			continue
		}
		seen[minute] = struct{}{}
		minutes = append(minutes, minute)
	}
	sort.Slice(minutes, func(i, j int) bool { return minutes[i].Before(minutes[j]) })
	return minutes
}

func collectTransactionMinutes(transactions []model.Transaction) []time.Time {
	seen := make(map[time.Time]struct{}, len(transactions))
	minutes := make([]time.Time, 0, len(transactions))
	for _, txn := range transactions {
		ts := txn.EndTime
		if ts.IsZero() {
			ts = txn.StartTime
		}
		if ts.IsZero() {
			continue
		}
		minute := truncateToMinute(ts)
		if _, ok := seen[minute]; ok {
			continue
		}
		seen[minute] = struct{}{}
		minutes = append(minutes, minute)
	}
	return minutes
}

func buildTxnSizeSeriesSummary(transactions []model.Transaction) model.TxnSizeSeriesSummary {
	type bucketRange struct {
		label string
		min   int
		max   int
	}
	ranges := []bucketRange{
		{label: "1-9", min: 1, max: 9},
		{label: "10-99", min: 10, max: 99},
		{label: "100-999", min: 100, max: 999},
		{label: "1000+", min: 1000, max: 0},
	}

	buckets := make([]model.TxnSizeBucket, len(ranges))
	for index, r := range ranges {
		buckets[index].Label = r.label
	}

	for _, txn := range transactions {
		for index, r := range ranges {
			if txn.TotalRows < r.min {
				continue
			}
			if r.max > 0 && txn.TotalRows > r.max {
				continue
			}
			buckets[index].TxnCount++
			buckets[index].Rows += txn.TotalRows
			buckets[index].BinlogBytes += txn.BinlogBytes
			break
		}
	}

	nonEmpty := make([]model.TxnSizeBucket, 0, len(buckets))
	for _, bucket := range buckets {
		if bucket.TxnCount == 0 {
			continue
		}
		nonEmpty = append(nonEmpty, bucket)
	}
	return model.TxnSizeSeriesSummary{Buckets: nonEmpty}
}
