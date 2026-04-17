// Package analyzer verifies timeseries projection helpers built from minute, event, and transaction inputs.
// input: aggregated minute buckets, normalized events, and finalized transactions.
// output: regression coverage for chart-ready series projection, zero-filling, sorting, and transaction-size summaries.
// pos: contract tests for future Analyzer timeseries integration without mutating existing aggregation paths.
package analyzer

import (
	"testing"
	"time"

	"binlogviz/internal/model"
)

func TestBuildTimeseriesProjectsMinuteAndOperationSeries(t *testing.T) {
	minuteA := time.Date(2026, 4, 15, 9, 1, 0, 0, time.UTC)
	minuteB := minuteA.Add(time.Minute)

	got := BuildTimeseries(TimeseriesBuildInput{
		Minutes: []model.MinuteBucket{
			{Minute: minuteB, TotalRows: 7, TxnCount: 2, EventCount: 3, BinlogBytes: 700, DDLCount: 1},
			{Minute: minuteA, TotalRows: 5, TxnCount: 1, EventCount: 2, BinlogBytes: 500, DDLCount: 0},
		},
		Events: []model.NormalizedEvent{
			{Timestamp: minuteA.Add(10 * time.Second), EventType: "ROWS", Operation: "INSERT", RowCount: 4},
			{Timestamp: minuteA.Add(20 * time.Second), EventType: "ROWS", Operation: "DELETE", RowCount: 1},
			{Timestamp: minuteB.Add(10 * time.Second), EventType: "ROWS", Operation: "INSERT", RowCount: 2},
			{Timestamp: minuteB.Add(20 * time.Second), EventType: "ROWS", Operation: "UPDATE", RowCount: 3},
			{Timestamp: minuteB.Add(30 * time.Second), EventType: "QUERY", QuerySQL: "ALTER TABLE app.orders ADD COLUMN note TEXT"},
		},
	})

	assertPointSeries(t, got.TPSSeries, []model.TimeseriesPoint{
		{Minute: minuteA, Value: 1.0 / 60.0},
		{Minute: minuteB, Value: 2.0 / 60.0},
	})
	assertPointSeries(t, got.RowsSeries, []model.TimeseriesPoint{
		{Minute: minuteA, Value: 5},
		{Minute: minuteB, Value: 7},
	})
	assertPointSeries(t, got.EventsSeries, []model.TimeseriesPoint{
		{Minute: minuteA, Value: 2},
		{Minute: minuteB, Value: 3},
	})
	assertPointSeries(t, got.InsertEventSeries, []model.TimeseriesPoint{
		{Minute: minuteA, Value: 1},
		{Minute: minuteB, Value: 1},
	})
	assertPointSeries(t, got.UpdateEventSeries, []model.TimeseriesPoint{
		{Minute: minuteA, Value: 0},
		{Minute: minuteB, Value: 1},
	})
	assertPointSeries(t, got.DeleteEventSeries, []model.TimeseriesPoint{
		{Minute: minuteA, Value: 1},
		{Minute: minuteB, Value: 0},
	})
	assertPointSeries(t, got.DDLEventSeries, []model.TimeseriesPoint{
		{Minute: minuteA, Value: 0},
		{Minute: minuteB, Value: 1},
	})
	assertPointSeries(t, got.BinlogBytesSeries, []model.TimeseriesPoint{
		{Minute: minuteA, Value: 500},
		{Minute: minuteB, Value: 700},
	})
}

func TestBuildTimeseriesBuildsTransactionSizeSummary(t *testing.T) {
	got := BuildTimeseries(TimeseriesBuildInput{
		Transactions: []model.Transaction{
			{TxnKey: "txn-1", TotalRows: 5, BinlogBytes: 50},
			{TxnKey: "txn-2", TotalRows: 50, BinlogBytes: 500},
			{TxnKey: "txn-3", TotalRows: 500, BinlogBytes: 5000},
			{TxnKey: "txn-4", TotalRows: 1200, BinlogBytes: 12000},
		},
	})

	want := []model.TxnSizeBucket{
		{Label: "1-9", TxnCount: 1, Rows: 5, BinlogBytes: 50},
		{Label: "10-99", TxnCount: 1, Rows: 50, BinlogBytes: 500},
		{Label: "100-999", TxnCount: 1, Rows: 500, BinlogBytes: 5000},
		{Label: "1000+", TxnCount: 1, Rows: 1200, BinlogBytes: 12000},
	}
	if len(got.TxnSizeSeriesSummary.Buckets) != len(want) {
		t.Fatalf("expected %d txn-size buckets, got %d", len(want), len(got.TxnSizeSeriesSummary.Buckets))
	}
	for index := range want {
		if got.TxnSizeSeriesSummary.Buckets[index] != want[index] {
			t.Fatalf("bucket %d: expected %+v, got %+v", index, want[index], got.TxnSizeSeriesSummary.Buckets[index])
		}
	}
}

func TestBuildTimeseriesUsesAverageTPSPerMinute(t *testing.T) {
	minute := time.Date(2026, 4, 17, 9, 0, 0, 0, time.UTC)
	series := BuildTimeseries(TimeseriesBuildInput{
		Minutes: []model.MinuteBucket{{
			Minute:   minute,
			TxnCount: 120,
		}},
	})

	if len(series.TPSSeries) != 1 {
		t.Fatalf("expected one TPS point, got %d", len(series.TPSSeries))
	}
	if got := series.TPSSeries[0].Value; got != 2 {
		t.Fatalf("expected avg TPS/min 2.0, got %.2f", got)
	}
}

func assertPointSeries(t *testing.T, got, want []model.TimeseriesPoint) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("expected %d points, got %d", len(want), len(got))
	}
	for index := range want {
		if !got[index].Minute.Equal(want[index].Minute) || got[index].Value != want[index].Value {
			t.Fatalf("point %d: expected (%s,%v), got (%s,%v)", index, want[index].Minute, want[index].Value, got[index].Minute, got[index].Value)
		}
	}
}
