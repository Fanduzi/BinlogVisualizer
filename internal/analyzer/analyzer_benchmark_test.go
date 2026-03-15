package analyzer_test

import (
	"fmt"
	"testing"
	"time"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/model"
)

func BenchmarkAnalyzeSmallEventSlice(b *testing.B) {
	events := []model.NormalizedEvent{
		{
			Timestamp: time.Date(2026, 3, 9, 0, 0, 0, 0, time.UTC),
			EventType:  "ROWS",
			Schema:    "shop",
			Table:     "orders",
			Operation: "INSERT",
			RowCount:  1,
		},
	}
	a := analyzer.New(analyzer.Options{})
	for i := 0; i < b.N; i++ {
		_, _ = a.Analyze(events)
	}
}

func BenchmarkAnalyzeMediumEventSlice(b *testing.B) {
	base := time.Date(2026, 3, 9, 0, 0, 0, 0, time.UTC)
	events := make([]model.NormalizedEvent, 100)
	for i := range events {
		events[i] = model.NormalizedEvent{
			Timestamp: base.Add(time.Duration(i) * time.Millisecond),
			EventType:  "ROWS",
			Schema:    "shop",
			Table:     fmt.Sprintf("orders_%d", i),
			Operation: "INSERT",
			RowCount:  1,
		}
	}
	a := analyzer.New(analyzer.Options{})
	for i := 0; i < b.N; i++ {
		_, _ = a.Analyze(events)
	}
}

func BenchmarkAnalyzeLargeEventSlice(b *testing.B) {
	base := time.Date(2026, 3, 9, 0, 0, 0, 0, time.UTC)
	events := make([]model.NormalizedEvent, 1000)
	for i := range events {
		events[i] = model.NormalizedEvent{
			Timestamp: base.Add(time.Duration(i) * time.Millisecond),
			EventType:  "ROWS",
			Schema:    fmt.Sprintf("db_%d", i/100),
			Table:     fmt.Sprintf("table_%d", i),
			Operation: "INSERT",
			RowCount:  1,
		}
	}
	a := analyzer.New(analyzer.Options{})
	for i := 0; i < b.N; i++ {
		_, _ = a.Analyze(events)
	}
}

func BenchmarkAnalyzeMultipleTables(b *testing.B) {
	base := time.Date(2026, 3, 9, 0, 0, 0, 0, time.UTC)
	events := make([]model.NormalizedEvent, 100)
	for i := range events {
		events[i] = model.NormalizedEvent{
			Timestamp: base,
			EventType:  "ROWS",
			Schema:    fmt.Sprintf("db_%d", i),
			Table:     fmt.Sprintf("table_%d", i),
			Operation: "INSERT",
			RowCount:  1,
		}
	}
	a := analyzer.New(analyzer.Options{})
	for i := 0; i < b.N; i++ {
		_, _ = a.Analyze(events)
	}
}

func BenchmarkAnalyzeDeepTransactions(b *testing.B) {
	base := time.Date(2026, 3, 9, 0, 0, 0, 0, time.UTC)
	events := make([]model.NormalizedEvent, 30)
	for i := 0; i < 10; i++ {
		events[i] = model.NormalizedEvent{
			Timestamp: base,
			EventType:  "BEGIN",
		}
		events[i+10] = model.NormalizedEvent{
			Timestamp: base.Add(100 * time.Millisecond),
			EventType:  "ROWS",
			Schema:    "shop",
			Table:     fmt.Sprintf("table_%d", i),
			Operation: "INSERT",
			RowCount:  1,
		}
		events[i+20] = model.NormalizedEvent{
			Timestamp: base.Add(200 * time.Millisecond),
			EventType:  "XID",
		}
	}
	a := analyzer.New(analyzer.Options{})
	for i := 0; i < b.N; i++ {
		_, _ = a.Analyze(events)
	}
}

func BenchmarkAnalyzeNoOp(b *testing.B) {
	events := []model.NormalizedEvent{}
	a := analyzer.New(analyzer.Options{})
	for i := 0; i < b.N; i++ {
		_, _ = a.Analyze(events)
	}
}
