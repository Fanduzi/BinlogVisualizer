// Package binlogviz benchmarks analyze streaming over DBA-oriented SQL corpus workloads.
// input: deterministic sql-corpus raw events expanded into larger multi-minute, multi-file workloads.
// output: benchmark evidence for mixed INSERT/UPDATE/DELETE, DDL, and transaction-evidence analyze paths.
// pos: command-layer performance guardrail between tiny fixture smoke tests and synthetic one-table stress tests.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
)

func BenchmarkStreamingCorpusEventMixBurstScaled(b *testing.B) {
	events := repeatCorpusRawEvents(loadCorpusRawEvents(b, "event-mix-burst"), 5000, 3*time.Minute)
	opts := analyzer.DefaultOptions()
	opts.DetectSpikes = true

	b.ResetTimer()
	benchmarkStreamingPipeline(b, []string{"corpus-event-mix-burst-scaled"}, &mockParser{events: events}, opts)
}

func BenchmarkStreamingCorpusIncidentMixedScaled(b *testing.B) {
	events := repeatCorpusRawEvents(loadCorpusRawEvents(b, "incident-mixed"), 5000, 8*time.Minute)
	opts := analyzer.DefaultOptions()
	opts.DetectSpikes = true
	opts.LargeTxnRows = 1000

	b.ResetTimer()
	benchmarkStreamingPipeline(b, []string{"corpus-incident-mixed-scaled"}, &mockParser{events: events}, opts)
}

func loadCorpusRawEvents(tb testing.TB, scenario string) []binlog.RawEvent {
	tb.Helper()

	path := filepath.Join("testdata", "sql-corpus", scenario, "events.json")
	rawBytes, err := os.ReadFile(path)
	if err != nil {
		tb.Fatalf("read corpus %s: %v", scenario, err)
	}

	var corpus []corpusRawEvent
	if err := json.Unmarshal(rawBytes, &corpus); err != nil {
		tb.Fatalf("decode corpus %s: %v", scenario, err)
	}

	events := make([]binlog.RawEvent, 0, len(corpus))
	for _, item := range corpus {
		ts, err := time.Parse(time.RFC3339, item.Timestamp)
		if err != nil {
			tb.Fatalf("parse corpus %s timestamp %q: %v", scenario, item.Timestamp, err)
		}
		events = append(events, binlog.RawEvent{
			Timestamp:     ts,
			BinlogPath:    item.BinlogPath,
			PositionStart: item.PositionStart,
			PositionEnd:   item.PositionEnd,
			BinlogBytes:   item.BinlogBytes,
			EventType:     item.EventType,
			Schema:        item.Schema,
			Table:         item.Table,
			Query:         item.Query,
			QuerySQL:      item.QuerySQL,
			RowCount:      item.RowCount,
		})
	}
	return events
}

func repeatCorpusRawEvents(raw []binlog.RawEvent, copies int, stride time.Duration) []binlog.RawEvent {
	if len(raw) == 0 || copies <= 0 {
		return nil
	}

	events := make([]binlog.RawEvent, 0, len(raw)*copies)
	for copyIndex := 0; copyIndex < copies; copyIndex++ {
		offset := time.Duration(copyIndex) * stride
		positionOffset := int64(copyIndex) * 1_000_000
		for _, event := range raw {
			cloned := event
			cloned.Timestamp = cloned.Timestamp.Add(offset)
			if cloned.BinlogPath != "" {
				cloned.BinlogPath = fmt.Sprintf("%s.copy%04d", cloned.BinlogPath, copyIndex)
			}
			if cloned.PositionStart != 0 {
				cloned.PositionStart += positionOffset
			}
			if cloned.PositionEnd != 0 {
				cloned.PositionEnd += positionOffset
			}
			events = append(events, cloned)
		}
	}
	return events
}
