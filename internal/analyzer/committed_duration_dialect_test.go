// Package analyzer covers committed multi-second duration through ParseFiles.
// input: testdata/mysql-8.0.46-committed-duration.binlog.
// output: the SLEEP transaction ranks first by duration, a non-<1s bucket is non-zero, and a lower --large-trx-duration alerts with that duration.
// pos: ParseFiles regression for the #89 committed duration path. Timestamps are the server clock; the leading GTID shares the XID commit second.
// note: if this file changes, update this header and README.md.
package analyzer

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/binlog"
)

func TestParseFilesCommittedDurationFixtureRanksAndAlerts(t *testing.T) {
	path := filepath.Join("..", "binlog", "testdata", "mysql-8.0.46-committed-duration.binlog")
	p := binlog.NewParser()
	opts := DefaultOptions()
	opts.LargeTxnDuration = time.Second
	a := New(opts)

	var (
		flavor  string
		version string
		group   committedDurationStamps
		spans   []committedDurationStamps
	)
	if err := p.ParseFiles([]string{path}, func(raw binlog.RawEvent) error {
		if raw.ServerFlavor != "" {
			flavor = raw.ServerFlavor
		}
		if raw.ServerVersion != "" {
			version = raw.ServerVersion
		}
		ev, normErr := binlog.NormalizeRawEvent(raw)
		if normErr != nil {
			return normErr
		}
		if ev == nil {
			return nil
		}
		switch ev.EventType {
		case "GTID":
			if group.seen() {
				spans = append(spans, group)
			}
			group = committedDurationStamps{gtid: ev.Timestamp}
		case "BEGIN":
			group.begin = ev.Timestamp
		case "XID":
			group.xid = ev.Timestamp
		}
		return a.Consume(*ev)
	}); err != nil {
		t.Fatal(err)
	}
	if group.seen() {
		spans = append(spans, group)
	}
	if flavor != "mysql" || !strings.HasPrefix(version, "8.0.46") {
		t.Fatalf("fixture must name MySQL 8.0.46, got flavor=%q version=%q", flavor, version)
	}

	var wall time.Duration
	for _, span := range spans {
		if span.begin.IsZero() || span.xid.IsZero() || !span.xid.After(span.begin) {
			continue
		}
		if !span.gtid.Equal(span.xid) {
			t.Fatalf("GTID stamp %s and XID stamp %s must share the commit second", span.gtid, span.xid)
		}
		got := span.xid.Sub(span.begin)
		if got > wall {
			wall = got
		}
	}
	if wall < time.Second || wall >= 10*time.Second {
		t.Fatalf("BEGIN-to-XID wall span = %s, want 1s-10s", wall)
	}

	result, err := a.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Diagnostics.OpenDMLGroups) != 0 {
		t.Fatalf("committed fixture leaked open DML: %+v", result.Diagnostics.OpenDMLGroups)
	}
	if len(result.Diagnostics.LongestTransactions) == 0 {
		t.Fatal("no longest-duration ranking")
	}
	longest := result.Diagnostics.LongestTransactions[0]
	if longest.Duration != wall || longest.Tables["testdb.users"] != 1 {
		t.Fatalf("longest = %+v, want duration %s on testdb.users", longest, wall)
	}
	if !hasDurationBucket(result.Diagnostics.DurationBuckets, "<1s", 1) || !hasDurationBucket(result.Diagnostics.DurationBuckets, "1s-10s", 1) {
		t.Fatalf("buckets = %+v", result.Diagnostics.DurationBuckets)
	}
	want := "exceeds duration threshold (" + longest.Duration.Truncate(time.Millisecond).String() + ")"
	var saw bool
	for _, finding := range result.Diagnostics.Findings {
		if strings.Contains(finding.Message, want) {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("findings = %+v, want %q", result.Diagnostics.Findings, want)
	}
}

type committedDurationStamps struct {
	gtid  time.Time
	begin time.Time
	xid   time.Time
}

func (s committedDurationStamps) seen() bool {
	return !s.gtid.IsZero() || !s.begin.IsZero() || !s.xid.IsZero()
}
