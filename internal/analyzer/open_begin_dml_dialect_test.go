// Package analyzer covers the open BEGIN+DML path through ParseFiles.
// input: testdata/mysql-8.0.46-open-begin-dml.binlog, and a prefix of that file that stops at the later GTID.
// output: OpenBeginError naming duration, rows, tables, and file span; the EOF prefix keeps one open DML group.
// pos: ParseFiles regression for the #89 open-uncommitted path. The fixture is MySQL 8.0.46 bytes with the first XID removed.
// note: if this file changes, update this header and README.md.
package analyzer

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"binlogviz/internal/binlog"

	"github.com/go-mysql-org/go-mysql/replication"
)

func TestParseFilesOpenBeginDMLNextGTIDNamesEvidence(t *testing.T) {
	path := filepath.Join("..", "binlog", "testdata", "mysql-8.0.46-open-begin-dml.binlog")
	p := binlog.NewParser()
	a := New(DefaultOptions())

	var (
		flavor   string
		version  string
		sawBegin bool
		rows     int
		sawClose bool
	)
	err := p.ParseFiles([]string{path}, func(raw binlog.RawEvent) error {
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
		case "BEGIN":
			sawBegin = true
		case "ROWS":
			if !sawBegin {
				t.Fatal("row image appeared before explicit BEGIN")
			}
			rows += ev.RowCount
		case "XID", "COMMIT", "ROLLBACK":
			sawClose = true
		}
		return a.Consume(*ev)
	})
	if flavor != "mysql" || !strings.HasPrefix(version, "8.0.46") {
		t.Fatalf("fixture must name MySQL 8.0.46, got flavor=%q version=%q", flavor, version)
	}
	if !sawBegin || rows != 2 || sawClose {
		t.Fatalf("open group begin=%v rows=%d closed=%v, want BEGIN and 2 rows with no COMMIT/ROLLBACK/XID", sawBegin, rows, sawClose)
	}
	var openBegin *OpenBeginError
	if err == nil || !errors.As(err, &openBegin) {
		t.Fatalf("expected OpenBeginError from the later GTID, got %v", err)
	}
	if openBegin.Rows != 2 || openBegin.Tables != "testdb.users" || openBegin.Duration <= 0 {
		t.Fatalf("open DML evidence = %+v", openBegin)
	}
	if !strings.Contains(openBegin.Location, "mysql-8.0.46-open-begin-dml.binlog:") || !strings.Contains(openBegin.Location, "-") {
		t.Fatalf("file span = %q", openBegin.Location)
	}
	for _, want := range []string{
		"open BEGIN without close",
		"not lock-contention proof",
		"dur=",
		"rows=2",
		"tables=testdb.users",
		openBegin.Location,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error missing %q: %v", want, err)
		}
	}
}

func TestParseFilesOpenBeginDMLPrefixKeepsGroup(t *testing.T) {
	src := filepath.Join("..", "binlog", "testdata", "mysql-8.0.46-open-begin-dml.binlog")
	path := openBeginDMLPrefix(t, src)
	p := binlog.NewParser()
	a := New(DefaultOptions())
	if err := p.ParseFiles([]string{path}, func(raw binlog.RawEvent) error {
		ev, normErr := binlog.NormalizeRawEvent(raw)
		if normErr != nil {
			return normErr
		}
		if ev == nil {
			return nil
		}
		return a.Consume(*ev)
	}); err != nil {
		t.Fatal(err)
	}
	result, err := a.Finalize()
	if err != nil {
		t.Fatalf("EOF-open prefix: %v", err)
	}
	if result.Diagnostics.OpenExplicitGroups != 1 || len(result.Diagnostics.OpenDMLGroups) != 1 {
		t.Fatalf("open groups=%d dml=%d", result.Diagnostics.OpenExplicitGroups, len(result.Diagnostics.OpenDMLGroups))
	}
	group := result.Diagnostics.OpenDMLGroups[0]
	if group.TotalRows != 2 || group.Tables["testdb.users"] != 2 || group.GTID == "" {
		t.Fatalf("open DML = %+v", group)
	}
	if group.PositionStart <= 0 || group.PositionEnd <= group.PositionStart {
		t.Fatalf("open DML span = %d-%d", group.PositionStart, group.PositionEnd)
	}
	if len(result.Diagnostics.LongestTransactions) != 0 {
		t.Fatalf("open DML must stay out of committed duration ranking, got %+v", result.Diagnostics.LongestTransactions)
	}
}

// openBeginDMLPrefix is the EOF-open reading: stop at the later business GTID.
func openBeginDMLPrefix(t *testing.T, src string) string {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) < 4 || string(data[:4]) != "\xfebin" {
		t.Fatal("bad binlog magic")
	}
	pos, gtids, cut := 4, 0, 0
	for pos+19 <= len(data) {
		size := int(binary.LittleEndian.Uint32(data[pos+9 : pos+13]))
		if size < 19 || pos+size > len(data) {
			t.Fatalf("truncated event at %d", pos)
		}
		if data[pos+4] == byte(replication.GTID_EVENT) {
			gtids++
			if gtids == 2 {
				cut = pos
				break
			}
		}
		pos += size
	}
	if cut == 0 {
		t.Fatal("fixture missing a later GTID")
	}
	dst := filepath.Join(t.TempDir(), "open-begin-eof.binlog")
	if err := os.WriteFile(dst, data[:cut], 0o644); err != nil {
		t.Fatal(err)
	}
	return dst
}
