// Package binlog verifies that corrupt event bytes stop the parse.
// input: the MySQL 8.0.46 minimal-image fixture with one byte flipped inside a rows event, and a stub event callback that panics.
// output: a CorruptEventError at the start offset of the damaged event, no event from or after it, open binlogs (in-use flag on the Format Description) still accepted, and decoder panics turned into the same error while consumer panics still propagate.
// pos: parser boundary regression for corrupt input that used to decode into wrong row values with a clean exit.
// note: if this file changes, update this header and README.md.
package binlog

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
)

const (
	checksumFixture   = "testdata/mysql-8.0.46-dml-minimal.binlog"
	checksumFlipAt    = 4693 // inside the rows event that starts at 2146
	checksumEventFrom = 2146
)

func flippedFixture(t *testing.T) string {
	t.Helper()
	body, err := os.ReadFile(checksumFixture)
	if err != nil {
		t.Fatal(err)
	}
	body[checksumFlipAt] ^= 0xff
	path := filepath.Join(t.TempDir(), "flipped.binlog")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestParseFilesRefusesChecksumMismatch(t *testing.T) {
	path := flippedFixture(t)
	var lastEnd int64
	err := NewParser().ParseFiles([]string{path}, func(raw RawEvent) error {
		if raw.PositionEnd > checksumEventFrom {
			t.Fatalf("event ending at %d was handed on past the corrupt event", raw.PositionEnd)
		}
		lastEnd = raw.PositionEnd
		return nil
	})
	var corrupt *CorruptEventError
	if !errors.As(err, &corrupt) {
		t.Fatalf("want CorruptEventError, got %v", err)
	}
	if corrupt.Path != path || corrupt.Offset != checksumEventFrom || corrupt.Cause != "CRC32 checksum mismatch" {
		t.Fatalf("got %+v", corrupt)
	}
	if lastEnd != checksumEventFrom {
		t.Fatalf("last good event ended at %d, want %d", lastEnd, checksumEventFrom)
	}
}

func TestParseFilesAcceptsIntactFixture(t *testing.T) {
	if err := NewParser().ParseFiles([]string{checksumFixture}, func(RawEvent) error { return nil }); err != nil {
		t.Fatalf("intact fixture: %v", err)
	}
}

func TestParseFileGuardedTurnsDecoderPanicIntoCorruptEvent(t *testing.T) {
	consumed := int64(120)
	inHandler := false
	err := parseFileGuarded(replication.NewBinlogParser(), checksumFixture, 0, &consumed, &inHandler, nil, func(*replication.BinlogEvent) error {
		panic("slice bounds out of range [65541:243]")
	})
	var corrupt *CorruptEventError
	if !errors.As(err, &corrupt) || corrupt.Offset != 120 || corrupt.Cause != "event decoder failed on malformed event bytes" {
		t.Fatalf("got %v", err)
	}
}

func TestParseFileGuardedKeepsConsumerPanics(t *testing.T) {
	consumed := int64(0)
	inHandler := true
	defer func() {
		if r := recover(); r != "consumer bug" {
			t.Fatalf("want the consumer panic to propagate, got %v", r)
		}
	}()
	_ = parseFileGuarded(replication.NewBinlogParser(), checksumFixture, 0, &consumed, &inHandler, nil, func(*replication.BinlogEvent) error {
		panic("consumer bug")
	})
	t.Fatal("panic was swallowed")
}

// MySQL leaves LOG_EVENT_BINLOG_IN_USE_F set on the Format Description of a
// binlog that was still open when copied, after the checksum was written.
func TestParseFilesAcceptsOpenBinlogFormatDescription(t *testing.T) {
	const open = "testdata/mysql-8.0.46-no-pk-full.binlog"
	body, err := os.ReadFile(open)
	if err != nil {
		t.Fatal(err)
	}
	if body[4+binlogFlagsOffset]&byte(replication.LOG_EVENT_BINLOG_IN_USE_F) == 0 {
		t.Fatal("fixture no longer has the in-use flag; pick another open binlog")
	}
	if err := NewParser().ParseFiles([]string{open}, func(RawEvent) error { return nil }); err != nil {
		t.Fatalf("open binlog: %v", err)
	}
	body[4+binlogFlagsOffset+1] ^= 0x40 // another flag bit is real damage
	path := filepath.Join(t.TempDir(), "fde.binlog")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	var corrupt *CorruptEventError
	if err := NewParser().ParseFiles([]string{path}, func(RawEvent) error { return nil }); !errors.As(err, &corrupt) || corrupt.Offset != 4 {
		t.Fatalf("damaged Format Description: %v", err)
	}
}
