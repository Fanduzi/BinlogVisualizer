// Run: go test -fuzz=FuzzBinlogParse -fuzztime=20s ./internal/binlog
// Run: go test -fuzz=FuzzRemoteEventFrame -fuzztime=20s ./internal/binlog
// For short file-parser runs, -fuzzminimizetime=1x limits coverage minimization.
package binlog

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// FuzzBinlogParse feeds whole mutated binlog files through the public parse
// entry. Errors are correct; only a panic escaping ParseFiles is a bug.
// mode toggles the row/flashback/redo capture paths, which decode more.
func FuzzBinlogParse(f *testing.F) {
	entries, err := os.ReadDir("testdata")
	if err != nil {
		f.Fatal(err)
	}
	var first []byte
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".binlog") {
			continue
		}
		data, err := os.ReadFile(filepath.Join("testdata", entry.Name()))
		if err != nil {
			f.Fatal(err)
		}
		if first == nil {
			first = data
		}
		f.Add(data, byte(0))
	}
	if len(first) == 0 {
		f.Fatal("testdata holds no .binlog fixtures")
	}
	// Truncated prefixes keep the magic so the event loop runs on partial input.
	for _, n := range []int{4, 19, len(first) / 2, len(first) - 1} {
		if n < len(first) {
			f.Add(first[:n], byte(0))
		}
	}
	// Byte-flipped variants: magic, an event-size field, mid-body, tail.
	for _, off := range []int{0, 13, len(first) / 2, len(first) - 1} {
		bad := append([]byte(nil), first...)
		bad[off] ^= 0xff
		f.Add(bad, byte(0))
	}
	// Full capture modes exercise the rows, flashback and redo decoders.
	f.Add(first, byte(7))
	f.Fuzz(func(t *testing.T, data []byte, mode byte) {
		path := filepath.Join(t.TempDir(), "fuzz.binlog")
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		p := NewParser()
		p.(RowImageParser).SetCaptureRowImages(mode&1 != 0)
		p.(FlashbackParser).SetCaptureFlashback(mode&2 != 0)
		p.(RedoParser).SetCaptureRedo(mode&4 != 0)
		_ = p.ParseFiles([]string{path}, func(RawEvent) error { return nil })
	})
}

// FuzzRemoteEventFrame feeds one mutated event frame through the remote
// parser's checked dispatch — the bytes a replication server controls. A
// constant Format Description frame goes first, as a real stream does, so
// the fuzzed frame is CRC-checked when the fixture declares the algorithm.
func FuzzRemoteEventFrame(f *testing.F) {
	fde := splitFrames(f, "testdata/mysql-8.0.46-dml-full.binlog")[0]
	for _, fixture := range []string{
		"testdata/mysql-8.0.46-dml-full.binlog",
		"testdata/mysql80_transaction_payload.binlog",
		"testdata/mariadb-10.11.14-dml.binlog",
		"testdata/qa274-spike-m80-head.binlog",
	} {
		for _, frame := range splitFrames(f, fixture) {
			f.Add(frame)
		}
	}
	f.Add(makeRotateFrame("mysql-bin.000005", 4, 0, true))
	f.Fuzz(func(t *testing.T, frame []byte) {
		src := &fakeSource{frames: [][]byte{fde, frame}}
		p := NewRemoteParser(context.Background(), src, RemoteBoundary{})
		_ = p.ParseFiles([]string{"remote://fuzz"}, func(RawEvent) error { return nil })
	})
}
