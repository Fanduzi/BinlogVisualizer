// Package binlog verifies that a transaction payload is decompressed once,
// as a bounded stream, and that its header fields are read as MySQL's
// net_field_length encoding.
// input: hand-built payload bodies (zstd frames of zeros up to 128 MiB, size
// fields with 0xfb, 0xff and a short 0xfe encoding) and the MySQL 8.0
// transaction payload fixture.
// output: a corruptCause allocated in proportion to the bytes actually
// decompressed, never in proportion to the declared size; a decoded payload
// event carrying the declared size and its inner events.
// pos: regression for BinlogQA's #293 QA at 1c51a0ae (bomb_gib: 33 KB file,
// out of memory at 3 GiB), the P3s in #278 comment 6223264959, and the r2
// many-small P1 (a fixture payload rebuilt with 600k XID events).
// note: if this file changes, update this header and README.md.
package binlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
)

func nineByteSize(v uint64) []byte {
	f := make([]byte, 9)
	f[0] = 0xfe
	binary.LittleEndian.PutUint64(f[1:], v)
	return f
}

// safeCheckPayload turns a panic into a test failure instead of a crash.
func safeCheckPayload(t *testing.T, body []byte) (err error) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("checkPayload panicked: %v", r)
		}
	}()
	return checkPayload(body, nil)
}

// allocatedBy reports the bytes f allocates on the heap.
func allocatedBy(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// A frame of zeros is the bomb: its first inner event header declares size
// 0. The check must stop there, not after buffering the whole frame.
func TestPayloadBombStopsAtFirstBadInnerEvent(t *testing.T) {
	const expanded = 128 << 20
	zeros := make([]byte, expanded)
	for _, declared := range []uint64{expanded, maxPayloadUncompressed, maxPayloadUncompressed - 1} {
		body := otwPayloadBody(t, nineByteSize(declared), zeros)
		var err error
		used := allocatedBy(func() { err = safeCheckPayload(t, body) })
		if _, ok := err.(corruptCause); !ok {
			t.Fatalf("declared %d: got %v, want a corruptCause", declared, err)
		}
		if used > 32<<20 {
			t.Fatalf("declared %d: allocated %d MiB for a %d-byte frame", declared, used>>20, len(body))
		}
	}
	zeros = nil
}

// A frame that expands past the declared size stops at the declared size.
func TestPayloadPastDeclaredStopsEarly(t *testing.T) {
	body := otwPayloadBody(t, nineByteSize(1<<24), make([]byte, 128<<20))
	var err error
	used := allocatedBy(func() { err = safeCheckPayload(t, body) })
	if _, ok := err.(corruptCause); !ok {
		t.Fatalf("got %v, want a corruptCause", err)
	}
	if used > 48<<20 {
		t.Fatalf("allocated %d MiB", used>>20)
	}
}

// 0xfb (NULL) and 0xff (error marker) are not sizes, and a 0xfe encoding
// needs 9 bytes; none may reach go-mysql or read past the field.
func TestPayloadSizeFieldEncoding(t *testing.T) {
	for _, field := range [][]byte{{0xfb}, {0xff}, {0xfe, 1, 0, 0}, {0xfc, 1}, {0xfd, 1, 0, 0, 0}} {
		err := safeCheckPayload(t, otwPayloadBody(t, field, []byte("x")))
		if err == nil || !strings.Contains(err.Error(), "malformed") {
			t.Fatalf("field % x: got %v, want malformed", field, err)
		}
		if strings.Contains(err.Error(), "runtime error") || strings.Contains(err.Error(), "index out of range") {
			t.Fatalf("field % x: leaks %v", field, err)
		}
	}
}

// The size bound names a human-readable size.
func TestPayloadBoundMessageIsReadable(t *testing.T) {
	err := safeCheckPayload(t, otwPayloadBody(t, nineByteSize(maxPayloadUncompressed+1), []byte("x")))
	if err == nil || !strings.Contains(err.Error(), "1.0 GiB") || !strings.Contains(err.Error(), "limit 1.0 GiB") {
		t.Fatalf("got %v, want sizes in GiB", err)
	}
}

// readEvents hands go-mysql's consumer a payload event it decoded itself:
// the declared size is the net_field_length value (fc ad 01 = 429; go-mysql
// v1.14 reads the bytes as a fixed-length integer) and the inner events are
// there, so the frame is not decompressed a second time.
func TestPayloadDecodedOnceWithDeclaredSize(t *testing.T) {
	var consumed int64
	seen := 0
	err := readEvents(replication.NewBinlogParser(), "testdata/mysql80_transaction_payload.binlog", 0, &consumed, nil, func(ev *replication.BinlogEvent) error {
		p, ok := ev.Event.(*replication.TransactionPayloadEvent)
		if !ok {
			return nil
		}
		seen++
		if p.UncompressedSize != 429 {
			return fmt.Errorf("UncompressedSize %d, want 429", p.UncompressedSize)
		}
		if len(p.Events) == 0 || p.CompressionType != replication.ZSTD {
			return fmt.Errorf("payload not decoded: %d events, compression %d", len(p.Events), p.CompressionType)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if seen == 0 {
		t.Fatal("no payload event")
	}
}

// manySmallPayloadFile replaces the fixture's payload event at 509 with one
// holding n XID events (27 bytes each), declared exactly, CRC32 recomputed.
func manySmallPayloadFile(t *testing.T, n int) (string, uint64) {
	t.Helper()
	fixture, err := os.ReadFile("testdata/mysql80_transaction_payload.binlog")
	if err != nil {
		t.Fatal(err)
	}
	const at, oldSize = 509, 216
	frame := make([]byte, 0, 27*n)
	for i := 0; i < n; i++ {
		x := make([]byte, 27)
		x[4] = byte(replication.XID_EVENT)
		binary.LittleEndian.PutUint32(x[5:], 1)
		binary.LittleEndian.PutUint32(x[9:], 27)
		binary.LittleEndian.PutUint64(x[19:], uint64(i))
		frame = append(frame, x...)
	}
	declared := uint64(len(frame))
	body := otwPayloadBody(t, nineByteSize(declared), frame)
	ev := append([]byte(nil), fixture[at:at+replication.EventHeaderSize]...)
	ev = append(ev, body...)
	ev = append(ev, 0, 0, 0, 0)
	binary.LittleEndian.PutUint32(ev[9:], uint32(len(ev)))
	binary.LittleEndian.PutUint32(ev[13:], uint32(at+len(ev)))
	binary.LittleEndian.PutUint32(ev[len(ev)-4:], crc32.ChecksumIEEE(ev[:len(ev)-4]))
	out := append(append([]byte(nil), fixture[:at]...), ev...)
	path := filepath.Join(t.TempDir(), "many_small.binlog")
	if err := os.WriteFile(path, out, 0o600); err != nil {
		t.Fatal(err)
	}
	return path, declared
}

// A payload of many tiny inner events must stream: the live heap while its
// events reach the handler stays a constant, not a multiple of the payload
// (#293 QA r2: 14.7M events declaring ~1 GiB reached 4 GB RSS).
func TestPayloadManySmallEventsStream(t *testing.T) {
	const n = 600_000
	path, declared := manySmallPayloadFile(t, n)
	for _, flash := range []bool{false, true} {
		p := NewParser()
		if flash {
			p.(FlashbackParser).SetCaptureFlashback(true)
		}
		var base runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&base)
		var peak uint64
		seen := 0
		sample := func() {
			var m runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&m)
			if m.HeapAlloc > base.HeapAlloc && m.HeapAlloc-base.HeapAlloc > peak {
				peak = m.HeapAlloc - base.HeapAlloc
			}
		}
		err := p.ParseFiles([]string{path}, func(raw RawEvent) error {
			seen++
			if seen == 1 || seen%100_000 == 0 {
				sample()
			}
			return nil
		})
		if err != nil {
			t.Fatalf("flash %v: %v", flash, err)
		}
		if seen < n {
			t.Fatalf("flash %v: %d events, want at least %d", flash, seen, n)
		}
		if peak > 24<<20 {
			t.Fatalf("flash %v: live heap reached %d MiB for a %d MiB payload of %d events", flash, peak>>20, declared>>20, n)
		}
	}
}

// An inner event go-mysql panics on (payload_inner_panic: a query event whose
// lengths run past it) or fails on with its own dump of the bytes
// (payload_inner_bad_query) is corrupt, named by type and offset, without Go
// runtime text or go-mysql's dump (#278 comment 6223874751). Both files are
// BinlogQA's ipe_q1 and ipe_unknown from the #293 r2 QA.
func TestPayloadInnerDecodeFailureIsPlain(t *testing.T) {
	for _, name := range []string{"payload_inner_panic.binlog", "payload_inner_bad_query.binlog"} {
		for _, flash := range []bool{false, true} {
			err := parseWithin(t, filepath.Join("testdata", name), flash)
			var corrupt *CorruptEventError
			if !errors.As(err, &corrupt) {
				t.Fatalf("%s flash %v: got %v, want a CorruptEventError", name, flash, err)
			}
			for _, leak := range []string{"runtime error", "index out of range", "slice bounds", "Data \"", "goroutine"} {
				if strings.Contains(err.Error(), leak) {
					t.Fatalf("%s flash %v: message leaks %q: %v", name, flash, leak, err)
				}
			}
			if !strings.Contains(err.Error(), "payload inner") {
				t.Fatalf("%s flash %v: %v does not name the inner event", name, flash, err)
			}
		}
	}
}
