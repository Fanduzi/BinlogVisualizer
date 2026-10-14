// Package binlog verifies that damaged or hostile event bytes stop the parse
// before go-mysql decodes them.
// input: the head of BinlogQA's #274 spike-m80 binlog (Format Description,
// Previous_GTIDs, one transaction with a WRITE_ROWS event at 1002) with single
// bytes flipped, once with the CRC32 footer left stale and once recomputed.
// output: a stale footer is a CorruptEventError (the CRC32 check runs before
// any decode); a recomputed footer still ends in an error, within a time limit,
// with no fatal out-of-memory and no hang.
// pos: regression for #274 QA (offsets 148-150 in Previous_GTIDs n_sids and
// 1029/1034/1035 in the rows event header), where a Go fatal error or a hang
// could not be recovered.
// note: if this file changes, update this header and README.md.
package binlog

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const hostileFixture = "testdata/qa274-spike-m80-head.binlog"

// flipAt flips one byte of the fixture and, when fixCRC is set, recomputes
// the CRC32 footer of the event that holds it, so only the bounds checks can
// catch the damage.
func flipAt(t *testing.T, off int, mask byte, fixCRC bool) string {
	t.Helper()
	body, err := os.ReadFile(hostileFixture)
	if err != nil {
		t.Fatal(err)
	}
	body[off] ^= mask
	if fixCRC {
		pos := binlogMagicSize
		for {
			size := int(binary.LittleEndian.Uint32(body[pos+9:]))
			if off < pos+size {
				binary.LittleEndian.PutUint32(body[pos+size-4:], crc32.ChecksumIEEE(body[pos:pos+size-4]))
				break
			}
			pos += size
		}
	}
	path := filepath.Join(t.TempDir(), "hostile.binlog")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func parseWithin(t *testing.T, path string, flash bool) error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		p := NewParser()
		if flash {
			p.(FlashbackParser).SetCaptureFlashback(true)
		}
		done <- p.ParseFiles([]string{path}, func(RawEvent) error { return nil })
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatalf("%s: parse did not finish", path)
		return nil
	}
}

func TestHostileFixtureIsIntact(t *testing.T) {
	if err := parseWithin(t, hostileFixture, true); err != nil {
		t.Fatalf("intact fixture: %v", err)
	}
}

func TestHostileEventBytesAreCorrupt(t *testing.T) {
	for _, off := range []int{148, 149, 150, 1029, 1034, 1035} {
		for _, mask := range []byte{0x01, 0xff} {
			for _, fixCRC := range []bool{false, true} {
				for _, flash := range []bool{false, true} {
					err := parseWithin(t, flipAt(t, off, mask, fixCRC), flash)
					var corrupt *CorruptEventError
					if err == nil || (!fixCRC && !errors.As(err, &corrupt)) {
						t.Fatalf("offset %d mask %#x fixCRC %v flash %v: want a corrupt-event error, got %v", off, mask, fixCRC, flash, err)
					}
				}
			}
		}
	}
}
