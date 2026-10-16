// Package binlog verifies that hostile values inside events with a correct
// CRC32 stop the parse before go-mysql decodes them.
// input: the #274 spike head fixture with the JSON value of its UPDATE rows
// event (at 493) given a self-referencing offset, and the MySQL 8.0 transaction
// payload fixture with its declared uncompressed size changed; CRC32 footers
// recomputed; hand-built payload bodies with a 9-byte uncompressed-size field.
// output: an error within a time limit, never a fatal out-of-memory, infinite
// recursion or an unbounded decompression.
// pos: regression for BinlogQA's #274 QA at 1f01cf51 (json-selfref-m80) and
// the transaction-payload size gap; #278 (9-byte size field reaches the size
// checks).
// note: if this file changes, update this header and README.md.
package binlog

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/klauspost/compress/zstd"
)

// rewriteEvent applies edit to the fixture and recomputes the CRC32 footer of
// the event that starts at eventStart.
func rewriteEvent(t *testing.T, fixture string, eventStart int, edit func([]byte)) string {
	t.Helper()
	body, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatal(err)
	}
	edit(body)
	size := int(binary.LittleEndian.Uint32(body[eventStart+9:]))
	end := eventStart + size
	binary.LittleEndian.PutUint32(body[end-4:], crc32.ChecksumIEEE(body[eventStart:end-4]))
	path := filepath.Join(t.TempDir(), "hostile.binlog")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// The UPDATE rows event at 493 carries JSON documents. Zeroing 660..663 makes
// a container value offset point back at the container itself, which made
// go-mysql's JSON decoder recurse until the process ran out of memory.
func TestJSONSelfReferenceIsCorrupt(t *testing.T) {
	path := rewriteEvent(t, hostileFixture, 493, func(b []byte) {
		for i := 660; i < 664; i++ {
			b[i] = 0
		}
	})
	for _, flash := range []bool{false, true} {
		if err := parseWithin(t, path, flash); err == nil {
			t.Fatalf("flash %v: self-referencing JSON parsed without error", flash)
		}
	}
}

// The payload event at 509 declares its uncompressed size in a 3-byte
// net_field_length field at body offset 5 (fc ad 01 = 429). A size that
// disagrees with the zstd frame is corrupt: a smaller one would let the frame
// expand past the bound, a larger one is not what MySQL wrote.
func TestPayloadUncompressedSizeMustMatch(t *testing.T) {
	const fixture = "testdata/mysql80_transaction_payload.binlog"
	const field = 509 + 19 + 5
	for _, declared := range []uint16{0x100, 0xffff} {
		path := rewriteEvent(t, fixture, 509, func(b []byte) {
			b[field] = 0xfc
			b[field+1] = byte(declared)
			b[field+2] = byte(declared >> 8)
		})
		for _, flash := range []bool{false, true} {
			if err := parseWithin(t, path, flash); err == nil {
				t.Fatalf("declared %d flash %v: payload parsed without error", declared, flash)
			}
		}
	}
}

// otwPayloadBody builds a Transaction_payload body whose uncompressed-size
// field carries sizeField verbatim (MySQL's net_store_length encoding),
// followed by a zstd frame of frame.
func otwPayloadBody(t *testing.T, sizeField []byte, frame []byte) []byte {
	t.Helper()
	zw, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer zw.Close()
	body := []byte{replication.OTW_PAYLOAD_COMPRESSION_TYPE_FIELD, 1, byte(replication.ZSTD)}
	body = append(body, replication.OTW_PAYLOAD_UNCOMPRESSED_SIZE_FIELD, byte(len(sizeField)))
	body = append(body, sizeField...)
	body = append(body, replication.OTW_PAYLOAD_HEADER_END_MARK)
	return zw.EncodeAll(frame, body)
}

// MySQL encodes a size of 2^24 or more in 9 bytes (0xfe and 8 value bytes).
// A 1 GiB declaration must reach the size checks, not be refused as a header
// field longer than the event (#278).
func TestPayloadNineByteSizeFieldReachesSizeChecks(t *testing.T) {
	nineByte := func(v uint64) []byte {
		f := make([]byte, 9)
		f[0] = 0xfe
		binary.LittleEndian.PutUint64(f[1:], v)
		return f
	}
	cases := []struct {
		declared uint64
		want     string
	}{
		{maxPayloadUncompressed + 1, "uncompressed, more than the limit"},
		{2 << 30, "uncompressed, more than the limit"},
		{maxPayloadUncompressed, "not the declared"},
	}
	for _, c := range cases {
		err := checkPayload(otwPayloadBody(t, nineByte(c.declared), []byte("x")), nil)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("declared %d: got %v, want an error containing %q", c.declared, err, c.want)
		}
	}
	// A field longer than any net_store_length encoding is still refused.
	err := checkPayload(otwPayloadBody(t, append(nineByte(1), 0), []byte("x")), nil)
	if err == nil || !strings.Contains(err.Error(), "header field exceeds the event") {
		t.Fatalf("10-byte field: got %v", err)
	}
}
