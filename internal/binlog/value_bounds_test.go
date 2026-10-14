// Package binlog verifies the binary JSON and payload bounds in value_bounds.go.
// input: hand-built MySQL binary JSON documents (nested arrays, shared and self-referencing offsets, keys outside the object).
// output: documents MySQL could have written pass; depth over 100, offsets outside or back into the header, and shared values that would blow up the walk are corrupt.
// pos: unit level for the JSON check that runs before go-mysql's recursive decoder.
// note: if this file changes, update this header and README.md.
package binlog

import (
	"encoding/binary"
	"strings"
	"testing"
)

// nestedArrays returns a document of depth nested small arrays around an
// int16 0: [[[...0...]]].
func nestedArrays(depth int) []byte {
	inner := []byte{0x05, 0, 0} // int16 0 (type + inline value for the top)
	value := inner[1:]
	tp := byte(0x05)
	for i := 0; i < depth; i++ {
		// small array, count 1, one value entry (type + 2-byte offset or inline)
		var arr []byte
		if tp == 0x05 {
			arr = make([]byte, 4+3)
			binary.LittleEndian.PutUint16(arr[0:], 1)
			binary.LittleEndian.PutUint16(arr[2:], uint16(len(arr)))
			arr[4] = tp
			copy(arr[5:], value)
		} else {
			arr = make([]byte, 4+3, 4+3+len(value))
			binary.LittleEndian.PutUint16(arr[0:], 1)
			arr[4] = tp
			binary.LittleEndian.PutUint16(arr[5:], 7)
			arr = append(arr, value...)
			binary.LittleEndian.PutUint16(arr[2:], uint16(len(arr)))
		}
		value, tp = arr, 0x02
	}
	return append([]byte{tp}, value...)
}

func TestValidateJSONBinaryDepth(t *testing.T) {
	if err := validateJSONBinary(nestedArrays(100)); err != nil {
		t.Fatalf("depth 100: %v", err)
	}
	if err := validateJSONBinary(nestedArrays(101)); err == nil || !strings.Contains(err.Error(), "nesting deeper than 100") {
		t.Fatalf("depth 101: %v", err)
	}
}

func TestValidateJSONBinaryOffsets(t *testing.T) {
	// [x] with the value entry pointing at the array itself (offset 0) or
	// into its header (offset 4), or past its end.
	for _, off := range []uint16{0, 4, 9, 200} {
		doc := []byte{0x02, 1, 0, 9, 0, 0x02, 0, 0, 0, 0}
		binary.LittleEndian.PutUint16(doc[6:], off)
		if err := validateJSONBinary(doc); err == nil {
			t.Fatalf("offset %d accepted", off)
		}
	}
	// {"k": 1} with the key offset inside the header.
	obj := []byte{0x00, 1, 0, 12, 0, 4, 0, 1, 0, 0x05, 1, 0, 'k'}
	if err := validateJSONBinary(obj); err == nil {
		t.Fatal("key offset inside the header accepted")
	}
	binary.LittleEndian.PutUint16(obj[5:], 11)
	if err := validateJSONBinary(obj); err != nil {
		t.Fatalf("valid object: %v", err)
	}
}

// Thirty levels of [a, a] where both entries point at the same child would
// make a decoder visit 2^30 values; the walk budget stops it.
func TestValidateJSONBinarySharedValues(t *testing.T) {
	value := []byte{0x05, 0, 0}[1:]
	tp := byte(0x05)
	for i := 0; i < 30; i++ {
		var arr []byte
		if tp == 0x05 {
			arr = []byte{2, 0, 10, 0, 0x05, 0, 0, 0x05, 0, 0}
		} else {
			arr = make([]byte, 10, 10+len(value))
			binary.LittleEndian.PutUint16(arr[0:], 2)
			arr[4], arr[7] = tp, tp
			binary.LittleEndian.PutUint16(arr[5:], 10)
			binary.LittleEndian.PutUint16(arr[8:], 10)
			arr = append(arr, value...)
			binary.LittleEndian.PutUint16(arr[2:], uint16(len(arr)))
		}
		value, tp = arr, 0x02
	}
	if err := validateJSONBinary(append([]byte{tp}, value...)); err == nil {
		t.Fatal("shared values accepted")
	}
}

func TestValidateJSONDiffs(t *testing.T) {
	// REPLACE $.a with int16 7, then REMOVE $.b.
	good := []byte{0, 3, '$', '.', 'a', 3, 0x05, 7, 0, 2, 3, '$', '.', 'b'}
	if err := validateJSONDiffs(good); err != nil {
		t.Fatalf("valid diff: %v", err)
	}
	for _, bad := range [][]byte{
		{9, 0},                           // unknown operation
		{0, 50, '$'},                     // path past the cell
		{0, 1, '$', 9, 0x05},             // value past the cell
		{0, 1, '$', 5, 0x02, 1, 0, 9, 0}, // array header past the value
		{0, 1, '$', 4, 0x02, 1, 0, 0},    // truncated container
	} {
		if err := validateJSONDiffs(bad); err == nil {
			t.Fatalf("%v accepted", bad)
		}
	}
}
