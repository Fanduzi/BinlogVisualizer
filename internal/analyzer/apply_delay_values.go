// Package analyzer keeps the apply-delay p95 input in bounded memory.
// input: one signed commit delay (microseconds) per counted transaction.
// output: the exact nearest-rank p95 over every delay, the count, and whether any delay was non-zero.
// pos: storage behind applyDelayTracker; values past delaySpillAt go to an unlinked temp file and the p95 is a radix select over it.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
)

// delaySpillAt is how many delays stay in memory before they are written to
// disk, 8 bytes each (64KiB). Past that, each transaction costs 8 bytes of
// temp file instead of heap, so analyze memory no longer grows with the
// transaction count (#364); 50M transactions is about 400MB on disk.
const delaySpillAt = 8 << 10

// delayValues stores every delay exactly. Up to spillAt values live in mem;
// past that, full chunks are appended to a temp file. p95 reads the file
// back four times (16 bits per pass) with a 65536-entry histogram, so the
// answer is the same value a full sort would pick.
type delayValues struct {
	spillAt int
	n       int
	nonZero bool
	mem     []int64
	buf     []byte

	file     *os.File
	path     string
	unlinked bool
	spilled  int64 // values safely in file[0 : spilled*8]
	noSpill  bool  // creating or writing the file failed; keep the rest in memory
	err      error

	cachedN   int
	cachedP95 int64
}

func (d *delayValues) add(us int64) {
	if d.spillAt <= 0 {
		d.spillAt = delaySpillAt
	}
	d.n++
	if us != 0 {
		d.nonZero = true
	}
	d.mem = append(d.mem, us)
	if !d.noSpill && len(d.mem) >= d.spillAt {
		d.spill()
	}
}

func (d *delayValues) spill() {
	if d.file == nil {
		f, err := os.CreateTemp("", "binlogviz-apply-delay-*.bin")
		if err != nil {
			d.noSpill = true
			return
		}
		d.file, d.path = f, f.Name()
		// Unlinked at once where the OS allows it, so a killed analyze
		// leaves nothing behind. close removes it otherwise.
		d.unlinked = os.Remove(d.path) == nil
	}
	need := len(d.mem) * 8
	if cap(d.buf) < need {
		d.buf = make([]byte, need)
	}
	b := d.buf[:need]
	for i, v := range d.mem {
		binary.LittleEndian.PutUint64(b[i*8:], uint64(v))
	}
	if _, err := d.file.WriteAt(b, d.spilled*8); err != nil {
		// Only file[0:spilled*8] is ever read back, so a short write is
		// harmless; keep these values, and later ones, in memory.
		d.noSpill = true
		return
	}
	d.spilled += int64(len(d.mem))
	d.mem = d.mem[:0]
}

// p95 is the nearest-rank 95th percentile, as percentile95 over the sorted
// values.
func (d *delayValues) p95() (int64, error) {
	if d.err != nil {
		return 0, d.err
	}
	if d.n == 0 {
		return 0, nil
	}
	if d.cachedN == d.n {
		return d.cachedP95, nil
	}
	var v int64
	if d.spilled == 0 {
		sorted := append([]int64(nil), d.mem...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
		v = percentile95(sorted)
	} else {
		var err error
		v, err = d.selectRank(nearestRank95(d.n))
		if err != nil {
			d.err = err
			return 0, err
		}
	}
	d.cachedN, d.cachedP95 = d.n, v
	return v, nil
}

// selectRank returns the k-th smallest delay (1-based). Signed values are
// mapped to uint64 with the sign bit flipped, which keeps their order.
func (d *delayValues) selectRank(k int) (int64, error) {
	if d.file == nil {
		return 0, errors.New("apply-delay values were spilled to a file that is closed")
	}
	counts := make([]uint64, 1<<16)
	var prefix uint64
	remaining := uint64(k)
	for pass := 0; pass < 4; pass++ {
		for i := range counts {
			counts[i] = 0
		}
		shift := uint(48 - 16*pass)
		match := func(u uint64) {
			if pass == 0 || u>>(shift+16) == prefix {
				counts[(u>>shift)&0xffff]++
			}
		}
		r := bufio.NewReaderSize(io.NewSectionReader(d.file, 0, d.spilled*8), 64<<10)
		var word [8]byte
		for i := int64(0); i < d.spilled; i++ {
			if _, err := io.ReadFull(r, word[:]); err != nil {
				return 0, fmt.Errorf("read spilled apply-delay values: %w", err)
			}
			match(binary.LittleEndian.Uint64(word[:]) ^ (1 << 63))
		}
		for _, v := range d.mem {
			match(uint64(v) ^ (1 << 63))
		}
		digit := -1
		for i, c := range counts {
			if remaining <= c {
				digit = i
				break
			}
			remaining -= c
		}
		if digit < 0 {
			return 0, fmt.Errorf("apply-delay rank %d not found among %d values", k, d.n)
		}
		prefix = prefix<<16 | uint64(digit)
	}
	return int64(prefix ^ (1 << 63)), nil
}

func (d *delayValues) close() {
	if d.file == nil {
		return
	}
	_ = d.file.Close()
	if !d.unlinked {
		_ = os.Remove(d.path)
	}
	d.file = nil
}

// nearestRank95 is the 1-based rank percentile95 picks for n values.
func nearestRank95(n int) int {
	rank := (95*n + 99) / 100
	if rank < 1 {
		rank = 1
	}
	if rank > n {
		rank = n
	}
	return rank
}
