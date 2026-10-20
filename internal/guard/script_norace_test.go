//go:build !race

package guard

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestVerifyDoesNotBufferTheScript is the streaming-verify guard. A buffered
// read keeps the script live, so post-GC heap during Read grows with the
// file. A line reader does not. The child compares a script to one four
// times larger and requires the extra peak to stay under a tenth of the
// extra bytes, and the small run's peak under maxVerifyPeak. GC runs inside
// Read, so the number is live heap, not a 1ms sample of whatever GOGC has not
// reclaimed yet.
func TestVerifyDoesNotBufferTheScript(t *testing.T) {
	if os.Getenv("BINLOGVIZ_GUARDMEM") == "child" {
		guardMemChild(t)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestVerifyDoesNotBufferTheScript$", "-test.count=1", "-test.v")
	cmd.Env = append(os.Environ(), "BINLOGVIZ_GUARDMEM=child")
	out, err := cmd.CombinedOutput()
	t.Logf("%s", out)
	if err != nil {
		t.Fatalf("verify buffered the script: %v\n%s", err, out)
	}
	if strings.Count(string(out), "peak_bytes=") < 2 {
		t.Fatalf("child printed no peak pair:\n%s", out)
	}
}

func guardMemChild(t *testing.T) {
	t.Helper()
	const smallN, largeN = 2500, 10000
	smallSize, smallPeak := verifyLivePeak(t, smallN)
	largeSize, largePeak := verifyLivePeak(t, largeN)
	fmt.Printf("n=%d size=%d peak_bytes=%d\n", smallN, smallSize, smallPeak)
	fmt.Printf("n=%d size=%d peak_bytes=%d\n", largeN, largeSize, largePeak)
	if smallSize < 1<<20 || largeSize < smallSize*3 {
		t.Fatalf("script did not scale: %d -> %d", smallSize, largeSize)
	}
	extraSize := largeSize - smallSize
	extraPeak := int64(largePeak) - int64(smallPeak)
	if extraPeak < 0 {
		extraPeak = 0
	}
	if extraPeak*10 >= extraSize {
		t.Fatalf("verify peak grew %d bytes for %d extra script bytes", extraPeak, extraSize)
	}
	if smallPeak >= maxVerifyPeak {
		t.Fatalf("verify held %d bytes reading a %d-byte script; a line reader needs under %d", smallPeak, smallSize, maxVerifyPeak)
	}
}

// maxVerifyPeak caps the live heap Verify may add while reading the small
// script. Streaming measures about 70KB (the 64KiB line buffer). A 16MiB or
// 64MiB read buffer does not grow with the file, so only this cap sees it.
const maxVerifyPeak = 1 << 20

// heapReader records the high post-GC HeapAlloc seen while Verify reads.
// The buffer a slurp retains is still reachable at the return from Read.
type heapReader struct {
	r     io.Reader
	peak  uint64
	reads int
}

func (h *heapReader) Read(p []byte) (int, error) {
	n, err := h.r.Read(p)
	h.reads++
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	if m.HeapAlloc > h.peak {
		h.peak = m.HeapAlloc
	}
	return n, err
}

func verifyLivePeak(t *testing.T, n int) (size int64, growth uint64) {
	t.Helper()
	g := New(nil)
	stmt := "UPDATE t SET v = '" + strings.Repeat("a", 1024) + "' WHERE id = 1"
	path := filepath.Join(t.TempDir(), "flashback.sql")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	err = g.Write(f, func(emit func(Piece) error) error {
		for i := 0; i < n; i++ {
			if err := emit(Piece{Kind: PieceStmt, Text: stmt}); err != nil {
				return err
			}
		}
		return nil
	})
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)
	in, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	hr := &heapReader{r: in, peak: base.HeapAlloc}
	res, err := Verify(hr)
	_ = in.Close()
	if err != nil {
		t.Fatal(err)
	}
	if res.Statements != n || len(res.Digest) != 64 {
		t.Fatalf("result %+v", res)
	}
	if hr.reads == 0 {
		t.Fatal("Verify never read the script, so nothing was measured")
	}
	if hr.peak <= base.HeapAlloc {
		return info.Size(), 0
	}
	return info.Size(), hr.peak - base.HeapAlloc
}
