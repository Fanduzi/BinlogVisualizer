//go:build !race

package guard

import (
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestVerifyDoesNotBufferTheScript(t *testing.T) {
	g := New(nil)
	stmt := "UPDATE t SET v = '" + strings.Repeat("a", 1024) + "' WHERE id = 1"
	path := filepath.Join(t.TempDir(), "flashback.sql")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	const n = 2500
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
	if info.Size() < 2<<20 {
		t.Fatalf("script %d too small", info.Size())
	}
	// A low GC percent drops line buffers. A full-script buffer stays live
	// until Verify returns, so the sampled heap still shows it.
	prev := debug.SetGCPercent(1)
	defer debug.SetGCPercent(prev)
	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)
	var peak atomic.Uint64
	peak.Store(base.HeapAlloc)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		var m runtime.MemStats
		for {
			runtime.ReadMemStats(&m)
			for {
				old := peak.Load()
				if m.HeapAlloc <= old || peak.CompareAndSwap(old, m.HeapAlloc) {
					break
				}
			}
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()
	in, err := os.Open(path)
	if err != nil {
		close(stop)
		<-done
		t.Fatal(err)
	}
	res, err := Verify(in)
	_ = in.Close()
	close(stop)
	<-done
	if err != nil {
		t.Fatal(err)
	}
	if res.Statements != n || len(res.Digest) != 64 {
		t.Fatalf("result %+v", res)
	}
	growth := peak.Load() - base.HeapAlloc
	// A buffered read keeps the script live. Line buffers do not.
	if growth >= uint64(info.Size()) {
		t.Fatalf("verify peak grew %d bytes for a %d byte script", growth, info.Size())
	}
}
