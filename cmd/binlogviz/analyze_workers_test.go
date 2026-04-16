// Package binlogviz verifies parallel file probing helpers used by analyze discovery planning.
// input: binlog path lists and injectable probe functions with controlled latency or failures.
// output: regression coverage for deterministic output ordering, worker throttling, and error propagation.
// pos: focused Task 4 worker-pool test layer before main analyze command integration.
package binlogviz

import (
	"errors"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"binlogviz/internal/binlog"
)

func TestProbeFilesWithWorkersPreservesInputOrder(t *testing.T) {
	paths := []string{"mysql-bin.000044", "mysql-bin.000045", "mysql-bin.000046"}

	delays := map[string]time.Duration{
		paths[0]: 40 * time.Millisecond,
		paths[1]: 5 * time.Millisecond,
		paths[2]: 20 * time.Millisecond,
	}

	got, err := probeFilesWithWorkers(paths, 3, func(path string) (binlog.FileProbe, error) {
		time.Sleep(delays[path])
		return binlog.FileProbe{BinlogPath: path}, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != len(paths) {
		t.Fatalf("expected %d probes, got %d", len(paths), len(got))
	}
	for index, path := range paths {
		if got[index].BinlogPath != path {
			t.Fatalf("expected probe %d to be %q, got %q", index, path, got[index].BinlogPath)
		}
	}
}

func TestProbeFilesWithWorkersPropagatesErrors(t *testing.T) {
	wantErr := errors.New("probe failed")
	_, err := probeFilesWithWorkers([]string{"mysql-bin.000044", "mysql-bin.000045"}, 2, func(path string) (binlog.FileProbe, error) {
		if path == "mysql-bin.000045" {
			return binlog.FileProbe{}, wantErr
		}
		return binlog.FileProbe{BinlogPath: path}, nil
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected %v, got %v", wantErr, err)
	}
}

func TestProbeFilesWithWorkersCapsConcurrency(t *testing.T) {
	paths := []string{"a", "b", "c", "d"}
	var mu sync.Mutex
	current := 0
	peak := 0

	_, err := probeFilesWithWorkers(paths, 2, func(path string) (binlog.FileProbe, error) {
		mu.Lock()
		current++
		if current > peak {
			peak = current
		}
		mu.Unlock()

		time.Sleep(10 * time.Millisecond)

		mu.Lock()
		current--
		mu.Unlock()
		return binlog.FileProbe{BinlogPath: path}, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if peak > 2 {
		t.Fatalf("expected peak concurrency <= 2, got %d", peak)
	}

	gotOrder := make([]string, len(paths))
	copy(gotOrder, paths)
	if !slices.Equal(gotOrder, paths) {
		t.Fatalf("unexpected path order mutation: %v", gotOrder)
	}
}

func TestProbeFilesWithWorkersMatchesSerialResults(t *testing.T) {
	paths := []string{"mysql-bin.000044", "mysql-bin.000045", "mysql-bin.000046", "mysql-bin.000047"}
	probe := func(path string) (binlog.FileProbe, error) {
		time.Sleep(time.Duration(len(path)%3) * time.Millisecond)
		return binlog.FileProbe{
			BinlogPath:   path,
			FirstEventAt: time.Date(2026, 4, 16, 10, len(path)%5, 0, 0, time.UTC),
			LastEventAt:  time.Date(2026, 4, 16, 10, len(path)%5, 30, 0, time.UTC),
		}, nil
	}

	serial, err := probeFilesWithWorkers(paths, 1, probe)
	if err != nil {
		t.Fatalf("serial probe returned error: %v", err)
	}
	parallel, err := probeFilesWithWorkers(paths, len(paths), probe)
	if err != nil {
		t.Fatalf("parallel probe returned error: %v", err)
	}
	if !reflect.DeepEqual(parallel, serial) {
		t.Fatalf("expected parallel probe results to match serial\nserial: %#v\nparallel: %#v", serial, parallel)
	}
}
