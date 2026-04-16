// Package binlogviz executes bounded parallel file probing for analyze discovery.
// input: ordered binlog paths, worker limits, and injectable probe functions.
// output: deterministic probe slices preserving input order plus first error propagation.
// pos: CLI worker-pool layer used by analyze discovery before parser/analyzer execution.
// note: if this file changes, update this header and README.md.
package binlogviz

import (
	"sync"

	"binlogviz/internal/binlog"
)

func probeFilesWithWorkers(paths []string, workerCount int, probe func(string) (binlog.FileProbe, error)) ([]binlog.FileProbe, error) {
	if len(paths) == 0 {
		return nil, nil
	}
	if workerCount <= 0 {
		workerCount = 1
	}
	if workerCount > len(paths) {
		workerCount = len(paths)
	}

	type probeResult struct {
		index int
		probe binlog.FileProbe
		err   error
	}

	jobs := make(chan int)
	results := make(chan probeResult, len(paths))
	var wg sync.WaitGroup

	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				probed, err := probe(paths[index])
				results <- probeResult{index: index, probe: probed, err: err}
			}
		}()
	}

	for index := range paths {
		jobs <- index
	}
	close(jobs)

	wg.Wait()
	close(results)

	probes := make([]binlog.FileProbe, len(paths))
	for result := range results {
		if result.err != nil {
			return nil, result.err
		}
		probes[result.index] = result.probe
	}
	return probes, nil
}
