// Package binlogviz coordinates bounded parallel parsing for multi-file analyze runs.
// input: ordered binlog path lists, progress-capable parser implementations, and normalized-event consumers.
// output: concurrent per-file parsing with deterministic file-order event delivery to the analyzer.
// pos: command-layer parse pipeline between analyze orchestration and single-threaded analyzer consumption.
// note: if this file changes, update this header and cmd/binlogviz/README.md.
package binlogviz

import (
	"errors"
	"sync"

	"binlogviz/internal/binlog"
)

const analyzeParseEventBuffer = 256

var errParallelParseStopped = errors.New("parallel parse stopped")

type orderedParseItem struct {
	raw         binlog.RawEvent
	progress    binlog.ParseProgress
	err         error
	hasRaw      bool
	hasProgress bool
}

func parseFilesWithProgressParallelOrdered(paths []string, parser binlog.ProgressParser, workerCount int, onProgress func(binlog.ParseProgress), handler func(binlog.RawEvent) error) error {
	if len(paths) <= 1 {
		return parser.ParseFilesWithProgress(paths, onProgress, handler)
	}
	if workerCount <= 0 {
		workerCount = 1
	}
	if workerCount > len(paths) {
		workerCount = len(paths)
	}

	channels := make([]chan orderedParseItem, len(paths))
	for index := range paths {
		channels[index] = make(chan orderedParseItem, analyzeParseEventBuffer)
	}

	jobs := make(chan int, len(paths))
	done := make(chan struct{})
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			close(done)
		})
	}

	var wg sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				parseSingleFileOrdered(paths, index, parser, channels[index], done)
			}
		}()
	}
	for index := range paths {
		jobs <- index
	}
	close(jobs)

	var resultErr error
	for index := range paths {
		for item := range channels[index] {
			if item.err != nil {
				resultErr = item.err
				stop()
				wg.Wait()
				return resultErr
			}
			if item.hasProgress && onProgress != nil {
				onProgress(item.progress)
			}
			if item.hasRaw {
				if err := handler(item.raw); err != nil {
					resultErr = err
					stop()
					wg.Wait()
					return resultErr
				}
			}
		}
	}
	stop()
	wg.Wait()
	return nil
}

func parseSingleFileOrdered(paths []string, index int, parser binlog.ProgressParser, out chan<- orderedParseItem, done <-chan struct{}) {
	defer close(out)

	path := paths[index]
	err := parser.ParseFilesWithProgress([]string{path}, func(progress binlog.ParseProgress) {
		progress.Path = path
		progress.Index = index
		_ = sendOrderedParseItem(out, done, orderedParseItem{progress: progress, hasProgress: true})
	}, func(raw binlog.RawEvent) error {
		if err := sendOrderedParseItem(out, done, orderedParseItem{raw: raw, hasRaw: true}); err != nil {
			return err
		}
		return nil
	})
	if err != nil && !errors.Is(err, errParallelParseStopped) {
		_ = sendOrderedParseItem(out, done, orderedParseItem{err: err})
	}
}

func sendOrderedParseItem(out chan<- orderedParseItem, done <-chan struct{}, item orderedParseItem) error {
	select {
	case <-done:
		return errParallelParseStopped
	case out <- item:
		return nil
	}
}
