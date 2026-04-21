// Package binlog probes file-level metadata from local MySQL binlog files.
// input: binlog file paths and parser-emitted raw event timestamps.
// output: reusable per-file size and event-time summaries for later planning and diagnostics work.
// pos: lightweight metadata scan layer that avoids coupling planner/report features to parser internals.
// note: if this file changes, update this header and module README.md.
package binlog

import (
	"errors"
	"fmt"
	"os"
	"time"
)

// FileProbe captures reusable metadata about one binlog file.
type FileProbe struct {
	BinlogPath   string
	SizeBytes    int64
	FirstEventAt time.Time // Earliest non-zero event timestamp observed in the file.
	LastEventAt  time.Time // Upper bound on last event time; inferred from next file's FirstEventAt in fast path.
}

var errStopFirstTimestamp = errors.New("stop after first binlog timestamp")

// ProbeFiles returns metadata for each binlog path in input order.
func ProbeFiles(paths []string) ([]FileProbe, error) {
	return probeFilesWithParser(paths, NewParser())
}

// ProbeFile returns reusable metadata for one binlog file.
func ProbeFile(path string) (FileProbe, error) {
	probes, err := ProbeFiles([]string{path})
	if err != nil {
		return FileProbe{}, err
	}
	if len(probes) == 0 {
		return FileProbe{}, nil
	}
	return probes[0], nil
}

// probeFirstTimestamp parses from offset 0 and returns the first non-zero event timestamp.
func probeFirstTimestamp(parser Parser, path string) (time.Time, error) {
	var first time.Time
	err := parser.ParseFiles([]string{path}, func(raw RawEvent) error {
		if raw.Timestamp.IsZero() {
			return nil
		}
		first = raw.Timestamp.UTC()
		return errStopFirstTimestamp
	})
	if err != nil && !errors.Is(err, errStopFirstTimestamp) {
		return time.Time{}, err
	}
	return first, nil
}

func probeFilesWithParser(paths []string, parser Parser) ([]FileProbe, error) {
	if len(paths) == 0 {
		return nil, nil
	}
	if parser == nil {
		return nil, errors.New("nil probe parser")
	}

	_, hasOffset := parser.(OffsetParser)

	probes := make([]FileProbe, len(paths))
	for index, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		probes[index] = FileProbe{
			BinlogPath: path,
			SizeBytes:  info.Size(),
		}
	}

	// Fast path: probe first timestamp per file, then infer last from sequence.
	if hasOffset {
		for index, path := range paths {
			first, err := probeFirstTimestamp(parser, path)
			if err != nil {
				return nil, fmt.Errorf("probe first timestamp %s: %w", path, err)
			}
			probes[index].FirstEventAt = first
		}
		// Infer LastEventAt from next file's FirstEventAt.
		// File N ends no later than file N+1 starts; last file stays zero.
		for i := 0; i < len(probes)-1; i++ {
			if probes[i].LastEventAt.IsZero() && !probes[i+1].FirstEventAt.IsZero() {
				probes[i].LastEventAt = probes[i+1].FirstEventAt
			}
		}
		return probes, nil
	}

	// Fallback: full parse when OffsetParser is not available
	indexesByPath := make(map[string][]int, len(paths))
	for index, path := range paths {
		indexesByPath[path] = append(indexesByPath[path], index)
	}

	if err := parser.ParseFiles(paths, func(raw RawEvent) error {
		path := raw.BinlogPath
		if path == "" && len(paths) == 1 {
			path = paths[0]
		}
		indexes, ok := indexesByPath[path]
		if !ok {
			return fmt.Errorf("probe parser returned unexpected binlog path %q", path)
		}
		for _, index := range indexes {
			updateProbeTimestampBounds(&probes[index], raw.Timestamp)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return probes, nil
}

func updateProbeTimestampBounds(probe *FileProbe, timestamp time.Time) {
	if probe == nil || timestamp.IsZero() {
		return
	}

	timestamp = timestamp.UTC()
	if probe.FirstEventAt.IsZero() || timestamp.Before(probe.FirstEventAt) {
		probe.FirstEventAt = timestamp
	}
	if probe.LastEventAt.IsZero() || timestamp.After(probe.LastEventAt) {
		probe.LastEventAt = timestamp
	}
}
