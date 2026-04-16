// Package binlog probes file-level metadata from local MySQL binlog files.
// input: binlog file paths and parser-emitted raw event timestamps.
// output: reusable per-file size and event-time summaries for later planning and diagnostics work.
// pos: lightweight metadata scan layer that avoids coupling planner/report features to parser internals.
// note: if this file changes, update this header and README.md.
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
	LastEventAt  time.Time // Latest non-zero event timestamp observed in the file.
}

// ProbeFiles returns metadata for each binlog path in input order.
func ProbeFiles(paths []string) ([]FileProbe, error) {
	return probeFilesWithParser(paths, NewParser())
}

// ProbeFile returns reusable metadata for one binlog file.
func ProbeFile(path string) (FileProbe, error) {
	probes, err := probeFilesWithParser([]string{path}, NewParser())
	if err != nil {
		return FileProbe{}, err
	}
	if len(probes) == 0 {
		return FileProbe{}, nil
	}
	return probes[0], nil
}

func probeFilesWithParser(paths []string, parser Parser) ([]FileProbe, error) {
	if len(paths) == 0 {
		return nil, nil
	}
	if parser == nil {
		return nil, errors.New("nil probe parser")
	}

	probes := make([]FileProbe, len(paths))
	indexesByPath := make(map[string][]int, len(paths))
	for index, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		probes[index] = FileProbe{
			BinlogPath: path,
			SizeBytes:  info.Size(),
		}
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
