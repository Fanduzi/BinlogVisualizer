// Package binlogviz validates exact analyze position selectors against parsed binlog event boundaries.
// input: one explicit binlog path, requested half-open position bounds, raw event offsets, and file size.
// output: clear out-of-range or mid-event errors before analysis finalization and report rendering.
// pos: command-layer trust boundary between parser offsets and analyzer position-window semantics.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"fmt"
	"os"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
)

const binlogHeaderPosition int64 = 4

type positionBoundaryValidator struct {
	start         *int64
	stop          *int64
	fileSize      int64
	startBoundary bool
	stopBoundary  bool
}

func newPositionBoundaryValidator(paths []string, opts analyzer.Options) (*positionBoundaryValidator, error) {
	if !opts.HasPositionSelectors() {
		return nil, nil
	}
	if len(paths) != 1 {
		return nil, fmt.Errorf("position selectors require exactly one explicit binlog file")
	}
	info, err := os.Stat(paths[0])
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("position selectors require exactly one explicit binlog file: %q is not a regular file", paths[0])
	}
	return &positionBoundaryValidator{
		start:    opts.StartPosition,
		stop:     opts.StopPosition,
		fileSize: info.Size(),
	}, nil
}

func (v *positionBoundaryValidator) Observe(raw binlog.RawEvent) {
	if v == nil {
		return
	}
	if v.start != nil && (raw.PositionStart == *v.start || raw.PositionEnd == *v.start) {
		v.startBoundary = true
	}
	if v.stop != nil && (raw.PositionStart == *v.stop || raw.PositionEnd == *v.stop) {
		v.stopBoundary = true
	}
}

func (v *positionBoundaryValidator) Validate() error {
	if v == nil {
		return nil
	}
	if err := v.validatePosition("--start-position", v.start, v.startBoundary); err != nil {
		return err
	}
	return v.validatePosition("--stop-position", v.stop, v.stopBoundary)
}

func (v *positionBoundaryValidator) validatePosition(flag string, value *int64, observedBoundary bool) error {
	if value == nil {
		return nil
	}
	if *value < binlogHeaderPosition || *value > v.fileSize {
		return fmt.Errorf("%s %d is out of range [%d,%d]", flag, *value, binlogHeaderPosition, v.fileSize)
	}
	if observedBoundary || *value == v.fileSize {
		return nil
	}
	return fmt.Errorf("%s %d is not an exact event boundary", flag, *value)
}
