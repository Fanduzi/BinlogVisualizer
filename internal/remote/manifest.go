// Package remote snapshots the server's binary log list at connect time.
// input: the dump connection's SHOW BINARY LOGS result.
// output: the ordered LogFile manifest and the file:pos start validation against it.
// pos: the bounded-read contract (ADR 0021) — the last manifest row is where a remote analyze stops.
// note: if this file changes, update this header and README.md.
package remote

import (
	"fmt"
	"strconv"

	"github.com/go-mysql-org/go-mysql/client"
)

// showBinaryLogs runs the manifest probe. It needs REPLICATION CLIENT; a
// privilege error and a malformed or empty result are distinct reasons.
func showBinaryLogs(c *client.Conn) ([]LogFile, error) {
	res, err := c.Execute("SHOW BINARY LOGS")
	if err != nil {
		return nil, &Error{Reason: manifestReason(err), Err: fmt.Errorf("SHOW BINARY LOGS: %w", err)}
	}
	if res == nil || res.Resultset == nil || res.RowNumber() == 0 {
		return nil, &Error{Reason: ReasonManifest, Err: fmt.Errorf("SHOW BINARY LOGS returned no rows: the server has binary logging off or hides it")}
	}
	files := make([]LogFile, 0, res.RowNumber())
	for i := 0; i < res.RowNumber(); i++ {
		name, err := res.GetString(i, 0)
		if err != nil || name == "" {
			return nil, &Error{Reason: ReasonManifest, Err: fmt.Errorf("SHOW BINARY LOGS row %d has no usable log name", i)}
		}
		sizeText, err := res.GetString(i, 1)
		if err != nil {
			return nil, &Error{Reason: ReasonManifest, Err: fmt.Errorf("SHOW BINARY LOGS row %d has no usable file size", i)}
		}
		size, err := strconv.ParseInt(sizeText, 10, 64)
		if err != nil {
			return nil, &Error{Reason: ReasonManifest, Err: fmt.Errorf("SHOW BINARY LOGS row %d file size %q is not a number", i, sizeText)}
		}
		files = append(files, LogFile{Name: name, Size: size})
	}
	return files, nil
}

// manifestReason separates "you may not ask" from "the answer made no sense".
func manifestReason(err error) Reason {
	if classifyOpenError(err) == ReasonPrivilege {
		return ReasonPrivilege
	}
	return ReasonManifest
}

// validateStart checks a file:pos start against the manifest taken at
// connect. A file the server no longer lists was purged or mistyped; a
// position past the probed end cannot be an event boundary.
func validateStart(start StartPos, manifest []LogFile) error {
	if start.File == "" {
		return nil
	}
	for _, file := range manifest {
		if file.Name != start.File {
			continue
		}
		if int64(start.Pos) > file.Size {
			return &Error{Reason: ReasonStartPos, Err: fmt.Errorf("--start-pos %d is past the end of %s (%d bytes at connect)", start.Pos, start.File, file.Size)}
		}
		return nil
	}
	return &Error{Reason: ReasonStartFile, Err: fmt.Errorf("--start-file %s is not in the server's binary log list", start.File)}
}
