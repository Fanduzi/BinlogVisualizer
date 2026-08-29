// Package binlogviz maps command errors onto operator-visible process exit codes.
// input: errors returned by cobra command execution.
// output: ExitError values and ExitCode lookup for the process main.
// pos: CLI process-exit seam between command RunE errors and os.Exit.
// note: if this file changes, update this header and module README.md.
package binlogviz

import "errors"

// ExitError is a command error that should terminate the process with Code.
type ExitError struct {
	Msg  string
	Code int
}

func (e *ExitError) Error() string {
	if e == nil {
		return ""
	}
	return e.Msg
}

// ExitCode returns the process exit code for err. Unknown errors are 1.
func ExitCode(err error) int {
	if err == nil {
		return 0
	}
	var exitErr *ExitError
	if errors.As(err, &exitErr) && exitErr != nil && exitErr.Code != 0 {
		return exitErr.Code
	}
	return 1
}
