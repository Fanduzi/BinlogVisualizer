// Package binlogviz maps command errors onto operator-visible process exit codes.
// input: errors returned by cobra command execution.
// output: ExitError values, ExitCode lookup, and PrintCommandError for the process main.
// pos: CLI process-exit seam between command RunE errors and os.Exit.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"binlogviz/internal/i18n"
)

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

// PrintCommandError writes one Error line, with the prefix in the output
// language ("Error: " in English, "错误：" in zh-CN, #227). A parse progress
// bar may still own the current stderr line; erase that line first so the
// error never shares it.
func PrintCommandError(w io.Writer, err error) {
	if w == nil || err == nil {
		return
	}
	_, _ = io.WriteString(w, "\r\033[2K")
	prefix := i18n.T("cli.errorPrefix")
	if prefix == "cli.errorPrefix" || prefix == "" {
		prefix = "Error:"
	}
	sep := " "
	if strings.HasSuffix(prefix, "：") {
		sep = ""
	}
	fmt.Fprintln(w, prefix+sep+err.Error())
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
