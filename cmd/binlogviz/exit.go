// Package binlogviz maps command errors onto operator-visible process exit codes.
// input: errors returned by cobra command execution.
// output: ExitError values, ExitCode lookup by error kind (internal/clierr, ADR 0008), and PrintCommandError (text, or one JSON line with --error-format json) for the process main.
// pos: CLI process-exit seam between command RunE errors and os.Exit.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"os"

	"binlogviz/internal/clierr"
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

// errorFormat is "text" or "json", from --error-format or
// BINLOGVIZ_ERROR_FORMAT. The flag wins.
var errorFormat string

func jsonErrors() bool {
	format := errorFormat
	if format == "" {
		format = os.Getenv("BINLOGVIZ_ERROR_FORMAT")
	}
	return strings.EqualFold(strings.TrimSpace(format), "json")
}

// ErrorKind is the stable kind of err (ADR 0008). An ExitError's code
// decides its kind, so ADR 0001's exit 2 is no_data.
func ErrorKind(err error) (clierr.Kind, map[string]any) {
	var exitErr *ExitError
	if errors.As(err, &exitErr) && exitErr != nil && exitErr.Code != 0 {
		return clierr.KindForCode(exitErr.Code), nil
	}
	return clierr.Of(err)
}

// PrintCommandError writes one Error line, with the prefix in the output
// language ("Error: " in English, "错误：" in zh-CN, #227), or with JSON
// errors one {"error":{...}} line whose kind and code do not depend on
// --lang. A parse progress bar may still own the current stderr line; erase
// that line first so the error never shares it.
func PrintCommandError(w io.Writer, err error) {
	if w == nil || err == nil {
		return
	}
	if jsonErrors() {
		kind, details := ErrorKind(err)
		_, _ = w.Write(clierr.JSONLine(kind, ExitCode(err), err.Error(), details))
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

// ExitCode returns the process exit code for err: an ExitError's code, or
// the code of its kind. An error with no kind is 1.
func ExitCode(err error) int {
	if err == nil {
		return 0
	}
	var exitErr *ExitError
	if errors.As(err, &exitErr) && exitErr != nil && exitErr.Code != 0 {
		return exitErr.Code
	}
	kind, _ := clierr.Of(err)
	return clierr.Code(kind)
}
