// Package main is the binlogviz CLI entrypoint.
// input: process args and errors returned by the cobra command tree.
// output: process exit codes by error kind (docs/reference/exit-codes.md, ADR 0008); a single Error: line, or one JSON error with --error-format json, on stderr for failed commands, after clearing a progress line. SIGHUP exits 129, SIGINT exits 130, SIGQUIT exits 131, and SIGTERM exits 143, after stdin temp copies are removed.
// pos: process boundary mapping command errors onto operator-visible exit codes.
// note: if this file changes, update this header and README.md.
package main

import (
	"os"

	"binlogviz/cmd/binlogviz"
)

func main() {
	binlogviz.InstallInterruptHandler()
	err := binlogviz.NewRootCommand().Execute()
	if err != nil {
		binlogviz.PrintCommandError(os.Stderr, err)
		os.Exit(binlogviz.ExitCode(err))
	}
}
