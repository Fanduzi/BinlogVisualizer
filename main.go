// Package main is the binlogviz CLI entrypoint.
// input: process args and errors returned by the cobra command tree.
// output: process exit codes; a single Error: line on stderr for failed commands, after clearing a progress line.
// pos: process boundary mapping command errors onto operator-visible exit codes.
// note: if this file changes, update this header and README.md.
package main

import (
	"os"

	"binlogviz/cmd/binlogviz"
)

func main() {
	err := binlogviz.NewRootCommand().Execute()
	if err != nil {
		binlogviz.PrintCommandError(os.Stderr, err)
		os.Exit(binlogviz.ExitCode(err))
	}
}
