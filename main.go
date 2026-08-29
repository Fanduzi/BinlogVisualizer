// Package main is the binlogviz CLI entrypoint.
// input: process args and errors returned by the cobra command tree.
// output: process exit codes; a single Error: line on stderr for failed commands.
// pos: process boundary mapping command errors onto operator-visible exit codes.
// note: if this file changes, update this header and README.md.
package main

import (
	"fmt"
	"os"

	"binlogviz/cmd/binlogviz"
)

func main() {
	err := binlogviz.NewRootCommand().Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(binlogviz.ExitCode(err))
	}
}
