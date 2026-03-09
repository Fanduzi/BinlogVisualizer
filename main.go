package main

import (
	"os"

	"binlogviz/cmd/binlogviz"
)

func main() {
	if err := binlogviz.NewRootCommand().Execute(); err != nil {
		os.Exit(1)
	}
}
