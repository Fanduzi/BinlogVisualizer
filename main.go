package main

import (
	"binlogviz/cmd/binlogviz"

	"github.com/spf13/cobra"
)

func main() {
	err := binlogviz.NewRootCommand().Execute()
	cobra.CheckErr(err)
}
