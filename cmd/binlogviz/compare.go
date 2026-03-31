package binlogviz

import (
	"fmt"

	"github.com/spf13/cobra"
)

type compareOptions struct {
	format string
}

func newCompareCommand() *cobra.Command {
	opts := &compareOptions{}

	cmd := &cobra.Command{
		Use:   "compare <current.json> <baseline.json>",
		Short: "Compare two BinlogViz JSON analysis reports",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			switch opts.format {
			case "text", "json", "html":
				return fmt.Errorf("compare execution not implemented")
			default:
				return fmt.Errorf("unsupported compare format: %s", opts.format)
			}
		},
	}

	cmd.Flags().StringVar(&opts.format, "format", "text", "Compare output format: text, json, or html")

	return cmd
}
