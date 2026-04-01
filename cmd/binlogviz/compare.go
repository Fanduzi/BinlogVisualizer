package binlogviz

import (
	"fmt"

	"github.com/spf13/cobra"

	comparepkg "binlogviz/internal/compare"
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
			default:
				return fmt.Errorf("unsupported compare format: %s", opts.format)
			}

			current, err := comparepkg.LoadReport(args[0])
			if err != nil {
				return fmt.Errorf("load current report %s: %w", args[0], err)
			}
			baseline, err := comparepkg.LoadReport(args[1])
			if err != nil {
				return fmt.Errorf("load baseline report %s: %w", args[1], err)
			}

			result := comparepkg.BuildCompareResult(current, baseline)

			var output string
			switch opts.format {
			case "json":
				output, err = comparepkg.RenderJSON(result)
			case "html":
				output, err = comparepkg.RenderHTML(result)
			default:
				output, err = comparepkg.RenderText(result)
			}
			if err != nil {
				return fmt.Errorf("render compare output (%s): %w", opts.format, err)
			}

			_, err = fmt.Fprint(cmd.OutOrStdout(), output)
			if err != nil {
				return fmt.Errorf("write compare output: %w", err)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&opts.format, "format", "text", "Compare output format: text, json, or html")

	return cmd
}
