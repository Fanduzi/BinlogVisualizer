package binlogviz

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func newWorkflowCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "workflow",
		Short: "Run multi-step BinlogViz investigation workflows",
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd.Help()
		},
	}
	cmd.AddCommand(newWorkflowRunCommand())
	return cmd
}

type workflowRunOptions struct {
	outputDir   string
	snapshotDir string
}

func newWorkflowRunCommand() *cobra.Command {
	opts := &workflowRunOptions{}

	cmd := &cobra.Command{
		Use:   "run <plan.yaml>",
		Short: "Execute a workflow plan",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			planPath := args[0]

			f, err := os.Open(planPath)
			if err != nil {
				return fmt.Errorf("open workflow plan: %w", err)
			}
			defer f.Close()

			// LoadPlan and validation happen in executeWorkflow
			_ = opts
			return fmt.Errorf("workflow run: not yet implemented")
		},
	}

	cmd.Flags().StringVar(&opts.outputDir, "output-dir", "", "Override the plan-defined output directory")
	cmd.Flags().StringVar(&opts.snapshotDir, "snapshot-dir", "", "Override the snapshot storage directory")

	return cmd
}
