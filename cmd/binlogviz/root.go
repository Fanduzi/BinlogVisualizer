package binlogviz

import "github.com/spf13/cobra"

func NewRootCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "binlogviz",
		Short: "Analyze MySQL binlog files",
	}
	cmd.AddCommand(newAnalyzeCommand())
	return cmd
}
