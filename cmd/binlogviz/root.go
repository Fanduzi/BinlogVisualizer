package binlogviz

import (
	"fmt"

	"github.com/spf13/cobra"

	"binlogviz/internal/version"
)

func NewRootCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "binlogviz",
		Short: "Analyze MySQL binlog files",
	}
	cmd.AddCommand(newAnalyzeCommand())
	cmd.AddCommand(newVersionCommand())

	// Add --version flag that only prints the version number
	cmd.Flags().BoolP("version", "v", false, "Print the version number")
	cmd.Run = func(cmd *cobra.Command, args []string) {
		if v, _ := cmd.Flags().GetBool("version"); v {
			fmt.Println(version.Version)
			return
		}
		cmd.Help()
	}

	return cmd
}
