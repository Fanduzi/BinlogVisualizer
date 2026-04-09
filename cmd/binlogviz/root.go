package binlogviz

import (
	"fmt"

	"github.com/spf13/cobra"

	"binlogviz/internal/i18n"
	"binlogviz/internal/version"
)

// langFlag holds the value of --lang flag.
var langFlag string

func NewRootCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "binlogviz",
		Short: i18n.T("cmd.root.short"),
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			// Initialize i18n with the specified language
			// If langFlag is empty, it will detect from environment
			if err := i18n.Init(langFlag); err != nil {
				// Fall back to English on error
				_ = i18n.Init("en")
			}
		},
	}
	cmd.AddCommand(newAnalyzeCommand())
	cmd.AddCommand(newCompareCommand())
	cmd.AddCommand(newTrendCommand())
	cmd.AddCommand(newSnapshotCommand())
	cmd.AddCommand(newVersionCommand())
	cmd.AddCommand(newWorkflowCommand())

	// Add global flags
	cmd.PersistentFlags().StringVar(&langFlag, "lang", "", "Language for output (e.g., en, zh-CN)")
	cmd.Flags().BoolP("version", "v", false, i18n.T("cmd.root.flag.version"))
	cmd.Run = func(cmd *cobra.Command, args []string) {
		if v, _ := cmd.Flags().GetBool("version"); v {
			fmt.Println(version.Version)
			return
		}
		cmd.Help()
	}

	return cmd
}
