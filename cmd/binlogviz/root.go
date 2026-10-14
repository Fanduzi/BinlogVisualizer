package binlogviz

import (
	"binlogviz/internal/clierr"
	"fmt"

	"github.com/spf13/cobra"

	"binlogviz/internal/i18n"
	"binlogviz/internal/version"
)

// langFlag holds the value of --lang flag.
var langFlag string

// typeUsageArgs makes every positional-argument check return a usage error.
func typeUsageArgs(cmd *cobra.Command) {
	for _, c := range cmd.Commands() {
		if check := c.Args; check != nil {
			c.Args = func(cc *cobra.Command, args []string) error {
				return clierr.Wrap(clierr.KindUsage, check(cc, args))
			}
		}
		typeUsageArgs(c)
	}
}

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
	cmd.AddCommand(newFlashbackCommand())
	cmd.AddCommand(newCompareCommand())
	cmd.AddCommand(newTrendCommand())
	cmd.AddCommand(newSnapshotCommand())
	cmd.AddCommand(newVersionCommand())
	cmd.AddCommand(newWorkflowCommand())

	// Add global flags
	cmd.PersistentFlags().StringVar(&langFlag, "lang", "", "Language for output (e.g., en, zh-CN)")
	cmd.PersistentFlags().BoolVarP(&quietFlag, "quiet", "q", false, i18n.T("cmd.root.flag.quiet"))
	cmd.PersistentFlags().StringVar(&errorFormat, "error-format", "", i18n.T("cmd.root.flag.errorFormat"))
	// Flag, argument and command-name mistakes are usage errors, exit 3 (ADR 0008).
	cmd.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return clierr.Wrap(clierr.KindUsage, err)
	})
	cmd.Args = func(c *cobra.Command, args []string) error {
		if len(args) > 0 {
			return clierr.Errorf(clierr.KindUsage, "unknown command %q for %q. Run '%s --help'", args[0], c.CommandPath(), c.CommandPath())
		}
		return nil
	}
	typeUsageArgs(cmd)
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
