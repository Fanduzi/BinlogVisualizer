package binlogviz

import (
	"binlogviz/internal/clierr"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"binlogviz/internal/displaytz"
	"binlogviz/internal/i18n"
	"binlogviz/internal/version"
)

// tzFlag is the --tz value. Empty and unchanged means historical UTC output.
var tzFlag string

// langFlag holds the value of --lang flag.
var langFlag string

// rejectUnknownSubcommand is the Args check of a command that only groups
// subcommands (root, snapshot, workflow). Without it cobra runs the group
// with the stray word as an argument, prints help and exits 0 (#268).
func rejectUnknownSubcommand(c *cobra.Command, args []string) error {
	if len(args) > 0 {
		return clierr.Errorf(clierr.KindUsage, "unknown command %q for %q. Run '%s --help'", args[0], c.CommandPath(), c.CommandPath())
	}
	return nil
}

// typeUsageArgs makes every positional-argument check return a usage error,
// and makes every group command refuse an unknown subcommand.
func typeUsageArgs(cmd *cobra.Command) {
	for _, c := range cmd.Commands() {
		if c.HasSubCommands() && c.Args == nil {
			c.Args = rejectUnknownSubcommand
		}
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
		Example: `  binlogviz version
  binlogviz analyze mysql-bin.000001`,
		// main prints the one error line (text or JSON); cobra prints
		// neither the error nor the usage text, so a JSON error is the only
		// stderr line (#268). The error says to run --help.
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			// Initialize i18n with the specified language
			// If langFlag is empty, it will detect from environment
			if err := i18n.Init(langFlag); err != nil {
				// Fall back to English on error
				_ = i18n.Init("en")
			}
			// #277: --error-format= (set but empty) is a usage error; only an
			// unset flag falls back to BINLOGVIZ_ERROR_FORMAT or text.
			if f := cmd.Flags().Lookup("error-format"); f != nil && f.Changed && strings.TrimSpace(errorFormat) == "" {
				return clierr.Errorf(clierr.KindUsage, "invalid --error-format %q: expected text or json", errorFormat)
			}
			if err := validateErrorFormatFlag(errorFormat); err != nil {
				return err
			}
			changed := false
			if f := cmd.Flags().Lookup("tz"); f != nil {
				changed = f.Changed
			}
			loc, err := displaytz.Parse(changed, tzFlag)
			if err != nil {
				return err
			}
			cmd.SetContext(displaytz.With(cmd.Context(), loc))
			return nil
		},
	}
	cmd.AddCommand(newAnalyzeCommand())
	cmd.AddCommand(newFlashbackCommand())
	cmd.AddCommand(newCompareCommand())
	cmd.AddCommand(newTrendCommand())
	cmd.AddCommand(newSnapshotCommand())
	cmd.AddCommand(newVersionCommand())
	cmd.AddCommand(newWorkflowCommand())
	cmd.AddCommand(newSchemaCommand())

	// Add global flags
	cmd.PersistentFlags().StringVar(&langFlag, "lang", "", "Language for output (e.g., en, zh-CN)")
	cmd.PersistentFlags().BoolVarP(&quietFlag, "quiet", "q", false, i18n.T("cmd.root.flag.quiet"))
	cmd.PersistentFlags().StringVar(&errorFormat, "error-format", "", i18n.T("cmd.root.flag.errorFormat"))
	cmd.PersistentFlags().StringVar(&tzFlag, "tz", "", "Display time zone for rendered timestamps (IANA name, for example Asia/Shanghai). Absent or UTC keeps the historical UTC spelling. Does not change --start or --end.")
	// Flag, argument and command-name mistakes are usage errors, exit 3 (ADR 0008).
	cmd.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return clierr.Wrap(clierr.KindUsage, err)
	})
	cmd.Args = rejectUnknownSubcommand
	typeUsageArgs(cmd)
	// Help and completion exist only after these calls. ExecuteC calls them
	// again and keeps the same command values, including Example.
	cmd.InitDefaultHelpCmd()
	cmd.InitDefaultCompletionCmd()
	setGeneratedExamples(cmd)
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

// setGeneratedExamples fills Example on cobra's help and completion commands.
// Product commands set their own. An empty Example is left empty so the tree
// test can see it.
func setGeneratedExamples(root *cobra.Command) {
	var walk func(*cobra.Command)
	walk = func(c *cobra.Command) {
		if c.Example == "" {
			switch c.CommandPath() {
			case "binlogviz help":
				c.Example = "  binlogviz help analyze\n  binlogviz help flashback"
			case "binlogviz completion":
				c.Example = "  binlogviz completion bash\n  binlogviz completion zsh"
			case "binlogviz completion bash":
				c.Example = "  binlogviz completion bash"
			case "binlogviz completion zsh":
				c.Example = "  binlogviz completion zsh"
			case "binlogviz completion fish":
				c.Example = "  binlogviz completion fish"
			case "binlogviz completion powershell":
				c.Example = "  binlogviz completion powershell"
			}
		}
		for _, child := range c.Commands() {
			walk(child)
		}
	}
	walk(root)
}

// validateErrorFormatFlag accepts "", text and json (any case). Anything
// else is a usage error, so a typo does not silently fall back to text.
func validateErrorFormatFlag(value string) error {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "text", "json":
		return nil
	}
	return clierr.Errorf(clierr.KindUsage, "invalid --error-format %q: expected text or json", value)
}
