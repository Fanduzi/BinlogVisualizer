// Package binlogviz defines the version CLI command.
package binlogviz

import (
	"fmt"

	"github.com/spf13/cobra"

	"binlogviz/internal/clierr"
	"binlogviz/internal/displaytz"
	"binlogviz/internal/i18n"
	"binlogviz/internal/jsonout"
	"binlogviz/internal/version"
)

const asciiLogo = ` ________  ___  ________   ___       ________  ________  ___      ___ ___  ________
|\   __  \|\  \|\   ___  \|\  \     |\   __  \|\   ____\|\  \    /  /|\  \|\_____  \
\ \  \|\ /\ \  \ \  \\ \  \ \  \    \ \  \|\  \ \  \___|\ \  \  /  / | \  \\|___/  /|
 \ \   __  \ \  \ \  \\ \  \ \  \    \ \  \\\  \ \  \  __\ \  \/  / / \ \  \   /  / /
  \ \  \|\  \ \  \ \  \\ \  \ \  \____\ \  \\\  \ \  \|\  \ \    / /   \ \  \ /  /_/__
   \ \_______\ \__\ \__\\ \__\ \_______\ \_______\ \_______\ \__/ /     \ \__\\________\
    \|_______|\|__|\|__| \|__|\|_______|\|_______|\|_______|\|__|/       \|__|\|_______|

`

func newVersionCommand() *cobra.Command {
	var format string
	cmd := &cobra.Command{
		Use:   "version",
		Short: i18n.T("cmd.version.short"),
		Example: `  binlogviz version
  binlogviz version --format json`,
		Args:          cobra.NoArgs,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			build := version.Current()
			if loc := displaytz.From(cmd.Context()); loc != nil {
				build.CommitTime = displaytz.Stored(build.CommitTime, loc)
			}
			out := cmd.OutOrStdout()
			switch format {
			case "json":
				data, err := jsonout.Marshal(versionSchemaVersion, build)
				if err != nil {
					return clierr.Errorf(clierr.KindOutput, "encode version output: %w", err)
				}
				_, err = fmt.Fprintln(out, string(data))
				return clierr.Wrap(clierr.KindOutput, err)
			case "text":
				if _, err := fmt.Fprint(out, asciiLogo); err != nil {
					return clierr.Wrap(clierr.KindOutput, err)
				}
				if _, err := fmt.Fprintf(out, "binlogviz %s\n", build.Version); err != nil {
					return clierr.Wrap(clierr.KindOutput, err)
				}
				if build.Commit != "" {
					commit := build.Commit
					if build.Modified {
						commit += "-dirty"
					}
					if build.CommitTime != "" {
						commit += " (" + build.CommitTime + ")"
					}
					if _, err := fmt.Fprintf(out, "commit %s\n", commit); err != nil {
						return clierr.Wrap(clierr.KindOutput, err)
					}
				}
				return nil
			default:
				return clierr.Errorf(clierr.KindUsage, "unsupported version format: %s (use text or json)", format)
			}
		},
	}
	cmd.Flags().StringVar(&format, "format", "text", "Version output format: text or json")
	return cmd
}
