// Package binlogviz defines the version CLI command.
// input: the version package and user invocation of `binlogviz version` or `--version`.
// output: version information printed to stdout: banner and version, plus the commit when known; or, with --format json, schema_version, version, commit, commit_time, modified, go_version (ADR 0010).
// pos: user-facing version reporting for installed binaries and troubleshooting.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"fmt"

	"github.com/spf13/cobra"

	"binlogviz/internal/clierr"
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
		Use:           "version",
		Short:         i18n.T("cmd.version.short"),
		Args:          cobra.NoArgs,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			build := version.Current()
			out := cmd.OutOrStdout()
			switch format {
			case "json":
				data, err := jsonout.Marshal(versionSchemaVersion, build)
				if err != nil {
					return clierr.Errorf(clierr.KindOutput, "encode version output: %w", err)
				}
				_, err = fmt.Fprintln(out, string(data))
				return err
			case "text":
				fmt.Fprint(out, asciiLogo)
				fmt.Fprintf(out, "binlogviz %s\n", build.Version)
				if build.Commit != "" {
					commit := build.Commit
					if build.Modified {
						commit += "-dirty"
					}
					if build.CommitTime != "" {
						commit += " (" + build.CommitTime + ")"
					}
					fmt.Fprintf(out, "commit %s\n", commit)
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
