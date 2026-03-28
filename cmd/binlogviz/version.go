// Package binlogviz defines the version CLI command.
// input: the version package and user invocation of `binlogviz version` or `--version`.
// output: version information printed to stdout.
// pos: user-facing version reporting for installed binaries and troubleshooting.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"fmt"

	"github.com/spf13/cobra"

	"binlogviz/internal/i18n"
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
	return &cobra.Command{
		Use:   "version",
		Short: i18n.T("cmd.version.short"),
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Print(asciiLogo)
			fmt.Printf("binlogviz %s\n", version.Version)
		},
	}
}
