package binlogviz

import (
	"strings"

	"github.com/spf13/cobra"

	"binlogviz/internal/i18n"
)

// Help text, including every flag usage line, is resolved when a command is
// built, but --lang is only known after cobra parses the flags and help is
// printed before any Run hook. So help builds the tree a second time in the
// requested language and prints that command's help. flashback installs its own
// help func and refreshes its strings in place instead.
func installLocalizedHelp(root *cobra.Command) {
	base := root.HelpFunc()
	root.SetHelpFunc(func(c *cobra.Command, args []string) {
		if langFlag != "" && i18n.Init(langFlag) == nil {
			c = rebuiltForLang(c)
		}
		base(c, args)
	})
}

// rebuiltForLang returns the command with the same path from a tree built in the
// current language. Building registers the same package-level flag targets, so
// their values are put back.
func rebuiltForLang(c *cobra.Command) *cobra.Command {
	lang, quiet, errFormat, tz := langFlag, quietFlag, errorFormat, tzFlag
	fresh := NewRootCommand()
	langFlag, quietFlag, errorFormat, tzFlag = lang, quiet, errFormat, tz
	fresh.SetOut(c.OutOrStdout())
	fresh.SetErr(c.ErrOrStderr())
	found, _, err := fresh.Find(strings.Split(c.CommandPath(), " ")[1:])
	if err != nil || found == nil {
		return c
	}
	return found
}
