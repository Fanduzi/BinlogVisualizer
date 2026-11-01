package binlogviz

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func TestCLIReferenceDocumentsPublicFlags(t *testing.T) {
	root := NewRootCommand()
	flags := make(map[string]bool)
	var collect func(*cobra.Command)
	collect = func(cmd *cobra.Command) {
		if cmd.Hidden || cmd.Name() == "help" || cmd.Name() == "completion" {
			return
		}
		add := func(flag *pflag.Flag) {
			if !flag.Hidden && flag.Name != "help" {
				flags["--"+flag.Name] = true
			}
		}
		cmd.LocalNonPersistentFlags().VisitAll(add)
		cmd.PersistentFlags().VisitAll(add)
		for _, child := range cmd.Commands() {
			collect(child)
		}
	}
	collect(root)
	token := regexp.MustCompile(`--[a-z][a-z0-9-]*`)
	for _, name := range []string{"cli.md", "cli.zh-CN.md"} {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("..", "..", "docs", "reference", name))
			if err != nil {
				t.Fatal(err)
			}
			documented := make(map[string]bool)
			for _, flag := range token.FindAllString(string(data), -1) {
				documented[flag] = true
			}
			var missing []string
			for flag := range flags {
				if !documented[flag] {
					missing = append(missing, flag)
				}
			}
			sort.Strings(missing)
			if len(missing) != 0 {
				t.Errorf("public flags missing from %s: %v", name, missing)
			}
		})
	}
}
