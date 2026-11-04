package binlogviz

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
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

// The Hot Rows rule changed once (ADR 0032) and the help text and the reference
// pages kept the old wording, so --help told users the new path cannot exist.
func TestHotRowsDocsNameTheDefinitionFallback(t *testing.T) {
	for _, name := range []string{"en.json", "zh-CN.json"} {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("..", "..", "internal", "i18n", "locales", name))
			if err != nil {
				t.Fatal(err)
			}
			var stringsByKey map[string]map[string]string
			if err := json.Unmarshal(data, &stringsByKey); err != nil {
				t.Fatalf("%s is not a flat JSON object: %v", name, err)
			}
			for _, key := range []string{"cmd.analyze.long", "cmd.analyze.flag.schemaFile"} {
				text := stringsByKey[key]["other"]
				if !strings.Contains(text, "--schema-file") || !strings.Contains(text, "[--schema-file]") {
					t.Errorf("%s: %s must say Hot Rows also uses --schema-file and marks such rows", name, key)
				}
			}
		})
	}
	for _, name := range []string{"cli.md", "cli.zh-CN.md", "output-format.md", "output-format.zh-CN.md"} {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("..", "..", "docs", "reference", name))
			if err != nil {
				t.Fatal(err)
			}
			text := string(data)
			for _, stale := range []string{"only from MySQL 8", "只来自 MySQL 8"} {
				if strings.Contains(text, stale) {
					t.Errorf("%s still claims Hot Rows identity comes only from binlog metadata", name)
				}
			}
			if !strings.Contains(text, "[--schema-file]") {
				t.Errorf("%s must say how a key from --schema-file is marked", name)
			}
		})
	}
}
