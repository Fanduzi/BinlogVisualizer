package binlogviz

import (
	"strings"
	"testing"

	"binlogviz/internal/i18n"
)

func TestCommandExamplesIncludeSpikeRecipe(t *testing.T) {
	wants := map[string][]string{
		"analyze":   {"Examples:", "--detect-spikes", "--diagnostics summary", "flashback", "--plan", "--conflict guard"},
		"flashback": {"Examples:", "--plan", "--conflict guard"},
		"snapshot":  {"Examples:", "snapshot list"},
		"compare":   {"Examples:", "compare", "--current-snapshot"},
		"trend":     {"Examples:", "trend"},
		"workflow":  {"Examples:", "workflow run"},
	}
	for name, tokens := range wants {
		t.Run(name, func(t *testing.T) {
			root := NewRootCommand()
			var out strings.Builder
			root.SetOut(&out)
			root.SetErr(&out)
			root.SetArgs([]string{name, "--help"})
			if err := root.Execute(); err != nil {
				t.Fatal(err)
			}
			help := out.String()
			for _, token := range tokens {
				if !strings.Contains(help, token) {
					t.Fatalf("%s --help missing %q:\n%s", name, token, help)
				}
			}
		})
	}
}

func TestFlashbackSQLContextHelpIsNotAnalyzeWording(t *testing.T) {
	cases := []struct {
		lang      string
		want      string
		forbidden string
	}{
		{lang: "en", want: "Neither changes the undo SQL", forbidden: "including default text, DDL statements, and --show-patterns"},
		{lang: "zh-CN", want: "两者都不改变撤销 SQL", forbidden: "所有格式里的查询文本"},
	}
	for _, tc := range cases {
		t.Run(tc.lang, func(t *testing.T) {
			t.Cleanup(func() {
				langFlag = ""
				i18n.ResetForTesting()
			})
			langFlag = tc.lang
			root := NewRootCommand()
			var out strings.Builder
			root.SetOut(&out)
			root.SetErr(&out)
			root.SetArgs([]string{"--lang", tc.lang, "flashback", "--help"})
			if err := root.Execute(); err != nil {
				t.Fatal(err)
			}
			help := out.String()
			if !strings.Contains(help, tc.want) {
				t.Fatalf("flashback --help missing %q:\n%s", tc.want, help)
			}
			if strings.Contains(help, tc.forbidden) {
				t.Fatalf("flashback --help still uses analyze wording %q:\n%s", tc.forbidden, help)
			}
		})
	}
}
