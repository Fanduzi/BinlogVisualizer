package binlogviz

import (
	"encoding/json"
	"path/filepath"
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

func TestFlashbackMarkedExperimental(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	for _, args := range [][]string{
		{"flashback", "--help"},
		{"--lang", "zh-CN", "flashback", "--help"},
		{"flashback", "verify", "--help"},
	} {
		code, stdout, stderr := runRoot(t, args...)
		out := stdout + stderr
		if code != 0 || !strings.Contains(out, "[experimental]") {
			t.Fatalf("%v exit %d, help has no [experimental] marker:\n%s", args, code, out)
		}
	}
	_, stdout, stderr := runRoot(t, "flashback", "--help")
	help := stdout + stderr
	for _, flag := range []string{"--plan", "--conflict", "--dml", "--schema-file"} {
		if !helpLineHas(help, flag, "[experimental]") {
			t.Fatalf("flag %s is not marked experimental:\n%s", flag, help)
		}
	}
	_, stdout, stderr = runRoot(t, "flashback", "verify", "--help")
	if !helpLineHas(stdout+stderr, "--format", "[experimental]") {
		t.Fatalf("verify --format is not marked experimental:\n%s", stdout+stderr)
	}

	langFlag = ""
	i18n.ResetForTesting()
	if err := i18n.Init("en"); err != nil {
		t.Fatal(err)
	}
	code, stdout, stderr := runRoot(t, "flashback", mustFixturePath(t, "minimal.binlog"), "--plan")
	const warn = "warning: flashback is experimental; review the generated SQL before applying it"
	if strings.Count(stderr, warn) != 1 {
		t.Fatalf("exit %d, experimental warning count %d\nstderr:\n%s", code, strings.Count(stderr, warn), stderr)
	}
	if strings.HasPrefix(strings.TrimSpace(stdout), "{") {
		var plan map[string]any
		if err := json.Unmarshal([]byte(stdout), &plan); err != nil {
			t.Fatalf("plan: %v\n%s", err, stdout)
		}
		if plan["experimental"] != true {
			t.Fatalf("plan experimental = %v\n%s", plan["experimental"], stdout)
		}
	}
	code, _, stderr = runRoot(t, "flashback", "verify", filepath.Join(t.TempDir(), "missing.sql"))
	if code == 0 || strings.Count(stderr, warn) != 1 {
		t.Fatalf("verify exit %d\n%s", code, stderr)
	}
}

func helpLineHas(out, a, b string) bool {
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, a) && strings.Contains(line, b) {
			return true
		}
	}
	return false
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
