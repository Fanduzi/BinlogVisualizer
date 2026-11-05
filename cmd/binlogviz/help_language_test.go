package binlogviz

import (
	"strings"
	"testing"

	"binlogviz/internal/i18n"
)

// --lang is parsed after the command tree is built, so help used to print in the
// language the process started in.
func TestHelpFollowsTheLanguageFlag(t *testing.T) {
	cases := []struct {
		lang     string
		args     []string
		wantLong string
		wantFlag string
	}{
		{"zh-CN", []string{"analyze", "--help"}, "分析 binlog 文件", "也用它给出热点行排序用的主键"},
		{"en", []string{"analyze", "--help"}, "Analyze binlog files", "supplies the Hot Rows key when the binlog records none"},
		{"zh-CN", []string{"compare", "--help"}, "对比两份", ""},
	}
	for _, tc := range cases {
		i18n.ResetForTesting()
		if err := i18n.Init("en"); err != nil {
			t.Fatal(err)
		}
		root := NewRootCommand()
		var out strings.Builder
		root.SetOut(&out)
		root.SetErr(&out)
		root.SetArgs(append([]string{"--lang", tc.lang}, tc.args...))
		if err := root.Execute(); err != nil {
			t.Fatalf("%v: %v", tc.args, err)
		}
		help := out.String()
		if !strings.Contains(help, tc.wantLong) {
			t.Fatalf("--lang %s %v: help is not in that language:\n%s", tc.lang, tc.args, help)
		}
		if tc.wantFlag != "" && !strings.Contains(help, tc.wantFlag) {
			t.Fatalf("--lang %s %v: flag usage is not in that language:\n%s", tc.lang, tc.args, help)
		}
	}
	langFlag = ""
	i18n.ResetForTesting()
	if err := i18n.Init("en"); err != nil {
		t.Fatal(err)
	}
}
