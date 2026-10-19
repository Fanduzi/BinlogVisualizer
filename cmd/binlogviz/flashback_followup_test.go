package binlogviz

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"binlogviz/internal/i18n"
)

// The least-privilege e2e grants this list on the data schema. Every place
// that documents the apply grants has to name the same privileges.
const documentedDataPrivs = "SELECT, INSERT, UPDATE, DELETE"

func TestDocumentedApplyGrantsMatchLeastPrivilege(t *testing.T) {
	e2e, err := os.ReadFile("flashback_grant_force_test.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(e2e), "GRANT "+documentedDataPrivs+" ON ") {
		t.Fatalf("least-privilege e2e no longer grants %s", documentedDataPrivs)
	}
	root := repoRoot(t)
	files := []string{
		"README.md",
		"README_ZH.md",
		filepath.Join("docs", "concept", "limitations.md"),
		filepath.Join("docs", "concept", "limitations.zh-CN.md"),
		filepath.Join("docs", "agents", "USING-BINLOGVIZ.md"),
		filepath.Join("docs", "agents", "USING-BINLOGVIZ.zh-CN.md"),
		filepath.Join("internal", "i18n", "locales", "en.json"),
		filepath.Join("internal", "i18n", "locales", "zh-CN.json"),
		filepath.Join("docs", "releases", "release-notes-v0.32.0.md"),
		filepath.Join("docs", "releases", "release-notes-v0.32.0.zh-CN.md"),
		filepath.Join("internal", "flashback", "planner.go"),
	}
	for _, name := range files {
		body, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(body), documentedDataPrivs) {
			t.Errorf("%s does not document %s", name, documentedDataPrivs)
		}
	}
	for _, name := range []string{
		filepath.Join("docs", "releases", "release-notes-v0.32.0.md"),
		filepath.Join("docs", "releases", "release-notes-v0.32.0.zh-CN.md"),
	} {
		body, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(body), "Correction") && !strings.Contains(string(body), "更正") {
			t.Errorf("%s has no correction for the v0.32.0 grant list", name)
		}
	}
}

func TestReadmeExitCodesAndTrendOrder(t *testing.T) {
	root := repoRoot(t)
	for _, name := range []string{"README.md", "README_ZH.md"} {
		body, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		text := string(body)
		if strings.Contains(text, "**1** when the file could not be analyzed (corrupt, truncated, or no Format Description)") || strings.Contains(text, "无法分析（损坏、截断、没有 Format Description）时退出 **1**") {
			t.Errorf("%s still says analyze of a corrupt file exits 1", name)
		}
		if strings.Contains(text, "Flashback exits 1") || strings.Contains(text, "flashback 退出 1") || strings.Contains(text, "contradicts the expression exits 1") || strings.Contains(text, "记下的值与表达式矛盾时退出 1") {
			t.Errorf("%s still says a flashback refusal exits 1", name)
		}
		if !strings.Contains(text, "exit 5") && !strings.Contains(text, "退出码 5") {
			t.Errorf("%s does not point corrupt input at exit 5", name)
		}
		if !strings.Contains(text, "exit 6") && !strings.Contains(text, "退出码 6") {
			t.Errorf("%s does not point a flashback refusal at exit 6", name)
		}
		if strings.Contains(text, "orders them by effective window start time") || strings.Contains(text, "按有效窗口开始时间排序") {
			t.Errorf("%s still says trend sorts by window start by default", name)
		}
		if !strings.Contains(text, "--order cli") {
			t.Errorf("%s does not document the default trend order", name)
		}
	}
	lim, err := os.ReadFile(filepath.Join(root, "docs", "concept", "limitations.md"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(lim), "still exits 1 and prints no SQL") {
		t.Error("limitations.md still says a generated-column contradiction exits 1")
	}
	zh, err := os.ReadFile(filepath.Join(root, "docs", "concept", "limitations.zh-CN.md"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(zh), "只改了这个所谓生成列的 `UPDATE`，退出 1，并且不打印 SQL") {
		t.Error("limitations.zh-CN.md still says a generated-column contradiction exits 1")
	}
}

func TestInstallScriptHelp(t *testing.T) {
	cmd := exec.Command("sh", filepath.Join(repoRoot(t), "install.sh"), "--help")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("install.sh --help: %v\n%s", err, out)
	}
	text := string(out)
	for _, flag := range []string{"--version", "--bin-dir", "--no-verify", "--dry-run", "--help"} {
		if !strings.Contains(text, flag) {
			t.Errorf("help missing %s\n%s", flag, text)
		}
	}
}

func TestFlashbackVerifyOldFormatMessage(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	path := filepath.Join(t.TempDir(), "v031.sql")
	body := "-- flashback reverses the selected row changes, last transaction first.\n" +
		"SET @binlogviz_digest = SHA2('binlogviz-flashback-v1', 256);\n" +
		"-- binlogviz-exec\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	code, stdout, stderr := runRoot(t, "--error-format", "json", "flashback", "verify", path)
	if code != 6 || stdout != "" {
		t.Fatalf("exit %d stdout %q\n%s", code, stdout, stderr)
	}
	if !strings.Contains(stderr, `"reason":"old_format"`) || !strings.Contains(stderr, "written by binlogviz v0.31.0 or earlier") {
		t.Fatalf("stderr:\n%s", stderr)
	}
}

func TestFlashbackRefusalJSONReasons(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	cases := []struct {
		name   string
		args   []string
		reason string
		table  string
		msg    string
	}{
		{
			name:   "row image",
			args:   []string{"flashback", mustFixturePath(t, "mysql-8.0.46-flashback-minimal-image.binlog")},
			reason: "row_image",
			table:  "shop.wide",
			msg:    "row image is not FULL",
		},
		{
			name:   "row metadata",
			args:   []string{"flashback", mustFixturePath(t, "mysql-8.0.46-dml-minimal.binlog"), "--include-gtids", "f4d3a60a-c1de-11f1-a30a-822b383dbcd0:4"},
			reason: "row_metadata",
			table:  "shop.orders",
			msg:    "MySQL 8.0.14+",
		},
		{
			name:   "ddl",
			args:   []string{"flashback", mustFixturePath(t, "mysql-8.0.46-index-ddl.binlog")},
			reason: "ddl_in_range",
			msg:    "DDL in the selected range",
		},
		{
			name:   "sql context",
			args:   []string{"flashback", mustFixturePath(t, "mysql-8.0.46-flashback-full.binlog"), "--sql-context", "off"},
			reason: "sql_context_off",
			msg:    "--sql-context off refuses that",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, stdout, stderr := runRoot(t, append([]string{"--error-format", "json"}, tc.args...)...)
			if code != 6 || stdout != "" {
				t.Fatalf("exit %d stdout %q\n%s", code, stdout, stderr)
			}
			var body struct {
				Error struct {
					Kind    string         `json:"kind"`
					Message string         `json:"message"`
					Details map[string]any `json:"details"`
				} `json:"error"`
			}
			if err := json.Unmarshal([]byte(strings.TrimSpace(stderr)), &body); err != nil {
				t.Fatalf("json: %v\n%s", err, stderr)
			}
			if body.Error.Kind != "refused" || body.Error.Details["reason"] != tc.reason {
				t.Fatalf("details %+v message %s", body.Error.Details, body.Error.Message)
			}
			if tc.table != "" && body.Error.Details["table"] != tc.table {
				t.Fatalf("table %v want %s", body.Error.Details["table"], tc.table)
			}
			if tc.reason == "ddl_in_range" {
				if _, ok := body.Error.Details["table"]; ok {
					t.Fatalf("ddl with no table put %v in details.table", body.Error.Details["table"])
				}
			}
			if !strings.Contains(body.Error.Message, tc.msg) {
				t.Fatalf("message %s", body.Error.Message)
			}
			if tc.reason == "row_metadata" && (!strings.Contains(body.Error.Message, "--schema-file does not supply row metadata") || !strings.Contains(body.Error.Message, "Restore the table from a backup")) {
				t.Fatalf("metadata message is not actionable: %s", body.Error.Message)
			}
		})
	}
}

func TestFlashbackHelpNamesCommitMarker(t *testing.T) {
	for _, lang := range []string{"en", "zh-CN"} {
		t.Run(lang, func(t *testing.T) {
			t.Cleanup(func() {
				langFlag = ""
				i18n.ResetForTesting()
			})
			langFlag = lang
			root := NewRootCommand()
			var out strings.Builder
			root.SetOut(&out)
			root.SetErr(&out)
			root.SetArgs([]string{"--lang", lang, "flashback", "--help"})
			if err := root.Execute(); err != nil {
				t.Fatal(err)
			}
			help := out.String()
			if !strings.Contains(help, "binlogviz: undo committed") || !strings.Contains(help, "pipefail") {
				t.Fatalf("--help does not say how to tell a commit:\n%s", help)
			}
		})
	}
}
