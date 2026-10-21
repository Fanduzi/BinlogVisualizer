package binlogviz

// ADR 0015: a table or schema filter never hides a DDL in range from the
// flashback refusal. The fixtures are BinlogQA's live MySQL 8.4.11 binlogs
// from the PR #366 QA (internal/binlog/testdata/README.md).

import (
	"encoding/json"
	"strings"
	"testing"
)

const ddlFilterUUID = "84ab694c-cc66-11f1-b6dd-822b383dbcd0"

func TestFlashbackDDLUnderTableFilter(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	cases := []struct {
		name    string
		fixture string
		args    []string
		table   string // "" passes with exit 0
		stmt    string
	}{
		// The selected table is the target of a later pair in a RENAME list.
		// The undo of the second DELETE would insert rn.t2's row into rn.t1.
		{"swap later pair", "rename-swap-later", []string{"--include-table", "rn.t1"}, "rn.t1", "RENAME TABLE rn.t2 TO rn.tmp, rn.t1 TO rn.t2"},
		{"swap unqualified selector", "rename-swap-later", []string{"--include-table", "t1"}, "rn.t1", "RENAME TABLE rn.t2"},
		{"swap exclude other", "rename-swap-later", []string{"--exclude-table", "rn.t2"}, "rn.tmp", "RENAME TABLE rn.t2"},
		{"swap include schema", "rename-swap-later", []string{"--include-schema", "rn"}, "rn.t2", "RENAME TABLE rn.t2"},
		{"swap gtids span the rename", "rename-swap-later", []string{"--include-table", "rn.t1", "--include-gtids", ddlFilterUUID + ":71-73"}, "rn.t1", "RENAME TABLE rn.t2"},
		// The selected table is the second name of a multi-table DROP, then the
		// target of a RENAME.
		{"drop second name", "drop-multi", []string{"--include-table", "rn.t1"}, "rn.z, rn.t1", "DROP TABLE `rn`.`z`,`rn`.`t1`"},
		// The selected table is named first.
		{"rename first", "rename-first", []string{"--include-table", "rn.t1"}, "rn.t1", "RENAME TABLE rn.t1 TO rn.t1_old"},
		{"alter rename first", "alter-rename", []string{"--include-table", "rn.t1"}, "rn.t1", "ALTER TABLE rn.t1 RENAME TO rn.t9"},
		// A DDL that names only another table still passes.
		{"other table include", "other-table", []string{"--include-table", "qa.a"}, "", ""},
		{"other table exclude", "other-table", []string{"--exclude-table", "qa.b"}, "", ""},
		{"other table schema", "other-table", []string{"--include-schema", "qa"}, "qa.b", "ALTER TABLE qa.b ADD INDEX iv (v)"},
		// GTIDs that leave the DDL out pass, as before.
		{"swap gtids skip the rename", "rename-swap-later", []string{"--include-table", "rn.t1", "--include-gtids", ddlFilterUUID + ":71," + ddlFilterUUID + ":73"}, "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"--error-format", "json", "flashback", "-q", mustFixturePath(t, "mysql-8.4.11-ddlfilter-"+tc.fixture+".binlog")}, tc.args...)
			code, stdout, stderr := runRoot(t, args...)
			if tc.table == "" {
				if code != 0 || !strings.Contains(stdout, "UPDATE") {
					t.Fatalf("exit %d\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
				}
				return
			}
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
			if err := json.Unmarshal([]byte(lastJSONLine(stderr)), &body); err != nil {
				t.Fatalf("json: %v\n%s", err, stderr)
			}
			if body.Error.Kind != "refused" || body.Error.Details["reason"] != "ddl_in_range" || body.Error.Details["table"] != tc.table {
				t.Fatalf("details %+v message %s", body.Error.Details, body.Error.Message)
			}
			if !strings.Contains(body.Error.Message, tc.stmt) || !strings.Contains(body.Error.Message, "flashback does not undo schema changes") {
				t.Fatalf("message %s", body.Error.Message)
			}
		})
	}
}

// With no table filter the refusal is unchanged: the first table names it.
func TestFlashbackDDLWithoutFilterUnchanged(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	for fixture, want := range map[string]string{
		"rename-swap-later": "rn.t2: DDL in the selected range (RENAME TABLE rn.t2 TO rn.tmp",
		"rename-first":      "rn.t1: DDL in the selected range (RENAME TABLE rn.t1 TO rn.t1_old",
		"other-table":       "qa.b: DDL in the selected range (ALTER TABLE qa.b ADD INDEX iv (v))",
	} {
		code, stdout, stderr := runRoot(t, "flashback", "-q", mustFixturePath(t, "mysql-8.4.11-ddlfilter-"+fixture+".binlog"))
		if code != 6 || stdout != "" || !strings.Contains(stderr, want) {
			t.Fatalf("%s: exit %d stdout %q\n%s", fixture, code, stdout, stderr)
		}
	}
}

func lastJSONLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if strings.HasPrefix(strings.TrimSpace(lines[i]), "{") {
			return lines[i]
		}
	}
	return s
}
