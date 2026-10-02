// Package binlogviz verifies the open BEGIN+DML dialect fixture at the analyze command.
// input: runAnalysis / the analyze command on internal/binlog/testdata/mysql-8.0.46-open-begin-dml.binlog, and on a prefix that stops at the later GTID.
// output: exit 1 and one Error line naming duration, rows, tables, and span; the prefix exits 0 and JSON keeps the open group.
// pos: operator I/O for the #89 open-uncommitted path through the real parser.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"binlogviz/internal/model"

	"github.com/go-mysql-org/go-mysql/replication"
)

func TestAnalyzeOpenBeginDMLDialectFixtureExitsOne(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	fixture := mustFixturePath(t, "mysql-8.0.46-open-begin-dml.binlog")
	for _, format := range []string{"text", "json"} {
		t.Run(format, func(t *testing.T) {
			stdout, stderr, err := executeAnalyzeLikeMain(t, fixture, "--format", format)
			if err == nil || ExitCode(err) != 1 {
				t.Fatalf("exit %d err=%v stdout=%q stderr=%s", ExitCode(err), err, stdout, stderr)
			}
			if stdout != "" {
				t.Fatalf("open BEGIN with a later GTID must not render a report, stdout=%q", stdout)
			}
			if strings.Count(stderr, "Error:") != 1 {
				t.Fatalf("stderr=%s", stderr)
			}
			assertNoUsageDump(t, stderr)
			for _, want := range []string{
				"open BEGIN without close",
				"not lock-contention proof",
				"dur=",
				"rows=2",
				"tables=testdb.users",
				"mysql-8.0.46-open-begin-dml.binlog:",
			} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error missing %q: %v", want, err)
				}
			}
		})
	}
}

func TestAnalyzeOpenBeginDMLPrefixJSONKeepsGroup(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	fixture := openBeginDMLCommandPrefix(t, mustFixturePath(t, "mysql-8.0.46-open-begin-dml.binlog"))
	stdout, stderr, err := executeAnalyzeLikeMain(t, fixture, "--format", "json")
	if err != nil {
		t.Fatalf("EOF-open prefix must exit 0, got %v stderr=%s", err, stderr)
	}
	if strings.Contains(stderr, "Error:") {
		t.Fatalf("successful analyze must not print Error:, got %q", stderr)
	}
	var decoded struct {
		Diagnostics struct {
			OpenExplicitGroups int `json:"open_explicit_groups"`
			OpenDMLGroups      []struct {
				GTID      string         `json:"gtid"`
				TotalRows int            `json:"total_rows"`
				Duration  string         `json:"duration"`
				Tables    map[string]int `json:"tables"`
				Note      string         `json:"note"`
				PosStart  int64          `json:"pos_start"`
				PosEnd    int64          `json:"pos_end"`
			} `json:"open_dml_groups"`
			LongestTransactions []struct {
				TxnKey string `json:"txn_key"`
			} `json:"longest_transactions"`
		} `json:"diagnostics"`
	}
	if jsonErr := json.Unmarshal([]byte(stdout), &decoded); jsonErr != nil {
		t.Fatalf("json: %v\n%s", jsonErr, stdout)
	}
	if decoded.Diagnostics.OpenExplicitGroups != 1 || len(decoded.Diagnostics.OpenDMLGroups) != 1 {
		t.Fatalf("diagnostics groups=%d dml=%d", decoded.Diagnostics.OpenExplicitGroups, len(decoded.Diagnostics.OpenDMLGroups))
	}
	group := decoded.Diagnostics.OpenDMLGroups[0]
	if group.TotalRows != 2 || group.Tables["testdb.users"] != 2 || group.GTID == "" || group.Duration == "" {
		t.Fatalf("group = %+v", group)
	}
	if group.PosStart <= 0 || group.PosEnd <= group.PosStart {
		t.Fatalf("span = %d-%d", group.PosStart, group.PosEnd)
	}
	if group.Note != model.OpenDMLNote {
		t.Fatalf("note = %q", group.Note)
	}
	if len(decoded.Diagnostics.LongestTransactions) != 0 {
		t.Fatalf("open DML entered committed duration ranking: %+v", decoded.Diagnostics.LongestTransactions)
	}
}

func openBeginDMLCommandPrefix(t *testing.T, src string) string {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	pos, gtids, cut := 4, 0, 0
	for pos+19 <= len(data) {
		size := int(binary.LittleEndian.Uint32(data[pos+9 : pos+13]))
		if size < 19 || pos+size > len(data) {
			t.Fatalf("truncated event at %d", pos)
		}
		if data[pos+4] == byte(replication.GTID_EVENT) {
			gtids++
			if gtids == 2 {
				cut = pos
				break
			}
		}
		pos += size
	}
	if cut == 0 {
		t.Fatal("fixture missing a later GTID")
	}
	dst := filepath.Join(t.TempDir(), "open-begin-eof.binlog")
	if err := os.WriteFile(dst, data[:cut], 0o644); err != nil {
		t.Fatal(err)
	}
	return dst
}
