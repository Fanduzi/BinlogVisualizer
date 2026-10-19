package guard

import (
	"strings"
	"testing"
)

func renderScript(t *testing.T, g Guard, pieces ...Piece) string {
	t.Helper()
	var b strings.Builder
	err := g.Write(&b, func(emit func(Piece) error) error {
		for _, p := range pieces {
			if err := emit(p); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return b.String()
}

func TestWriteVerifyRoundTrip(t *testing.T) {
	g := New([]Col{{Schema: "shop", Table: "t", Column: "g"}})
	script := renderScript(t, g,
		Piece{Kind: PieceComment, Text: "-- banner\n"},
		Piece{Kind: PieceStmt, Text: "SET NAMES utf8mb4"},
		Piece{Kind: PieceRaw, Text: g.Header()},
		Piece{Kind: PieceRaw, Text: "START TRANSACTION;\n"},
		Piece{Kind: PieceStmt, Text: "UPDATE `shop`.`t` SET `id` = 1 WHERE " + g.RowCond()},
	)
	if strings.Contains(script, guardTokenMark) {
		t.Fatal("placeholder left in the script")
	}
	if strings.Contains(script, "\nCOMMIT;") || strings.HasPrefix(script, "COMMIT;") {
		// The header lock still COMMITs the information_schema read. The undo must not.
		if strings.Count(script, "\nCOMMIT;\n") != 1 {
			t.Fatalf("undo commit leaked:\n%s", script)
		}
	}
	if !strings.HasSuffix(script, noCommitSQL+"\n") {
		t.Fatal("script does not end on the no-commit trailer")
	}
	res, err := Verify(strings.NewReader(script))
	if err != nil {
		t.Fatal(err)
	}
	if res.Statements == 0 || len(res.Digest) != 64 {
		t.Fatalf("result %+v", res)
	}
	apply, err := ApplySQL(script)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(apply, noCommitSQL) || !strings.Contains(apply, "SET @binlogviz_commit = IF(@binlogviz_sealed <=> 1, 'COMMIT', 'ROLLBACK');") {
		t.Fatal("apply SQL did not replace the trailer")
	}
	again := renderScript(t, g,
		Piece{Kind: PieceComment, Text: "-- banner\n"},
		Piece{Kind: PieceStmt, Text: "SET NAMES utf8mb4"},
		Piece{Kind: PieceRaw, Text: g.Header()},
		Piece{Kind: PieceRaw, Text: "START TRANSACTION;\n"},
		Piece{Kind: PieceStmt, Text: "UPDATE `shop`.`t` SET `id` = 1 WHERE " + g.RowCond()},
	)
	if again != script {
		t.Fatal("Write is not deterministic")
	}
}

func TestVerifyRefusesTamperedScripts(t *testing.T) {
	g := New(nil)
	script := renderScript(t, g,
		Piece{Kind: PieceStmt, Text: "UPDATE t SET v = 1 WHERE id = 1"},
		Piece{Kind: PieceStmt, Text: "UPDATE t SET v = 2 WHERE id = 2"},
		Piece{Kind: PieceRaw, Text: "START TRANSACTION;\n"},
	)
	cases := []struct {
		name   string
		mutate func(string) string
		reason string
	}{
		{"edited value", func(s string) string {
			return strings.Replace(s, "SET v = 1", "SET v = 9", 1)
		}, "digest_mismatch"},
		{"deleted statement", func(s string) string {
			start := strings.Index(s, markerExec)
			second := strings.Index(s[start+len(markerExec):], markerExec)
			if start < 0 || second < 0 {
				t.Fatal("markers")
			}
			return s[:start] + s[start+len(markerExec)+second:]
		}, "digest_mismatch"},
		{"reordered", func(s string) string {
			start := strings.Index(s, markerExec)
			second := start + len(markerExec) + strings.Index(s[start+len(markerExec):], markerExec)
			third := strings.Index(s, markerRaw)
			if start < 0 || second < 0 || third < 0 {
				t.Fatalf("markers %d %d %d", start, second, third)
			}
			return s[:start] + s[second:third] + s[start:second] + s[third:]
		}, "digest_mismatch"},
		{"appended", func(s string) string { return s + "DELETE FROM t;\n" }, "appended"},
		{"truncated", func(s string) string { return s[:len(s)/2] }, "truncated"},
		{"missing trailer", func(s string) string {
			i := strings.Index(s, markerTrailer)
			return s[:i]
		}, "missing_trailer"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bad := tc.mutate(script)
			_, err := Verify(strings.NewReader(bad))
			ve, ok := err.(*VerifyError)
			if !ok || ve.Reason != tc.reason {
				t.Fatalf("reason %v, want %s", err, tc.reason)
			}
			if _, err := ApplySQL(bad); err == nil {
				t.Fatal("ApplySQL accepted a tampered script")
			}
		})
	}
	if _, err := Verify(strings.NewReader("")); err == nil || err.(*VerifyError).Reason != "not_flashback" {
		t.Fatalf("empty: %v", err)
	}
	if _, err := Verify(strings.NewReader("SELECT 1;\n")); err == nil || err.(*VerifyError).Reason != "not_flashback" {
		t.Fatalf("other sql: %v", err)
	}
}

func TestTokenMismatchIsAnErrorCondition(t *testing.T) {
	g := New([]Col{{Schema: "d", Table: "t", Column: "g"}})
	if !strings.Contains(g.RowCond(), "SELECT @binlogviz_bad := 1 UNION ALL SELECT 1") {
		t.Fatalf("row condition: %s", g.RowCond())
	}
	gate := g.BlockGate()
	if !strings.Contains(gate, guardTokenMark) || !strings.Contains(gate, "SET SESSION TRANSACTION READ ONLY") || strings.Contains(gate, "transaction_read_only") {
		t.Fatalf("gate %q", gate)
	}
	if strings.Contains(gate, "COMMIT") {
		t.Fatal("block gate commits")
	}
}

func TestPayloadQuoteRoundTrip(t *testing.T) {
	mode := "SET SESSION sql_mode = REPLACE(@@SESSION.sql_mode, 'NO_BACKSLASH_ESCAPES', '')"
	quoted := payloadQuote(mode)
	if strings.Contains(quoted, `\`) {
		t.Fatalf("mode strip payload uses a backslash: %s", quoted)
	}
	got, err := sqlUnquote(quoted)
	if err != nil || got != mode {
		t.Fatalf("unquote %q err %v", got, err)
	}
	raw := "START TRANSACTION;\n"
	got, err = sqlUnquote(payloadQuote(raw))
	if err != nil || got != raw {
		t.Fatalf("raw unquote %q err %v", got, err)
	}
}
