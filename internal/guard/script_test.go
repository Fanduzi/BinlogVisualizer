package guard

import (
	"bytes"
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
	orig := newApplyNonce
	newApplyNonce = func() (string, error) { return "0123456789abcdef", nil }
	t.Cleanup(func() { newApplyNonce = orig })
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
	if strings.Contains(apply, noCommitSQL) || !strings.Contains(apply, "SET @binlogviz_want = 1;") || strings.Contains(apply, "SET @binlogviz_want = 0;") {
		t.Fatal("apply SQL did not replace the trailer")
	}
	var streamed bytes.Buffer
	if err := CopyApply(&streamed, strings.NewReader(script), int64(len(script))); err != nil {
		t.Fatal(err)
	}
	if streamed.String() != apply {
		t.Fatal("CopyApply diverged from ApplySQL")
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
		{"truncated", func(s string) string {
			i := strings.Index(s, "UPDATE t SET v = 1")
			if i < 0 {
				t.Fatal("statement")
			}
			return s[:i+8]
		}, "truncated"},
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
	if !strings.Contains(gate, guardTokenMark) || !strings.Contains(gate, "SET @binlogviz_mode = 3;") || strings.Contains(gate, "COMMIT") {
		t.Fatalf("gate %q", gate)
	}
	if !strings.Contains(ProcedureSQL(), "SET SESSION TRANSACTION READ ONLY") || strings.Contains(ProcedureSQL(), "transaction_read_only") {
		t.Fatal("read-only gate moved out of the procedure")
	}
	if strings.Contains(gate, "COMMIT") {
		t.Fatal("block gate commits")
	}
}

func TestApplySQLRenamesTheFinishProcedure(t *testing.T) {
	g := New(nil)
	script := renderScript(t, g, Piece{Kind: PieceRaw, Text: ProcedureSQL()})
	if !strings.Contains(script, "SQL SECURITY INVOKER") {
		t.Fatal("published procedure is not SQL SECURITY INVOKER")
	}
	first, err := ApplySQL(script)
	if err != nil {
		t.Fatal(err)
	}
	second, err := ApplySQL(script)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("two applies reused one procedure name")
	}
	for _, apply := range []string{first, second} {
		if strings.Contains(apply, noCommitSQL) || !strings.Contains(apply, "SET @binlogviz_want = 1;") {
			t.Fatal("apply SQL did not replace the trailer")
		}
		if !applyProcedureRenamed(script, apply) {
			t.Fatalf("apply SQL did not rename the finish procedure:\n%s", apply)
		}
		got, err := chainQuotedPayloads(apply)
		if err != nil {
			t.Fatal(err)
		}
		seal := sealedHex(apply)
		if seal == "" || got != seal {
			t.Fatalf("trailer hex %s, chain %s", seal, got)
		}
		if strings.Contains(apply, "XA END '"+tokenOf(script)+"_") || strings.Contains(apply, "XA COMMIT '"+tokenOf(script)+"_") {
			t.Fatal("XA id was renamed with the procedure")
		}
	}
}

func tokenOf(script string) string {
	const marker = "CREATE PROCEDURE binlogviz.binlogviz_fb_end_"
	i := strings.Index(script, marker)
	if i < 0 || len(script) < i+len(marker)+16 {
		return ""
	}
	return script[i+len(marker) : i+len(marker)+16]
}

func sealedHex(script string) string {
	const prefix = "SET @binlogviz_sealed = IF(@binlogviz_digest = '"
	i := strings.Index(script, prefix)
	if i < 0 {
		return ""
	}
	rest := script[i+len(prefix):]
	j := strings.Index(rest, "'")
	if j != 64 {
		return ""
	}
	return rest[:j]
}

func chainQuotedPayloads(script string) (string, error) {
	d := sha256hex(digestDomain)
	for _, line := range strings.Split(script, "\n") {
		if !strings.HasPrefix(line, "SET @binlogviz_sql = ") {
			continue
		}
		payload, err := unquoteSQLLine(line)
		if err != nil {
			return "", err
		}
		d = chainStep(d, payload)
	}
	return chainStep(d, trailerMark), nil
}

// applyProcedureRenamed reports whether apply calls a procedure whose name
// is the published name plus a 16-hex nonce, and drops that same name.
func applyProcedureRenamed(script, apply string) bool {
	const marker = "CREATE PROCEDURE binlogviz.binlogviz_fb_end_"
	i := strings.Index(script, marker)
	if i < 0 {
		return false
	}
	rest := script[i+len(marker):]
	token := rest[:16]
	if strings.Trim(token, "0123456789abcdef") != "" {
		return false
	}
	published := "binlogviz.binlogviz_fb_end_" + token
	call := strings.Index(apply, "CALL "+published+"_")
	if call < 0 {
		return false
	}
	nonce := apply[call+len("CALL "+published+"_"):]
	if len(nonce) < 16 || strings.Trim(nonce[:16], "0123456789abcdef") != "" {
		return false
	}
	nonce = nonce[:16]
	name := published + "_" + nonce
	return strings.Contains(apply, "CREATE PROCEDURE "+name+"()") &&
		strings.Contains(apply, "DROP PROCEDURE IF EXISTS "+name+";") &&
		strings.Contains(apply, "CALL "+name+"();") &&
		strings.Contains(script, "CREATE PROCEDURE "+published+"()") &&
		!strings.Contains(script, name)
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

func TestProcedureDropsItselfBeforeRollbackSignal(t *testing.T) {
	body := ProcedureSQL()
	// ERROR 1357: a procedure cannot drop itself. The opening DROP is the
	// one before CREATE. The finish returns, and the client drops it.
	if strings.Contains(body, "SIGNAL SQLSTATE") || strings.Count(body, "DROP PROCEDURE") != 1 {
		t.Fatalf("procedure body must not drop itself or SIGNAL\n%s", body)
	}
	if !strings.Contains(body, "CONTINUE HANDLER FOR SQLEXCEPTION") || !strings.Contains(body, "XA ROLLBACK '"+guardTokenMark+"'") {
		t.Fatal("statement errors must be handled, and rollback stays on the token")
	}
	if !strings.Contains(UndoOpenSQL(), "SET @binlogviz_mode = 1;") {
		t.Fatal("XA START must run inside the procedure")
	}
	finish := finishBody(procedureName("0123456789abcdef"), 1)
	drop := strings.Index(finish, "DROP PROCEDURE IF EXISTS")
	msg := strings.Index(finish, rollbackMessage)
	bad := strings.Index(finish, "SELECT 1 UNION ALL SELECT 1")
	if drop < 0 || msg < 0 || bad < 0 || !(drop < msg && msg < bad) {
		t.Fatalf("client must drop, then say so, then fail\n%s", finish)
	}
}

func TestVerifyV031InitIsOldFormat(t *testing.T) {
	script := "-- banner\nSET @binlogviz_digest = SHA2('binlogviz-flashback-v1', 256);\n-- binlogviz-exec\n"
	_, err := Verify(strings.NewReader(script))
	ve, ok := err.(*VerifyError)
	if !ok || ve.Reason != "old_format" {
		t.Fatalf("got %#v", err)
	}
}

func TestVerifyV032PrepareWrapperIsOldFormat(t *testing.T) {
	script := "" +
		"SET @binlogviz_digest = SHA2('binlogviz-flashback-v1', 256), @binlogviz_applied = 1;\n" +
		"-- binlogviz-exec\n" +
		"SET @binlogviz_sql = 'DO 0';\n" +
		"SET @binlogviz_digest = SHA2(CONCAT(@binlogviz_digest, @binlogviz_sql), 256);\n" +
		"PREPARE binlogviz_fb_stmt_x9q FROM @binlogviz_sql;\n"
	_, err := Verify(strings.NewReader(script))
	ve, ok := err.(*VerifyError)
	if !ok || ve.Reason != "old_format" {
		t.Fatalf("got %#v", err)
	}
}
