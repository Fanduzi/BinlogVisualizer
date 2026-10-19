package guard

import (
	"strings"
	"testing"
)

// The public seal is Write. A zero guard still publishes a digest and a
// trailer, and it does not invent a session token.
func TestZeroGuardWriteHasDigestAndNoToken(t *testing.T) {
	script := renderScript(t, New(nil), Piece{Kind: PieceStmt, Text: "INSERT INTO t VALUES (1)"})
	if strings.Contains(script, "binlogviz_ok") || !strings.Contains(script, digestInitSQL) {
		t.Fatalf("zero guard:\n%s", script)
	}
	if _, err := Verify(strings.NewReader(script)); err != nil {
		t.Fatal(err)
	}
}

func TestWriteDigestsTheExecutedString(t *testing.T) {
	g := New(nil)
	var b strings.Builder
	if err := g.Write(&b, func(emit func(Piece) error) error {
		return emit(Piece{Kind: PieceStmt, Text: "UPDATE t SET v = 1 WHERE id = 1"})
	}); err != nil {
		t.Fatal(err)
	}
	script := b.String()
	const want = "SET @binlogviz_sql = 'UPDATE t SET v = 1 WHERE id = 1';"
	if !strings.Contains(script, want) || !strings.Contains(script, "SET @binlogviz_mode = 0;") {
		t.Fatalf("prepared statement:\n%s", script)
	}
	res, err := Verify(strings.NewReader(script))
	if err != nil {
		t.Fatal(err)
	}
	// SHA2('binlogviz-flashback-v1', 256) then the statement then the trailer.
	// The vector is pinned so a MySQL SHA2 mismatch is obvious in the e2e.
	if res.Digest != chain([]string{"UPDATE t SET v = 1 WHERE id = 1"}) {
		t.Fatalf("digest %s", res.Digest)
	}
	if res.Statements != 1 {
		t.Fatalf("statements %d", res.Statements)
	}
	if res.Digest != "b1f2050e8124ec1c6603e431137b678ba587dff45284756a7505c908ce55cd45" {
		t.Fatalf("digest %s", res.Digest)
	}
}
