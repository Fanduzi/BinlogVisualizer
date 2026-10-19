package guard

import (
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"
)

// Guard tests go through the guard's interface (New and its methods). The
// message cut exists only in the emitted SQL, so the cut tests run that SQL
// on a real server. Set BINLOGVIZ_GUARD_MYSQL to one or more client commands
// separated by "|", for example
//
//	BINLOGVIZ_GUARD_MYSQL='docker exec -i -e MYSQL_PWD=root m80 mysql -uroot|docker exec -i -e MYSQL_PWD=root m57 mysql -uroot'
//
// Without it the server tests are skipped; the offline tests still run.

func TestFlashGuardZeroValueIsNoGuard(t *testing.T) {
	g := New(nil)
	if g.Header() != "" || g.BlockLock() != "" || g.RowCond() != "" {
		t.Fatalf("no columns must mean no guard: %+v", g)
	}
	if got := g.Seal("x" + guardTokenMark); got != "x"+guardTokenMark {
		t.Fatalf("no guard must not seal: %q", got)
	}
}

func TestFlashGuardSealsOneToken(t *testing.T) {
	g := New([]Col{{Schema: "d", Table: "t", Column: "g"}})
	if !strings.Contains(g.Header(), guardTokenMark) || g.BlockLock() != guardLockSQL || g.RowCond() != guardTokenCond {
		t.Fatal("guard parts missing")
	}
	script := g.Header() + g.BlockLock() + "DELETE FROM d.t WHERE " + g.RowCond() + ";\n"
	a, b := g.Seal(script), g.Seal(script)
	if a != b || strings.Contains(a, guardTokenMark) {
		t.Fatal("seal must be deterministic and replace every placeholder")
	}
	tok := regexp.MustCompile(`@binlogviz_ok <=> '([0-9a-f]{16})'`).FindAllStringSubmatch(a, -1)
	if len(tok) < 2 {
		t.Fatalf("token count %d", len(tok))
	}
	for _, m := range tok {
		if m[1] != tok[0][1] {
			t.Fatal("one script, one token")
		}
	}
	if g.Seal(script+"-- other\n") == a {
		t.Fatal("different scripts must get different tokens")
	}
}

func TestGuardSQLShape(t *testing.T) {
	g := New([]Col{{Schema: "shop", Table: "t", Column: "c"}})
	h := g.Header()
	if !strings.Contains(h, guardServerCheckSQL()) {
		t.Fatal("header missing version check")
	}
	for _, name := range []string{guardStmtMsg, guardStmtRO, guardStmtUnlock} {
		if !strings.HasPrefix(name, "binlogviz_fb_") || !strings.HasSuffix(name, "_x9q") || !strings.Contains(h, name) {
			t.Fatalf("statement name %q", name)
		}
	}
	for _, msg := range []string{guardOldServerMsg, guardNotRunMsg, guardReadOnlyMsg} {
		if strings.Contains(msg, ",") || len(msg) > guardValueLimit || !strings.Contains(h, sqlQuote(msg)) {
			t.Fatalf("message %q", msg)
		}
	}
	if !strings.Contains(h, sqlQuote(guardMismatchHint)) {
		t.Fatal("mismatch hint missing")
	}
}

func TestGuardSafeLabel(t *testing.T) {
	if got := guardSafeLabel("db.t.a,b"); got != "db.t.a;b" {
		t.Fatalf("comma: %q", got)
	}
	if got := guardSafeLabel("db.t.c | d"); got != "db.t.c / d" {
		t.Fatalf("separator: %q", got)
	}
	if got := guardSafeLabel("shop.t.c"); got != "shop.t.c" {
		t.Fatalf("plain: %q", got)
	}
	// #208: a name ending in " |" or starting with "| " must not form a separator.
	if got := guardSafeLabel("db.t.x |"); got != "db.t.x /" {
		t.Fatalf("suffix: %q", got)
	}
	if got := guardSafeLabel("db.t.| y"); got != "db.t./ y" {
		t.Fatalf("prefix: %q", got)
	}
}

type guardServer struct{ argv []string }

func guardServers(t *testing.T) []guardServer {
	t.Helper()
	spec := os.Getenv("BINLOGVIZ_GUARD_MYSQL")
	if strings.TrimSpace(spec) == "" {
		t.Skip("set BINLOGVIZ_GUARD_MYSQL to run the guard SQL on MySQL")
	}
	var out []guardServer
	for _, part := range strings.Split(spec, "|") {
		if f := strings.Fields(part); len(f) > 0 {
			out = append(out, guardServer{argv: f})
		}
	}
	return out
}

func (s guardServer) run(t *testing.T, sql string) (string, error) {
	t.Helper()
	argv := append(append([]string(nil), s.argv...), "--default-character-set=utf8mb4", "--batch", "--raw")
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Stdin = strings.NewReader(sql)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

var guardValueRE = regexp.MustCompile(`(?s)Variable 'sql_mode' can't be set to the value of '(.*)'`)

// applyGuard runs the sealed guard header for cols on a fresh session of a
// server that has none of these columns, and returns the sql_mode value the
// guard failed with.
func applyGuard(t *testing.T, s guardServer, cols []Col) string {
	t.Helper()
	g := New(cols)
	out, err := s.run(t, g.Seal("SET NAMES utf8mb4;\n"+g.Header()+"SELECT 'binlogviz-test-continued';\n"))
	if err == nil || strings.Contains(out, "binlogviz-test-continued\n") && !strings.Contains(out, "ERROR 1231") {
		t.Fatalf("guard did not stop the apply:\n%s", out)
	}
	m := guardValueRE.FindStringSubmatch(strings.TrimRight(out, "\n"))
	if m == nil {
		t.Fatalf("no ERROR 1231 value:\n%s", out)
	}
	v := m[1]
	if !utf8.ValidString(v) || len(v) > guardValueLimit || strings.Contains(v, ",") {
		t.Fatalf("value %q (%d bytes) must be valid UTF-8, at most %d bytes, without a comma", v, len(v), guardValueLimit)
	}
	return v
}

func cols3(names ...string) []Col {
	out := make([]Col, len(names))
	for i, n := range names {
		p := strings.SplitN(n, ".", 3)
		out[i] = Col{Schema: p[0], Table: p[1], Column: p[2]}
	}
	return out
}

func TestGuardMessageOnServer(t *testing.T) {
	lead := guardMismatchLead
	wide := make([]string, 12)
	for i := range wide {
		wide[i] = fmt.Sprintf("p186.guardwide.col_%02d", i+1)
	}
	cjk := make([]string, 6)
	for i := range cjk {
		cjk[i] = fmt.Sprintf("p186.z6.生成列_客户全名_%d", i+1)
	}
	pipeA := "p192.t." + strings.Repeat("a", 37)
	pipeB := "p192.t.生成列客户全名订单金额生成列客户全名订单金额生成列客户全 |"
	for _, s := range guardServers(t) {
		// Every name fits: listed in order, joined by " | ".
		if got := applyGuard(t, s, cols3("p183.t.c", "p183.heap.note")); got != lead+": p183.t.c | p183.heap.note" {
			t.Fatalf("two: %q", got)
		}
		// Commas and pipes in names cannot split the list (#208).
		if got := applyGuard(t, s, cols3("db.t.a,b", "db.t.c | d")); got != lead+": db.t.a;b | db.t.c / d" {
			t.Fatalf("safe names: %q", got)
		}
		// Too long: the count, then whole names that fit in 200 bytes.
		if got := applyGuard(t, s, cols3(wide...)); got != lead+" (12 columns): "+strings.Join(wide[:5], " | ") {
			t.Fatalf("wide: %q", got)
		}
		// #192: cut by bytes, so CJK names that fit stay listed.
		if got := applyGuard(t, s, cols3(cjk...)); !strings.HasPrefix(got, lead+" (6 columns): "+cjk[0]+" | "+cjk[1]) || strings.Contains(got, cjk[5]) {
			t.Fatalf("cjk: %q", got)
		}
		// #208: a name ending in " |" is never cut in half.
		if got := applyGuard(t, s, cols3(pipeA, pipeB, "p192.t.c")); got != lead+" (3 columns): "+pipeA {
			t.Fatalf("pipe suffix: %q", got)
		}
		// No whole name fits: count only, no colon.
		if got := applyGuard(t, s, cols3("d.t."+strings.Repeat("字", 60))); got != lead+" (1 columns)" {
			t.Fatalf("huge: %q", got)
		}
	}
}

// Moving the cut point across names that contain " |", "| " and multibyte
// characters always keeps a prefix of whole safe names.
func TestGuardMessageCutSweepOnServer(t *testing.T) {
	servers := guardServers(t)
	for pad := 0; pad <= 60; pad += 4 {
		table := "t" + strings.Repeat("x", pad)
		names := []string{"d." + table + ".x |", "d." + table + ".| y", "d." + table + ".字 |", "d." + table + ".z"}
		safe := make([]string, len(names))
		for i, n := range names {
			safe[i] = guardSafeLabel(n)
		}
		head := guardMismatchLead + " (4 columns)"
		for _, s := range servers {
			got := applyGuard(t, s, cols3(names...))
			ok := got == guardMismatchLead+": "+strings.Join(safe, " | ") || got == head
			for k := 1; k <= len(safe) && !ok; k++ {
				ok = got == head+": "+strings.Join(safe[:k], " | ")
			}
			if !ok {
				t.Fatalf("pad %d: %q is not a prefix of whole names", pad, got)
			}
		}
	}
}

// A target whose columns are generated passes the guard and stays writable.
func TestGuardMatchOnServer(t *testing.T) {
	for _, s := range guardServers(t) {
		if out, err := s.run(t, "DROP DATABASE IF EXISTS bvguard_ok;\nCREATE DATABASE bvguard_ok;\nCREATE TABLE bvguard_ok.t (id INT PRIMARY KEY, g INT AS (id + 1) VIRTUAL);\n"); err != nil {
			t.Fatalf("setup: %v\n%s", err, out)
		}
		g := New(cols3("bvguard_ok.t.g"))
		out, err := s.run(t, g.Seal("SET NAMES utf8mb4;\n"+g.Header()+g.BlockLock()+"INSERT INTO bvguard_ok.t (id) SELECT 7 FROM DUAL WHERE "+g.RowCond()+";\nSELECT COUNT(*) AS n FROM bvguard_ok.t;\n"))
		if err != nil || !strings.Contains(out, "n\n1\n") {
			t.Fatalf("matching target: %v\n%s", err, out)
		}
		s.run(t, "DROP DATABASE bvguard_ok;\n")
	}
}
