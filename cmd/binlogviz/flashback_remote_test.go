// Package binlogviz tests flashback's remote input mode and the converged
// local discovery contract: every mutual exclusion rule plus the fake-seam
// golden path — flags → source → remote parser → undo SQL.
// input: cobra args plus a fake remoteSource and fixture frames.
// output: verified usage/input error kinds and emitted undo SQL (ADR 0023).
// pos: regression tests for flashback.go's remote branch.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"binlogviz/internal/clierr"
	"binlogviz/internal/remote"
)

// runFlashbackRemote executes flashback with the connect seam stubbed.
func runFlashbackRemote(t *testing.T, args []string, src remoteSource, openErr error) (*remote.Config, string, string, error) {
	t.Helper()
	got := stubRemoteOpen(t, src, openErr)
	cmd := newFlashbackCommand()
	cmd.SetArgs(args)
	stdout, stderr, err := captureStdoutStderrRun(t, cmd.Execute)
	return got, stdout, stderr, err
}

// flashbackFixtureSource replays a binlog fixture through the remote seam:
// event frames sliced like a replication dump, manifest sized past the file.
func flashbackFixtureSource(t *testing.T, name, logName string) *fakeRemoteSource {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "internal", "binlog", "testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	var frames [][]byte
	for pos := 4; pos < len(data); {
		size := int(data[pos+9]) | int(data[pos+10])<<8 | int(data[pos+11])<<16 | int(data[pos+12])<<24
		if size <= 0 || pos+size > len(data) {
			t.Fatalf("fixture frame at %d declares size %d", pos, size)
		}
		frames = append(frames, data[pos:pos+size])
		pos += size
	}
	return &fakeRemoteSource{
		manifest: []remote.LogFile{{Name: logName, Size: int64(len(data) + 4)}},
		frames:   frames,
	}
}

func TestFlashbackRequiresInput(t *testing.T) {
	_, _, err := executeFlashbackLikeMain(t)
	if err == nil || ExitCode(err) != 3 {
		t.Fatalf("flashback without input = %v, want usage error", err)
	}
}

func TestFlashbackRejectsMixedInputModes(t *testing.T) {
	// Positional files cannot combine with discovery (ADR 0023 adopts
	// analyze's input contract verbatim).
	_, _, err := executeFlashbackLikeMain(t, "mysql-bin.000001", "--from-dir", "/tmp", "--prefix", "mysql-bin.")
	if err == nil || ExitCode(err) != 3 {
		t.Fatalf("positional + discovery = %v, want usage error", err)
	}
}

func TestFlashbackRejectsFromDirWithoutSelector(t *testing.T) {
	_, _, err := executeFlashbackLikeMain(t, "--from-dir", "/tmp")
	if err == nil || ExitCode(err) != 3 {
		t.Fatalf("--from-dir alone = %v, want usage error", err)
	}
	if !strings.Contains(err.Error(), "--prefix") || !strings.Contains(err.Error(), "--index-file") {
		t.Fatalf("missing-selector message must name both alternatives, got %v", err)
	}
}

func TestFlashbackRejectsPrefixWithIndexFile(t *testing.T) {
	// --from-dir takes exactly one of --prefix/--index-file (ADR 0023).
	_, _, err := executeFlashbackLikeMain(t, "--from-dir", "/tmp", "--prefix", "mysql-bin.", "--index-file", "binlog.index")
	if err == nil || ExitCode(err) != 3 {
		t.Fatalf("--prefix + --index-file = %v, want usage error", err)
	}
}

func TestFlashbackRejectsIndexFileWithoutFromDir(t *testing.T) {
	_, _, err := executeFlashbackLikeMain(t, "--index-file", "binlog.index")
	if err == nil || ExitCode(err) != 3 {
		t.Fatalf("--index-file alone = %v, want usage error", err)
	}
}

func TestFlashbackRejectsRemoteWithPositional(t *testing.T) {
	_, _, err := executeFlashbackLikeMain(t, "mysql-bin.000001", "--host", "db.internal", "--user", "replica")
	if err == nil || ExitCode(err) != 3 {
		t.Fatalf("remote + positional = %v, want usage error", err)
	}
}

func TestFlashbackRemoteUsageMatrix(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"remote plus stdin dash", []string{"-", "--host", "db"}},
		{"remote plus from-dir index", []string{"--host", "db", "--from-dir", "/tmp/x", "--index-file", "binlog.index"}},
		{"remote plus prefix", []string{"--host", "db", "--prefix", "binlog."}},
		{"remote plus start-position", []string{"--host", "db", "--start-position", "100"}},
		{"remote plus stop-position", []string{"--host", "db", "--stop-position", "200"}},
		{"port without host", []string{"--port", "3307"}},
		{"user without host", []string{"--user", "replica"}},
		{"password without host", []string{"--password", "x"}},
		{"server-id without host", []string{"--server-id", "7"}},
		{"start-file without host", []string{"--start-file", "binlog.000001"}},
		{"start-pos without host", []string{"--start-pos", "100"}},
		{"start-gtid without host", []string{"--start-gtid", "0-1-3"}},
		{"ssl-mode without host", []string{"--ssl-mode", "required"}},
		{"empty host", []string{"--host", ""}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, err := runFlashbackRemote(t, tc.args, &fakeRemoteSource{}, nil)
			if kind := clierrKind(t, err); kind != clierr.KindUsage {
				t.Fatalf("%v: kind = %s, want usage; err = %v", tc.args, kind, err)
			}
		})
	}
}

func TestFlashbackRemoteSelectorValidation(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"start-pos without start-file", []string{"--host", "db", "--start-pos", "100"}},
		{"start-gtid with start-file", []string{"--host", "db", "--start-gtid", "0-1-3", "--start-file", "binlog.000001"}},
		{"start-pos below header", []string{"--host", "db", "--start-file", "binlog.000001", "--start-pos", "2"}},
		{"invalid ssl-mode", []string{"--host", "db", "--ssl-mode", "verify_ca"}},
		{"invalid gtid", []string{"--host", "db", "--start-gtid", "garbage"}},
		{"port zero", []string{"--host", "db", "--port", "0"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, err := runFlashbackRemote(t, tc.args, &fakeRemoteSource{}, nil)
			if kind := clierrKind(t, err); kind != clierr.KindUsage {
				t.Fatalf("%v: kind = %s, want usage; err = %v", tc.args, kind, err)
			}
		})
	}
}

// TestFlashbackRemoteOpenFailure: a connect-level failure is kind input with
// the reason preserved in details — the ADR 0021 taxonomy carried over.
func TestFlashbackRemoteOpenFailure(t *testing.T) {
	openErr := &remote.Error{Reason: remote.ReasonAuth, Err: errors.New("Error 1045: Access denied")}
	got, _, _, err := runFlashbackRemote(t, []string{"--host", "db", "--user", "replica"}, nil, openErr)
	if kind := clierrKind(t, err); kind != clierr.KindInput {
		t.Fatalf("kind = %s, want input; err = %v", kind, err)
	}
	var cerr *clierr.Error
	if !errors.As(err, &cerr) || cerr.Details["reason"] != string(remote.ReasonAuth) {
		t.Fatalf("details.reason = %v, want auth", cerr.Details)
	}
	if got.User != "replica" || got.Host != "db" {
		t.Fatalf("config = %+v", got)
	}
}

func TestFlashbackRemoteConfigFromFlags(t *testing.T) {
	t.Setenv("BINLOGVIZ_MYSQL_PASSWORD", "env-secret")
	src := flashbackFixtureSource(t, "mysql-8.0.46-flashback-full.binlog", "binlog.000001")
	got, _, _, err := runFlashbackRemote(t, []string{
		"--host", "db.internal:3307", "--user", "replica",
		"--server-id", "42", "--ssl-mode", "required",
		"--start-file", "binlog.000001", "--start-pos", "120",
	}, src, nil)
	if err != nil {
		t.Fatalf("remote flashback failed: %v", err)
	}
	if got.Host != "db.internal" || got.Port != 3307 {
		t.Fatalf("host:port = %s:%d", got.Host, got.Port)
	}
	if got.User != "replica" || got.Password != "env-secret" {
		t.Fatalf("credentials = %q / %q", got.User, got.Password)
	}
	if got.ServerID != 42 || got.SSLMode != remote.SSLRequired {
		t.Fatalf("server-id/ssl = %d/%s", got.ServerID, got.SSLMode)
	}
	if got.Start.File != "binlog.000001" || got.Start.Pos != 120 {
		t.Fatalf("start = %+v", got.Start)
	}
	if got.GTIDSet != nil {
		t.Fatal("file:pos mode must not carry a GTID set")
	}
}

func TestFlashbackRemotePasswordFlagBeatsEnv(t *testing.T) {
	t.Setenv("BINLOGVIZ_MYSQL_PASSWORD", "env-secret")
	src := flashbackFixtureSource(t, "mysql-8.0.46-flashback-full.binlog", "binlog.000001")
	got, _, _, _ := runFlashbackRemote(t, []string{"--host", "db", "--password", "flag-secret"}, src, nil)
	if got.Password != "flag-secret" {
		t.Fatalf("explicit flag must win over the environment, got %q", got.Password)
	}
}

// TestFlashbackRemoteEndToEnd feeds the flashback fixture through the fake
// stream — flags → source → remote parser → planner → undo SQL (ADR 0023).
func TestFlashbackRemoteEndToEnd(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	src := flashbackFixtureSource(t, "mysql-8.0.46-flashback-full.binlog", "mysql-bin.000004")
	_, stdout, _, err := runFlashbackRemote(t, []string{"--host", "db", "--user", "replica"}, src, nil)
	if err != nil {
		t.Fatalf("remote flashback failed: %v", err)
	}
	for _, want := range []string{
		"XA START",
		"-- binlog: db:3306:",
		"INSERT INTO `shop`.`",
		"-- binlogviz-trailer",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("remote undo script missing %q:\n%.400s", want, stdout)
		}
	}
	if strings.Contains(stdout, "mysql-8.0.46-flashback-full.binlog") {
		t.Fatalf("remote undo leaked the local fixture path:\n%.400s", stdout)
	}
}

// TestFlashbackRemotePlan checks --plan rides the same remote path: the
// plan JSON is the planner's own contract (ADR 0023 leaves it untouched).
func TestFlashbackRemotePlan(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	src := flashbackFixtureSource(t, "mysql-8.0.46-flashback-full.binlog", "mysql-bin.000004")
	_, stdout, _, err := runFlashbackRemote(t, []string{"--host", "db", "--user", "replica", "--plan"}, src, nil)
	if err != nil {
		t.Fatalf("remote plan failed: %v", err)
	}
	if !strings.Contains(stdout, `"plan_version"`) {
		t.Fatalf("plan JSON missing plan_version:\n%.400s", stdout)
	}
}

// TestFlashbackFromDirIndexFile pins the ADR 0023 contract convergence:
// --from-dir + --index-file resolves the listed binlog like analyze/redo.
func TestFlashbackFromDirIndexFile(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	dir := t.TempDir()
	data, err := os.ReadFile(mustFixturePath(t, "mysql-8.0.46-flashback-full.binlog"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "mysql-bin.000001"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "binlog.index"), []byte("./mysql-bin.000001\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	stdout, _, err := executeFlashbackLikeMain(t, "--from-dir", dir, "--index-file", "binlog.index")
	if err != nil {
		t.Fatalf("--from-dir --index-file flashback: %v", err)
	}
	if !strings.Contains(stdout, "XA START") {
		t.Fatalf("index-file discovery produced no undo SQL:\n%.400s", stdout)
	}
}
