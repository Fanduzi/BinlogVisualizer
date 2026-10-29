// Package binlogviz tests the remote analyze input mode: every mutual
// exclusion rule, every flag-value validation, and the config the flags
// build — with the connect seam faked so no test needs a server.
// input: cobra args plus a fake remoteSource and captured remote.Config.
// output: verified usage/input error kinds and flag-to-config mapping (ADR 0021).
// pos: regression tests for analyze_remote.go.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"binlogviz/internal/clierr"
	"binlogviz/internal/remote"
)

// fakeRemoteSource replays frames like remote.Source: a manifest plus a
// bounded stream, ending io.EOF.
type fakeRemoteSource struct {
	manifest []remote.LogFile
	frames   [][]byte
	err      error
	i        int
}

func (f *fakeRemoteSource) Next(context.Context) ([]byte, error) {
	if f.err != nil {
		err := f.err
		f.err = nil
		return nil, err
	}
	if f.i >= len(f.frames) {
		return nil, io.EOF
	}
	frame := f.frames[f.i]
	f.i++
	return frame, nil
}

func (f *fakeRemoteSource) Manifest() []remote.LogFile { return f.manifest }
func (f *fakeRemoteSource) Close()                     {}

// stubRemoteOpen replaces the connect seam and captures the Config it was
// given. src may be nil; the stub then fails with openErr.
func stubRemoteOpen(t *testing.T, src remoteSource, openErr error) *remote.Config {
	t.Helper()
	got := &remote.Config{}
	prev := remoteOpenSource
	remoteOpenSource = func(ctx context.Context, cfg remote.Config) (remoteSource, error) {
		*got = cfg
		return src, openErr
	}
	t.Cleanup(func() { remoteOpenSource = prev })
	return got
}

func clierrKind(t *testing.T, err error) clierr.Kind {
	t.Helper()
	var cerr *clierr.Error
	if !errors.As(err, &cerr) {
		t.Fatalf("expected a typed clierr, got %T: %v", err, err)
	}
	return cerr.Kind
}

// runAnalyzeRemote executes analyze with the connect seam stubbed.
func runAnalyzeRemote(t *testing.T, args []string, src remoteSource, openErr error) (error, *remote.Config) {
	t.Helper()
	got := stubRemoteOpen(t, src, openErr)
	cmd := newAnalyzeCommand()
	cmd.SetArgs(args)
	return cmd.Execute(), got
}

func TestAnalyzeRemoteUsageMatrix(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"remote plus positional file", []string{"x.binlog", "--host", "db"}},
		{"remote plus stdin dash", []string{"-", "--host", "db"}},
		{"remote plus from-dir", []string{"--host", "db", "--from-dir", "/tmp/x", "--prefix", "binlog."}},
		{"remote plus index-file", []string{"--host", "db", "--from-dir", "/tmp/x", "--index-file", "binlog.index"}},
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
			err, _ := runAnalyzeRemote(t, tc.args, &fakeRemoteSource{}, nil)
			if kind := clierrKind(t, err); kind != clierr.KindUsage {
				t.Fatalf("%v: kind = %s, want usage; err = %v", tc.args, kind, err)
			}
		})
	}
}

func TestAnalyzeRemoteSelectorValidation(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"start-pos without start-file", []string{"--host", "db", "--start-pos", "100"}},
		{"start-gtid with start-file", []string{"--host", "db", "--start-gtid", "0-1-3", "--start-file", "binlog.000001"}},
		{"start-gtid with start-pos", []string{"--host", "db", "--start-gtid", "0-1-3", "--start-file", "binlog.000001", "--start-pos", "4"}},
		{"start-pos below header", []string{"--host", "db", "--start-file", "binlog.000001", "--start-pos", "2"}},
		{"invalid ssl-mode", []string{"--host", "db", "--ssl-mode", "verify_ca"}},
		{"invalid gtid", []string{"--host", "db", "--start-gtid", "garbage"}},
		{"port zero", []string{"--host", "db", "--port", "0"}},
		{"bad port in host", []string{"--host", "db:nope"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err, _ := runAnalyzeRemote(t, tc.args, &fakeRemoteSource{}, nil)
			if kind := clierrKind(t, err); kind != clierr.KindUsage {
				t.Fatalf("%v: kind = %s, want usage; err = %v", tc.args, kind, err)
			}
		})
	}
}

// TestAnalyzeRemoteOpenFailure: a connect-level failure is kind input with
// the reason preserved in details — not a bare error.
func TestAnalyzeRemoteOpenFailure(t *testing.T) {
	openErr := &remote.Error{Reason: remote.ReasonAuth, Err: errors.New("Error 1045: Access denied")}
	err, got := runAnalyzeRemote(t, []string{"--host", "db", "--user", "replica"}, nil, openErr)
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

func TestAnalyzeRemoteConfigFromFlags(t *testing.T) {
	t.Setenv("BINLOGVIZ_MYSQL_PASSWORD", "env-secret")
	src := &fakeRemoteSource{manifest: []remote.LogFile{{Name: "binlog.000001", Size: 4}}}
	err, got := runAnalyzeRemote(t, []string{
		"--host", "db.internal:3307", "--user", "replica",
		"--server-id", "42", "--ssl-mode", "required",
		"--start-file", "binlog.000001", "--start-pos", "120",
	}, src, nil)
	if err != nil {
		t.Fatalf("remote analyze failed: %v", err)
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

func TestAnalyzeRemotePasswordFlagBeatsEnv(t *testing.T) {
	t.Setenv("BINLOGVIZ_MYSQL_PASSWORD", "env-secret")
	src := &fakeRemoteSource{manifest: []remote.LogFile{{Name: "binlog.000001", Size: 4}}}
	_, got := runAnalyzeRemote(t, []string{"--host", "db", "--password", "flag-secret"}, src, nil)
	if got.Password != "flag-secret" {
		t.Fatalf("explicit flag must win over the environment, got %q", got.Password)
	}
}

func TestAnalyzeRemoteGTIDSelectsFlavor(t *testing.T) {
	src := &fakeRemoteSource{manifest: []remote.LogFile{{Name: "binlog.000001", Size: 4}}}
	_, got := runAnalyzeRemote(t, []string{"--host", "db", "--start-gtid", "0-1-3"}, src, nil)
	if got.GTIDSet == nil {
		t.Fatal("GTID start must carry a set")
	}
	if got.Flavor != "mariadb" {
		t.Fatalf("domain-server-seq must select mariadb flavor, got %q", got.Flavor)
	}
	_, got = runAnalyzeRemote(t, []string{"--host", "db", "--start-gtid", "3E11FA47-71CA-11E1-9E33-C80AA9429562:5"}, src, nil)
	if got.Flavor != "mysql" || got.GTIDSet == nil {
		t.Fatalf("uuid set must select mysql flavor, got %+v", got)
	}
}

// TestAnalyzeRemoteEndToEnd feeds a real fixture through the fake stream —
// the remote golden path end to end: flags → source → parser → report.
func TestAnalyzeRemoteEndToEnd(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "internal", "binlog", "testdata", "mysql-8.0.46-dml-full.binlog"))
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
	src := &fakeRemoteSource{
		manifest: []remote.LogFile{{Name: "mysql-bin.000004", Size: int64(len(data) + 4)}},
		frames:   frames,
	}
	cmd := newAnalyzeCommand()
	stubRemoteOpen(t, src, nil)
	cmd.SetArgs([]string{"--host", "db", "--user", "replica", "--format", "json"})
	out := captureStdout(t, func() {
		if err := cmd.Execute(); err != nil {
			t.Fatalf("remote analyze failed: %v", err)
		}
	})
	if !strings.Contains(out, `"total_events"`) && !strings.Contains(out, `"totalEvents"`) {
		t.Fatalf("json report missing totals: %.200s", out)
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	defer func() { os.Stdout = old }()
	done := make(chan string)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	fn()
	_ = w.Close()
	os.Stdout = old
	return <-done
}
