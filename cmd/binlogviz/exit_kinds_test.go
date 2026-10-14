package binlogviz

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// runRoot runs the CLI as main does and returns the exit code.
func runRoot(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	code := 0
	stdout, stderr, _ := captureStdoutStderrRun(t, func() error {
		cmd := NewRootCommand()
		cmd.SetArgs(args)
		err := cmd.Execute()
		if err != nil {
			PrintCommandError(os.Stderr, err)
			code = ExitCode(err)
		}
		return err
	})
	return code, stdout, stderr
}

// #254 / ADR 0008: each kind of failure has its own documented exit code.
func TestExitCodesByKind(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	dir := t.TempDir()
	good := filepath.Join("testdata", "minimal.binlog")
	body, err := os.ReadFile(good)
	if err != nil {
		t.Fatal(err)
	}
	truncated := filepath.Join(dir, "truncated.binlog")
	if err := os.WriteFile(truncated, body[:len(body)-7], 0o600); err != nil {
		t.Fatal(err)
	}
	flipped := append([]byte(nil), body...)
	flipped[len(flipped)/2] ^= 0xff
	crcBad := filepath.Join(dir, "crc.binlog")
	if err := os.WriteFile(crcBad, flipped, 0o600); err != nil {
		t.Fatal(err)
	}
	notBinlog := filepath.Join(dir, "x.txt")
	if err := os.WriteFile(notBinlog, []byte("hello, not a binlog"), 0o600); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		args []string
		code int
	}{
		{"unknown flag", []string{"analyze", "--bogus", good}, 3},
		{"unknown command", []string{"analyse", good}, 3},
		{"bad start", []string{"analyze", "--start", "yesterday", good}, 3},
		{"invalid format #251", []string{"analyze", "--format", "yaml", good}, 3},
		{"uppercase format #251", []string{"analyze", "--format", "JSON", good}, 3},
		{"bad conflict", []string{"flashback", "--conflict", "maybe", good}, 3},
		{"missing file", []string{"analyze", filepath.Join(dir, "nosuch.binlog")}, 4},
		{"not a binlog", []string{"analyze", notBinlog}, 5},
		{"truncated", []string{"analyze", truncated}, 5},
		{"checksum mismatch", []string{"analyze", crcBad}, 5},
		{"checksum mismatch flashback", []string{"flashback", crcBad}, 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, stdout, stderr := runRoot(t, tc.args...)
			if code != tc.code {
				t.Fatalf("exit %d, want %d\n%s", code, tc.code, stderr)
			}
			if stdout != "" {
				t.Fatalf("a failed run prints no report:\n%s", stdout)
			}
		})
	}
}

// --error-format json writes one JSON object on stderr with a stable kind.
func TestJSONErrorOnStderr(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	t.Cleanup(func() { errorFormat = "" })
	code, _, stderr := runRoot(t, "analyze", "--error-format", "json", "--format", "yaml", filepath.Join("testdata", "minimal.binlog"))
	if code != 3 {
		t.Fatalf("exit %d", code)
	}
	lines := strings.Split(strings.TrimSpace(stderr), "\n")
	var got struct {
		Error struct {
			Kind    string `json:"kind"`
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &got); err != nil {
		t.Fatalf("%v: %q", err, stderr)
	}
	if got.Error.Kind != "usage" || got.Error.Code != 3 || !strings.Contains(got.Error.Message, "yaml") {
		t.Fatalf("%+v", got)
	}
}

// A corrupt event never prints a Go %#v struct or raw event bytes.
func TestCorruptEventMessageHasNoGoDump(t *testing.T) {
	raw := errors.New(`Header &replication.EventHeader{Timestamp:0x6acf1b92, EventType:0x1f, ServerID:0x1, EventSize:0x2d, LogPos:0x1f4, Flags:0x0}, Data "i\x00\x00\xff", Err: invalid table id 0, no corresponding table map event`)
	err := wrapParseError(raw)
	// The release binary also appended raw data and a TableMapEvent dump to the cause.
	panicky := wrapParseError(errors.New(`Header &replication.EventHeader{EventType:0x1f, EventSize:0x1f78, LogPos:0x3270a}, Data "i", Err: parse rows event panic runtime error: slice bounds out of range [:-1], data "i\x00\x02", table map &replication.TableMapEvent{ColumnName:[][]uint8{}}`))
	if m := panicky.Error(); strings.Contains(m, "data") || strings.Contains(m, "&replication") || !strings.Contains(m, "slice bounds out of range") {
		t.Fatalf("cause not cleaned: %s", m)
	}
	var buf bytes.Buffer
	PrintCommandError(&buf, err)
	msg := buf.String()
	if strings.Contains(msg, "&replication") || strings.Contains(msg, `\x00`) || strings.Contains(msg, "Data ") {
		t.Fatalf("Go dump leaked: %s", msg)
	}
	if !strings.Contains(msg, "500") || !strings.Contains(msg, "31") || !strings.Contains(msg, "invalid table id") {
		t.Fatalf("message must keep event type, end position and cause: %s", msg)
	}
	if ExitCode(err) != 5 {
		t.Fatalf("exit %d", ExitCode(err))
	}
}
