// Package remote tests the flag-value parsers, the manifest boundary rules,
// the error taxonomy, and the stream seam — all without a live server.
// input: synthetic configs, manifests, and a hand-fed BinlogStreamer.
// output: verified behavior for ParseStartGTID, SSL modes, host:port splitting, validateStart, classify, and Source.Next.
// pos: unit tests for the remote leaf; protocol round trips are the e2e job.
// note: if this file changes, update this header and README.md.
package remote

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

func TestParseSSLMode(t *testing.T) {
	for _, value := range []string{"disabled", "preferred", "required"} {
		mode, err := ParseSSLMode(value)
		if err != nil {
			t.Fatalf("ParseSSLMode(%q) failed: %v", value, err)
		}
		if string(mode) != value {
			t.Fatalf("ParseSSLMode(%q) = %q", value, mode)
		}
	}
	if _, err := ParseSSLMode("verify_ca"); err == nil {
		t.Fatal("verify_ca must be rejected: the certificate flags it needs are out of scope (ADR 0021)")
	}
	if _, err := ParseSSLMode(""); err == nil {
		t.Fatal("empty ssl-mode must be rejected")
	}
}

func TestSSLModeTLSConfig(t *testing.T) {
	if SSLDisabled.tlsConfig() != nil {
		t.Fatal("disabled must not configure TLS")
	}
	for _, mode := range []SSLMode{SSLPreferred, SSLRequired} {
		cfg := mode.tlsConfig()
		if cfg == nil || !cfg.InsecureSkipVerify {
			t.Fatalf("%s must encrypt without verifying the certificate", mode)
		}
	}
}

func TestParseStartGTIDMySQL(t *testing.T) {
	set, flavor, err := ParseStartGTID("3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5")
	if err != nil {
		t.Fatalf("mysql GTID set failed to parse: %v", err)
	}
	if flavor != mysql.MySQLFlavor {
		t.Fatalf("mysql format must select flavor mysql, got %q", flavor)
	}
	if set == nil {
		t.Fatal("parsed set must not be nil")
	}
}

func TestParseStartGTIDBarePointExpands(t *testing.T) {
	// A bare uuid:N means "start after GTID N": the executed set is 1-N.
	set, flavor, err := ParseStartGTID("3E11FA47-71CA-11E1-9E33-C80AA9429562:5")
	if err != nil {
		t.Fatalf("bare uuid:N failed to parse: %v", err)
	}
	if flavor != mysql.MySQLFlavor {
		t.Fatalf("flavor = %q, want mysql", flavor)
	}
	if got := set.String(); got != "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5" {
		t.Fatalf("bare uuid:5 must expand to uuid:1-5, got %q", got)
	}
}

func TestParseStartGTIDMariaDB(t *testing.T) {
	set, flavor, err := ParseStartGTID("0-1-3")
	if err != nil {
		t.Fatalf("mariadb GTID set failed to parse: %v", err)
	}
	if flavor != mysql.MariaDBFlavor {
		t.Fatalf("domain-server-seq must select flavor mariadb, got %q", flavor)
	}
	if set == nil {
		t.Fatal("parsed set must not be nil")
	}
}

func TestParseStartGTIDRejectsGarbage(t *testing.T) {
	for _, value := range []string{"", "not-a-gtid", "1:2:3", "uuid::5"} {
		if _, _, err := ParseStartGTID(value); err == nil {
			t.Fatalf("ParseStartGTID(%q) must fail", value)
		}
	}
}

func TestNormalizeHost(t *testing.T) {
	cases := []struct {
		host    string
		port    uint16
		portSet bool
		want    string
		wantP   uint16
	}{
		{"db.internal", 3306, false, "db.internal", 3306},
		{"db.internal:3307", 3306, false, "db.internal", 3307},
		{"db.internal:3307", 3308, true, "db.internal", 3308},
		{"127.0.0.1", 3306, false, "127.0.0.1", 3306},
	}
	for _, tc := range cases {
		host, port, err := NormalizeHost(tc.host, tc.port, tc.portSet)
		if err != nil {
			t.Fatalf("NormalizeHost(%q) failed: %v", tc.host, err)
		}
		if host != tc.want || port != tc.wantP {
			t.Fatalf("NormalizeHost(%q,%d,%v) = %q,%d want %q,%d", tc.host, tc.port, tc.portSet, host, port, tc.want, tc.wantP)
		}
	}
	if _, _, err := NormalizeHost("db.internal:nope", 3306, false); err == nil {
		t.Fatal("a non-numeric port in --host must fail")
	}
	if _, _, err := NormalizeHost("db.internal:0", 3306, false); err == nil {
		t.Fatal("port 0 in --host must fail")
	}
	if _, _, err := NormalizeHost(":3306", 3306, false); err == nil {
		t.Fatal("an empty host in --host would silently dial localhost")
	}
	if _, _, err := NormalizeHost("db.internal:", 3306, false); err == nil {
		t.Fatal("an empty port in --host must fail")
	}
}

func TestValidateStart(t *testing.T) {
	manifest := []LogFile{{Name: "binlog.000001", Size: 1000}, {Name: "binlog.000002", Size: 500}}
	if err := validateStart(StartPos{}, manifest); err != nil {
		t.Fatalf("empty start file must pass: %v", err)
	}
	if err := validateStart(StartPos{File: "binlog.000002", Pos: 500}, manifest); err != nil {
		t.Fatalf("start at the probed end must pass: %v", err)
	}
	err := validateStart(StartPos{File: "binlog.000002", Pos: 501}, manifest)
	var rerr *Error
	if !errors.As(err, &rerr) || rerr.Reason != ReasonStartPos {
		t.Fatalf("position past the probed end must be start_pos, got %v", err)
	}
	err = validateStart(StartPos{File: "binlog.000009", Pos: 4}, manifest)
	if !errors.As(err, &rerr) || rerr.Reason != ReasonStartFile {
		t.Fatalf("a file absent from the manifest must be start_file, got %v", err)
	}
}

func TestClassifyOpenError(t *testing.T) {
	cases := []struct {
		msg  string
		want Reason
	}{
		{"client does not support TLS", ReasonTLS},
		{"Error 1045: Access denied for user 'replica'", ReasonAuth},
		{"Error 1227: Access denied; you need the REPLICATION SLAVE privilege", ReasonPrivilege},
		{"Error 1130: Host 'x' is not allowed to connect", ReasonAuth},
		{"Error 1236: Got fatal error 1236 from master when reading data from binary log: 'Could not find first log file name in binary log index file'", ReasonStartFile},
		{"dial tcp 10.0.0.1:3306: connect: connection refused", ReasonConnect},
	}
	for _, tc := range cases {
		if got := classifyOpenError(errors.New(tc.msg)); got != tc.want {
			t.Fatalf("classifyOpenError(%q) = %s, want %s", tc.msg, got, tc.want)
		}
	}
}

func TestManifestReason(t *testing.T) {
	if got := manifestReason(errors.New("Error 1227: Access denied; you need the REPLICATION CLIENT privilege")); got != ReasonPrivilege {
		t.Fatalf("manifest privilege denial = %s, want privilege", got)
	}
	if got := manifestReason(errors.New("unexpected packet")); got != ReasonManifest {
		t.Fatalf("manifest garble = %s, want manifest", got)
	}
}

func TestRandomServerID(t *testing.T) {
	a, err := randomServerID()
	if err != nil {
		t.Fatal(err)
	}
	b, err := randomServerID()
	if err != nil {
		t.Fatal(err)
	}
	if a == 0 || b == 0 {
		t.Fatal("server id must be nonzero")
	}
	if a == b {
		t.Fatal("two draws produced the same id")
	}
}

// TestSourceNext exercises the GetEvent seam: raw bytes out, ErrSyncClosed
// becomes io.EOF, any other stream error becomes ReasonStream.
func TestSourceNext(t *testing.T) {
	src := &Source{stream: replication.NewBinlogStreamer()}
	frame := make([]byte, replication.EventHeaderSize+4)
	if err := src.stream.AddEventToStreamer(&replication.BinlogEvent{RawData: frame}); err != nil {
		t.Fatal(err)
	}
	got, err := src.Next(context.Background())
	if err != nil || len(got) != len(frame) {
		t.Fatalf("Next = %d bytes, %v; want %d bytes, nil", len(got), err, len(frame))
	}
	if !src.stream.AddErrorToStreamer(replication.ErrSyncClosed) {
		t.Fatal("AddErrorToStreamer rejected")
	}
	if _, err := src.Next(context.Background()); err != io.EOF {
		t.Fatalf("a closed stream must surface as the io.EOF sentinel, got %v", err)
	}
}

// TestSourceNextWrappedEOF: a mid-stream connection drop looks like io.EOF
// from GetEvent, but it is NOT an end of binlog. Next wraps it as a stream
// error so the parse loop cannot mistake a truncation for a clean close
// (ADR 0021).
func TestSourceNextWrappedEOF(t *testing.T) {
	src := &Source{stream: replication.NewBinlogStreamer()}
	if !src.stream.AddErrorToStreamer(io.EOF) {
		t.Fatal("AddErrorToStreamer rejected")
	}
	_, err := src.Next(context.Background())
	if err == io.EOF {
		t.Fatal("a raw stream EOF must not surface as the clean-close sentinel")
	}
	var rerr *Error
	if !errors.As(err, &rerr) || rerr.Reason != ReasonStream {
		t.Fatalf("stream EOF = %v, want reason stream", err)
	}
}

func TestSourceNextStreamError(t *testing.T) {
	src := &Source{stream: replication.NewBinlogStreamer()}
	if !src.stream.AddErrorToStreamer(fmt.Errorf("connection reset by peer")) {
		t.Fatal("AddErrorToStreamer rejected")
	}
	_, err := src.Next(context.Background())
	var rerr *Error
	if !errors.As(err, &rerr) || rerr.Reason != ReasonStream {
		t.Fatalf("mid-stream failure = %v, want reason stream", err)
	}
}

func TestSourceNextShortFrame(t *testing.T) {
	src := &Source{stream: replication.NewBinlogStreamer()}
	if err := src.stream.AddEventToStreamer(&replication.BinlogEvent{RawData: []byte{1, 2, 3}}); err != nil {
		t.Fatal(err)
	}
	_, err := src.Next(context.Background())
	var rerr *Error
	if !errors.As(err, &rerr) || rerr.Reason != ReasonStream {
		t.Fatalf("a frame shorter than the header = %v, want reason stream", err)
	}
}
