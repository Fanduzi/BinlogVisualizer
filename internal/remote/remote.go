// Package remote is the replication-protocol client for remote binlog input.
// input: host/port/user credentials, TLS mode, a server-id, and a file:pos or GTID start point.
// output: a Source of raw binlog event frames plus the SHOW BINARY LOGS manifest taken at connect time.
// pos: leaf protocol adapter between go-mysql's BinlogSyncer and the binlog remote parser; it knows nothing about RawEvent or analysis.
// note: if this file changes, update this header and README.md.
package remote

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// probeTimeout bounds the SHOW BINARY LOGS round trip on the dump connection.
// The manifest answers in milliseconds on a healthy server; a wedged read is
// a connect failure, not a wait.
const probeTimeout = 15 * time.Second

// SSLMode is the MySQL --ssl-mode vocabulary: disabled, preferred, required.
type SSLMode string

const (
	SSLDisabled  SSLMode = "disabled"
	SSLPreferred SSLMode = "preferred"
	SSLRequired  SSLMode = "required"
)

// ParseSSLMode validates a --ssl-mode value.
func ParseSSLMode(value string) (SSLMode, error) {
	switch SSLMode(value) {
	case SSLDisabled, SSLPreferred, SSLRequired:
		return SSLMode(value), nil
	default:
		return "", fmt.Errorf("invalid ssl-mode %q", value)
	}
}

// tlsConfig follows MySQL semantics: REQUIRED encrypts without verifying the
// certificate; verify modes need certificate flags this package does not have.
func (m SSLMode) tlsConfig() *tls.Config {
	if m == SSLRequired || m == SSLPreferred {
		return &tls.Config{InsecureSkipVerify: true}
	}
	return nil
}

// StartPos is a file:pos dump start. File "" means the server's first binlog,
// the same meaning an empty MASTER_LOG_FILE carries for a real replica.
type StartPos struct {
	File string
	Pos  uint32
}

// Config is one remote analyze input: where to connect and where to start.
// Password is never printed; BinlogSyncer clears it before logging config too.
type Config struct {
	Host     string
	Port     uint16
	User     string
	Password string
	ServerID uint32
	SSLMode  SSLMode
	// Flavor is go-mysql's "mysql" or "mariadb". GTID start sets it from the
	// parsed set; file:pos leaves it empty, which defaults to mysql.
	Flavor  string
	Start   StartPos
	GTIDSet mysql.GTIDSet
}

// LogFile is one row of SHOW BINARY LOGS at connect time.
type LogFile struct {
	Name string
	Size int64
}

// Source is a live dump: frames of raw event bytes plus the connect-time
// manifest the boundary decisions are made against.
type Source struct {
	syncer        *replication.BinlogSyncer
	stream        *replication.BinlogStreamer
	manifest      []LogFile
	serverVersion string
	probeErr      error
}

// Next returns the next event's raw bytes, io.EOF at a clean close, or a
// stream error. The bytes include header and checksum footer, exactly the
// shape a binlog file stores.
func (s *Source) Next(ctx context.Context) ([]byte, error) {
	ev, err := s.stream.GetEvent(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err == replication.ErrSyncClosed {
			return nil, io.EOF
		}
		return nil, &Error{Reason: ReasonStream, Err: err}
	}
	if ev == nil || len(ev.RawData) < replication.EventHeaderSize {
		return nil, &Error{Reason: ReasonStream, Err: fmt.Errorf("server sent an event frame shorter than the event header")}
	}
	return ev.RawData, nil
}

// Manifest is the SHOW BINARY LOGS snapshot taken at connect time.
func (s *Source) Manifest() []LogFile {
	return s.manifest
}

// ServerVersion is the version string the handshake reported.
func (s *Source) ServerVersion() string {
	return s.serverVersion
}

// Close drops the dump and the connection.
func (s *Source) Close() {
	if s != nil && s.syncer != nil {
		s.syncer.Close()
	}
}

// Open connects, probes the manifest on the dump connection between auth and
// the dump command, validates the start point against it, and starts the
// dump. A preferred ssl-mode retries plain only when the server reports it
// cannot do TLS at all; any other failure is returned as-is (ADR 0021).
func Open(ctx context.Context, cfg Config) (*Source, error) {
	src, err := open(ctx, cfg)
	if err != nil && cfg.SSLMode == SSLPreferred && isTLSCapabilityError(err) {
		retry := cfg
		retry.SSLMode = SSLDisabled
		return open(ctx, retry)
	}
	return src, err
}

func open(ctx context.Context, cfg Config) (*Source, error) {
	serverID := cfg.ServerID
	if serverID == 0 {
		var err error
		serverID, err = randomServerID()
		if err != nil {
			return nil, &Error{Reason: ReasonConnect, Err: err}
		}
	}

	src := &Source{}
	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID:  serverID,
		Flavor:    cfg.Flavor,
		Host:      cfg.Host,
		Port:      cfg.Port,
		User:      cfg.User,
		Password:  cfg.Password,
		TLSConfig: cfg.SSLMode.tlsConfig(),
		// Raw frames feed the same checked decode as file bytes. go-mysql's
		// own checksum verify does not mask the in-use flag of an open
		// binlog's Format Description; event_reader.go's check does.
		RawModeEnabled:   true,
		DisableRetrySync: true,
		Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
		// Option runs post-auth, pre-dump, on the dump connection itself:
		// the manifest probe rides it with no second connect (ADR 0021).
		Option: func(c *client.Conn) error {
			c.ReadTimeout = probeTimeout
			src.probeErr = src.probe(c, cfg.Start)
			c.ReadTimeout = 0
			src.serverVersion = c.GetServerVersion()
			return src.probeErr
		},
	})
	src.syncer = syncer

	var stream *replication.BinlogStreamer
	var err error
	if cfg.GTIDSet != nil {
		stream, err = syncer.StartSyncGTID(cfg.GTIDSet)
	} else {
		stream, err = syncer.StartSync(mysql.Position{Name: cfg.Start.File, Pos: cfg.Start.Pos})
	}
	if err != nil {
		syncer.Close()
		if src.probeErr != nil {
			return nil, src.probeErr
		}
		return nil, &Error{Reason: classifyOpenError(err), Err: err}
	}
	src.stream = stream
	return src, nil
}

// probe runs inside go-mysql's Option hook: post-auth, pre-dump, on the dump
// connection itself. It snapshots SHOW BINARY LOGS and validates a file:pos
// start against the snapshot.
func (s *Source) probe(c *client.Conn, start StartPos) error {
	manifest, err := showBinaryLogs(c)
	if err != nil {
		return err
	}
	if err := validateStart(start, manifest); err != nil {
		return err
	}
	s.manifest = manifest
	return nil
}

// randomServerID derives a nonzero pseudo-replica id; collisions are the
// server's error to report (ADR 0021).
func randomServerID() (uint32, error) {
	var buf [4]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return 0, fmt.Errorf("derive server id: %w", err)
	}
	id := uint32(buf[0]) | uint32(buf[1])<<8 | uint32(buf[2])<<16 | uint32(buf[3])<<24
	if id == 0 {
		id = 1
	}
	return id, nil
}

// NormalizeHost splits an optional host:port form. A port carried by --host
// wins over the default but not over an explicit --port value. Malformed
// halves are usage errors: an empty host (:3306) would silently dial
// localhost, and an empty port (db:, [::1]:) would reach the dialer as a
// broken hostname.
func NormalizeHost(host string, port uint16, portSet bool) (string, uint16, error) {
	h, p, err := net.SplitHostPort(host)
	if err != nil {
		// Bare hostname or bare IPv6; those keep the flag port.
		return host, port, nil
	}
	if h == "" {
		return "", 0, fmt.Errorf("empty host in --host %q", host)
	}
	if p == "" {
		return "", 0, fmt.Errorf("missing port in --host %q", host)
	}
	n, err := strconv.ParseUint(p, 10, 16)
	if err != nil || n == 0 {
		return "", 0, fmt.Errorf("invalid port in --host %q", host)
	}
	if !portSet {
		port = uint16(n)
	}
	return h, port, nil
}
