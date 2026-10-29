// Package binlogviz wires analyze's remote replication input: flag validation, connect, and parser injection.
// input: --host/--user/--start-file/--start-gtid and friends plus BINLOGVIZ_MYSQL_PASSWORD.
// output: a validated remote.Config, the opened Source, the synthetic remote:// origin path, the stream boundary, and manifest file coverage.
// pos: command-layer remote mode. clierr kinds and i18n live here; protocol detail stays in internal/remote.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"context"
	"errors"
	"fmt"
	"os"
	osuser "os/user"
	"strings"

	"github.com/spf13/cobra"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
	"binlogviz/internal/clierr"
	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
	"binlogviz/internal/remote"
)

// remoteFlagNames is the flag surface that selects remote input mode (ADR
// 0021): any of them being set asks for a replication dump, and --host must
// be among them.
var remoteFlagNames = []string{
	"host", "port", "user", "password", "server-id",
	"start-file", "start-pos", "start-gtid", "ssl-mode",
}

// remoteSource is what analyze needs from an opened dump: the frame stream
// plus the connect-time manifest. remote.Source satisfies it; tests fake it.
type remoteSource interface {
	Next(context.Context) ([]byte, error)
	Manifest() []remote.LogFile
	Close()
}

// remoteOpenSource is the connect seam; tests replace it so no unit test
// needs a live server.
var remoteOpenSource = func(ctx context.Context, cfg remote.Config) (remoteSource, error) {
	return remote.Open(ctx, cfg)
}

// analyzeRemoteRequested reports whether any remote flag was passed.
func analyzeRemoteRequested(cmd *cobra.Command) bool {
	for _, name := range remoteFlagNames {
		if cmd.Flags().Changed(name) {
			return true
		}
	}
	return false
}

// validateRemoteArgs is the Args-stage gate: remote is a fourth input mode
// and excludes the other three plus the local position selectors (ADR 0021).
func validateRemoteArgs(cmd *cobra.Command, args []string, opts *analyzeOptions) error {
	if !analyzeRemoteRequested(cmd) {
		return nil
	}
	if len(args) > 0 || opts.fromDir != "" || opts.prefix != "" || opts.indexFile != "" {
		return clierr.Errorf(clierr.KindUsage, "%s", i18n.T("error.remoteCombineArgs"))
	}
	if cmd.Flags().Changed("start-position") || cmd.Flags().Changed("stop-position") {
		return clierr.Errorf(clierr.KindUsage, "%s", i18n.T("error.remoteCombineArgs"))
	}
	if strings.TrimSpace(opts.remoteHost) == "" {
		return clierr.Errorf(clierr.KindUsage, "%s", i18n.T("error.remoteRequiresHost"))
	}
	return nil
}

// remoteRunConfig is the validated connection request plus its display label.
type remoteRunConfig struct {
	cfg    remote.Config
	origin string // remote://host:port, the synthetic path prefix for events
}

// buildRemoteRunConfig validates the remote flag combination into a Config.
// Every failure here is a usage error — nothing has touched the network yet.
func buildRemoteRunConfig(cmd *cobra.Command, opts *analyzeOptions) (remoteRunConfig, error) {
	out := remoteRunConfig{}
	host := strings.TrimSpace(opts.remoteHost)
	host, port, err := remote.NormalizeHost(host, opts.remotePort, cmd.Flags().Changed("port"))
	if err != nil {
		return out, clierr.Errorf(clierr.KindUsage, "%s", err.Error())
	}
	if port == 0 {
		return out, clierr.Errorf(clierr.KindUsage, "invalid --port 0")
	}
	if cmd.Flags().Changed("start-pos") && !cmd.Flags().Changed("start-file") {
		return out, clierr.Errorf(clierr.KindUsage, "%s", i18n.T("error.remotePosRequiresFile"))
	}
	if opts.remoteStartPos < uint32(binlogHeaderPosition) {
		return out, clierr.Errorf(clierr.KindUsage, "%s", i18n.Tf("error.remotePosBeforeHeader", map[string]any{"Pos": opts.remoteStartPos}))
	}
	if cmd.Flags().Changed("start-gtid") && opts.remoteStartGTID == "" {
		return out, clierr.Errorf(clierr.KindUsage, "invalid --start-gtid: empty value")
	}
	if opts.remoteStartGTID != "" && (opts.remoteStartFile != "" || cmd.Flags().Changed("start-pos")) {
		return out, clierr.Errorf(clierr.KindUsage, "%s", i18n.T("error.remoteGTIDConflict"))
	}
	sslMode, err := remote.ParseSSLMode(opts.remoteSSLMode)
	if err != nil {
		return out, clierr.Errorf(clierr.KindUsage, "invalid --ssl-mode %q (allowed: disabled, preferred, required)", opts.remoteSSLMode)
	}
	user := opts.remoteUser
	if user == "" {
		user = defaultRemoteUser()
	}
	if user == "" {
		return out, clierr.Errorf(clierr.KindUsage, "%s", i18n.T("error.remoteUserMissing"))
	}
	// Explicit beats ambient: the flag wins over the environment variable,
	// which is the documented way to keep the secret out of argv.
	password := os.Getenv("BINLOGVIZ_MYSQL_PASSWORD")
	if cmd.Flags().Changed("password") {
		password = opts.remotePassword
	}

	out.cfg = remote.Config{
		Host:     host,
		Port:     port,
		User:     user,
		Password: password,
		ServerID: opts.remoteServerID,
		SSLMode:  sslMode,
	}
	if opts.remoteStartGTID != "" {
		set, flavor, err := remote.ParseStartGTID(opts.remoteStartGTID)
		if err != nil {
			return out, clierr.Errorf(clierr.KindUsage, "invalid --start-gtid: %s", err.Error())
		}
		out.cfg.GTIDSet = set
		out.cfg.Flavor = flavor
	} else {
		out.cfg.Start = remote.StartPos{File: opts.remoteStartFile, Pos: opts.remoteStartPos}
	}
	out.origin = fmt.Sprintf("remote://%s:%d", host, port)
	return out, nil
}

// defaultRemoteUser follows the mysql client convention: the account name is
// the OS login when --user is absent. Empty means we could not determine one.
func defaultRemoteUser() string {
	if current, err := osuser.Current(); err == nil && current != nil {
		name := current.Username
		if i := strings.LastIndexByte(name, '\\'); i >= 0 {
			name = name[i+1:]
		}
		return name
	}
	for _, key := range []string{"USER", "LOGNAME", "USERNAME"} {
		if name := os.Getenv(key); name != "" {
			return name
		}
	}
	return ""
}

// remoteClierr maps a remote.Error to the shared taxonomy (ADR 0021): a
// stale --start-pos is usage; connect/auth/manifest/stream failures are
// input, all carrying details.reason for scripts.
func remoteClierr(err error) error {
	var rerr *remote.Error
	if !errors.As(err, &rerr) {
		return clierr.Wrap(clierr.KindInput, err)
	}
	kind := clierr.KindInput
	if rerr.Reason == remote.ReasonStartPos {
		kind = clierr.KindUsage
	}
	return clierr.WithDetails(kind, clierr.Errorf(kind, "%s", rerr.Error()), map[string]any{"reason": string(rerr.Reason)})
}

// remoteBoundary is the connect-time end of a bounded read: the last row of
// the SHOW BINARY LOGS manifest (ADR 0021). An empty manifest yields a zero
// boundary, which stops at the first rotate past nothing — it cannot happen
// because showBinaryLogs refuses an empty result, so this stays defensive.
func remoteBoundary(manifest []remote.LogFile) binlog.RemoteBoundary {
	if len(manifest) == 0 {
		return binlog.RemoteBoundary{}
	}
	last := manifest[len(manifest)-1]
	return binlog.RemoteBoundary{File: last.Name, Size: last.Size}
}

// remoteFileCoverage reports the manifest as coverage: files before the
// --start-file are skipped, the rest selected. GTID start selects all — the
// server decides where the set begins.
func remoteFileCoverage(origin string, manifest []remote.LogFile, startFile string, gtidStart bool) model.FileCoverage {
	var selected, skipped []model.FileCoverageItem
	before := startFile != "" && !gtidStart
	for _, file := range manifest {
		item := model.FileCoverageItem{BinlogPath: origin + "/" + file.Name, Size: file.Size}
		if before && file.Name == startFile {
			before = false
		}
		if before {
			item.Reason = "before remote start file"
			skipped = append(skipped, item)
		} else {
			item.Reason = "selected"
			selected = append(selected, item)
		}
	}
	return analyzer.BuildFileCoverage(selected, skipped)
}

// openRemoteInput connects, validates the start, and returns the source plus
// the parser boundary derived from its manifest.
func openRemoteInput(ctx context.Context, runCfg remoteRunConfig) (remoteSource, binlog.RemoteBoundary, error) {
	src, err := remoteOpenSource(ctx, runCfg.cfg)
	if err != nil {
		return nil, binlog.RemoteBoundary{}, remoteClierr(err)
	}
	return src, remoteBoundary(src.Manifest()), nil
}
