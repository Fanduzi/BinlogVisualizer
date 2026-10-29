// Package remote classifies connect/probe/stream failures into stable reasons.
// input: go-mysql client and streamer errors.
// output: *Error values with a machine-readable Reason; cmd maps them to clierr kinds.
// pos: error taxonomy leaf for the remote package.
// note: if this file changes, update this header and README.md.
package remote

import (
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// Reason names the remote failure class; cmd turns it into details.reason and
// picks the clierr kind. The strings are stable JSON values.
type Reason string

const (
	ReasonConnect   Reason = "connect"
	ReasonTLS       Reason = "tls"
	ReasonAuth      Reason = "auth"
	ReasonManifest  Reason = "manifest"
	ReasonPrivilege Reason = "privilege"
	ReasonStartFile Reason = "start_file"
	ReasonStartPos  Reason = "start_pos"
	ReasonStream    Reason = "stream"
)

// Error is a remote failure with a stable reason and the underlying cause.
// The cause never carries the password: go-mysql does not include it.
type Error struct {
	Reason Reason
	Err    error
}

func (e *Error) Error() string { return e.Err.Error() }
func (e *Error) Unwrap() error { return e.Err }

// isTLSCapabilityError matches go-mysql's handshake refusal when the server
// has no CLIENT_SSL capability — the only error safe to retry plain for
// ssl-mode preferred, because it fires before credentials are sent.
func isTLSCapabilityError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "does not support TLS")
}

// classifyOpenError maps a StartSync failure to a reason. MySQL error codes
// in the text come straight from the server's error packet; a code we do not
// list is a generic connect failure.
func classifyOpenError(err error) Reason {
	if err == nil {
		return ReasonConnect
	}
	if isTLSCapabilityError(err) {
		return ReasonTLS
	}
	text := err.Error()
	switch mysql.ErrorCode(text) {
	case 1045, 1044, 1129, 1130, 1251, 1698:
		return ReasonAuth
	case 1227, 1370, 1412:
		return ReasonPrivilege
	case 1236:
		// ER_MASTER_FATAL_ERROR_READING_BINLOG: the dump start names a
		// purged binlog file or GTIDs the server no longer retains — a
		// start-point failure, not a connect one.
		return ReasonStartFile
	}
	if strings.Contains(text, "Access denied") {
		return ReasonAuth
	}
	return ReasonConnect
}
