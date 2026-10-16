// Package clierr is the CLI error taxonomy (ADR 0008, #254).
// input: an error and the kind of failure it is.
// output: Kind, the exit code for that kind, and a one-line JSON form for stderr.
// pos: stateless; commands return typed errors, the process boundary maps them to exit codes.
// note: if this file changes, update this header, docs/reference/exit-codes.md and README.md.
package clierr

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Kind names a class of failure. The string is a stable JSON value; it does
// not change with --lang.
type Kind string

// Kinds and their exit codes. 1 is any failure that has no other kind.
const (
	KindError    Kind = "error"    // 1
	KindNoData   Kind = "no_data"  // 2
	KindUsage    Kind = "usage"    // 3
	KindInput    Kind = "input"    // 4
	KindCorrupt  Kind = "corrupt"  // 5
	KindRefused  Kind = "refused"  // 6
	KindConflict Kind = "conflict" // 7
	KindOutput   Kind = "output"   // 8
	// KindUnsupported is valid input that binlogviz does not analyze, such as
	// a STATEMENT or MIXED binlog with no ROW images (ADR 0009, #277).
	KindUnsupported Kind = "unsupported" // 9
)

var codes = map[Kind]int{
	KindError: 1, KindNoData: 2, KindUsage: 3, KindInput: 4,
	KindCorrupt: 5, KindRefused: 6, KindConflict: 7, KindOutput: 8,
	KindUnsupported: 9,
}

// Code is the process exit code for kind.
func Code(kind Kind) int {
	if code, ok := codes[kind]; ok {
		return code
	}
	return 1
}

// KindForCode maps an exit code back to its kind.
func KindForCode(code int) Kind {
	for kind, c := range codes {
		if c == code {
			return kind
		}
	}
	return KindError
}

// Error is a failure with a kind and optional machine-readable details.
type Error struct {
	Kind    Kind
	Err     error
	Details map[string]any
}

func (e *Error) Error() string {
	if e == nil || e.Err == nil {
		return ""
	}
	return e.Err.Error()
}

func (e *Error) Unwrap() error { return e.Err }

// Wrap gives err a kind. An error that already has a kind keeps it: the
// innermost classification is the most specific.
func Wrap(kind Kind, err error) error {
	if err == nil {
		return nil
	}
	var typed *Error
	if errors.As(err, &typed) {
		return err
	}
	return &Error{Kind: kind, Err: err}
}

// WithDetails is Wrap plus details for the JSON form.
func WithDetails(kind Kind, err error, details map[string]any) error {
	if err == nil {
		return nil
	}
	return &Error{Kind: kind, Err: err, Details: details}
}

// Reasonf is an error of kind with details.reason set, for failures that
// share a kind but need different fixes (for example invalid_plan,
// plan_changed, trust_boundary).
func Reasonf(kind Kind, reason, format string, args ...any) error {
	return &Error{Kind: kind, Err: fmt.Errorf(format, args...), Details: map[string]any{"reason": reason}}
}

// Errorf is Wrap(kind, fmt.Errorf(...)): a %w operand that already has a
// kind keeps it, so context added on the way up never hides the innermost,
// most specific classification.
func Errorf(kind Kind, format string, args ...any) error {
	return Wrap(kind, fmt.Errorf(format, args...))
}

// New is Wrap(kind, errors.New(msg)).
func New(kind Kind, msg string) error {
	return &Error{Kind: kind, Err: errors.New(msg)}
}

// Of returns the kind of err, or KindError when it has none.
func Of(err error) (Kind, map[string]any) {
	var typed *Error
	if errors.As(err, &typed) && typed != nil {
		return typed.Kind, typed.Details
	}
	return KindError, nil
}

// JSONLine is the one-line stderr form:
// {"error":{"kind":"usage","code":3,"message":"...","details":{...}}}
func JSONLine(kind Kind, code int, message string, details map[string]any) []byte {
	body := struct {
		Error struct {
			Kind    Kind           `json:"kind"`
			Code    int            `json:"code"`
			Message string         `json:"message"`
			Details map[string]any `json:"details,omitempty"`
		} `json:"error"`
	}{}
	body.Error.Kind, body.Error.Code, body.Error.Message, body.Error.Details = kind, code, message, details
	out, err := json.Marshal(body)
	if err != nil {
		out, _ = json.Marshal(map[string]any{"error": map[string]any{"kind": kind, "code": code, "message": message}})
	}
	return append(out, '\n')
}
