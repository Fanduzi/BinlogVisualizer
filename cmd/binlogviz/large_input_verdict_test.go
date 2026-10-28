package binlogviz

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

// largeInputInput is the child's record of the generated files, printed as
// one "large-input input {json}" line.
type largeInputInput struct {
	Fault      string `json:"fault,omitempty"`
	LargeBytes int64  `json:"large_bytes"`
	SmallBytes int64  `json:"small_bytes"`
	Txns       [2]int `json:"txns"`
	Minutes    [2]int `json:"minutes"`
	Tables     int    `json:"tables"`
}

const largeInputInputPrefix = "large-input input "

func (in largeInputInput) line() string {
	b, err := json.Marshal(in)
	if err != nil {
		panic(err)
	}
	return largeInputInputPrefix + string(b)
}

// largeInputOutcome is the parent's reading of the child's output.
type largeInputOutcome struct {
	input    *largeInputInput
	verdicts []largeInputVerdict // in the order printed: analyze, flashback
	// failing is the first failed check, when the failure is a gate.
	failing *gateFailure
	err     error // nil: the gate passed
}

// largeInputPaths are the verdicts a passing child must print, in order.
var largeInputPaths = []string{"analyze", "flashback"}

// evaluateLargeInputChild judges the child from its records alone: one input
// line and one verdict line per path. It does not count measurement lines,
// so a retried pair, which prints its scale= lines twice, is judged on the
// verdict like any other. A passing child must exit 0, print both verdicts
// with Pass set, no failures, one or two attempts, no gauge over its cap,
// a large file at or above the floor, and go test's PASS line.
func evaluateLargeInputChild(out string, runErr error, timedOut bool) largeInputOutcome {
	var o largeInputOutcome
	var parseErrs []string
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, largeInputVerdictPrefix):
			var v largeInputVerdict
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, largeInputVerdictPrefix)), &v); err != nil {
				parseErrs = append(parseErrs, fmt.Sprintf("verdict line %q: %v", line, err))
				continue
			}
			o.verdicts = append(o.verdicts, v)
		case strings.HasPrefix(line, largeInputInputPrefix):
			var in largeInputInput
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, largeInputInputPrefix)), &in); err != nil {
				parseErrs = append(parseErrs, fmt.Sprintf("input line %q: %v", line, err))
				continue
			}
			if o.input != nil {
				parseErrs = append(parseErrs, "more than one input line")
			}
			o.input = &in
		}
	}
	// The first failing verdict names the check, pass or exit status aside.
	for _, v := range o.verdicts {
		if !v.Pass && len(v.Failures) > 0 {
			f := v.Failures[0]
			o.failing = &f
			o.err = v.err()
			return o
		}
	}
	switch {
	case len(parseErrs) > 0:
		o.err = errors.New(strings.Join(parseErrs, "; "))
	case runErr != nil:
		if timedOut {
			o.err = fmt.Errorf("child ran out of time (%v): %s", runErr, lastFailureLine(out))
		} else {
			o.err = fmt.Errorf("child exited with %v before a failing verdict: %s", runErr, lastFailureLine(out))
		}
	case o.input == nil:
		o.err = errors.New("child printed no input record")
	case o.input.LargeBytes < largeInputMinBytes:
		o.err = fmt.Errorf("large file is %d bytes, want at least %d", o.input.LargeBytes, largeInputMinBytes)
	case len(o.verdicts) != len(largeInputPaths):
		o.err = fmt.Errorf("child printed %d verdicts, want %d (%s)", len(o.verdicts), len(largeInputPaths), strings.Join(largeInputPaths, ", "))
	case !strings.Contains(out, "--- PASS: TestLargeInputBaseline"):
		o.err = errors.New("child did not print --- PASS: TestLargeInputBaseline")
	}
	if o.err != nil {
		return o
	}
	for i, v := range o.verdicts {
		switch {
		case v.Path != largeInputPaths[i]:
			o.err = fmt.Errorf("verdict %d is for %q, want %q", i+1, v.Path, largeInputPaths[i])
		case !v.Pass:
			o.err = fmt.Errorf("%s verdict is not a pass and names no failed check", v.Path)
		case len(v.Failures) > 0:
			o.err = fmt.Errorf("%s verdict says pass but names failed checks: %s", v.Path, joinFailures(v.Failures))
		case v.Attempts < 1 || v.Attempts > 2:
			o.err = fmt.Errorf("%s took %d attempts, want 1 or 2", v.Path, v.Attempts)
		case v.Attempts == 1 && len(v.Retried) > 0:
			o.err = fmt.Errorf("%s took one attempt but lists retried failures: %s", v.Path, joinFailures(v.Retried))
		case v.Attempts == 2 && len(v.Retried) == 0:
			o.err = fmt.Errorf("%s took two attempts but lists no retried failure", v.Path)
		case v.Attempts == 2 && !allCPU(v.Retried):
			o.err = fmt.Errorf("%s was retried after a non-CPU failure: %s", v.Path, joinFailures(v.Retried))
		case len(v.Gauges) == 0:
			o.err = fmt.Errorf("%s verdict has no measurements", v.Path)
		case !gaugesMatchPath(v):
			o.err = fmt.Errorf("%s verdict gauges %v do not cover the expected measurements", v.Path, gaugeNames(v.Gauges))
		}
		if o.err != nil {
			return o
		}
		// A pass must agree with its own numbers.
		for _, g := range v.Gauges {
			if g.over() {
				f := gateFailure{Check: g.Name, Value: g.Value, Cap: g.Cap,
					Msg: fmt.Sprintf("%s verdict says pass but %s=%s is over its cap %s", v.Path, g.Name, fmtGauge(g.Value), fmtGauge(g.Cap))}
				o.failing = &f
				o.err = errors.New(f.Msg)
				return o
			}
		}
	}
	return o
}

// lastFailureLine names the child's cause of death: the first panic or
// fatal error line when one exists (a stack's last frame names a helper,
// not the cause), else the last line that names a test file, which is
// where t.Fatal puts its message. The heading is one line, so the cause
// is cut rather than a garbled verdict blob.
func lastFailureLine(out string) string {
	last := ""
	for _, line := range strings.Split(out, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "panic:") || strings.HasPrefix(trimmed, "fatal error:") {
			return truncateLine(trimmed)
		}
		if strings.Contains(line, "_test.go:") {
			last = trimmed
		}
	}
	if last == "" {
		return "no failure message"
	}
	return truncateLine(last)
}

// largeInputGauges are the gauge names a verdict may carry per path; the
// spill gauges are conditional on /proc/self/fd, the rest are required.
var largeInputCoreGauges = []string{"cpu_ratio", "cpu_ns_per_event", "peak_heap", "alloc_per_event", "alloc_ratio", "extra_heap"}

var largeInputGauges = map[string]map[string]bool{
	"analyze":   {"cpu_ratio": true, "cpu_ns_per_event": true, "peak_heap": true, "alloc_per_event": true, "alloc_ratio": true, "extra_heap": true, "spill_per_txn": true},
	"flashback": {"cpu_ratio": true, "cpu_ns_per_event": true, "peak_heap": true, "alloc_per_event": true, "alloc_ratio": true, "extra_heap": true, "sql_ratio": true, "spill_per_byte": true},
}

// gaugesMatchPath reports that every gauge a verdict carries is a known
// name for its path and that every required gauge is present: a verdict
// missing a gauge stops gating that measurement.
func gaugesMatchPath(v largeInputVerdict) bool {
	allowed, ok := largeInputGauges[v.Path]
	if !ok {
		return false
	}
	seen := map[string]bool{}
	for _, g := range v.Gauges {
		if !allowed[g.Name] {
			return false
		}
		seen[g.Name] = true
	}
	for _, name := range largeInputCoreGauges {
		if !seen[name] {
			return false
		}
	}
	return true
}

func gaugeNames(gs []gauge) []string {
	names := make([]string, len(gs))
	for i, g := range gs {
		names[i] = g.Name
	}
	return names
}

func truncateLine(s string) string {
	const max = 160
	if len(s) > max {
		return s[:max] + "..."
	}
	return s
}

// summary is the markdown for the CI step summary and the test log: a
// heading with the parent's verdict, then one line per path. On a failure
// the heading is the child's failure line, the text t.Fatal printed.
// alert is set on a failure, a retry, or a value within 1.5x of its cap.
func (o largeInputOutcome) summary() (body string, alert bool) {
	var lines []string
	retried := false
	for _, v := range o.verdicts {
		state := "pass"
		if !v.Pass {
			state = "FAIL"
		}
		line := fmt.Sprintf("%s attempts=%d %s:%s", v.Path, v.Attempts, state, strings.TrimPrefix(summaryLine("", v.Gauges), "large-input summary "))
		lines = append(lines, line)
		for _, g := range v.Gauges {
			alert = alert || g.near()
		}
		if len(v.Retried) > 0 {
			retried = true
			lines = append(lines, fmt.Sprintf("%s attempt 1 (retried): %s", v.Path, joinFailures(v.Retried)))
		}
	}
	if o.input != nil {
		lines = append([]string{fmt.Sprintf("input: %d and %d bytes, %d and %d transactions, %d and %d minutes, %d tables",
			o.input.SmallBytes, o.input.LargeBytes, o.input.Txns[0], o.input.Txns[1], o.input.Minutes[0], o.input.Minutes[1], o.input.Tables)}, lines...)
		if o.input.Fault != "" {
			lines = append([]string{"fault: " + o.input.Fault}, lines...)
		}
	}
	var head string
	switch {
	case o.err != nil:
		head = "### ❌ TestLargeInputBaseline FAIL: " + firstLine(o.err.Error())
	case retried || alert:
		head = "### ⚠️ TestLargeInputBaseline pass, retried or near a cap"
	default:
		head = "### ✅ TestLargeInputBaseline pass"
	}
	alert = alert || retried || o.err != nil
	var b strings.Builder
	b.WriteString(head + "\n\n")
	if o.err != nil {
		b.WriteString("Parent verdict: " + o.err.Error() + "\n\n")
	}
	b.WriteString("value/cap(headroom). over_cap: past the cap; near_cap: within 1.5x (tight deterministic gates are never near_cap).\n\n```\n")
	b.WriteString(strings.Join(lines, "\n"))
	b.WriteString("\n```\n")
	return b.String(), alert
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// appendStepSummary appends body to $GITHUB_STEP_SUMMARY. In CI's plain
// `go test ./...` a passing package's output is discarded, t.Log and stderr
// alike; GitHub Actions gives every step this file and shows its markdown
// on the run's summary page. An empty path (outside Actions) is not an
// error. The caller fails the test on an error, so a broken summary is
// visible.
func appendStepSummary(path, body string) error {
	if path == "" {
		return nil
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(body); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
