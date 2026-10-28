// Package binlogviz maps analyze's failure surface: parse-error wrapping,
// cause cleanup, localized operator errors, and the no-workload outcome
// guards that decide between a report, exit 2, and an Error line.
// input: analyzer/binlog errors and the finished AnalysisResult.
// output: the error to print, or nil when the outcome is a valid report.
// pos: CLI orchestration helper for analyze.go.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
	"binlogviz/internal/clierr"
	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

func wrapParseError(err error) error {
	if err == nil {
		return nil
	}
	// An open BEGIN or unclassified QUERY is a readable binlog that cannot
	// be analyzed safely: exit 1, as ADR 0001 documents, not corrupt.
	if localized := localizedOperatorGroupError(err); localized != nil {
		return clierr.Wrap(clierr.KindError, localized)
	}
	// The matched-GTID interval ceiling (#379) is a resource limit hit
	// while consuming a readable binlog: exit 10, not corrupt.
	if resource := analyzerResourceError(err); resource != nil {
		return resource
	}
	var corrupt *binlog.CorruptEventError
	if errors.As(err, &corrupt) {
		msg := i18n.Tf("error.corruptEvent", map[string]any{"Path": corrupt.Path, "Offset": corrupt.Offset, "Cause": corrupt.Cause})
		return clierr.WithDetails(clierr.KindCorrupt, clierr.Errorf(clierr.KindCorrupt, "%s", msg), map[string]any{"path": corrupt.Path, "offset": corrupt.Offset, "cause": corrupt.Cause})
	}
	if mapped := mapBinlogParseError(err.Error()); mapped != "" {
		return clierr.Errorf(clierr.KindCorrupt, "%s", mapped)
	}
	if msg, details := describeEventError(err.Error()); msg != "" {
		return clierr.WithDetails(clierr.KindCorrupt, clierr.Errorf(clierr.KindCorrupt, "%s", i18n.Tf("error.parseError", map[string]any{"Error": msg})), details)
	}
	return clierr.Errorf(clierr.KindCorrupt, "%s", i18n.Tf("error.parseError", map[string]any{"Error": err.Error()}))
}

var (
	eventDumpRe = regexp.MustCompile(`&replication\.EventHeader\{[^}]*\}`)
	eventTypeRe = regexp.MustCompile(`EventType:0x([0-9a-fA-F]+)`)
	eventPosRe  = regexp.MustCompile(`LogPos:0x([0-9a-fA-F]+)`)
	eventSizeRe = regexp.MustCompile(`EventSize:0x([0-9a-fA-F]+)`)
	eventErrRe  = regexp.MustCompile(`Err: (.*)$`)
)

// cleanEventCause keeps the parser's reason and drops what follows it: raw
// event bytes ("data ...") and Go struct dumps (&replication.TableMapEvent{...}).
func cleanEventCause(cause string) string {
	cut := len(cause)
	for _, mark := range []string{", data ", " data \"", "data \"", "&replication.", "%!"} {
		if i := strings.Index(cause, mark); i >= 0 && i < cut {
			cut = i
		}
	}
	cause = strings.TrimRight(strings.TrimSpace(cause[:cut]), ",:;")
	if len(cause) > 200 {
		cause = cause[:200] + "..."
	}
	if cause == "" {
		cause = "undecodable event"
	}
	return cause
}

// describeEventError replaces go-mysql's %#v event dump (header struct plus
// raw Data bytes) with the event type, end position, size and cause (#254).
func describeEventError(msg string) (string, map[string]any) {
	header := eventDumpRe.FindString(msg)
	if header == "" {
		return "", nil
	}
	hex := func(re *regexp.Regexp) (int64, bool) {
		m := re.FindStringSubmatch(header)
		if m == nil {
			return 0, false
		}
		v, err := strconv.ParseInt(m[1], 16, 64)
		return v, err == nil
	}
	details := map[string]any{}
	parts := []string{"corrupt event"}
	if v, ok := hex(eventTypeRe); ok {
		details["event_type"] = v
		parts = append(parts, fmt.Sprintf("type %d", v))
	}
	if v, ok := hex(eventPosRe); ok {
		details["end_position"] = v
		parts = append(parts, fmt.Sprintf("ending at position %d", v))
	}
	if v, ok := hex(eventSizeRe); ok {
		details["event_size"] = v
		parts = append(parts, fmt.Sprintf("size %d", v))
	}
	out := strings.Join(parts, ", ")
	if m := eventErrRe.FindStringSubmatch(msg); m != nil {
		cause := cleanEventCause(m[1])
		details["cause"] = cause
		out += ": " + cause
	}
	return out, details
}

func noteSkippedIgnoredQuery(stream commandAnalyzer, raw binlog.RawEvent) {
	if !binlog.IsIgnoredQueryEvent(raw) {
		return
	}
	notifier, ok := stream.(interface{ NoteIgnoredQuery() })
	if !ok {
		return
	}
	notifier.NoteIgnoredQuery()
}

// analyzerResourceError maps the matched-GTID interval ceiling (#379) to
// exit 10, the resource kind, instead of a generic analysis error.
func analyzerResourceError(err error) error {
	var limit *analyzer.MatchedGTIDLimitError
	if !errors.As(err, &limit) {
		return nil
	}
	key := "error.matchedGTIDIntervals"
	switch limit.Reason {
	case analyzer.MatchedGTIDLimitRepeat:
		// A sequence number repeats after the per-server form was dropped
		// for every domain (fail-safe).
		key = "error.matchedGTIDIntervalsDropped"
	case analyzer.MatchedGTIDLimitPerServer:
		key = "error.matchedGTIDIntervalsPerServer"
		if !limit.SharedSmaller {
			key = "error.matchedGTIDIntervalsPerServerAtLeast"
		}
	case analyzer.MatchedGTIDLimitFormDropped:
		key = "error.matchedGTIDIntervalsFormDropped"
	}
	msg := i18n.Tf(key, map[string]any{"Limit": limit.Limit, "Intervals": limit.Intervals})
	return clierr.WithDetails(clierr.KindResource, &localizedCause{msg: msg, cause: limit}, map[string]any{
		"reason":    "matched_gtid_intervals",
		"limit":     limit.Limit,
		"intervals": limit.Intervals,
	})
}

// localizedCause shows a localized message and keeps the analyzer error in
// the chain, so a second mapping (the parse-error wrapper) still finds it.
type localizedCause struct {
	msg   string
	cause error
}

func (e *localizedCause) Error() string { return e.msg }
func (e *localizedCause) Unwrap() error { return e.cause }

func operatorGroupError(err error) bool {
	var unclassified *analyzer.UnclassifiedQueryError
	var openBegin *analyzer.OpenBeginError
	var ignored *analyzer.IgnoredOnlyGroupError
	return errors.As(err, &unclassified) || errors.As(err, &openBegin) || errors.As(err, &ignored)
}

func localizedOperatorGroupError(err error) error {
	if localized := localizedUnclassifiedQueryError(err); localized != nil {
		return localized
	}
	var openBegin *analyzer.OpenBeginError
	if errors.As(err, &openBegin) && openBegin != nil {
		return localizedOpenBeginError(openBegin)
	}
	var ignored *analyzer.IgnoredOnlyGroupError
	if errors.As(err, &ignored) {
		return clierr.Errorf(clierr.KindError, "%s", i18n.T("error.ignoredOnlyGroup"))
	}
	return nil
}

func localizedOpenBeginError(openBegin *analyzer.OpenBeginError) error {
	base := i18n.T("error.openBeginWithoutClose")
	if openBegin.SavepointRollback {
		base = i18n.T("error.openBeginSavepoint")
	}
	if openBegin.Rows <= 0 {
		return clierr.Errorf(clierr.KindError, "%s", base)
	}
	tables := openBegin.Tables
	if tables == "" {
		tables = "-"
	}
	location := openBegin.Location
	if location == "" {
		location = i18n.T("time.notAvailable")
	}
	detail := i18n.Tf("error.openBeginDMLDetail", map[string]any{
		"Duration": openBegin.Duration.Truncate(time.Millisecond).String(),
		"Rows":     openBegin.Rows,
		"Tables":   tables,
		"File":     location,
	})
	return clierr.Errorf(clierr.KindError, "%s; %s", base, detail)
}

func localizedUnclassifiedQueryError(err error) error {
	var unclassified *analyzer.UnclassifiedQueryError
	if !errors.As(err, &unclassified) || unclassified == nil {
		return nil
	}
	return clierr.Errorf(clierr.KindError, "%s", i18n.Tf("error.unclassifiedQuery", map[string]any{"Prefix": unclassified.Prefix}))
}

func mapBinlogParseError(msg string) string {
	if msg == "" {
		return ""
	}
	lower := strings.ToLower(msg)
	switch {
	case strings.Contains(lower, "normalize error") || strings.Contains(lower, "analysis consume error"):
		return ""
	case strings.Contains(lower, "fe'bin"):
		return i18n.T("error.corruptBinlogMagic")
	case strings.Contains(lower, "get event"), strings.Contains(lower, "unread bytes"):
		return i18n.Tf("error.truncatedBinlog", map[string]any{"Detail": msg})
	case msg == "EOF" || strings.HasSuffix(msg, ": EOF") || lower == "eof":
		return i18n.T("error.emptyBinlog")
	default:
		return ""
	}
}

func applyAnalyzeOutcomeGuards(paths []string, opts analyzer.Options, result *model.AnalysisResult, rawEvents int, observer binlog.FormatObserver) error {
	if err := rejectEmptyOrIncompleteBinlog(paths, rawEvents); err != nil {
		return err
	}
	if result != nil && opts.HasDMLFilter() && result.Summary.TotalRows == 0 {
		return &ExitError{Code: 2, Msg: i18n.T("error.dmlFilterMatchedNothing")}
	}
	if result != nil && opts.HasObjectFilters() && filterMatchedNoWorkload(result) {
		return &ExitError{Code: 2, Msg: i18n.T("error.filterMatchedNothing")}
	}
	if isAdminOnlyNoData(result, observer) {
		return &ExitError{Code: 2, Msg: i18n.T("error.noAnalyzableEvents")}
	}
	if result == nil || result.Summary.TotalEvents > 0 {
		return nil
	}
	if opts.HasSelectionFilters() {
		return &ExitError{Code: 2, Msg: i18n.T("progress.windowMatchedZero")}
	}
	if rawEvents > 0 && observer.QueryDMLEvents == 0 && observer.RowImageEvents == 0 {
		return &ExitError{Code: 2, Msg: i18n.T("error.noAnalyzableEvents")}
	}
	return nil
}

// filterMatchedNoWorkload is exit 2: the filter kept no row changes and no
// view, event, routine, or trigger DDL. A table that only has DDL still
// counts as no workload. A matching non-table object does not.
func filterMatchedNoWorkload(result *model.AnalysisResult) bool {
	if result == nil || result.Summary.TotalRows > 0 {
		return false
	}
	for _, ev := range result.Diagnostics.DDLEvents {
		switch ev.Object {
		case "view", "event", "routine", "trigger":
			return false
		}
	}
	return true
}

func isAdminOnlyNoData(result *model.AnalysisResult, observer binlog.FormatObserver) bool {
	if result == nil {
		return false
	}
	if result.Summary.TotalTransactions > 0 || result.Summary.TotalRows > 0 {
		return false
	}
	if observer.RowImageEvents > 0 || observer.QueryDMLEvents > 0 {
		return false
	}
	if len(result.Diagnostics.DDLEvents) > 0 {
		return false
	}
	return observer.AdminQueryEvents > 0
}

func rejectEmptyOrIncompleteBinlog(paths []string, rawEvents int) error {
	if rawEvents > 0 || !anyNonEmptyFile(paths) {
		return nil
	}
	return clierr.Errorf(clierr.KindCorrupt, "%s", i18n.T("error.noFormatDescription"))
}

func anyNonEmptyFile(paths []string) bool {
	for _, path := range paths {
		info, err := os.Stat(path)
		if err == nil && info.Size() > 0 {
			return true
		}
	}
	return false
}
