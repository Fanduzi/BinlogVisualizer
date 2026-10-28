// Package binlogviz parses analyze's --start/--end time flags.
// input: RFC3339 or YYYY-MM-DD HH:MM:SS flag text and a --tz location.
// output: the resolved instants, or a usage error for an offset past 14:00,
// offset minutes outside 00/30/45, or a local-time gap/overlap in the zoneless form.
// pos: CLI orchestration helper for analyze.go.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"os"
	"strings"
	"time"

	"binlogviz/internal/clierr"
	"binlogviz/internal/i18n"
)

const (
	analyzeLocalTimeLayout = "2006-01-02 15:04:05"
	analyzeLocalFracLayout = "2006-01-02 15:04:05.000000"
)

// parseTimeRange parses start and end for every command that takes them.
// loc is the --tz zone. Nil means the flag was not set and a zoneless value
// uses the machine zone. time.UTC (--tz UTC) and any other location, including
// Etc/UTC, are the zone of a zoneless value. An explicit offset or a trailing
// " UTC" is that instant either way.
func parseTimeRange(startStr, endStr string, loc *time.Location) (time.Time, time.Time, error) {
	startTime, err := parseAnalyzeTimeInLocation(startStr, loc)
	if err != nil {
		return time.Time{}, time.Time{}, clierr.Errorf(clierr.KindUsage, "%s", i18n.Tf("error.invalidStartTime", map[string]any{"Error": err.Error()}))
	}
	endTime, err := parseAnalyzeTimeInLocation(endStr, loc)
	if err != nil {
		return time.Time{}, time.Time{}, clierr.Errorf(clierr.KindUsage, "%s", i18n.Tf("error.invalidEndTime", map[string]any{"Error": err.Error()}))
	}
	if !startTime.IsZero() && !endTime.IsZero() && endTime.Before(startTime) {
		return time.Time{}, time.Time{}, clierr.Errorf(clierr.KindUsage, "%s", i18n.T("error.endTimeBeforeStart"))
	}
	return startTime, endTime, nil
}

func parseAnalyzeTimeInLocation(value string, loc *time.Location) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, nil
	}
	zone := loc
	if zone == nil {
		zone = time.Local
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return rejectUTCOffset(value, parsed)
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return rejectUTCOffset(value, parsed)
	}
	if bare, ok := strings.CutSuffix(value, " UTC"); ok {
		for _, layout := range []string{analyzeLocalFracLayout, analyzeLocalTimeLayout} {
			if parsed, err := time.ParseInLocation(layout, bare, time.UTC); err == nil {
				return parsed, nil
			}
		}
	}
	for _, layout := range []string{analyzeLocalFracLayout + " -07:00", analyzeLocalTimeLayout + " -07:00"} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return rejectUTCOffset(value, parsed)
		}
	}
	if parsed, err := time.ParseInLocation(analyzeLocalFracLayout, value, zone); err == nil {
		return rejectZoneCivil(value, analyzeLocalFracLayout, parsed, zone)
	}
	// The last attempt's *time.ParseError is the usage detail. It is not an
	// untyped fmt.Errorf (ADR 0009).
	parsed, err := time.ParseInLocation(analyzeLocalTimeLayout, value, zone)
	if err != nil {
		return time.Time{}, err
	}
	return rejectZoneCivil(value, analyzeLocalTimeLayout, parsed, zone)
}

// rejectUTCOffset refuses an offset that is not a real UTC offset.
// |offset| > 14:00 (so +24:00 and +14:30) and a minutes part other than
// 00, 30, or 45 are usage errors. The caller wraps the error.
func rejectUTCOffset(value string, parsed time.Time) (time.Time, error) {
	_, seconds := parsed.Zone()
	abs := time.Duration(seconds) * time.Second
	if abs < 0 {
		abs = -abs
	}
	minutes := int((abs % time.Hour) / time.Minute)
	if abs > 14*time.Hour || abs%time.Minute != 0 || (minutes != 0 && minutes != 30 && minutes != 45) {
		return time.Time{}, clierr.Errorf(clierr.KindUsage, "%s", i18n.Tf("error.invalidUTCOffset", map[string]any{"Value": value}))
	}
	return parsed, nil
}

// rejectZoneCivil refuses a zoneless wall time that does not exist once in
// loc. A gap is a local time the zone skipped at an offset change (DST or
// otherwise). An overlap is a local time the zone repeated. Neither is
// resolved to an instant.
func rejectZoneCivil(value, layout string, parsed time.Time, loc *time.Location) (time.Time, error) {
	requested, err := time.ParseInLocation(layout, value, time.UTC)
	if err != nil {
		return parsed, nil
	}
	if !sameWallClock(parsed, requested) {
		return time.Time{}, clierr.Errorf(clierr.KindUsage, "%s", i18n.Tf("error.zoneTimeGap", map[string]any{"Value": value, "Zone": zoneName(loc)}))
	}
	if ambiguousWall(parsed) {
		return time.Time{}, clierr.Errorf(clierr.KindUsage, "%s", i18n.Tf("error.zoneTimeOverlap", map[string]any{"Value": value, "Zone": zoneName(loc)}))
	}
	return parsed, nil
}

func zoneName(loc *time.Location) string {
	if loc == nil || loc == time.Local {
		if tz := strings.TrimSpace(os.Getenv("TZ")); tz != "" {
			return tz
		}
		return "Local"
	}
	return loc.String()
}

func sameWallClock(a, b time.Time) bool {
	ay, am, ad := a.Date()
	by, bm, bd := b.Date()
	if ay != by || am != bm || ad != bd || a.Nanosecond() != b.Nanosecond() {
		return false
	}
	ah, amin, as := a.Clock()
	bh, bmin, bs := b.Clock()
	return ah == bh && amin == bmin && as == bs
}

// ambiguousWall reports whether another instant in the same location has the
// same wall clock. Real folds are at most a couple of hours, so three hours
// of minute steps covers them.
func ambiguousWall(t time.Time) bool {
	y, m, d := t.Date()
	h, min, s := t.Clock()
	nsec := t.Nanosecond()
	_, off := t.Zone()
	for delta := -3 * time.Hour; delta <= 3*time.Hour; delta += time.Minute {
		if delta == 0 {
			continue
		}
		other := t.Add(delta)
		oy, om, od := other.Date()
		oh, omin, os := other.Clock()
		if oy != y || om != m || od != d || oh != h || omin != min || os != s || other.Nanosecond() != nsec {
			continue
		}
		_, ooff := other.Zone()
		if ooff != off {
			return true
		}
	}
	return false
}

// buildAnalyzerOptions converts CLI options to analyzer.Options.
