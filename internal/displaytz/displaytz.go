// Package displaytz parses the --tz flag and renders a timestamp in that zone.
// input: the flag value, and a time.Time plus the layout the report already uses.
// output: an IANA location, or the historical UTC spelling when the flag is absent or UTC.
// pos: display-only timezone for text, Markdown, HTML, and JSON. It does not parse --start or --end.
// note: if this file changes, update docs/adr/0017-json-schema-and-display-timezone.md.
package displaytz

import (
	"context"
	"strings"
	"time"

	_ "time/tzdata"

	"binlogviz/internal/clierr"
)

type ctxKey struct{}

// Parse validates --tz. An unchanged flag is the historical UTC rendering
// (a nil location). UTC is that same rendering. Any other IANA name is a
// location. An empty or unknown name is a usage error.
func Parse(changed bool, value string) (*time.Location, error) {
	if !changed {
		return nil, nil
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, clierr.Errorf(clierr.KindUsage, "invalid --tz %q: expected an IANA time zone name such as Asia/Shanghai", value)
	}
	loc, err := time.LoadLocation(value)
	if err != nil {
		return nil, clierr.Errorf(clierr.KindUsage, "invalid --tz %q: %v", value, err)
	}
	if loc == time.UTC {
		return nil, nil
	}
	return loc, nil
}

// With stores loc on ctx for the command that is about to run.
func With(ctx context.Context, loc *time.Location) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, ctxKey{}, loc)
}

// From returns the location With stored. A nil result means historical UTC.
func From(ctx context.Context) *time.Location {
	if ctx == nil {
		return nil
	}
	loc, _ := ctx.Value(ctxKey{}).(*time.Location)
	return loc
}

// Wall renders t in loc. A nil loc, or time.UTC, keeps the historical
// "<layout> UTC" spelling so default output stays byte-identical.
// Any other zone uses the same layout and a numeric offset, never an
// abbreviation: 2006-01-02 15:04:05 -07:00.
func Wall(t time.Time, layout string, loc *time.Location) string {
	if loc == nil || loc == time.UTC {
		return t.UTC().Format(layout + " UTC")
	}
	return t.In(loc).Format(layout + " -07:00")
}

// RFC3339 renders t in loc. A nil loc keeps the Z spelling. Any other zone
// uses the offset RFC3339 form (Z only when that instant's offset is zero).
func RFC3339(t time.Time, loc *time.Location) string {
	if loc == nil || loc == time.UTC {
		return t.UTC().Format(time.RFC3339)
	}
	return t.In(loc).Format(time.RFC3339)
}

// RFC3339Nano is RFC3339 with a fractional second, same zone rule.
func RFC3339Nano(t time.Time, loc *time.Location) string {
	if loc == nil || loc == time.UTC {
		return t.UTC().Format(time.RFC3339Nano)
	}
	return t.In(loc).Format(time.RFC3339Nano)
}

// Stored rewrites an RFC3339 timestamp that was saved in UTC. An unparsable
// string is returned unchanged. A nil loc returns value unchanged.
func Stored(value string, loc *time.Location) string {
	if loc == nil || loc == time.UTC || strings.TrimSpace(value) == "" {
		return value
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		parsed, err = time.Parse(time.RFC3339Nano, value)
		if err != nil {
			return value
		}
	}
	return RFC3339(parsed, loc)
}
