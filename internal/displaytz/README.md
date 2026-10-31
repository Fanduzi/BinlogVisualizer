# Display timezone

The `--tz` display zone (ADR 0017). `Parse` validates the flag once, the
command stores the location on its context, and the renderers produce
timestamps in that zone for text, Markdown, HTML, and JSON. A nil location
keeps the historical UTC spelling so default output stays byte-identical.
`time/tzdata` is embedded, so an IANA name resolves without the system
zone database.

## Members

| File | Responsibility |
|------|----------------|
| `displaytz.go` | `Parse` flag validation, `With`/`From` context storage, the `Wall`/`RFC3339`/`RFC3339Nano` renderers, and `Stored` (rewrites a stored UTC RFC3339 string into the zone). |
| `displaytz_test.go` | Verifies `Stored` renders the same instant with the right offset across DST transitions and leaves nil-location or unparsable input unchanged, and `Parse`'s `Local` rejection (any case or padding), `UTC` mapping to `time.UTC`, and unset-flag nil. |

## Interfaces

| API | Contract |
|-----|----------|
| `Parse(changed bool, value string) (*time.Location, error)` | An unchanged flag is a nil location: historical UTC spelling, and a zoneless `--start`/`--end` uses the machine zone. `UTC` is `time.UTC` — keeps the `UTC` spelling and a zoneless value is read in UTC. Any other IANA name is that location. An empty name, `Local`, or an unknown name is a usage error (exit 3). |
| `With(ctx, loc) context.Context` | Stores the location for the command that is about to run; nil `ctx` starts from `context.Background()`. |
| `From(ctx) *time.Location` | Returns what `With` stored; nil means historical UTC. |
| `Wall(t, layout, loc) string` | nil or `time.UTC` renders `"<layout> UTC"`; any other zone uses the same layout with a numeric `-07:00` offset, never an abbreviation. |
| `RFC3339(t, loc)` / `RFC3339Nano(t, loc)` | nil or `time.UTC` keeps the `Z` spelling; other zones use the offset RFC3339 form (`Z` only when that instant's offset is zero). |
| `Stored(value, loc) string` | Rewrites an RFC3339 or RFC3339Nano timestamp that was saved in UTC; nil `loc`, `time.UTC`, blank, or unparsable input returns the value unchanged. |

## Dependencies

- Upstream: `context`, `time`, embedded `time/tzdata`; `internal/clierr` for usage errors.
- Downstream: `cmd/binlogviz` parses `--tz` and stores the location on the command context; `internal/report`, `internal/compare`, `internal/trend`, and `internal/workflow` render through it.

## Notes

- `Etc/UTC` is a different location from `time.UTC` with the same offset: `--tz Etc/UTC` renders the numeric `+00:00` form, not the `UTC` spelling.
