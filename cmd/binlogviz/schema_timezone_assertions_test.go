package binlogviz

import (
	"strings"
	"testing"
	"time"
)

func assertAnalyzeDisplayZones(t *testing.T, binlog, baseJSON string, baseTimes []string) {
	t.Helper()
	zones := []string{"Asia/Kolkata", "America/New_York", "Etc/UTC"}
	for _, zone := range zones {
		code, out, stderr := runRoot(t, "analyze", binlog, "--format", "json", "-q", "--tz", zone)
		if code != 0 {
			t.Fatalf("%s analyze exit %d: %s", zone, code, stderr)
		}
		got := collectRFC3339(t, out)
		if len(got) != len(baseTimes) {
			t.Fatalf("%s: %d timestamps, want %d", zone, len(got), len(baseTimes))
		}
		loc, err := time.LoadLocation(zone)
		if err != nil {
			t.Fatal(err)
		}
		for i := range baseTimes {
			bt := parseRFC3339(t, baseTimes[i])
			zt := parseRFC3339(t, got[i])
			if !bt.Equal(zt) {
				t.Fatalf("%s: instant %s became %s", zone, baseTimes[i], got[i])
			}
			_, off := zt.In(loc).Zone()
			want := rfc3339Offset(off)
			if !strings.HasSuffix(got[i], want) {
				t.Fatalf("%s: %s want offset suffix %s", zone, got[i], want)
			}
			if strings.Contains(got[i], "EDT") || strings.Contains(got[i], "EST") || strings.Contains(got[i], "IST") {
				t.Fatalf("%s: timestamp uses an abbreviation: %s", zone, got[i])
			}
		}
		if zone == "Asia/Kolkata" && out == baseJSON {
			t.Fatal("--tz Asia/Kolkata left analyze JSON unchanged")
		}

		code, text, stderr := runRoot(t, "analyze", binlog, "--format", "text", "-q", "--tz", zone)
		if code != 0 {
			t.Fatalf("%s text exit %d: %s", zone, code, stderr)
		}
		code, md, stderr := runRoot(t, "analyze", binlog, "--format", "markdown", "-q", "--tz", zone)
		if code != 0 {
			t.Fatalf("%s markdown exit %d: %s", zone, code, stderr)
		}
		code, html, stderr := runRoot(t, "analyze", binlog, "--format", "html", "-q", "--tz", zone)
		if code != 0 {
			t.Fatalf("%s html exit %d: %s", zone, code, stderr)
		}
		_, sampleOff := parseRFC3339(t, baseTimes[0]).In(loc).Zone()
		human := humanOffset(sampleOff)
		for _, body := range []string{text, md, html} {
			if !strings.Contains(body, human) {
				t.Fatalf("%s output missing human offset %s", zone, human)
			}
		}
		if zone != "Etc/UTC" && !strings.Contains(text, zone) {
			t.Fatalf("text timestamps line should name %s", zone)
		}
	}
}

func assertDefaultTimezoneRendering(t *testing.T, binlog string) {
	t.Helper()
	code, text, stderr := runRoot(t, "analyze", binlog, "--format", "text", "-q")
	if code != 0 {
		t.Fatalf("default text exit %d: %s", code, stderr)
	}
	if !strings.Contains(text, " UTC") {
		t.Fatal("default text lost the UTC spelling")
	}
	code, html, stderr := runRoot(t, "analyze", binlog, "--format", "html", "-q")
	if code != 0 {
		t.Fatalf("default html exit %d: %s", code, stderr)
	}
	if !strings.Contains(html, " UTC") {
		t.Fatal("default html lost the UTC spelling")
	}
}
