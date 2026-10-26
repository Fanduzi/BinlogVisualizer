package binlogviz

import (
	"strings"
	"testing"

	"binlogviz/internal/clierr"
	"binlogviz/internal/flashback"
	"binlogviz/internal/model"
)

func refusedDrop(t *testing.T, ev model.NormalizedEvent) (string, map[string]any) {
	t.Helper()
	p := flashback.New(flashback.Options{})
	p.Note(ev)
	_, err := p.Prepare()
	if err == nil {
		t.Fatal("expected a DDL refusal")
	}
	err = flashbackExit(err)
	kind, details := clierr.Of(err)
	if kind != clierr.KindRefused {
		t.Fatalf("kind %s, want refused", kind)
	}
	return err.Error(), details
}

// The session schema fills an unqualified name in the message and in details.table.
// QuerySchema wins. The event schema is the fallback when the query schema was not kept.
func TestFlashbackDropSessionSchemaInRefusalAndDetails(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	cases := []model.NormalizedEvent{
		{EventType: "DDL", Schema: "qa410a", QuerySQL: "DROP TABLE z, qa410b.t1"},
		{EventType: "DDL", Schema: "other", QuerySchema: "qa410a", QuerySQL: "DROP TABLE z, qa410b.t1"},
	}
	for _, ev := range cases {
		msg, details := refusedDrop(t, ev)
		if details["reason"] != "ddl_in_range" || details["table"] != "qa410a.z, qa410b.t1" {
			t.Fatalf("details %#v message %s", details, msg)
		}
		if !strings.Contains(msg, "qa410a.z, qa410b.t1") {
			t.Fatalf("message %s", msg)
		}
	}
}

// The message keeps five names. details.table keeps the raw list, including a TAB.
func TestFlashbackDropRefusalShowsFiveNamesAndEscapesControls(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	sql := "DROP TABLE `we``ird`, `a\tb`, t3, t4, t5, t6, t7"
	msg, details := refusedDrop(t, model.NormalizedEvent{
		EventType: "DDL",
		Schema:    "s",
		QuerySQL:  sql,
	})
	full, _ := details["table"].(string)
	wantFull := "s.we`ird, s.a\tb, s.t3, s.t4, s.t5, s.t6, s.t7"
	if full != wantFull {
		t.Fatalf("details.table %q", full)
	}
	if strings.Contains(full, "and 2 more") {
		t.Fatalf("details.table was shortened: %q", full)
	}
	if !strings.Contains(full, "\t") {
		t.Fatal("details.table dropped the raw TAB")
	}
	head, _, ok := strings.Cut(msg, ": DDL")
	if !ok {
		t.Fatalf("message %s", msg)
	}
	wantHead := "s.we`ird, s.a\\tb, s.t3, s.t4, s.t5 and 2 more"
	if head != wantHead {
		t.Fatalf("message head %q", head)
	}
	if strings.Contains(head, "\t") {
		t.Fatalf("message kept a raw TAB: %q", head)
	}
}
