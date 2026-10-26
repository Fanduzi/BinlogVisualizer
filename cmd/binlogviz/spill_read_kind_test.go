package binlogviz

import (
	"errors"
	"testing"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/clierr"
)

func TestSpillReadReportsSpillKind(t *testing.T) {
	spill := &analyzer.SpillReadError{Err: errors.New("closed")}
	for _, format := range []string{"json", "text", "markdown", "html"} {
		err := classifyReportWriteErr(format, spill)
		kind, _ := clierr.Of(err)
		if kind != clierr.KindSpill || clierr.Code(kind) != 11 {
			t.Fatalf("%s kind %s code %d, want spill 11", format, kind, clierr.Code(kind))
		}
		if got := err.Error(); got != "read spilled activity: closed" {
			t.Fatalf("%s message %q", format, got)
		}
	}
	plain := errors.New("write failed")
	if kind, _ := clierr.Of(classifyReportWriteErr("json", plain)); kind != clierr.KindOutput || clierr.Code(kind) != 8 {
		t.Fatalf("json write kind %s", kind)
	}
	if kind, _ := clierr.Of(classifyReportWriteErr("text", plain)); kind != clierr.KindOutput {
		t.Fatalf("text write kind %s", kind)
	}
	if kind, _ := clierr.Of(classifyReportWriteErr("markdown", plain)); kind != clierr.KindOutput {
		t.Fatalf("markdown write kind %s", kind)
	}
	if kind, _ := clierr.Of(classifyReportWriteErr("html", plain)); kind != clierr.KindError || clierr.Code(kind) != 1 {
		t.Fatalf("html write kind %s code %d, want untyped", kind, clierr.Code(kind))
	}
}
