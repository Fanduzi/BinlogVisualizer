package analyzer

import (
	"strings"
	"testing"
	"time"
)

// TestAnalyzerFailsWhenSpilledDelaysCannotBeRead pins the #364 failure rule:
// if the spilled apply-delay values cannot be read back, analyze fails with
// the read error. It must not drop the apply-delay section and exit 0.
func TestAnalyzerFailsWhenSpilledDelaysCannotBeRead(t *testing.T) {
	base := time.Date(2026, 3, 15, 14, 0, 0, 0, time.UTC)
	a := New(DefaultOptions())
	n := delaySpillAt + 100
	for i := 1; i <= n; i++ {
		at := base.Add(time.Duration(i) * time.Millisecond)
		orig := uint64(at.UnixMicro())
		for _, ev := range delayGroup(at, gtidN(i), "shop", "orders", "INSERT", 1, int64(i)*200, orig, orig+uint64(i)) {
			if err := a.Consume(ev); err != nil {
				t.Fatal(err)
			}
		}
	}
	values := &a.reportAgg.applyDelay.values
	if values.spilled == 0 || values.file == nil {
		t.Fatalf("no spill after %d transactions (spilled=%d)", n, values.spilled)
	}
	// Break the read-back: the file handle stays set but every read fails.
	if err := values.file.Close(); err != nil {
		t.Fatal(err)
	}
	result, err := a.Finalize()
	if err == nil {
		t.Fatalf("Finalize succeeded after a spill read failure; apply delay=%+v", result.Diagnostics.ApplyDelay)
	}
	if !strings.Contains(err.Error(), "read spilled apply-delay values") {
		t.Fatalf("Finalize error %q does not name the spill read failure", err)
	}
	if result != nil {
		t.Fatalf("Finalize returned a result with the error")
	}
}
