package analyzer

import (
	"strings"
	"testing"

	"binlogviz/internal/i18n"
)

func warningText(ws []FlashbackWarning) string {
	lines := make([]string, len(ws))
	for i, w := range ws {
		lines[i] = w.Line()
	}
	return strings.Join(lines, "\n")
}

// The plan carries a code and an unprefixed message; stderr keeps the
// localized prefix (#270 item 4, #282 item 4).
func TestFlashbackWarningLineKeepsLocalizedPrefix(t *testing.T) {
	for _, tc := range []struct{ lang, prefix string }{{"en", "warning: "}, {"zh-CN", "警告："}} {
		if err := i18n.Init(tc.lang); err != nil {
			t.Fatal(err)
		}
		w := newFlashWarning(WarnPartialTransaction, "warning.flashbackSplit", map[string]any{"GTID": "u:7"})
		if w.Code != "partial_transaction" || strings.HasPrefix(w.Message, tc.prefix) || !strings.Contains(w.Message, "u:7") {
			t.Fatalf("%s: warning = %+v", tc.lang, w)
		}
		if line := w.Line(); line != tc.prefix+w.Message {
			t.Fatalf("%s: line = %q", tc.lang, line)
		}
	}
	_ = i18n.Init("en")
}
