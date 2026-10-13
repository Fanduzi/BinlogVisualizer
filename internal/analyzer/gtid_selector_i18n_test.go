package analyzer

import (
	"testing"

	"binlogviz/internal/i18n"
)

// #244: the MariaDB GTID error is translated.
func TestParseMariaDBGTIDErrorLocalized(t *testing.T) {
	defer i18n.MustInit("en")
	for lang, want := range map[string]string{
		"en":    `invalid MariaDB GTID "AUTOMATIC"`,
		"zh-CN": `无效的 MariaDB GTID "AUTOMATIC"`,
	} {
		i18n.MustInit(lang)
		if _, err := parseMariaDBGTID("AUTOMATIC"); err == nil || err.Error() != want {
			t.Errorf("%s: %v, want %s", lang, err, want)
		}
	}
}
