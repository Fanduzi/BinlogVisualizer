package binlogviz

import (
	"bytes"
	"errors"
	"testing"

	"binlogviz/internal/i18n"
)

// #227: the error line prefix follows the output language.
func TestPrintCommandErrorPrefixFollowsLanguage(t *testing.T) {
	defer i18n.MustInit("en")
	for _, tc := range []struct{ lang, want string }{
		{"en", "\r\033[2KError: boom\n"},
		{"zh-CN", "\r\033[2K错误：boom\n"},
	} {
		i18n.MustInit(tc.lang)
		var b bytes.Buffer
		PrintCommandError(&b, errors.New("boom"))
		if b.String() != tc.want {
			t.Errorf("%s: %q, want %q", tc.lang, b.String(), tc.want)
		}
	}
}
