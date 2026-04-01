// Package report verifies renderer option parsing and normalization rules.
// input: CLI-style raw format/sql-context strings and report Options values.
// output: regression coverage for supported formats, invalid values, and default fallbacks.
// pos: option parsing regression suite guarding renderer configuration behavior.
// note: if this file changes, update this header and module README.md.
package report

import "testing"

func TestParseFormatAcceptsSupportedFormatsAndDefault(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want Format
	}{
		{name: "default empty", raw: "", want: FormatText},
		{name: "text", raw: "text", want: FormatText},
		{name: "json", raw: "json", want: FormatJSON},
		{name: "markdown", raw: "markdown", want: FormatMarkdown},
		{name: "html", raw: "html", want: FormatHTML},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseFormat(tt.raw)
			if err != nil {
				t.Fatalf("ParseFormat(%q) returned error: %v", tt.raw, err)
			}
			if got != tt.want {
				t.Fatalf("ParseFormat(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}

func TestParseFormatRejectsUnknownFormat(t *testing.T) {
	got, err := ParseFormat("binary")
	if err == nil {
		t.Fatalf("expected error, got format %q", got)
	}
}

func TestNormalizeOptionsPreservesValidSQLContextAndFallsBackOnInvalid(t *testing.T) {
	if got := normalizeOptions(Options{SQLContextMode: SQLContextFull}); got.SQLContextMode != SQLContextFull {
		t.Fatalf("expected full mode to be preserved, got %q", got.SQLContextMode)
	}

	got := normalizeOptions(Options{SQLContextMode: SQLContextMode("invalid")})
	if got.SQLContextMode != SQLContextSummary {
		t.Fatalf("expected invalid mode to fall back to summary, got %q", got.SQLContextMode)
	}
}
