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

func TestNormalizeOptionsAppliesProductDefaults(t *testing.T) {
	opts := normalizeOptions(Options{})

	if opts.TopN != DefaultTopN {
		t.Fatalf("expected default top N %d, got %d", DefaultTopN, opts.TopN)
	}
	if opts.TopTables != DefaultTopN {
		t.Fatalf("expected default table limit %d, got %d", DefaultTopN, opts.TopTables)
	}
	if opts.ShowMinutes {
		t.Fatal("default text output must not show minute details")
	}
	if opts.ShowPatterns {
		t.Fatal("default text output must not show write pattern details")
	}
}

func TestNormalizeOptionsUsesTopNForUnspecifiedTableLimit(t *testing.T) {
	opts := normalizeOptions(Options{TopN: 3})

	if opts.TopTables != 3 {
		t.Fatalf("expected table limit to follow TopN, got %d", opts.TopTables)
	}

	opts = normalizeOptions(Options{TopN: 3, TopTables: 1})
	if opts.TopTables != 1 {
		t.Fatalf("expected explicit table limit to be preserved, got %d", opts.TopTables)
	}

	opts = normalizeOptions(Options{TopN: 3, TopTablesSet: true})
	if opts.TopTables != 0 {
		t.Fatalf("expected explicit zero table limit to remain unlimited, got %d", opts.TopTables)
	}
}

func TestNormalizeOptionsDetailsEnablesDetailedTextSections(t *testing.T) {
	opts := normalizeOptions(Options{Details: true})

	if !opts.ShowMinutes {
		t.Fatal("--details should enable minute details")
	}
	if !opts.ShowPatterns {
		t.Fatal("--details should enable write pattern details")
	}
}

func TestNormalizeOptionsRejectsInvalidTopN(t *testing.T) {
	opts := normalizeOptions(Options{TopN: -5})

	if opts.TopN != DefaultTopN {
		t.Fatalf("expected invalid top N to fall back to %d, got %d", DefaultTopN, opts.TopN)
	}
}
