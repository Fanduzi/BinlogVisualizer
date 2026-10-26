// Package report writes one analyze report.
// input: a finished AnalysisResult, presentation options, and a format name.
// output: text, JSON, Markdown, or HTML written to a writer. An unknown format writes nothing. A listed format with no case returns an unhandled-format error and writes nothing.
// pos: the format dispatch under the analyze command. The view is built before the switch. The switch has no default arm.
// note: if this file changes, update this header and README.md.
package report

import (
	"bytes"
	"fmt"
	"io"

	"binlogviz/internal/model"
	"binlogviz/internal/report/view"
)

// Write builds one view, then writes format to w.
// An unknown format returns the ParseFormat error and writes nothing.
// The switch has no default arm. A format ParseFormat accepted that has no
// case returns an unhandled-format error and writes nothing.
func Write(w io.Writer, result model.AnalysisResult, opts Options, format string) error {
	if w == nil {
		return fmt.Errorf("nil writer")
	}
	opts = normalizeOptions(opts)
	built := newView(result, opts)
	parsed, err := ParseFormat(format)
	if err != nil {
		return err
	}
	switch parsed {
	case FormatJSON:
		return writeJSON(w, result, opts, built)
	case FormatText:
		text, err := renderText(result, opts, built)
		if err != nil {
			return err
		}
		if err := spilledErr(result); err != nil {
			return err
		}
		_, err = io.WriteString(w, text)
		return err
	case FormatMarkdown:
		text, err := renderMarkdown(result, opts, built)
		if err != nil {
			return err
		}
		if err := spilledErr(result); err != nil {
			return err
		}
		_, err = io.WriteString(w, text)
		return err
	case FormatHTML:
		text, err := renderHTML(result, opts, built)
		if err != nil {
			return err
		}
		if err := spilledErr(result); err != nil {
			return err
		}
		_, err = io.WriteString(w, text)
		return err
	}
	return fmt.Errorf("unhandled format %q", parsed)
}

// Render is Write into a string. JSON includes the trailing newline Encode adds.
func Render(result model.AnalysisResult, opts Options, format string) (string, error) {
	var buf bytes.Buffer
	if err := Write(&buf, result, opts, format); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func spilledErr(result model.AnalysisResult) error {
	if result.MinuteTables == nil {
		return nil
	}
	return result.MinuteTables.Err()
}

func newView(result model.AnalysisResult, opts Options) view.View {
	opts = normalizeOptions(opts)
	mode := opts.SQLContextMode
	return view.Build(result, view.Limits{
		TopN:            opts.TopN,
		DiagnosticLists: opts.DiagnosticsMode == DiagnosticsFull,
		HasQuery: func(txn model.Transaction) bool {
			return transactionTextQuery(txn, mode) != ""
		},
	})
}
