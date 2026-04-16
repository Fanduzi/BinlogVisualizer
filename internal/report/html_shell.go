// Package report shares HTML shell helpers across analyze, compare, and trend reports.
// input: HTML template bodies plus renderer-specific template funcs.
// output: parsed templates with common i18n funcs and current language injection.
// pos: shared presentation layer between report-producing packages and HTML templates.
// note: if this file changes, update this header and module README.md.
package report

import (
	"fmt"
	"html/template"

	"binlogviz/internal/i18n"
)

// CurrentHTMLLang returns the active HTML language tag for report templates.
func CurrentHTMLLang() string {
	lang := i18n.CurrentLanguage()
	if lang == "" {
		return "en"
	}
	return lang
}

// NewHTMLTemplate parses a template with the shared HTML/i18n func map applied.
func NewHTMLTemplate(name, body string, extra template.FuncMap) (*template.Template, error) {
	funcs := template.FuncMap{
		"t":    i18n.T,
		"lang": CurrentHTMLLang,
	}
	for key, fn := range extra {
		funcs[key] = fn
	}

	tmpl, err := template.New(name).Funcs(funcs).Parse(body)
	if err != nil {
		return nil, fmt.Errorf("parse %s html template: %w", name, err)
	}
	return tmpl, nil
}
