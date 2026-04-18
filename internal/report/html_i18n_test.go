// Package report renders self-contained HTML reports from bounded analysis results.
// input: analyzer-produced AnalysisResult values plus optional SQL context presentation controls.
// output: single self-contained HTML file with embedded ECharts, dark OLED theme, inline CSS.
// pos: HTML renderer for the CLI output path after analyzer Finalize.
// note: if this file changes, update this header and module README.md.
package report

import (
	"strings"
	"testing"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

func TestRenderHTMLLocalizesAnalyzeShellToChinese(t *testing.T) {
	i18n.ResetForTesting()
	i18n.MustInit("zh-CN")
	t.Cleanup(i18n.ResetForTesting)

	out, err := RenderHTML(model.AnalysisResult{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, token := range []string{
		`<html lang="zh-CN">`,
		`<title>BinlogViz 分析报告</title>`,
		"分析报告",
		"活动图表",
		"热点对象",
		"诊断证据",
		"风险与发现",
		"活动概览",
	} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected localized analyze HTML to contain %q", token)
		}
	}
}
