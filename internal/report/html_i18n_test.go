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
		"热点表",
	} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected localized analyze HTML to contain %q", token)
		}
	}
}
