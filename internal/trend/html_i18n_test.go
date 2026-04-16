package trend

import (
	"strings"
	"testing"

	"binlogviz/internal/i18n"
)

func TestRenderHTMLLocalizesTrendShellToChinese(t *testing.T) {
	i18n.ResetForTesting()
	i18n.MustInit("zh-CN")
	t.Cleanup(i18n.ResetForTesting)

	result := mustBuildPatternTrendResult(t)

	out, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("render html: %v", err)
	}

	for _, token := range []string{
		`<html lang="zh-CN">`,
		`<title>BinlogViz 趋势报告</title>`,
		"趋势概览",
		"模式趋势",
		"热点表趋势",
		"钻取：",
	} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected localized trend HTML to contain %q", token)
		}
	}
	if strings.Contains(out, "drilldown: ") {
		t.Fatalf("expected localized trend HTML not to contain hardcoded drilldown label")
	}
}
