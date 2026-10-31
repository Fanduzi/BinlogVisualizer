package trend

import (
	"strings"
	"testing"

	"binlogviz/internal/i18n"
)

func TestRenderTextLocalizesSectionsToChinese(t *testing.T) {
	i18n.ResetForTesting()
	i18n.MustInit("zh-CN")
	t.Cleanup(i18n.ResetForTesting)

	result := mustBuildPatternTrendResult(t)

	output, err := RenderText(result)
	if err != nil {
		t.Fatalf("render text: %v", err)
	}

	for _, token := range []string{
		"趋势概览",
		"输入模式：",
		"有序快照点",
		"操作类型分布",
		"热点表趋势",
		"模式趋势",
		"聚合洞察",
		"首个快照",
		"最后快照",
		"行数变化",
		"事务变化",
		"事件变化",
		"告警变化",
	} {
		if !strings.Contains(output, token) {
			t.Fatalf("expected localized trend text to contain %q, got:\n%s", token, output)
		}
	}
	for _, token := range []string{"Trend Summary", "Ordered Points", "Operation Mix", "Aggregate Insights"} {
		if strings.Contains(output, token) {
			t.Fatalf("expected localized trend text not to contain %q", token)
		}
	}
}
