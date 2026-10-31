package compare

import (
	"path/filepath"
	"strings"
	"testing"

	"binlogviz/internal/i18n"
)

func TestRenderTextLocalizesSectionsToChinese(t *testing.T) {
	i18n.ResetForTesting()
	i18n.MustInit("zh-CN")
	t.Cleanup(i18n.ResetForTesting)

	current, err := LoadReport(filepath.Join("testdata", "current.json"))
	if err != nil {
		t.Fatalf("load current report: %v", err)
	}
	baseline, err := LoadReport(filepath.Join("testdata", "baseline.json"))
	if err != nil {
		t.Fatalf("load baseline report: %v", err)
	}

	output, err := RenderText(BuildCompareResult(current, baseline))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, token := range []string{
		"对比概览",
		"当前标签",
		"基线标签",
		"行数：1500 -> 2400（+900）",
		"事务数：90 -> 120（+30）",
		"关键发现",
		"建议后续检查",
		"热点表变化",
		"操作类型分布",
		"新增告警（2）",
		"消失告警（1）",
	} {
		if !strings.Contains(output, token) {
			t.Fatalf("expected localized compare text to contain %q, got:\n%s", token, output)
		}
	}
	for _, token := range []string{"Compare Summary", "Key Findings", "Top Table Changes", "Operation Mix"} {
		if strings.Contains(output, token) {
			t.Fatalf("expected localized compare text not to contain %q", token)
		}
	}
}
