package compare

import (
	"path/filepath"
	"strings"
	"testing"

	"binlogviz/internal/i18n"
)

func TestRenderHTMLLocalizesCompareShellToChinese(t *testing.T) {
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

	out, err := RenderHTML(BuildCompareResult(current, baseline))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, token := range []string{
		`<html lang="zh-CN">`,
		`<title>BinlogViz 对比报告</title>`,
		"对比概览",
		"模式变化",
		"热点表变化",
		"新增 DDL 事件",
		"消失 DDL 事件",
		"钻取：",
	} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected localized compare HTML to contain %q", token)
		}
	}
	for _, token := range []string{
		"No added DDL events.",
		"No removed DDL events.",
		"drilldown: ",
		"data: ['Baseline', 'Current']",
	} {
		if strings.Contains(out, token) {
			t.Fatalf("expected localized compare HTML not to contain %q", token)
		}
	}
}

func TestRenderHTMLLocalizesFallbackSnapshotLabelsToChinese(t *testing.T) {
	i18n.ResetForTesting()
	i18n.MustInit("zh-CN")
	t.Cleanup(i18n.ResetForTesting)

	result := CompareResult{
		CurrentLabel:  "current",
		BaselineLabel: "baseline",
		Summary: SummaryDelta{
			CurrentTotalRows:          10,
			BaselineTotalRows:         5,
			TotalRowsDelta:            5,
			CurrentTotalTransactions:  4,
			BaselineTotalTransactions: 2,
			TotalTransactionsDelta:    2,
		},
	}

	out, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("RenderHTML: %v", err)
	}

	for _, token := range []string{
		"基线 5 -> 当前 10",
		"基线 2 -> 当前 4",
		"新增告警出现在 当前",
		"仅在以下快照出现告警 基线",
	} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected localized fallback label %q", token)
		}
	}
	for _, token := range []string{
		"baseline 5 -> current 10",
		"baseline 2 -> current 4",
		"新增告警出现在 current",
		"仅在以下快照出现告警 baseline",
	} {
		if strings.Contains(out, token) {
			t.Fatalf("expected fallback label not to leak English token %q", token)
		}
	}
}
