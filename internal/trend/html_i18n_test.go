// Package trend verifies localized trend HTML presentation.
// input: trend results rendered under explicit Chinese runtime locale settings.
// output: regression coverage for localized trend shell and comparability guards.
// pos: trend HTML localization tests at the renderer boundary.
// note: if this file changes, update this header and internal/trend/README.md.
package trend

import (
	"strings"
	"testing"

	comparepkg "binlogviz/internal/compare"
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

func TestRenderHTMLLocalizesComparabilityGuardToChinese(t *testing.T) {
	i18n.ResetForTesting()
	i18n.MustInit("zh-CN")
	t.Cleanup(i18n.ResetForTesting)

	result := Result{Comparability: Comparability{
		Verdict:     comparepkg.VerdictUnknown,
		ReasonCodes: []string{comparepkg.ReasonMissingWorkloadIdentity},
	}}
	out, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("RenderHTML: %v", err)
	}
	for _, token := range []string{"可比性保护", "由于输入可比性未知，已抑制因果发现", "原因代码："} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected localized comparability guard %q", token)
		}
	}
	for _, token := range []string{"Comparability Guard", "Causal findings suppressed", "Reason Codes:"} {
		if strings.Contains(out, token) {
			t.Fatalf("comparability guard leaked English token %q", token)
		}
	}
}
