// Package report embeds the ECharts runtime used by self-contained HTML reports.
// input: bundled static asset bytes from internal/report/static/echarts.min.js.
// output: read access to the embedded ECharts JavaScript payload.
// pos: asset-loading helper shared by analyze, compare, and trend HTML renderers.
// note: if this file changes, keep internal/report/README.md synchronized.
package report

import (
	"embed"
	"fmt"
)

//go:embed static/echarts.min.js
var echartsFS embed.FS

// ReadEmbeddedECharts returns the bundled ECharts runtime used by HTML reports.
func ReadEmbeddedECharts() (string, error) {
	data, err := echartsFS.ReadFile("static/echarts.min.js")
	if err != nil {
		return "", fmt.Errorf("read echarts: %w", err)
	}
	return string(data), nil
}
