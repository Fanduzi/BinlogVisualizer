package report

import "fmt"

// ReadEmbeddedECharts returns the bundled ECharts runtime used by HTML reports.
func ReadEmbeddedECharts() (string, error) {
	data, err := echartsFS.ReadFile("static/echarts.min.js")
	if err != nil {
		return "", fmt.Errorf("read echarts: %w", err)
	}
	return string(data), nil
}
