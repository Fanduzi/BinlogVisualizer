// Package report verifies text-report localization: headers are translated and
// columns stay aligned when a header or a name occupies two cells per rune.
package report

import (
	"strings"
	"testing"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

// A localized header is twice as wide as its runes, and a schema or table name
// can be too. The columns must still line up under the heading that names them.
func TestRenderTextLocalizedTopTablesKeepsColumnsAligned(t *testing.T) {
	i18n.ResetForTesting()
	if err := i18n.Init("zh-CN"); err != nil {
		t.Fatalf("init i18n: %v", err)
	}
	t.Cleanup(func() {
		i18n.ResetForTesting()
		_ = i18n.Init("en")
	})

	result := productTextFixture()
	result.Tables = []model.TableStats{
		{Schema: "商店", Table: "订单", TotalRows: 1234, TxnCount: 12, EventCount: 40},
	}
	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var header, row string
	for _, line := range strings.Split(out, "\n") {
		// The footnote also says 影响行数, so the header is the line that
		// carries the SQL operation words too.
		if strings.Contains(line, "影响行数") && strings.Contains(line, "INSERT") {
			header = line
		}
		if strings.Contains(line, "订单") && strings.Contains(line, "1234") {
			row = line
		}
	}
	if header == "" || row == "" {
		t.Fatalf("expected a zh-CN top tables header and row\n%s", out)
	}

	// Compare the right edge of the value column in terminal cells, not bytes:
	// the header cell and the CJK table name both occupy two cells per rune.
	headerEdge := displayWidth(header[:strings.Index(header, "影响行数")]) + displayWidth("影响行数")
	rowEdge := displayWidth(row[:strings.Index(row, "1234")]) + displayWidth("1234")
	if headerEdge != rowEdge {
		t.Fatalf("affected-rows column is misaligned: header ends at cell %d, row ends at cell %d\n%s", headerEdge, rowEdge, out)
	}
	if !strings.Contains(header, "binlog 事件") || !strings.Contains(header, "行占比") {
		t.Fatalf("expected localized column headers, got %q", header)
	}
}
