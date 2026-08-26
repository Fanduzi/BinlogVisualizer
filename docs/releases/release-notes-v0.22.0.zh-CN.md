# BinlogViz v0.22.0 发布说明

发布日期：2026-08-26

## 核心亮点

- **HTML 报告 UI 全面重构**：采用现代化深色科技质感设计，优化排版层次、毛玻璃拟物卡片、发光徽章与响应式布局，全面提升 Analyze、Compare 与 Trend 报告视觉体验。
- **Binlog 物理体积（Binlog Bytes）全链路展示**：Top Tables 新增支持精确数字排序的 `BINLOG BYTES` 物理体积列（如 `36.2 MB`、`850 KB`），直观定位大字段/JSON 写入放大与磁盘开销；大事务诊断卡片与热点时段同步展示人类可读格式化体积。
- **多图表实时跨维联动（Chart Synchronization）**：同时间维度的图表全面接入实时联动（`AVG TPS/MIN` 与 `ROWS PER MINUTE`、单表下钻活动折线图与操作堆叠图、多快照趋势图），鼠标悬浮、准星与区间缩放完全同步。
- **鼠标多维范围 Zoom 与框选（Range Zoom & Box-Select）**：所有时间轴图表均配备半透明霓虹选区滑动条（DataZoom Slider）与右上角快捷工具箱（Toolbox），支持鼠标区域拉框放大、滚轮平滑缩放与一键重置还原。
- **交互与诊断体验打磨**：新增一键平滑回到顶部悬浮按钮、多套科技主题快速切换、`mysqlbinlog` 重放与排查命令一键复制、优化 Key Findings 边距与图例排版。

## 变更详情

- `internal/report`：在表格、大事务证据卡片和热点时段中集成物理字节数；实现 `makeToolbox()`、`makeDataZoom()` 与 `echarts.connect` 图表联动。
- `internal/compare`：重构 Key Findings 与 Recommended Checks 诊断卡片；为对比图表接入框选缩放与滑动条；在热点时段变化表中增加物理体积。
- `internal/trend`：实现跨快照趋势图表的统一联动组（`trend_group`）；优化模式趋势图的占比/行数切换与选区缩放。
- `internal/i18n`：在英文与简体中文语言包中补充物理体积与图表交互的本地化词条。

## 兼容性说明

- 完全向后兼容现有的 CLI 命令行参数、JSON 输出格式、Snapshot 快照与 Workflow 工作流。
