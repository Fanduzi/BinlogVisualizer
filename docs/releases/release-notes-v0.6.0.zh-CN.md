# BinlogViz v0.6.0 发布说明

发布日期：2026-03-30

## 概述

v0.6.0 为 `analyze` 命令新增了 Markdown 和 HTML 两种输出格式。HTML 报告内置交互式 ECharts 图表和 5 套主题切换器，主题选择持久化到 `localStorage`。

## 新功能

### Markdown 输出（`--format markdown`）

`analyze` 命令现支持 `--format markdown`（别名：`md`），输出 GitHub Flavored Markdown 格式的报告，包含：工作负载摘要、热点表、热点事务、分钟级活动和告警等表格。

### HTML 输出（`--format html`）

`analyze` 命令现支持 `--format html`，输出无外部依赖的单文件 HTML 报告，包含：

- 摘要统计卡片（事务数、行数、事件数、时间范围）
- 交互式折线图：每分钟行数和事务数
- 交互式条形图：行数最多的热点表
- 交互式环形图：INSERT / UPDATE / DELETE 操作分布
- 热点表详情表格（含各操作行数）
- 告警列表（含严重等级徽标）

所有图表使用 ECharts 在客户端渲染（已内联打包），查看报告无需外部依赖或网络连接。

### 5 套主题切换器

HTML 报告 header 右侧提供主题切换器（五个彩色圆点），可用主题：

| 主题 | 风格 |
|---|---|
| Nebula（默认） | 深色，靛紫 + 青色 |
| Forest | 深色，翠绿 + 琥珀 |
| Navy | 深色，天蓝 + 金色 |
| Ember | 深色，橙色 + 玫红 |
| Light | 浅色，简洁报告风 |

主题选择保存在 `localStorage`，下次打开时自动恢复。

## Bug 修复

无。

## 破坏性变更

无。现有 `--format text` 和 `--format json` 行为不变。
