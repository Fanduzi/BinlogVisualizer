# BinlogViz v0.16.0 发布说明

发布日期：2026-04-12

## 范围

`v0.16.0` 在 analyze 报告中新增了一个可选的解释层：**选择性模式钻取（Selective Pattern Drilldowns）**。当一个或多个写入模式超过高信号阈值时，BinlogViz 会将其展开为有界的钻取摘要，解释该模式为何值得关注，并提供工作负载上下文。

这是一个新增功能版本。现有报告结构、CLI 参数和输出契约不变。

## 主要内容

- 新增 `pattern_drilldowns` 作为 JSON analyze 输出的顶层数组（始终存在，无模式达到阈值时为空数组）
- 文本输出在符合条件模式下方渲染缩进的 `drilldown:` 块，包含 `workload minute` 和 `workload txn` 行
- HTML 输出渲染可折叠的钻取卡片，包含信号标志（`dominance`、`anomaly`）、指标帮助提示和工作负载上下文分区
- 钻取选择采用混合信号模型，结合 dominance 和 anomaly 信号，按分数排序，每次分析最多选择 2 个模式
- 每个钻取最多包含 2 个工作负载峰值分钟和 2 个工作负载事务（窗口级上下文，不保证完全属于该模式本身）
- anomaly 信号是模式局部的：基于表对齐的告警匹配和高 rows-per-txn 比率，而非全局分钟重叠

## 新增 JSON 契约字段

```json
{
  "pattern_drilldowns": [
    {
      "pattern_key": "...",
      "label": "...",
      "why_selected": "...",
      "share_of_rows": 0.80,
      "share_of_txns": 0.70,
      "avg_rows_per_txn": 500,
      "signal_flags": { "dominance": true, "anomaly": true },
      "busiest_minutes": [...],
      "representative_transactions": [...]
    }
  ]
}
```

## 兼容性说明

- `pattern_drilldowns` 是新增字段；忽略未知字段的现有 JSON 消费者不受影响
- 没有新增 CLI 参数或配置选项——钻取在信号阈值达到时自动触发
- Markdown 输出有意省略钻取（Top Patterns 分区在 Markdown 中不渲染）
- compare、trend 和 workflow 输出无变更

## 破坏性变更

无。
