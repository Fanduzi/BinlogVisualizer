# BinlogViz v0.17.0 发布说明

发布日期：2026-04-13

## 范围

`v0.17.0` 将模式钻取从单窗口 analyze 输出扩展到了跨窗口表面：**compare** 和 **trend**。

这个版本为高信号模式变化和模式趋势新增了有界解释性 drilldown，同时保持现有 compare/trend 的 findings 和 recommendations 语义不变。

## 主要内容

- 在 compare 和 trend 的 JSON 输出中新增顶层数组 `pattern_drilldowns`
- compare 现在会为 dominant、新出现、消失、或 row/txn 变化分歧明显的模式变化输出有界 drilldown
- trend 现在会为 dominant 份额变化、稳定上升/下降、以及集中式份额跳变输出有界 drilldown
- 文本输出会在符合条件的 compare/trend 模式下方渲染简短缩进的 `drilldown:` 块
- compare 和 trend 的 HTML 报告现在会在模式分区下方渲染带标签的 drilldown 详情卡片
- 有界载荷约束仍然成立：
  - 每个 compare 或 trend 报告最多 2 个 drilldown
  - 每个 drilldown 最多 2 个 key point

## JSON 契约新增

compare 和 trend 现在都会始终包含：

```json
{
  "pattern_drilldowns": [
    {
      "pattern_key": "...",
      "label": "...",
      "why_selected": "...",
      "signal_flags": { "...": true },
      "key_points": [
        { "label": "...", "summary": "..." }
      ]
    }
  ]
}
```

compare drilldown 关注 current 与 baseline 窗口之间的 row/txn 差异；trend drilldown 关注有序 snapshots 之间的 rows/share 变化。

## 兼容性说明

- `pattern_drilldowns` 是 compare 和 trend JSON 中的新增字段；忽略未知字段的现有消费者保持兼容
- 没有新增 CLI 参数或配置项
- workflow 行为不变；workflow 仍按原样消费 compare/trend 输出
- Markdown 行为不变；这些 drilldown 由 compare/trend 的 text、JSON 和 HTML 输出承载

## 破坏性变更

无。
