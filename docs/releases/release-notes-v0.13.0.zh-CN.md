# BinlogViz v0.13.0 发布说明

发布日期：2026-04-11

## 范围

`v0.13.0` 是一个以 compare / trend 发现摘要的证据钻取为核心主题的功能版本。

这个版本建立在 `v0.12.0` 的变化解释层之上。现在，每条 finding 都可以携带结构化 `evidence_refs`，指向支撑该结论的报告章节，让用户可以从摘要直接跳到证据，而不需要手动在报告里翻找。

## 主要内容

- compare 的 `key_findings` 新增 `evidence_refs`
- trend 的 `trend_summary` 新增 `evidence_refs`
- `text`、`json`、`html` 三种输出都已渲染证据引用
- HTML 报告中 finding 可以点击跳转到支撑它的报告章节或行
- 加固 HTML finding 渲染，改用 DOM API 而不是 `innerHTML`，避免把报告数据当作 HTML 标记解释
- 增加 trend 锚点落点、锚点唯一性和 hostile finding content 的回归覆盖

## 兼容性说明

- 这个版本没有新增命令，也没有引入新的 parser 能力
- compare JSON 的 finding 对象现在可能包含 `evidence_refs`
- trend JSON 的 finding 对象现在可能包含 `evidence_refs`
- 现有 `key_findings` 和 `trend_summary` 字段保持兼容；低信号发现仍然可以省略 `evidence_refs`

## 破坏性变更

无。
