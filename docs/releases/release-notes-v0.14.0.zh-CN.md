# BinlogViz v0.14.0 发布说明

发布日期：2026-04-11

## 范围

`v0.14.0` 是一个以 workflow 交付闭环和面向操作者的 workflow 摘要为核心主题的功能版本。

这个版本把 workflow surface 推进成更完整的交付路径。workflow 输出现在会持久化聚合后的 findings 和 recommendations，`workflow status` 可以直接把这些摘要暴露给 CLI 和自动化用户，而 `workflow export` 则可以把整个 workflow root 打包成确定性的 zip bundle，便于分享和归档。

## 主要内容

- 在 `manifest.json` 和 `index.html` 中新增 workflow 级 summary 聚合
- 新增 workflow 级 recommendations / findings，来源于 compare / trend 报告摘要
- `workflow status` 现在在 text 和 JSON 输出中都支持持久化的 `workflow_summary`
- 新增 `workflow export`，可把 manifest 声明的 workflow artifacts 打包成确定性的 zip archive
- `workflow export` 通过显式 `--include-snapshots` 支持可选 snapshot inclusion
- 补强 workflow summary 契约、export path 归一化以及 workflow export containment 规则，并加入回归测试覆盖
- 在 CLI / output-format 参考文档中补充新的 workflow summary 和 export 契约

## 兼容性说明

- 现有 compare / trend finding kinds 没有删除或改名
- `workflow_summary` 仍然是 best-effort，不会改变 workflow 成功/失败语义
- `workflow status` 仍然是只读命令，不会重建 workflow summary
- `workflow export` 只打包 manifest 声明的 workflow 输出，以及 best-effort 的 `index.html` 和 rooted `plan.yaml`
- export archive 默认写到 `<output_dir>.zip`，并拒绝落在 workflow root 内部的 archive target

## 破坏性变更

无。
