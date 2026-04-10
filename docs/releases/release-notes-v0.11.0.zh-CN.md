# BinlogViz v0.11.0 发布说明

发布日期：2026-04-10

## 范围

`v0.11.0` 是一个以 workflow v1 为核心主题的功能版本。

这个版本把 BinlogViz 从“一组独立但强大的命令”推进成了一条完整的工作流调查链路。现在，使用者可以在执行前校验 plan，用一份 YAML 文件跑多步调查，在执行后检查 workflow 状态，恢复不完整运行，并安全清理陈旧的生成物。

## 主要内容

- 新增 `workflow run <plan.yaml>`，可从一份声明式 YAML plan 执行多窗口 analyze、compare 和 trend 调查
- 将 `manifest.json` 和 `index.html` 变成一等 workflow 输出，分别承载机器可读状态和面向人的导航入口
- 新增 `workflow resume <output_dir>`，支持 resume-safe manifest 元数据、显式 `--rerun` 选择器，以及带依赖感知的复用/重跑规划
- 新增 `workflow validate <plan.yaml>` 与 `workflow describe <plan.yaml>`，分别用于执行前的 plan 校验和静态执行预览
- 新增 `workflow status <output_dir>`，用于只读运行态检查、artifact presence 检查和 dry resume preview
- 新增 `workflow clean <output_dir>`，作为默认 dry-run 的 maintenance 命令，用于清理孤儿 workflow artifacts 和可选的孤儿 snapshots
- 加强了 workflow 结果完整性、manifest 语义、snapshot overwrite 行为，以及 release-readiness 回归覆盖

## Workflow v1 能力面

workflow 生命周期现在已经完整覆盖：

- 跑前：`workflow validate`、`workflow describe`
- 执行：`workflow run`
- 落地入口：`index.html`
- 恢复：`workflow resume`
- 运行态检查：`workflow status`
- 维护清理：`workflow clean`

## 兼容性说明

- workflow manifest 现在使用 manifest v2 元数据，以支持可安全恢复的执行与检查
- 旧的 pre-v2 workflow manifest 仍然可以通过 `workflow status` 检查，但不能用于 resume
- `workflow clean` 以当前 manifest 为唯一真相源，只会删除 workflow artifact 范围内已失效的孤儿生成文件
- 现有的 analyze、compare、trend 和 snapshot 命令仍然保留，也仍可在 workflow 模式之外直接使用

## 破坏性变更

无。
