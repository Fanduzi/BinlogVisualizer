# BinlogViz v0.15.0 发布说明

发布日期：2026-04-11

## 范围

`v0.15.0` 是一个以 workflow 信任边界安全加固为核心的安全增强版本。

本版本确保 `workflow resume` 和 `workflow status` 只会打开位于 workflow 输出根目录内的 plan 文件。在此之前，精心构造的 `manifest.json` 可以通过 `plan_path` 指向磁盘上的任意文件——包括通过符号链接、相对路径逃逸或绝对路径指向 workflow root 外部的文件。

## 主要内容

- 新增 `ValidateWorkflowPlanPath`，在任何文件 I/O 之前对 `plan_path` 进行规范化校验
- 新增符号链接逃逸检测：`plan_path` 解析到 workflow root 外部的 manifest 将被拒绝
- 收紧 plan 接受策略为仅限 rooted `plan.yaml`——嵌套路径（`sub/plan.yaml`）和重命名文件（`other-plan.yaml`）被拒绝
- `workflow resume` 在信任边界违规时立即硬失败，不会打开任何 plan 文件
- `workflow status` 优雅降级：不可信 plan 导致 non-resumable 状态而不会崩溃
- 所有调用方现在使用规范化后的解析路径，而非原始 `manifest.PlanPath` 值

## 兼容性说明

- `workflow resume` 和 `workflow status` 现在会拒绝 `plan_path` 指向 workflow root 外部的 manifest
- 仅接受 `<output_dir>/plan.yaml`——这已是 `workflow run` 的默认产物，因此现有 workflow 不受影响
- `plan_path` 为空或缺失的 manifest 继续按 non-resumable 处理（行为不变）

## 破坏性变更

对于由 `workflow run` 创建的 workflow 无破坏性变更。手动构造的、使用非标准 `plan_path` 值的 manifest 将被拒绝。
