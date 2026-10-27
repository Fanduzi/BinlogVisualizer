# 编码规范

English: [coding-standards.md](coding-standards.md)

这些规则适用于每一次改动，不管是人写的还是 agent 写的。由 CI 强制的规则都写明了怎么检查。现有的技术债记在一份只能往下调的基线里：旧代码不会挡住新改动，但任何改动都不能让它变得更糟。

## 1. 规模

- **文件：软上限 800 行**（非测试、非生成的 Go 文件）。新文件超过 800 行，CI 失败。已经超限的文件最多只能再长 20 行；需要更多空间时，先把代码搬出去。
- **函数：新代码圈复杂度不超过 40，目标 25 以内。** 已经超过 40 的函数不能再变复杂。
- **新函数：不超过 150 行或 100 条语句**（测试除外）。
- 长流程写成一串阶段：顶层函数每个阶段调用一个函数，每个阶段返回自己的结果或错误。

CI：`codehealth`（文件、复杂度），以及 golangci-lint 在改动行上的 `gocyclo`、`funlen`。

## 2. 锁

- **每个临界区只有一个加锁点和一个解锁点。** 把加锁的部分写成单独的函数，开头 `mu.Lock(); defer mu.Unlock()`。
- 不要在每个提前返回的地方解锁。函数必须中途放锁时，就在那里拆开：一个持锁的阶段函数，一个不持锁的。

```go
// 不好：每条退出路径都得记得解锁。
s.mu.Lock()
if err := a(); err != nil {
	s.mu.Unlock()
	return err
}
s.mu.Unlock()
return b()

// 好：一个临界区，一次解锁。
if err := s.lockedA(); err != nil {
	return err
}
return b()

func (s *S) lockedA() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return a()
}
```

## 3. 注释

- **注释解释为什么**这样写：约束、不变量、它防止的故障。
- **代码里不写 issue 编号、QA 轮次或 ADR 章节历史**（`#123`、`§29.10`、"第 3 轮修复"）。这些历史写进提交信息、PR 或 ADR。读者需要设计背景时，可以写 ADR 的路径（`see docs/adr/0011-....md`）。

CI：`codehealth` 统计 Go 注释里的 `#123`、`§4` 引用，数量不能上升。

## 4. 不留死代码

- 没用的代码直接删。不要"留着以后用"，不要留注释掉的代码，不要留没人调用的路径。
- **迁移到新路径时，在同一个改动里删掉旧路径**，或者在本 PR 写明的下一个 PR 里删。没有调用方需要时，不加兼容层。

CI：golangci-lint 在改动行上的 `unused`，以及 `deadcode` 计数，不能上升。

## 5. 测试断言行为

- 测试检查代码使用者能观察到的东西：输出、返回的错误、存下的状态、发出的事件。
- 行为坏了它要失败，只改实现时它要继续通过。不要断言私有调用顺序，也不要把实现抄进测试。
- 读源码或文档的守卫测试（lint 守卫、文档版本钉）在守卫本身就是目的时可以用；失败信息里写清楚该怎么修。

## 6. README 和文档不堆积

- README 和 landing page 介绍产品：它做什么、怎么安装当前版本、文档在哪。它们不是 changelog。
- 逐版本说明写进 release notes 和升级指南；功能细节写进 `docs/` 下的页面，从 README 链过去。
- README 形状守卫和 `make release-check`（`scripts/check-release-prep.sh`）负责检查。

## 7. 第三次打补丁之前先设计

- **同一块代码 QA 失败两次，就停止打补丁。** 在 `docs/adr/` 下新写或更新一份 ADR：失败的前提、备选方案、决定。评审通过后再实现。
- 同一个函数里补丁摞补丁，说明模型错了，而不是还差一个分支。

## 检查与命令

| 检查 | 在哪跑 | 什么时候失败 |
| --- | --- | --- |
| golangci-lint（`.golangci.yml`） | `lint` workflow、`make lint` | 改动行上出现新问题：errcheck、govet、staticcheck、ineffassign、unused、misspell、gocyclo > 40、funlen、gofmt、goimports |
| `codehealth check` | `lint` workflow、`make codehealth`，以及 `go test ./...`（`TestRepoWithinBaseline`，不含 deadcode） | 新文件超过 800 行、超限文件增长超过 20 行、新函数复杂度超过 40、已有超限函数变复杂、issue 引用变多、deadcode 变多 |
| `codehealth compare` | `lint` workflow | PR 调高了 `internal/codehealth/baseline.json` 里的任何上限 |

- 还完技术债后，`make codehealth-baseline` 把基线降到当前数字，任何一项要升高它都会拒绝。基线里还列着已经降到上限以下的文件或函数时，`TestRepoWithinBaseline` 会失败，这样上限会跟着债务一起往下走。
- `go run ./internal/codehealth/cmd/codehealth refs` 列出注释里的 issue 引用。
- golangci-lint 不带 `--new-from-rev` 运行时才会报改动行以外的已有问题；碰到那段代码时顺手修掉。
