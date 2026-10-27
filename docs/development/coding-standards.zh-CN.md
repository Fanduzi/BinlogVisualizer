# 编码规范

English: [coding-standards.md](coding-standards.md)

这些规则适用于每一次改动，不管是人写的还是 agent 写的。由 CI 强制的规则都写明了怎么检查。现有的技术债记在一份只能往下调的基线里：旧代码不会挡住新改动，但任何改动都不能让它变得更糟。

## 1. 规模

- **文件：软上限 800 行**，测试文件也算。新文件超过 800 行，CI 失败。已经超限的文件最多比自己的上限多 20 行。需要更多空间时，先把代码搬出去。
- **函数：圈复杂度不超过 40，目标 25 以内。** 测试函数、赋给包级变量的函数字面量都算。已经超过 40 的函数不能再变复杂，加 1 也不行。
- **函数：不超过 150 行、100 条语句**，按 golangci-lint `funlen` 的算法计数：`func` 那一行和右花括号之间的行数，函数里每个注释减一行。已经超限的函数最多比上限多 5 行或 5 条语句，小修补不必先拆函数；复杂度没有这个余量。测试函数不限长度：表驱动测试本来就长，但复杂度照样算。
- 长流程写成一串阶段：顶层函数每个阶段调用一个函数，每个阶段返回自己的结果或错误。

CI：`codehealth`（文件、复杂度、函数行数和语句数），以及 golangci-lint 的 `gocyclo`、`funlen`（只看新问题）。

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

CI：`codehealth` 按文件统计 Go 注释里不会是普通行文的引用写法：非单词字符后面的 `#123`（`Column`、`Field` 这类词后面的不算）、`owner/repo#123`、`GH-123`、`PR #123`、`issue 123`、`issue: 123`、`issue number 123`、`Issues 123`、`issues/123` 和 `pull/123`（URL）、`QA r6`、`r3 review`。全角 `＃１２３` 按 `#123` 算。每个文件的数量不能上升，没有引用的文件不能新增。计数器优先保证不误报：`round 3`、`§9.2`、`bug 12`、`see 12`、`PR-3` 和单独的 `(12)` 不计数，因为同样的字眼在行文、SQL 或 binlog 位置里很常见（`Column #3`、`The server issues 2 events`、`mysql-bin/000001#4`）。评审时这条规则对它们照样适用；规则针对的是历史本身，不是写法。

## 4. 不留死代码

- 没用的代码直接删。不要"留着以后用"，不要留注释掉的代码，不要留没人调用的路径。
- **迁移到新路径时，在同一个改动里删掉旧路径**，或者在本 PR 写明的下一个 PR 里删。没有调用方需要时，不加兼容层。

CI：golangci-lint 的 `unused`（只看新问题），以及 `deadcode`：每个不可达函数按名字记在基线里，新增一个就失败，并报出函数名和位置。deadcode 不带 `-test` 运行，所以只被测试调用的函数也算死代码；带 `-generated` 运行，所以生成文件也算，只有 `generated_files` 里的文件除外。不可达函数不能靠改名或拆分进基线，直接删掉。用 `go run ./internal/codehealth/cmd/codehealth deadcode` 列出来。

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

## 8. 基线

`internal/codehealth/baseline.json` 给每个超限的文件或函数记一个上限：`files`、`test_files`、`complex_functions`、`test_complex_functions`、`long_functions`、`statement_functions`、`issue_refs`（按文件）和 `deadcode`（按函数）。没有合计值，一个条目省出来的空间不能挪给另一个条目用。里面没有任何东西由作者声明：代码从哪来，CI 拿内容和基准提交的代码树比出来。

- **check**（`codehealth check`，`go test ./...` 里的 `TestRepoWithinBaseline` 也跑）在这些情况下失败：超限的值没有条目，或者高于上限加容差。容差是文件 20 行，长函数 5 行或 5 条语句，复杂度、issue 引用和 deadcode 为 0。值等于或低于上限永远通过，所以两个合并进来的改动还了同一笔债，main 也不会变红。
- **compare**（`codehealth -base REV compare`）用 `git archive` 取出基准提交的代码树，读它的基线，两棵树都量一遍。限制不能放宽，`generated_files` 不能改，已有条目不能升高。
- **拆分和改名靠推导，不靠声明。** 跑 `make codehealth-baseline`：它降低上限，并给每个新的超限文件或函数加条目。compare 只在新条目至少 80% 的有效行（非空、不只是标点、不是 package 子句或 import）原样出现在同类的某个基线条目（按基准提交上的内容）里时才接受它，那个条目就是它的来源。对每个来源：所有新块加上来源剩下的部分（都按当前实测），不能超过来源的上限加容差，再加上第一块之后每块的余量：文件 40 行（package 子句、import、文档注释），函数 1 复杂度、3 行或 2 条语句，issue 引用 0。改名就是一块，所以改名加少量增长和不改名的同样增长效果一样。函数按函数体匹配，所以 `App.Run` 改名成 `RunMain` 仍然沿用原条目。复制文件时原文件的行还留在原文件里，所以副本会失败。
- **还债要记下来。** 在 PR 上，如果改动把某个值降了至少 50 行（文件）、5 复杂度、20 行或 10 条语句（函数）、3 个 issue 引用，却没有降低它的上限，compare 失败：跑 `make codehealth-baseline`。只算这个改动自己改变的值，所以通过别的合并进 main 的还债不会让后来的 PR 失败。低于这个阈值的下降可以不记，这几行以后还能再用；容差本来就允许这么多。
- **生成文件。** `generated_files` 把路径映射到重新生成它的命令。只跳过列出的路径，任何文件头或标记都豁免不了。PR 里不能改这个列表：compare 会失败，这是有意的，新增生成器要仓库所有者手动放行。CI 运行每条命令（`codehealth regen`），再跑 `git diff --exit-code`，所以列出的文件必须和生成器的输出逐字节一致，手改会失败。golangci-lint 同样不认标记（`generated: disable`），只跳过列出的路径。目前列表是空的。
- **搬动代码上的 lint。** `make lint` 和 CI 用本树的配置对基准树和本树各跑一次 golangci-lint，本树有而基准树没有的问题才失败。一个问题由 linter、消息（数字和函数自己的名字归一化）和所在函数确定，从不看行号。本树里的函数对应基准树里同键的函数；没有同键的，就对应包含它至少 80% 函数体的基准函数。所以拆分或改名时随代码搬走的问题仍然对得上，新问题对不上。
- **所有 Go 文件都查。** codehealth 检查 `.git` 以外的所有 `.go` 文件，包括 `testdata`、`vendor`、`node_modules`、`_` 和 `.` 开头的目录，因为总有工具会编译或发布它们。`testdata` 以外的嵌套 `go.mod` 会让 check 失败：`go test ./...`、golangci-lint 和 deadcode 都覆盖不到嵌套模块，把代码搬进主模块。`testdata` 下不是合法 Go 的文件只按行数计并报告出来；在别处则是错误。
- **`//nolint`** 必须写明 linter，并给出至少三个词的理由（`//nolint:errcheck // close error is logged by the caller`）。理由由 `nolintlint` 要求；`// fine` 这种敷衍理由由 codehealth check 拒绝。
- CI 不检查的：锁、注释意图、测试和设计这几条（第 2、3、5、7 节）是评审时的约定。门禁的代码和 workflow 都从 PR 自己的代码树运行，PR 可以改它们，评审时要看这些改动。

## 检查与命令

| 检查 | 在哪跑 | 什么时候失败 |
| --- | --- | --- |
| `codehealth -base REV lint`（golangci-lint，`.golangci.yml`） | `lint` workflow、`make lint` | 基准树没有的问题：errcheck、govet、staticcheck、ineffassign、unused、misspell、nolintlint、gocyclo > 40、funlen、gofmt、goimports |
| `codehealth check` | `lint` workflow、`make codehealth`，以及 `go test ./...`（`TestRepoWithinBaseline`，不含 deadcode） | 新的超限文件或函数没有条目、条目高于上限加容差、新的 issue 引用、新的不可达函数、嵌套模块、敷衍的 `//nolint` 理由 |
| `codehealth regen` 加 `git diff --exit-code` | `lint` workflow、`make codehealth-regen` | `generated_files` 里的文件（或任何其他文件）和命令生成的不一样 |
| `codehealth -base REV compare` | `lint` workflow（PR 上带 `-paydown`） | 基线比基准提交上的宽松：调高了限制、改了 `generated_files`、调高了条目、新条目推导不出来源或和来源一起超过上限加余量、还了债没记 |

- `make codehealth-baseline` 把基线降到当前数字，删掉已经不超限的条目，并给拆分或改名的代码加条目。任何一项要升高它都会拒绝。
- `go run ./internal/codehealth/cmd/codehealth refs` 列出注释里的 issue 引用；`go run ./internal/codehealth/cmd/codehealth deadcode` 列出不可达函数。
- 直接运行 golangci-lint（`golangci-lint run ./...`）会把已有问题也报出来；碰到那段代码时顺手修掉。
