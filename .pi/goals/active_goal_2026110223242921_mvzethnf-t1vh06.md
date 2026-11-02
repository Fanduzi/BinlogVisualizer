{
  "version": 3,
  "id": "mvzethnf-t1vh06",
  "objective": "让 `--schema-file` 提供的表定义成为列名、类型事实和主键身份的正式来源：`flashback` 在 MySQL 8.x `binlog_row_metadata=MINIMAL` 和 MySQL 5.7 binlog 上可用（与 `redo` 对齐），同一份定义也用于 `analyze --show-rows` 的列名；采信 DDL 主键但在输出中标注来源，所有现有安全拒绝保持不变。",
  "status": "active",
  "autoContinue": true,
  "usage": {
    "tokensUsed": 1020418,
    "activeSeconds": 108
  },
  "sisyphus": false,
  "createdAt": "2026-11-02T15:24:29.211Z",
  "updatedAt": "2026-11-02T15:26:19.397Z",
  "activePath": ".pi/goals/active_goal_2026110223242921_mvzethnf-t1vh06.md",
  "revision": 8,
  "scheduler": {
    "version": 1,
    "owner": "01a197ab-f821-77d0-b708-8f589287c3ff",
    "generation": "39f2691a-4580-49ee-b88c-c86be940d859",
    "used": 1,
    "phase": "running",
    "repairUsed": false,
    "decision": {
      "kind": "ready",
      "purpose": "ready"
    },
    "dispatch": {
      "id": "8591a389-5d0a-46ac-9932-796a4af65af0",
      "kind": "ready",
      "claimedAt": 1793633069441
    }
  },
  "taskList": {
    "tasks": [
      {
        "id": "adr-identity",
        "title": "写 ADR：schema 定义作为列名/类型/主键来源的规则",
        "status": "pending",
        "verificationContract": "docs/adr/ 新 ADR 记录：采信 DDL 主键并标注来源、DDL 无主键时退回全列 + LIMIT 1、schema 与 binlog 矛盾仍拒绝、MariaDB 与 DDL 撤销仍在范围外；与 ADR 0022（redo）并列且不矛盾"
      },
      {
        "id": "capture-gate",
        "title": "解除采集层早退：让 FlashRow 能表达\"位置式但可解析\"",
        "status": "pending",
        "verificationContract": "internal/binlog/flashback.go 不再在 names != FULL 时直接标记 FlashProblemNames；新增单测覆盖 MINIMAL + 有定义 / MINIMAL + 无定义两条路径，后者仍以 row_metadata 拒绝"
      },
      {
        "id": "catalog-pk",
        "title": "schemacatalog 从 DDL 抓 PRIMARY KEY，SchemaCol 携带主键标记",
        "status": "pending",
        "verificationContract": "建表解析产出主键列集合（单列与多列）；mysqlexpr.SchemaCol 有新字段；单测覆盖 PRIMARY KEY / 复合主键 / 无主键 / 事后 ALTER 变更"
      },
      {
        "id": "undo-address",
        "title": "flashback 用定义来源的主键做定位与冲突匹配，并在输出标注来源",
        "status": "pending",
        "verificationContract": "脚本注释与 stderr 出现 schema-file 来源标记；--plan 的 JSON 报告同一来源；DDL 主键与 binlog 可比事实矛盾时仍 exit 6；无主键时保持全列 + LIMIT 1 且注释说明"
      },
      {
        "id": "analyze-schema-flag",
        "title": "analyze 新增 --schema-file，--show-rows 用真实列名",
        "status": "pending",
        "verificationContract": "analyze --show-rows --schema-file 在 MINIMAL binlog 上打印真实列名（text 与 JSON）；JSON 仅新增字段、report_version 仍为 4；docs/schema/analyze.json 由生成器重写并通过 TestSchema*"
      },
      {
        "id": "e2e-minimal-80",
        "title": "MySQL 8.0 MINIMAL 端到端：生成、verify、应用、逐行比对",
        "status": "pending",
        "verificationContract": "在 binlog_row_metadata=MINIMAL 的 8.0 服务上：flashback --schema-file 生成脚本 → flashback verify | mysql 应用 → 表格快照与应用前逐行一致；同一测试在 CI 的 8.0 容器里 PASS（不接受 SKIP）"
      },
      {
        "id": "e2e-mysql57",
        "title": "MySQL 5.7.44 端到端：同一路径 + 真库应用",
        "status": "pending",
        "verificationContract": "5.7.44 容器（binlog_row_image=FULL）上完成生成、verify、应用、逐行比对；接到现有 5.7 CI job，SKIP 视为失败"
      },
      {
        "id": "docs-and-deliver",
        "title": "文档同步并通过 PR 交付（含关闭 #482）",
        "status": "pending",
        "verificationContract": "limitations/flashback/cli/analyze-behavior 的 EN+zh 更新（含\"必须用事故当时的转储\"与漂移风险）；本地 go test ./...、make lint、make codehealth、全仓链接检查全绿；PR 的必需检查全过、rebase 合并进 main，并在 #482 说明落地范围"
      }
    ],
    "blockCompletion": true,
    "proposedAt": "2026-11-02T15:21:00.499Z"
  }
}

# Goal Prompt

让 `--schema-file` 提供的表定义成为列名、类型事实和主键身份的正式来源：`flashback` 在 MySQL 8.x `binlog_row_metadata=MINIMAL` 和 MySQL 5.7 binlog 上可用（与 `redo` 对齐），同一份定义也用于 `analyze --show-rows` 的列名；采信 DDL 主键但在输出中标注来源，所有现有安全拒绝保持不变。

## Progress

- Status: running
- Auto-continue: on
- Sisyphus mode: no
- Time spent: 1m48s
- Tokens used: 1M (1,020,418) tokens
## Tasks

<!-- blockCompletion: true -->
- [ ] adr-identity: 写 ADR：schema 定义作为列名/类型/主键来源的规则 — contract: docs/adr/ 新 ADR 记录：采信 DDL 主键并标注来源、DDL 无主键时退回全列 + LIMIT 1、schema 与 binlog 矛盾仍拒绝、MariaDB 与 DDL 撤销仍在范围外；与 ADR 0022（redo）并列且不矛盾
- [ ] capture-gate: 解除采集层早退：让 FlashRow 能表达"位置式但可解析" — contract: internal/binlog/flashback.go 不再在 names != FULL 时直接标记 FlashProblemNames；新增单测覆盖 MINIMAL + 有定义 / MINIMAL + 无定义两条路径，后者仍以 row_metadata 拒绝
- [ ] catalog-pk: schemacatalog 从 DDL 抓 PRIMARY KEY，SchemaCol 携带主键标记 — contract: 建表解析产出主键列集合（单列与多列）；mysqlexpr.SchemaCol 有新字段；单测覆盖 PRIMARY KEY / 复合主键 / 无主键 / 事后 ALTER 变更
- [ ] undo-address: flashback 用定义来源的主键做定位与冲突匹配，并在输出标注来源 — contract: 脚本注释与 stderr 出现 schema-file 来源标记；--plan 的 JSON 报告同一来源；DDL 主键与 binlog 可比事实矛盾时仍 exit 6；无主键时保持全列 + LIMIT 1 且注释说明
- [ ] analyze-schema-flag: analyze 新增 --schema-file，--show-rows 用真实列名 — contract: analyze --show-rows --schema-file 在 MINIMAL binlog 上打印真实列名（text 与 JSON）；JSON 仅新增字段、report_version 仍为 4；docs/schema/analyze.json 由生成器重写并通过 TestSchema*
- [ ] e2e-minimal-80: MySQL 8.0 MINIMAL 端到端：生成、verify、应用、逐行比对 — contract: 在 binlog_row_metadata=MINIMAL 的 8.0 服务上：flashback --schema-file 生成脚本 → flashback verify | mysql 应用 → 表格快照与应用前逐行一致；同一测试在 CI 的 8.0 容器里 PASS（不接受 SKIP）
- [ ] e2e-mysql57: MySQL 5.7.44 端到端：同一路径 + 真库应用 — contract: 5.7.44 容器（binlog_row_image=FULL）上完成生成、verify、应用、逐行比对；接到现有 5.7 CI job，SKIP 视为失败
- [ ] docs-and-deliver: 文档同步并通过 PR 交付（含关闭 #482） — contract: limitations/flashback/cli/analyze-behavior 的 EN+zh 更新（含"必须用事故当时的转储"与漂移风险）；本地 go test ./...、make lint、make codehealth、全仓链接检查全绿；PR 的必需检查全过、rebase 合并进 main，并在 #482 说明落地范围

