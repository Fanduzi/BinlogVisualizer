# BinlogViz v0.18.0 发布说明

发布日期：2026-04-16

## 范围

`v0.18.0` 把 BinlogViz 推进成了更面向 DBA 的事故分析工作流。

这个版本新增了多文件时间窗发现、更完整的 analyzer diagnostics、重新设计的 analyze HTML 报告、compare/trend 诊断差异，以及 analyze/compare/trend 主 HTML 表面的完整中英文本地化。

## 主要内容

- 新增基于目录的 analyze 发现模式：`--from-dir`、`--prefix`、`--start`、`--end`
- 新增 file probe 规划层与 `diagnostics.file_coverage`，让命中的 binlog 和被跳过的 binlog 都可见
- 新增 DDL timeline、hot interval、TPS 与 event mix 序列、binlog throughput 分段和 transaction evidence
- analyze HTML 现在包含更大的图表，以及 Top Tables 行内 drilldown、File Coverage、Binlog Throughput、Transaction Evidence 分区
- compare 和 trend 现在会消费 diagnostics/timeseries 来渲染更适合 DBA 的差异与趋势
- HTML 报告现在会在 analyze、compare、trend 三个表面稳定遵循 `--lang`
- 在 normalize、transaction building、store batching、minute-bucket draining 路径上降低了 streaming 分配压力

## JSON 契约新增

analyze 报告现在会新增可选的顶层 `timeseries` 和 `diagnostics` 载荷：

```json
{
  "timeseries": {
    "tps_series": [],
    "rows_series": [],
    "events_series": []
  },
  "diagnostics": {
    "file_coverage": {
      "selected": [],
      "skipped": []
    },
    "ddl_events": [],
    "hot_intervals": [],
    "largest_transactions": [],
    "longest_transactions": [],
    "widest_transactions": [],
    "file_segments": []
  }
}
```

compare 和 trend 会消费这些新增结构，同时继续兼容不包含它们的旧 snapshot。

## 兼容性说明

- 现有 `binlogviz analyze <files...>` 用法保持不变
- 新增 JSON 字段都是 additive；忽略未知字段的现有消费者保持兼容
- compare 和 trend 仍然可以处理不带 `diagnostics` 或 `timeseries` 的旧 snapshot
- release artifact 仍然是按平台打包的 tarball 加 checksum manifest

## 破坏性变更

无。
