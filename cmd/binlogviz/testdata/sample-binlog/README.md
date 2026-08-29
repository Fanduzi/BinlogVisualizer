# Sample ROW binlog (discovery layout)

`mysql-bin.000001` is the same 1500-byte MySQL 5.7 ROW fixture as
`../minimal.binlog`, renamed so `workflow` discovery (`prefix: mysql-bin.`)
can resolve it.

This directory is the `from_dir` used by the repository-root `incident.yaml`
sample plan. Release archives copy it to `testdata/sample-binlog/` next to a
bundled `incident.yaml` whose `from_dir` is that archive-relative path.
