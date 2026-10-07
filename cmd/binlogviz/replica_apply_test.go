// Package binlogviz checks replica apply delay against a real MySQL 8.0 source and replica pair.
// input: mysql-8.0.46-replica-apply.binlog, mysql-8.0.46-source-apply.binlog, and their mysqlbinlog -vv dumps, plus minimal.binlog and mariadb-10.11.14-dml.binlog.
// output: text, Markdown, HTML, and JSON match the dump's original_commit_timestamp and immediate_commit_timestamp lines; filters, --top, and --sql-context off keep that contract.
// pos: command-layer proof for the replica apply-delay section.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"encoding/json"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/i18n"
)

// These are the checked-in replica fixture, read from mysqlbinlog -vv.
// A regeneration must update them together with the dump.
const (
	replicaApplyTopGTID = "11458b63-c244-11f1-a0d2-822b383dbcd0:7"
	replicaApplyMaxUs   = int64(5426519)
	replicaApplyP95Us   = int64(5208595)
	replicaApplyTopPos  = int64(197)
	replicaApplyTxns    = 22
	replicaApplyPeak    = "2026-10-07T11:41:00Z"
)

func TestAnalyzeReplicaApplyDelayMatchesMysqlbinlog(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	fixture := mustFixturePath(t, "mysql-8.0.46-replica-apply.binlog")
	dump := parseCommitDump(t, strings.TrimSuffix(fixture, ".binlog")+".mysqlbinlog.txt")
	if len(dump) != replicaApplyTxns {
		t.Fatalf("dump transactions = %d, want %d", len(dump), replicaApplyTxns)
	}
	maxUs, p95Us, peak, top := rankDump(dump)
	if top.GTID != replicaApplyTopGTID || top.At != replicaApplyTopPos || maxUs != replicaApplyMaxUs || p95Us != replicaApplyP95Us {
		t.Fatalf("dump pins max=%d p95=%d gtid=%s at=%d, want max=%d p95=%d gtid=%s at=%d",
			maxUs, p95Us, top.GTID, top.At, replicaApplyMaxUs, replicaApplyP95Us, replicaApplyTopGTID, replicaApplyTopPos)
	}
	if peak.UTC().Format(time.RFC3339) != replicaApplyPeak {
		t.Fatalf("dump peak minute %s, want %s", peak.UTC().Format(time.RFC3339), replicaApplyPeak)
	}
	if top.Tables["shop.audit"] != 1 || len(top.Tables) != 1 {
		t.Fatalf("top tables = %+v", top.Tables)
	}

	stdout, stderr, err := executeAnalyzeLikeMain(t, fixture, "--format", "json", "--top", "30")
	if err != nil {
		t.Fatalf("analyze json: %v\n%s", err, stderr)
	}
	payload := decodeApplyPayload(t, stdout)
	if payload.Summary.TotalTransactions != replicaApplyTxns {
		t.Fatalf("total_transactions = %d", payload.Summary.TotalTransactions)
	}
	for _, alert := range payload.Alerts {
		if alert.Kind == "replica_apply_delay" {
			t.Fatalf("delay became a finding: %+v", payload.Alerts)
		}
	}
	delay := decodeApplyDelay(t, payload.ReplicaApplyDelay)
	if delay.Origin != "replica" || delay.MaxDelayUs != maxUs || delay.P95DelayUs != p95Us || delay.PeakMinute != peak.UTC().Format(time.RFC3339) {
		t.Fatalf("summary = %+v", delay)
	}
	if len(delay.Transactions) != len(dump) {
		t.Fatalf("listed %d transactions, dump has %d", len(delay.Transactions), len(dump))
	}
	ranked := append([]dumpTxn(nil), dump...)
	sortDump(ranked)
	for i, txn := range delay.Transactions {
		want := ranked[i]
		if txn.GTID != want.GTID || txn.TxnStartPos != want.At || txn.TxnStartFile != fixture {
			t.Fatalf("txn %d identity = %+v, want gtid=%s file=%s pos=%d", i, txn, want.GTID, fixture, want.At)
		}
		if txn.OriginalCommitUs != want.Original || txn.ImmediateCommitUs != want.Immediate || txn.DelayUs != int64(want.Immediate-want.Original) {
			t.Fatalf("txn %s timestamps orig=%d imm=%d delay=%d, dump orig=%d imm=%d", txn.GTID, txn.OriginalCommitUs, txn.ImmediateCommitUs, txn.DelayUs, want.Original, want.Immediate)
		}
		if txn.OriginalCommitTime == "" || txn.ImmediateCommitTime == "" {
			t.Fatalf("txn %s omitted a commit time", txn.GTID)
		}
		if !sameTables(txn.Tables, want.Tables) {
			t.Fatalf("txn %s tables = %+v, dump %+v", txn.GTID, txn.Tables, want.Tables)
		}
	}
	assertNoEmptyJSONStrings(t, payload.ReplicaApplyDelay)

	text, stderr, err := executeAnalyzeLikeMain(t, fixture, "--format", "text", "--top", "30")
	if err != nil {
		t.Fatalf("analyze text: %v\n%s", err, stderr)
	}
	section := sectionBetweenHeadings(t, text, "=== Replica Apply Delay ===", "===")
	if !strings.Contains(section, "This assumes the source and replica clocks agree.") {
		t.Fatalf("clock sentence missing:\n%s", section)
	}
	if !strings.Contains(section, "Replica: at least one transaction committed here after it committed on the source.") {
		t.Fatalf("replica line missing:\n%s", section)
	}
	stats := "max " + (time.Duration(maxUs) * time.Microsecond).String() + "  p95 " + (time.Duration(p95Us) * time.Microsecond).String() + "  peak minute " + peak.UTC().Format("2006-01-02 15:04:05 UTC")
	if !strings.Contains(section, stats) {
		t.Fatalf("stats %q missing:\n%s", stats, section)
	}
	if busiest := strings.Index(text, "=== Busiest Minutes ==="); busiest < 0 || busiest > strings.Index(text, "=== Replica Apply Delay ===") {
		t.Fatalf("delay section is not after busiest minutes")
	}
	first := "gtid=" + top.GTID + "  " + fixture + ":" + strconv.FormatInt(top.At, 10)
	if !strings.Contains(section, first) || !strings.Contains(section, "shop.audit  1") {
		t.Fatalf("top transaction line missing:\n%s", section)
	}
	if !strings.Contains(section, "original="+formatDumpMicros(top.Original)) || !strings.Contains(section, "immediate="+formatDumpMicros(top.Immediate)) {
		t.Fatalf("commit times missing:\n%s", section)
	}
	if strings.Contains(section, "BEGIN") || strings.Contains(section, "lag-audit") {
		t.Fatalf("delay section leaked statement text:\n%s", section)
	}

	md, stderr, err := executeAnalyzeLikeMain(t, fixture, "--format", "markdown", "--top", "30")
	if err != nil {
		t.Fatalf("analyze markdown: %v\n%s", err, stderr)
	}
	if !strings.Contains(md, "## Replica Apply Delay") || !strings.Contains(md, top.GTID) || !strings.Contains(md, stats) {
		t.Fatalf("markdown delay section missing the top transaction")
	}

	html, stderr, err := executeAnalyzeLikeMain(t, fixture, "--format", "html", "--top", "30")
	if err != nil {
		t.Fatalf("analyze html: %v\n%s", err, stderr)
	}
	activity := strings.Index(html, `id="section-activity"`)
	delayAt := strings.Index(html, `id="section-replica-delay"`)
	objects := strings.Index(html, `id="section-objects"`)
	if activity < 0 || delayAt < activity || objects < delayAt {
		t.Fatalf("html section order activity=%d delay=%d objects=%d", activity, delayAt, objects)
	}
	if !strings.Contains(html, top.GTID) || !strings.Contains(html, "This assumes the source and replica clocks agree.") {
		t.Fatalf("html delay section missing the clock note or top GTID")
	}
}

func TestAnalyzeReplicaApplyDelayRespectsTopFiltersAndSQLContext(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	fixture := mustFixturePath(t, "mysql-8.0.46-replica-apply.binlog")
	dump := parseCommitDump(t, strings.TrimSuffix(fixture, ".binlog")+".mysqlbinlog.txt")

	limited, stderr, err := executeAnalyzeLikeMain(t, fixture, "--format", "json", "--top", "1")
	if err != nil {
		t.Fatalf("top 1: %v\n%s", err, stderr)
	}
	limitedDelay := decodeApplyDelay(t, decodeApplyPayload(t, limited).ReplicaApplyDelay)
	if limitedDelay.MaxDelayUs != replicaApplyMaxUs || limitedDelay.P95DelayUs != replicaApplyP95Us || len(limitedDelay.Transactions) != 1 || limitedDelay.Transactions[0].GTID != replicaApplyTopGTID {
		t.Fatalf("--top 1 changed the summary or the leader: %+v", limitedDelay)
	}

	orders := filterDump(dump, func(txn dumpTxn) bool { return txn.Tables["shop.orders"] > 0 })
	for i := range orders {
		if orders[i].GTID == "11458b63-c244-11f1-a0d2-822b383dbcd0:8" {
			orders[i].Tables = map[string]int{"shop.orders": 1}
		}
	}
	ordersMax, ordersP95, _, ordersTop := rankDump(orders)
	if ordersTop.GTID != "11458b63-c244-11f1-a0d2-822b383dbcd0:8" || ordersMax != 5208595 {
		t.Fatalf("include-table expectation gtid=%s max=%d", ordersTop.GTID, ordersMax)
	}
	filtered, stderr, err := executeAnalyzeLikeMain(t, fixture, "--format", "json", "--top", "30", "--include-table", "shop.orders")
	if err != nil {
		t.Fatalf("include-table: %v\n%s", err, stderr)
	}
	filteredDelay := decodeApplyDelay(t, decodeApplyPayload(t, filtered).ReplicaApplyDelay)
	if filteredDelay.MaxDelayUs != ordersMax || filteredDelay.P95DelayUs != ordersP95 || filteredDelay.Transactions[0].GTID != ordersTop.GTID {
		t.Fatalf("include-table delay = %+v, want max=%d p95=%d gtid=%s", filteredDelay, ordersMax, ordersP95, ordersTop.GTID)
	}
	if _, ok := filteredDelay.Transactions[0].Tables["shop.audit"]; ok || filteredDelay.Transactions[0].Tables["shop.catalog"] != 0 {
		t.Fatalf("include-table kept a filtered table: %+v", filteredDelay.Transactions[0].Tables)
	}
	for _, txn := range filteredDelay.Transactions {
		if strings.Contains(txn.GTID, ":7") {
			t.Fatalf("audit transaction survived --include-table shop.orders")
		}
	}

	deletes := filterDump(dump, func(txn dumpTxn) bool { return txn.Deletes["shop.orders"] > 0 })
	deleteMax, deleteP95, _, deleteTop := rankDump(deletes)
	if len(deletes) != 1 || deleteTop.GTID != "11458b63-c244-11f1-a0d2-822b383dbcd0:27" || deleteMax != 2055087 || deleteP95 != deleteMax || deleteTop.Deletes["shop.orders"] != 4 {
		t.Fatalf("delete expectation %+v max=%d", deletes, deleteMax)
	}
	deleted, stderr, err := executeAnalyzeLikeMain(t, fixture, "--format", "json", "--top", "30", "--dml", "delete")
	if err != nil {
		t.Fatalf("dml delete: %v\n%s", err, stderr)
	}
	deletedDelay := decodeApplyDelay(t, decodeApplyPayload(t, deleted).ReplicaApplyDelay)
	if deletedDelay.MaxDelayUs != deleteMax || len(deletedDelay.Transactions) != 1 || deletedDelay.Transactions[0].GTID != deleteTop.GTID || deletedDelay.Transactions[0].Tables["shop.orders"] != 4 {
		t.Fatalf("dml delete delay = %+v", deletedDelay)
	}

	only, stderr, err := executeAnalyzeLikeMain(t, fixture, "--format", "json", "--include-gtids", replicaApplyTopGTID)
	if err != nil {
		t.Fatalf("include-gtids: %v\n%s", err, stderr)
	}
	onlyDelay := decodeApplyDelay(t, decodeApplyPayload(t, only).ReplicaApplyDelay)
	if onlyDelay.MaxDelayUs != replicaApplyMaxUs || onlyDelay.P95DelayUs != replicaApplyMaxUs || len(onlyDelay.Transactions) != 1 {
		t.Fatalf("include-gtids delay = %+v", onlyDelay)
	}

	excluded, stderr, err := executeAnalyzeLikeMain(t, fixture, "--format", "json", "--top", "1", "--exclude-gtids", replicaApplyTopGTID)
	if err != nil {
		t.Fatalf("exclude-gtids: %v\n%s", err, stderr)
	}
	excludedDelay := decodeApplyDelay(t, decodeApplyPayload(t, excluded).ReplicaApplyDelay)
	if excludedDelay.MaxDelayUs != 5208595 || excludedDelay.Transactions[0].GTID == replicaApplyTopGTID {
		t.Fatalf("exclude-gtids delay = %+v", excludedDelay)
	}

	windowed := filterDump(dump, func(txn dumpTxn) bool { return txn.At >= 509 })
	windowMax, _, _, windowTop := rankDump(windowed)
	positioned, stderr, err := executeAnalyzeLikeMain(t, fixture, "--format", "json", "--top", "1", "--start-position", "509")
	if err != nil {
		t.Fatalf("start-position: %v\n%s", err, stderr)
	}
	positionDelay := decodeApplyDelay(t, decodeApplyPayload(t, positioned).ReplicaApplyDelay)
	if positionDelay.MaxDelayUs != windowMax || positionDelay.Transactions[0].GTID != windowTop.GTID {
		t.Fatalf("start-position delay = %+v, want max=%d gtid=%s", positionDelay, windowMax, windowTop.GTID)
	}

	quiet, stderr, err := executeAnalyzeLikeMain(t, fixture, "--format", "json", "--top", "1", "--sql-context", "off")
	if err != nil {
		t.Fatalf("sql-context off: %v\n%s", err, stderr)
	}
	if strings.Contains(quiet, "BEGIN") || strings.Contains(quiet, "lag-audit") {
		t.Fatalf("sql-context off leaked statement text")
	}
	quietDelay := decodeApplyDelay(t, decodeApplyPayload(t, quiet).ReplicaApplyDelay)
	if quietDelay.Transactions[0].TxnStartPos != replicaApplyTopPos || quietDelay.Transactions[0].OriginalCommitUs == 0 || quietDelay.Transactions[0].ImmediateCommitUs == 0 {
		t.Fatalf("sql-context off dropped timestamps: %+v", quietDelay.Transactions[0])
	}
}

func TestAnalyzeSourceApplyDelayIsOneLine(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	fixture := mustFixturePath(t, "mysql-8.0.46-source-apply.binlog")
	dump := parseCommitDump(t, strings.TrimSuffix(fixture, ".binlog")+".mysqlbinlog.txt")
	for _, txn := range dump {
		if txn.Original != txn.Immediate {
			t.Fatalf("source dump delay is not zero: %+v", txn)
		}
	}
	_, _, peak, _ := rankDump(dump)

	stdout, stderr, err := executeAnalyzeLikeMain(t, fixture, "--format", "json", "--top", "30")
	if err != nil {
		t.Fatalf("source json: %v\n%s", err, stderr)
	}
	payload := decodeApplyPayload(t, stdout)
	var raw map[string]any
	if err := json.Unmarshal(payload.ReplicaApplyDelay, &raw); err != nil {
		t.Fatal(err)
	}
	if _, ok := raw["transactions"]; ok {
		t.Fatalf("source JSON included a ranking: %s", payload.ReplicaApplyDelay)
	}
	delay := decodeApplyDelay(t, payload.ReplicaApplyDelay)
	if delay.Origin != "source" || delay.MaxDelayUs != 0 || delay.P95DelayUs != 0 || delay.PeakMinute != peak.UTC().Format(time.RFC3339) {
		t.Fatalf("source delay = %+v, peak want %s", delay, peak.UTC().Format(time.RFC3339))
	}

	text, stderr, err := executeAnalyzeLikeMain(t, fixture, "--format", "text")
	if err != nil {
		t.Fatalf("source text: %v\n%s", err, stderr)
	}
	section := sectionBetweenHeadings(t, text, "=== Replica Apply Delay ===", "===")
	if !strings.Contains(section, "This assumes the source and replica clocks agree.") {
		t.Fatalf("source clock sentence missing:\n%s", section)
	}
	if !strings.Contains(section, "Source: original and immediate commit timestamps are equal.") {
		t.Fatalf("source conclusion missing:\n%s", section)
	}
	if strings.Contains(section, "gtid=") || strings.Contains(section, "max ") || strings.Contains(section, "shop.") {
		t.Fatalf("source section printed a table:\n%s", section)
	}
}

func TestAnalyzeCommitTimestampsUnavailable(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	for _, name := range []string{"minimal.binlog", "mariadb-10.11.14-dml.binlog"} {
		t.Run(name, func(t *testing.T) {
			fixture := mustFixturePath(t, name)
			stdout, stderr, err := executeAnalyzeLikeMain(t, fixture, "--format", "json")
			if err != nil {
				t.Fatalf("analyze json: %v\n%s", err, stderr)
			}
			if strings.Contains(stdout, "replica_apply_delay") || strings.Contains(stdout, "max_delay_us") {
				t.Fatalf("unavailable JSON invented a delay:\n%s", stdout)
			}
			text, stderr, err := executeAnalyzeLikeMain(t, fixture, "--format", "text")
			if err != nil {
				t.Fatalf("analyze text: %v\n%s", err, stderr)
			}
			section := sectionBetweenHeadings(t, text, "=== Replica Apply Delay ===", "===")
			if strings.TrimSpace(section) != "commit timestamps unavailable" {
				t.Fatalf("unavailable section = %q", strings.TrimSpace(section))
			}
			if strings.Contains(section, "clock") || strings.Contains(section, "max ") {
				t.Fatalf("unavailable section added a conclusion:\n%s", section)
			}
		})
	}
}

func TestAnalyzeReplicaApplyDelayChinese(t *testing.T) {
	t.Setenv("LANG", "zh-CN")
	t.Setenv("LC_ALL", "zh-CN")
	i18n.ResetForTesting()
	t.Cleanup(i18n.ResetForTesting)

	fixture := mustFixturePath(t, "mysql-8.0.46-replica-apply.binlog")
	cmd := NewRootCommand()
	cmd.SetArgs([]string{"--lang", "zh-CN", "analyze", fixture, "--format", "text", "--top", "1"})
	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
	if err != nil {
		t.Fatalf("zh-CN analyze: %v\n%s", err, stderr)
	}
	section := sectionBetweenHeadings(t, stdout, "=== 副本应用延迟 ===", "===")
	if !strings.Contains(section, "这假设源库和副本的时钟一致。") || !strings.Contains(section, "最大 ") || !strings.Contains(section, "峰值分钟 ") {
		t.Fatalf("zh-CN delay section:\n%s", section)
	}
	if !strings.Contains(section, replicaApplyTopGTID) {
		t.Fatalf("zh-CN section dropped the GTID:\n%s", section)
	}

	minimal := mustFixturePath(t, "minimal.binlog")
	cmd = NewRootCommand()
	cmd.SetArgs([]string{"--lang", "zh-CN", "analyze", minimal, "--format", "text"})
	stdout, stderr, err = captureStdoutStderrRun(t, func() error { return cmd.Execute() })
	if err != nil {
		t.Fatalf("zh-CN minimal: %v\n%s", err, stderr)
	}
	section = sectionBetweenHeadings(t, stdout, "=== 副本应用延迟 ===", "===")
	if strings.TrimSpace(section) != "提交时间戳不可用" {
		t.Fatalf("zh-CN unavailable = %q", strings.TrimSpace(section))
	}
}

type dumpTxn struct {
	GTID      string
	At        int64
	Original  uint64
	Immediate uint64
	Tables    map[string]int
	Deletes   map[string]int
}

func parseCommitDump(t *testing.T, path string) []dumpTxn {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	atRe := regexp.MustCompile(`^# at (\d+)$`)
	origRe := regexp.MustCompile(`^# original_commit_timestamp=(\d+) `)
	immRe := regexp.MustCompile(`^# immediate_commit_timestamp=(\d+) `)
	gtidRe := regexp.MustCompile(`^SET @@SESSION\.GTID_NEXT= '([^']+)'`)
	rowRe := regexp.MustCompile("^### (INSERT INTO|UPDATE|DELETE FROM) `([^`]+)`\\.`([^`]+)`")

	var out []dumpTxn
	var current *dumpTxn
	var at int64
	lines := strings.Split(string(body), "\n")
	for i := 0; i < len(lines); i++ {
		line := lines[i]
		if m := atRe.FindStringSubmatch(line); m != nil {
			at, _ = strconv.ParseInt(m[1], 10, 64)
			continue
		}
		if m := origRe.FindStringSubmatch(line); m != nil {
			if i+1 >= len(lines) {
				t.Fatalf("original timestamp without an immediate line: %s", line)
			}
			imm := immRe.FindStringSubmatch(lines[i+1])
			if imm == nil {
				t.Fatalf("immediate timestamp missing after %s", line)
			}
			orig, _ := strconv.ParseUint(m[1], 10, 64)
			immediate, _ := strconv.ParseUint(imm[1], 10, 64)
			if orig == 0 || immediate == 0 {
				t.Fatalf("dump timestamp is zero: %s", line)
			}
			current = &dumpTxn{At: at, Original: orig, Immediate: immediate, Tables: map[string]int{}, Deletes: map[string]int{}}
			out = append(out, *current)
			current = &out[len(out)-1]
			i++
			continue
		}
		if current == nil {
			continue
		}
		if m := gtidRe.FindStringSubmatch(line); m != nil && current.GTID == "" {
			current.GTID = m[1]
			continue
		}
		if m := rowRe.FindStringSubmatch(line); m != nil {
			name := m[2] + "." + m[3]
			current.Tables[name]++
			if m[1] == "DELETE FROM" {
				current.Deletes[name]++
			}
		}
	}
	for _, txn := range out {
		if txn.GTID == "" || txn.At <= 0 {
			t.Fatalf("dump transaction missing identity: %+v", txn)
		}
	}
	return out
}

func rankDump(txns []dumpTxn) (maxUs, p95Us int64, peak time.Time, top dumpTxn) {
	if len(txns) == 0 {
		return 0, 0, time.Time{}, dumpTxn{}
	}
	values := make([]int64, len(txns))
	var have bool
	for i, txn := range txns {
		delay := int64(txn.Immediate - txn.Original)
		values[i] = delay
		immediate := time.Unix(0, int64(txn.Immediate)*int64(time.Microsecond)).UTC()
		if !have || delay > maxUs || (delay == maxUs && immediate.Before(peak)) {
			have = true
			maxUs = delay
			peak = immediate
			top = txn
		}
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	rank := (95*len(values) + 99) / 100
	p95Us = values[rank-1]
	return maxUs, p95Us, peak.UTC().Truncate(time.Minute), top
}

func sortDump(txns []dumpTxn) {
	sort.Slice(txns, func(i, j int) bool {
		left := int64(txns[i].Immediate - txns[i].Original)
		right := int64(txns[j].Immediate - txns[j].Original)
		if left != right {
			return left > right
		}
		if txns[i].Immediate != txns[j].Immediate {
			return txns[i].Immediate < txns[j].Immediate
		}
		return txns[i].GTID < txns[j].GTID
	})
}

func filterDump(txns []dumpTxn, keep func(dumpTxn) bool) []dumpTxn {
	var out []dumpTxn
	for _, txn := range txns {
		if keep(txn) {
			out = append(out, txn)
		}
	}
	return out
}

func formatDumpMicros(us uint64) string {
	return time.Unix(0, int64(us)*int64(time.Microsecond)).UTC().Format("2006-01-02 15:04:05.000000 UTC")
}

func sameTables(got, want map[string]int) bool {
	if len(got) != len(want) {
		return false
	}
	for name, rows := range want {
		if got[name] != rows {
			return false
		}
	}
	return true
}

type applyPayload struct {
	ReplicaApplyDelay json.RawMessage `json:"replica_apply_delay"`
	Summary           struct {
		TotalTransactions int `json:"total_transactions"`
	} `json:"summary"`
	Alerts []struct {
		Kind string `json:"kind"`
	} `json:"alerts"`
}

type applyDelayJSON struct {
	Origin       string              `json:"origin"`
	MaxDelayUs   int64               `json:"max_delay_us"`
	P95DelayUs   int64               `json:"p95_delay_us"`
	PeakMinute   string              `json:"peak_minute"`
	Transactions []applyDelayTxnJSON `json:"transactions"`
}

type applyDelayTxnJSON struct {
	GTID                string         `json:"gtid"`
	TxnStartFile        string         `json:"txn_start_file"`
	TxnStartPos         int64          `json:"txn_start_pos"`
	OriginalCommitUs    uint64         `json:"original_commit_us"`
	ImmediateCommitUs   uint64         `json:"immediate_commit_us"`
	OriginalCommitTime  string         `json:"original_commit_time"`
	ImmediateCommitTime string         `json:"immediate_commit_time"`
	DelayUs             int64          `json:"delay_us"`
	Tables              map[string]int `json:"tables"`
}

func decodeApplyPayload(t *testing.T, stdout string) applyPayload {
	t.Helper()
	var payload applyPayload
	if err := json.Unmarshal([]byte(stdout), &payload); err != nil {
		t.Fatalf("json: %v\n%s", err, stdout)
	}
	return payload
}

func decodeApplyDelay(t *testing.T, raw json.RawMessage) applyDelayJSON {
	t.Helper()
	if len(raw) == 0 {
		t.Fatal("replica_apply_delay missing")
	}
	var delay applyDelayJSON
	if err := json.Unmarshal(raw, &delay); err != nil {
		t.Fatalf("replica_apply_delay: %v\n%s", err, raw)
	}
	return delay
}

func assertNoEmptyJSONStrings(t *testing.T, raw json.RawMessage) {
	t.Helper()
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	var walk func(any)
	walk = func(node any) {
		switch typed := node.(type) {
		case map[string]any:
			for key, child := range typed {
				if text, ok := child.(string); ok && text == "" {
					t.Fatalf("empty JSON string at %s", key)
				}
				walk(child)
			}
		case []any:
			for _, child := range typed {
				walk(child)
			}
		}
	}
	walk(value)
}

func sectionBetweenHeadings(t *testing.T, text, heading, next string) string {
	t.Helper()
	start := strings.Index(text, heading)
	if start < 0 {
		t.Fatalf("heading %q missing", heading)
	}
	body := text[start+len(heading):]
	body = strings.TrimPrefix(body, "\n")
	if end := strings.Index(body, next); end >= 0 {
		body = body[:end]
	}
	return strings.TrimRight(body, "\n")
}
