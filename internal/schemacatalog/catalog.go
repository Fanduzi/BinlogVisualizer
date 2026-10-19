// Package schemacatalog: the schema catalog answers "which definition does flashback use for db.table, and why".
// input: --schema-file text (mysqldump --no-data, SHOW CREATE TABLE, mysql --batch rows; USE, Database: headers and --schema-file-db), then every table map and DDL statement of the parsed binlogs in order, including ones later excluded by GTID or time.
// output: a typed resolution per db.table (exact, case-fold, ambiguous, missing, refused with a reason) plus the schema warnings to print. Callers build every message from the resolution; the catalog never rewrites error text.
// pos: flashback-only module. Analyze does not call it. Parsing helpers live in internal/mysqlexpr.
// note: if this file changes, update this header, CONTEXT.md (Schema catalog) and docs/adr/0005-schema-catalog.md.
package schemacatalog

import (
	"sort"
	"strings"

	"binlogviz/internal/model"
	"binlogviz/internal/mysqlexpr"
)

// Kind says how a binlog db.table was bound to a definition.
type Kind int

const (
	// Missing: no definition in the schema file or the parsed binlogs.
	Missing Kind = iota
	// Exact: a definition under the same name (schema file, binlog DDL,
	// or an unqualified schema-file table bound to its only schema).
	Exact
	// Fold: the schema-file table differs only in letter case and was
	// used because the binlog name is all lower case and the match is unique.
	Fold
	// Ambiguous: candidates exist but none was used (an unqualified
	// schema-file table seen in several schemas, or several / non-folded
	// letter-case near misses).
	Ambiguous
	// Refused: a definition was seen and cannot be used.
	Refused
)

// RefuseReason says why a definition was refused.
type RefuseReason int

const (
	RefuseNone RefuseReason = iota
	// UnreadableCreate: CREATE TABLE had no readable column list.
	UnreadableCreate
	// CreateSelect: CREATE TABLE ... AS SELECT; columns are not stated.
	CreateSelect
	// LikeSource: CREATE TABLE ... LIKE a table with no usable definition.
	LikeSource
	// AlterUnknown: ALTER TABLE on a table with no definition.
	AlterUnknown
	// AlterUnreadable: an ALTER action that could not be replayed.
	AlterUnreadable
)

// resolution is the catalog's answer for one binlog db.table.
type Resolution struct {
	Kind Kind
	// cols is the column list in effect (Exact, Fold).
	Cols []mysqlexpr.SchemaCol
	// checkRows: the list came from --schema-file and must be checked
	// against the logged rows before it is trusted.
	CheckRows bool
	// deferred: bound from an unqualified schema-file name; the check waits
	// until every schema of that table has been seen.
	Deferred bool
	// fileTable names the schema-file table (db.tbl) this binlog name was
	// case-folded to. Set for Fold, and kept afterwards so a later
	// refusal of the same table still names it.
	FileTable string
	// candidates are the unused near matches of Ambiguous.
	Candidates []string
	// reason is set for Refused.
	Reason RefuseReason
}

// New returns an empty catalog. flagDB is --schema-file-db.
func New(flagDB string) *Catalog {
	return &Catalog{flagDB: strings.Trim(strings.TrimSpace(flagDB), "`")}
}

// loadFile reads one --schema-file. Call it before any observe call.
func (g *Catalog) LoadFile(sql string) { g.noteScript(sql) }

// observeTable records that the binlog has a table map for schema.table.
// It returns tables whose unqualified schema-file binding was dropped by this
// call because the name is now seen in a second schema.
func (g *Catalog) ObserveTable(schema, table string) []string {
	if schema == "" || table == "" {
		return nil
	}
	before := len(g.abandoned)
	g.observe(schema, table)
	return g.abandonedSince(before)
}

// observeDDL replays one binlog DDL statement run with session schema.
// It returns tables whose unqualified binding was dropped, as observeTable.
func (g *Catalog) ObserveDDL(session, sql string) []string {
	before := len(g.abandoned)
	g.note(session, sql)
	return g.abandonedSince(before)
}

func (g *Catalog) abandonedSince(before int) []string {
	var out []string
	for _, item := range g.abandoned[before:] {
		out = append(out, item.table)
	}
	return out
}

// resolve binds schema.table for a selected row event. The first call for a
// name may bind a letter-case match from the schema file; later calls and
// peek return the same binding.
func (g *Catalog) Resolve(schema, table string) Resolution {
	g.foldCase(schema, table)
	return g.Peek(schema, table)
}

// peek reports the current binding of schema.table without binding anything.
func (g *Catalog) Peek(schema, table string) Resolution {
	if g == nil {
		return Resolution{}
	}
	key := generatedKey(schema, table)
	res := Resolution{FileTable: g.foldFrom[key]}
	if why, bad := g.bad[key]; bad {
		res.Kind = Refused
		res.Reason = why
		return res
	}
	if cols, ok := g.defs[key]; ok {
		res.Kind = Exact
		if res.FileTable != "" {
			res.Kind = Fold
		}
		res.Cols = mysqlexpr.CloneSchemaCols(cols)
		res.CheckRows = g.fromFile[key]
		res.Deferred = g.pendingBound[key]
		return res
	}
	if miss := g.foldMiss[key]; len(miss) > 0 {
		res.Kind = Ambiguous
		res.Candidates = append([]string(nil), miss...)
		return res
	}
	for _, item := range g.abandoned {
		if item.table != table {
			continue
		}
		for _, s := range item.schemas {
			if s == schema {
				res.Kind = Ambiguous
				for _, other := range item.schemas {
					res.Candidates = append(res.Candidates, other+"."+table)
				}
				return res
			}
		}
	}
	res.Kind = Missing
	return res
}

// warnings returns the schema warnings in print order: letter-case notes,
// the header/--schema-file-db conflict, then unqualified tables not used.
func (g *Catalog) Warnings() []model.FlashbackWarning { return g.schemaWarnings() }

// Catalog records generated column names learned from CREATE and ALTER
// statements in the parsed binlog. A table missing from cols was not defined
// in this input. bad means a definition was seen and could not be read, so
// undo SQL must refuse that table instead of assigning a generated column.
type Catalog struct {
	cols           map[string]map[string]struct{}
	defs           map[string][]mysqlexpr.SchemaCol
	fromFile       map[string]bool
	pendingBound   map[string]bool
	bad            map[string]RefuseReason
	pending        map[string]*pendingDef
	seen           map[string]map[string]struct{}
	abandoned      []abandonedTable
	flagDB         string
	headerDB       string
	ambiguousDB    bool
	sawUnqualified bool
	readingFile    bool
	folded         map[string]bool
	foldNotes      []model.FlashbackWarning
	// foldFrom maps a folded binlog key to the schema-file table (db.tbl) it used.
	foldFrom map[string]string
	// foldMiss maps a binlog key to schema-file tables that differ only in
	// letter case but were not used.
	foldMiss map[string][]string
}

// noteScript reads mysqldump --no-data output or SHOW CREATE TABLE text.
// USE sets the schema for a following unqualified CREATE. Without USE, the
// mysqldump "-- Host: ... Database:" header or flagDB is that schema. Each
// header of concatenated dumps sets the schema for the dump that follows it.
// A row of "name<TAB>CREATE TABLE ..." is the mysql batch format. Several of
// those rows may sit in one file with no ';' between them. mysql --batch
// writes each newline inside a field as the two characters \ n.
func (g *Catalog) noteScript(sql string) {
	if g == nil || strings.TrimSpace(sql) == "" {
		return
	}
	g.readingFile = true
	defer func() { g.readingFile = false }()
	g.headerDB = mysqlexpr.DumpDatabase(sql)
	for _, seg := range mysqlexpr.DumpSegments(sql) {
		session := ""
		switch {
		case seg.DB != "" && g.flagDB != "" && !strings.EqualFold(seg.DB, g.flagDB):
			g.ambiguousDB = true
			g.headerDB = seg.DB
		case seg.DB != "":
			session = seg.DB
		case g.headerDB == "" && g.flagDB != "":
			session = g.flagDB
		}
		for _, stmt := range mysqlexpr.SplitSQL(seg.SQL) {
			g.noteChunk(&session, stmt)
		}
	}
}

func (g *Catalog) noteChunk(session *string, stmt string) {
	stmt = strings.TrimSpace(stmt)
	for stmt != "" {
		sc := mysqlexpr.NewScan(stmt)
		if sc.WordIs("USE") {
			name, ok := sc.Ident()
			if ok {
				*session = name
			}
			stmt = strings.TrimSpace(sc.Rest())
			continue
		}
		at := mysqlexpr.IndexCreateTable(stmt)
		if at < 0 {
			g.note(*session, stmt)
			return
		}
		rest := stmt[at:]
		next := -1
		if len(rest) > 1 {
			next = mysqlexpr.IndexCreateTable(rest[1:])
		}
		if next < 0 {
			g.note(*session, rest)
			return
		}
		cut := next + 1
		g.note(*session, rest[:cut])
		stmt = strings.TrimSpace(rest[cut:])
	}
}

func (g *Catalog) note(sessionSchema, sql string) {
	if g == nil {
		return
	}
	sc := mysqlexpr.NewScan(strings.TrimSpace(sql))
	verb := strings.ToUpper(sc.Word())
	switch verb {
	case "CREATE":
		g.noteCreate(sessionSchema, sc)
	case "ALTER":
		g.noteAlter(sessionSchema, sc)
	case "DROP":
		g.noteDrop(sessionSchema, sc)
	case "RENAME":
		g.noteRename(sessionSchema, sc)
	}
}

func (g *Catalog) unknown(schema, table string) bool {
	if g == nil || g.bad == nil {
		return false
	}
	_, ok := g.bad[generatedKey(schema, table)]
	return ok
}

func generatedKey(schema, table string) string {
	return schema + "\x00" + table
}

func (g *Catalog) ensure() {
	if g.cols == nil {
		g.cols = map[string]map[string]struct{}{}
	}
	if g.bad == nil {
		g.bad = map[string]RefuseReason{}
	}
}

func (g *Catalog) noteCreate(session string, sc *mysqlexpr.Scan) {
	if strings.EqualFold(sc.Word(), "TEMPORARY") {
		// consumed
	} else {
		sc.UnreadWord()
	}
	if !sc.WordIs("TABLE") {
		return
	}
	if sc.WordIs("IF") {
		sc.WordIs("NOT")
		sc.WordIs("EXISTS")
	}
	schema, table, ok := sc.Qualified(session)
	if !ok {
		return
	}
	key := generatedKey(schema, table)
	g.ensure()
	if sc.WordIs("LIKE") {
		srcSchema, src, ok := sc.Qualified(session)
		if !ok {
			g.markBad(key, LikeSource)
			return
		}
		if srcSchema == "" {
			if p := g.pending[src]; p != nil && !p.abandoned && !p.bad {
				g.installCreate(schema, table, mysqlexpr.CloneSchemaCols(p.cols), true)
				return
			}
			if schema == "" {
				g.putPendingBad(table, LikeSource)
				return
			}
			g.markBad(key, LikeSource)
			return
		}
		srcKey := generatedKey(srcSchema, src)
		if _, bad := g.bad[srcKey]; bad {
			g.markBad(key, LikeSource)
			return
		}
		if cols, known := g.defs[srcKey]; known {
			g.installCreate(schema, table, cols, g.fromFile[srcKey] || g.readingFile)
			return
		}
		g.markBad(key, LikeSource)
		return
	}
	if sc.WordIs("AS") || sc.WordIs("SELECT") {
		if schema == "" {
			g.putPendingBad(table, CreateSelect)
			return
		}
		g.markBad(key, CreateSelect)
		return
	}
	body, ok := sc.ParenBody()
	if !ok {
		if schema == "" {
			g.putPendingBad(table, UnreadableCreate)
			return
		}
		g.markBad(key, UnreadableCreate)
		return
	}
	cols, ok := mysqlexpr.ParseSchemaColumns(body)
	if !ok {
		if schema == "" {
			g.putPendingBad(table, UnreadableCreate)
			return
		}
		g.markBad(key, UnreadableCreate)
		return
	}
	g.installCreate(schema, table, mysqlexpr.ApplyTableCharset(cols, sc.Rest()), g.readingFile)
}

func (g *Catalog) installCreate(schema, table string, cols []mysqlexpr.SchemaCol, fromFile bool) {
	if schema == "" {
		g.sawUnqualified = true
		if g.ambiguousDB {
			return
		}
		g.putPending(table, cols)
		return
	}
	key := generatedKey(schema, table)
	delete(g.pending, table)
	delete(g.pendingBound, key)
	g.setDef(key, cols, fromFile)
}

func (g *Catalog) noteAlter(session string, sc *mysqlexpr.Scan) {
	if !sc.WordIs("TABLE") {
		return
	}
	schema, table, ok := sc.Qualified(session)
	if !ok {
		return
	}
	if schema == "" {
		g.alterPending(table, sc.Rest())
		return
	}
	if !g.readingFile {
		g.observe(schema, table)
	}
	if p := g.pending[table]; p != nil && p.abandoned {
		return
	}
	key := generatedKey(schema, table)
	g.ensure()
	for _, action := range mysqlexpr.SplitComma(sc.Rest()) {
		g.applyAlter(key, action)
	}
}

func (g *Catalog) alterPending(table, rest string) {
	g.sawUnqualified = true
	if g.ambiguousDB {
		return
	}
	p := g.pending[table]
	if p == nil || p.abandoned {
		g.putPendingBad(table, AlterUnknown)
		return
	}
	if p.bad {
		return
	}
	for _, action := range mysqlexpr.SplitComma(rest) {
		if !mysqlexpr.AlterSlice(&p.cols, action) {
			p.bad = true
			p.why = AlterUnreadable
			p.cols = nil
			return
		}
	}
}

func (g *Catalog) applyAlter(key, action string) {
	if mysqlexpr.AlterNoColumnChange(action) {
		return
	}
	cols, ok := g.defs[key]
	if !ok {
		g.markBad(key, AlterUnknown)
		return
	}
	next := mysqlexpr.CloneSchemaCols(cols)
	if !mysqlexpr.AlterSlice(&next, action) {
		g.markBad(key, AlterUnreadable)
		return
	}
	g.setDef(key, next, g.fromFile[key])
}

func (g *Catalog) known(key string) bool {
	_, ok := g.cols[key]
	return ok
}

func (g *Catalog) noteDrop(session string, sc *mysqlexpr.Scan) {
	if !sc.WordIs("TABLE") {
		return
	}
	sc.WordIs("IF")
	if strings.EqualFold(sc.PeekWord(), "EXISTS") {
		sc.Word()
	}
	for {
		schema, table, ok := sc.Qualified(session)
		if !ok {
			return
		}
		key := generatedKey(schema, table)
		delete(g.cols, key)
		delete(g.defs, key)
		delete(g.bad, key)
		delete(g.fromFile, key)
		delete(g.pendingBound, key)
		delete(g.pending, table)
		if !sc.WordIs(",") && sc.PeekByte() != ',' {
			sc.Skip()
			if sc.I < len(sc.S) && sc.S[sc.I] == ',' {
				sc.I++
				continue
			}
			return
		}
	}
}

func (g *Catalog) noteRename(session string, sc *mysqlexpr.Scan) {
	if !sc.WordIs("TABLE") {
		return
	}
	for {
		srcSchema, src, ok := sc.Qualified(session)
		if !ok || !sc.WordIs("TO") {
			return
		}
		dstSchema, dst, ok := sc.Qualified(session)
		if !ok {
			return
		}
		srcKey := generatedKey(srcSchema, src)
		dstKey := generatedKey(dstSchema, dst)
		g.ensure()
		if cols, known := g.cols[srcKey]; known {
			g.cols[dstKey] = cols
			delete(g.cols, srcKey)
		}
		if cols, known := g.defs[srcKey]; known {
			g.defs[dstKey] = cols
			delete(g.defs, srcKey)
		}
		if g.fromFile[srcKey] {
			if g.fromFile == nil {
				g.fromFile = map[string]bool{}
			}
			g.fromFile[dstKey] = true
			delete(g.fromFile, srcKey)
		}
		if g.pendingBound[srcKey] {
			if g.pendingBound == nil {
				g.pendingBound = map[string]bool{}
			}
			g.pendingBound[dstKey] = true
			delete(g.pendingBound, srcKey)
		}
		if _, bad := g.bad[srcKey]; bad {
			g.bad[dstKey] = g.bad[srcKey]
			delete(g.bad, srcKey)
		}
		if !sc.WordIs(",") && sc.PeekByte() != ',' {
			return
		}
		sc.Skip()
		if sc.I < len(sc.S) && sc.S[sc.I] == ',' {
			sc.I++
		}
	}
}

type pendingDef struct {
	table       string
	cols        []mysqlexpr.SchemaCol
	bad         bool
	abandoned   bool
	boundSchema string
	why         RefuseReason
}

type abandonedTable struct {
	table   string
	schemas []string
}

func (g *Catalog) setDef(key string, cols []mysqlexpr.SchemaCol, fromFile bool) {
	g.ensure()
	if g.defs == nil {
		g.defs = map[string][]mysqlexpr.SchemaCol{}
	}
	copied := mysqlexpr.CloneSchemaCols(cols)
	g.defs[key] = copied
	g.cols[key] = mysqlexpr.GeneratedNameSet(copied)
	delete(g.bad, key)
	if fromFile {
		if g.fromFile == nil {
			g.fromFile = map[string]bool{}
		}
		g.fromFile[key] = true
		return
	}
	delete(g.fromFile, key)
	delete(g.pendingBound, key)
}

func (g *Catalog) markBad(key string, why RefuseReason) {
	g.ensure()
	g.bad[key] = why
	delete(g.cols, key)
	delete(g.defs, key)
	delete(g.fromFile, key)
	delete(g.pendingBound, key)
}

func (g *Catalog) putPending(table string, cols []mysqlexpr.SchemaCol) {
	if g.pending == nil {
		g.pending = map[string]*pendingDef{}
	}
	g.pending[table] = &pendingDef{table: table, cols: mysqlexpr.CloneSchemaCols(cols)}
}

func (g *Catalog) putPendingBad(table string, why RefuseReason) {
	g.sawUnqualified = true
	if g.ambiguousDB {
		return
	}
	if g.pending == nil {
		g.pending = map[string]*pendingDef{}
	}
	g.pending[table] = &pendingDef{table: table, bad: true, why: why}
}

// observe records one schema that contains table. A schema-file definition
// that was unqualified is bound to that schema when it is the only one.
// A second schema abandons the binding so the definition is not guessed.
// abandoned is true only on the call that drops the binding.
func (g *Catalog) observe(schema, table string) bool {
	if g == nil || schema == "" || table == "" {
		return false
	}
	if g.seen == nil {
		g.seen = map[string]map[string]struct{}{}
	}
	set := g.seen[table]
	if set == nil {
		set = map[string]struct{}{}
		g.seen[table] = set
	}
	set[schema] = struct{}{}
	p := g.pending[table]
	if p == nil || p.abandoned {
		return false
	}
	if len(set) > 1 {
		return g.abandon(table)
	}
	g.bindPending(schema, table)
	return false
}

func (g *Catalog) bindPending(schema, table string) {
	p := g.pending[table]
	if p == nil || p.abandoned || p.boundSchema != "" {
		return
	}
	key := generatedKey(schema, table)
	if g.known(key) || g.unknown(schema, table) {
		delete(g.pending, table)
		return
	}
	if p.bad {
		g.markBad(key, p.why)
		p.boundSchema = schema
		if g.pendingBound == nil {
			g.pendingBound = map[string]bool{}
		}
		g.pendingBound[key] = true
		return
	}
	g.setDef(key, p.cols, true)
	if g.pendingBound == nil {
		g.pendingBound = map[string]bool{}
	}
	g.pendingBound[key] = true
	p.boundSchema = schema
}

func (g *Catalog) abandon(table string) bool {
	p := g.pending[table]
	if p == nil || p.abandoned {
		return false
	}
	p.abandoned = true
	if p.boundSchema != "" {
		key := generatedKey(p.boundSchema, p.table)
		if g.pendingBound[key] {
			delete(g.defs, key)
			delete(g.cols, key)
			delete(g.fromFile, key)
			delete(g.bad, key)
			delete(g.pendingBound, key)
		}
	}
	var schemas []string
	for schema := range g.seen[table] {
		schemas = append(schemas, schema)
	}
	sort.Strings(schemas)
	g.abandoned = append(g.abandoned, abandonedTable{table: p.table, schemas: schemas})
	return true
}

// foldCase handles a --schema-file whose names differ from the binlog only in
// letter case, as when the dump comes from a lower_case_table_names=0 server
// and the binlog from one with 1 or 2. It runs only when schema.table has no
// definition of its own. A single case-insensitive match is used when the
// binlog names are all lower case, because a server that folds names logs
// them that way; the match is then checked against the rows like any other
// schema-file table. Otherwise the near miss is only named in a warning.
func (g *Catalog) foldCase(schema, table string) {
	if g == nil || g.defs == nil || schema == "" {
		return
	}
	key := generatedKey(schema, table)
	if _, ok := g.defs[key]; ok {
		return
	}
	if _, ok := g.bad[key]; ok {
		return
	}
	if _, ok := g.cols[key]; ok {
		return
	}
	if g.folded[key] {
		return
	}
	if g.folded == nil {
		g.folded = map[string]bool{}
	}
	g.folded[key] = true
	var hits []string
	for k := range g.defs {
		if g.fromFile[k] && !g.pendingBound[k] && strings.EqualFold(k, key) {
			hits = append(hits, k)
		}
	}
	if len(hits) == 0 {
		return
	}
	sort.Strings(hits)
	names := make([]string, len(hits))
	for i, k := range hits {
		names[i] = strings.Replace(k, "\x00", ".", 1)
	}
	binlog := schema + "." + table
	folded := schema == strings.ToLower(schema) && table == strings.ToLower(table)
	if len(hits) == 1 && folded {
		g.setDef(key, g.defs[hits[0]], true)
		if g.foldFrom == nil {
			g.foldFrom = map[string]string{}
		}
		g.foldFrom[key] = names[0]
		g.foldNotes = append(g.foldNotes, model.NewFlashWarning(model.WarnSchemaFileCaseFolded, "warning.flashbackSchemaFold", map[string]any{
			"Binlog": binlog,
			"File":   names[0],
		}))
		return
	}
	if g.foldMiss == nil {
		g.foldMiss = map[string][]string{}
	}
	g.foldMiss[key] = names
	g.foldNotes = append(g.foldNotes, model.NewFlashWarning(model.WarnSchemaFileCaseMismatch, "warning.flashbackSchemaFoldMiss", map[string]any{
		"Binlog": binlog,
		"File":   strings.Join(names, ", "),
	}))
}

func (g *Catalog) schemaWarnings() []model.FlashbackWarning {
	if g == nil {
		return nil
	}
	var out []model.FlashbackWarning
	out = append(out, g.foldNotes...)
	if g.ambiguousDB && g.sawUnqualified {
		out = append(out, model.NewFlashWarning(model.WarnSchemaFileDBMismatch, "warning.flashbackSchemaDB", map[string]any{
			"Header": g.headerDB,
			"Flag":   g.flagDB,
		}))
	}
	for _, item := range g.abandoned {
		out = append(out, model.NewFlashWarning(model.WarnSchemaFileTableAmbiguous, "warning.flashbackSchemaTable", map[string]any{
			"Table":   item.table,
			"Schemas": strings.Join(item.schemas, ", "),
		}))
	}
	return out
}
