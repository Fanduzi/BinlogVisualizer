// Package analyzer: the schema catalog answers "which definition does flashback use for db.table, and why".
// input: --schema-file text (mysqldump --no-data, SHOW CREATE TABLE, mysql --batch rows; USE, Database: headers and --schema-file-db), then every table map and DDL statement of the parsed binlogs in order, including ones later excluded by GTID or time.
// output: a typed resolution per db.table (exact, case-fold, ambiguous, missing, refused with a reason) plus the schema warnings to print. Callers build every message from the resolution; the catalog never rewrites error text.
// pos: flashback-only module. Analyze does not call it. Parsing helpers live in flashback_generated.go and flashback_schema.go.
// note: if this file changes, update this header, CONTEXT.md (Schema catalog) and docs/adr/0005-schema-catalog.md.
package analyzer

import (
	"sort"
	"strings"
)

// resolveKind says how a binlog db.table was bound to a definition.
type resolveKind int

const (
	// resolveMissing: no definition in the schema file or the parsed binlogs.
	resolveMissing resolveKind = iota
	// resolveExact: a definition under the same name (schema file, binlog DDL,
	// or an unqualified schema-file table bound to its only schema).
	resolveExact
	// resolveFold: the schema-file table differs only in letter case and was
	// used because the binlog name is all lower case and the match is unique.
	resolveFold
	// resolveAmbiguous: candidates exist but none was used (an unqualified
	// schema-file table seen in several schemas, or several / non-folded
	// letter-case near misses).
	resolveAmbiguous
	// resolveRefused: a definition was seen and cannot be used.
	resolveRefused
)

// refuseReason says why a definition was refused.
type refuseReason int

const (
	refuseNone refuseReason = iota
	// refuseUnreadableCreate: CREATE TABLE had no readable column list.
	refuseUnreadableCreate
	// refuseCreateSelect: CREATE TABLE ... AS SELECT; columns are not stated.
	refuseCreateSelect
	// refuseLikeSource: CREATE TABLE ... LIKE a table with no usable definition.
	refuseLikeSource
	// refuseAlterUnknown: ALTER TABLE on a table with no definition.
	refuseAlterUnknown
	// refuseAlterUnreadable: an ALTER action that could not be replayed.
	refuseAlterUnreadable
)

// resolution is the catalog's answer for one binlog db.table.
type resolution struct {
	kind resolveKind
	// cols is the column list in effect (resolveExact, resolveFold).
	cols []schemaCol
	// checkRows: the list came from --schema-file and must be checked
	// against the logged rows before it is trusted.
	checkRows bool
	// deferred: bound from an unqualified schema-file name; the check waits
	// until every schema of that table has been seen.
	deferred bool
	// fileTable names the schema-file table (db.tbl) this binlog name was
	// case-folded to. Set for resolveFold, and kept afterwards so a later
	// refusal of the same table still names it.
	fileTable string
	// candidates are the unused near matches of resolveAmbiguous.
	candidates []string
	// reason is set for resolveRefused.
	reason refuseReason
}

// newSchemaCatalog returns an empty catalog. flagDB is --schema-file-db.
func newSchemaCatalog(flagDB string) *schemaCatalog {
	return &schemaCatalog{flagDB: strings.Trim(strings.TrimSpace(flagDB), "`")}
}

// loadFile reads one --schema-file. Call it before any observe call.
func (g *schemaCatalog) loadFile(sql string) { g.noteScript(sql) }

// observeTable records that the binlog has a table map for schema.table.
// It returns tables whose unqualified schema-file binding was dropped by this
// call because the name is now seen in a second schema.
func (g *schemaCatalog) observeTable(schema, table string) []string {
	if schema == "" || table == "" {
		return nil
	}
	before := len(g.abandoned)
	g.observe(schema, table)
	return g.abandonedSince(before)
}

// observeDDL replays one binlog DDL statement run with session schema.
// It returns tables whose unqualified binding was dropped, as observeTable.
func (g *schemaCatalog) observeDDL(session, sql string) []string {
	before := len(g.abandoned)
	g.note(session, sql)
	return g.abandonedSince(before)
}

func (g *schemaCatalog) abandonedSince(before int) []string {
	var out []string
	for _, item := range g.abandoned[before:] {
		out = append(out, item.table)
	}
	return out
}

// resolve binds schema.table for a selected row event. The first call for a
// name may bind a letter-case match from the schema file; later calls and
// peek return the same binding.
func (g *schemaCatalog) resolve(schema, table string) resolution {
	g.foldCase(schema, table)
	return g.peek(schema, table)
}

// peek reports the current binding of schema.table without binding anything.
func (g *schemaCatalog) peek(schema, table string) resolution {
	if g == nil {
		return resolution{}
	}
	key := generatedKey(schema, table)
	res := resolution{fileTable: g.foldFrom[key]}
	if why, bad := g.bad[key]; bad {
		res.kind = resolveRefused
		res.reason = why
		return res
	}
	if cols, ok := g.defs[key]; ok {
		res.kind = resolveExact
		if res.fileTable != "" {
			res.kind = resolveFold
		}
		res.cols = cloneSchemaCols(cols)
		res.checkRows = g.fromFile[key]
		res.deferred = g.pendingBound[key]
		return res
	}
	if miss := g.foldMiss[key]; len(miss) > 0 {
		res.kind = resolveAmbiguous
		res.candidates = append([]string(nil), miss...)
		return res
	}
	for _, item := range g.abandoned {
		if item.table != table {
			continue
		}
		for _, s := range item.schemas {
			if s == schema {
				res.kind = resolveAmbiguous
				for _, other := range item.schemas {
					res.candidates = append(res.candidates, other+"."+table)
				}
				return res
			}
		}
	}
	res.kind = resolveMissing
	return res
}

// warnings returns the schema warnings in print order: letter-case notes,
// the header/--schema-file-db conflict, then unqualified tables not used.
func (g *schemaCatalog) warnings() []FlashbackWarning { return g.schemaWarnings() }

// schemaCatalog records generated column names learned from CREATE and ALTER
// statements in the parsed binlog. A table missing from cols was not defined
// in this input. bad means a definition was seen and could not be read, so
// undo SQL must refuse that table instead of assigning a generated column.
type schemaCatalog struct {
	cols           map[string]map[string]struct{}
	defs           map[string][]schemaCol
	fromFile       map[string]bool
	pendingBound   map[string]bool
	bad            map[string]refuseReason
	pending        map[string]*pendingDef
	seen           map[string]map[string]struct{}
	abandoned      []abandonedTable
	flagDB         string
	headerDB       string
	ambiguousDB    bool
	sawUnqualified bool
	readingFile    bool
	folded         map[string]bool
	foldNotes      []FlashbackWarning
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
func (g *schemaCatalog) noteScript(sql string) {
	if g == nil || strings.TrimSpace(sql) == "" {
		return
	}
	g.readingFile = true
	defer func() { g.readingFile = false }()
	g.headerDB = dumpDatabase(sql)
	for _, seg := range dumpSegments(sql) {
		session := ""
		switch {
		case seg.db != "" && g.flagDB != "" && !strings.EqualFold(seg.db, g.flagDB):
			g.ambiguousDB = true
			g.headerDB = seg.db
		case seg.db != "":
			session = seg.db
		case g.headerDB == "" && g.flagDB != "":
			session = g.flagDB
		}
		for _, stmt := range splitSQL(seg.sql) {
			g.noteChunk(&session, stmt)
		}
	}
}

func (g *schemaCatalog) noteChunk(session *string, stmt string) {
	stmt = strings.TrimSpace(stmt)
	for stmt != "" {
		sc := &sqlScan{s: stmt}
		if sc.wordIs("USE") {
			name, ok := sc.ident()
			if ok {
				*session = name
			}
			stmt = strings.TrimSpace(sc.rest())
			continue
		}
		at := indexCreateTable(stmt)
		if at < 0 {
			g.note(*session, stmt)
			return
		}
		rest := stmt[at:]
		next := -1
		if len(rest) > 1 {
			next = indexCreateTable(rest[1:])
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

func (g *schemaCatalog) note(sessionSchema, sql string) {
	if g == nil {
		return
	}
	sc := &sqlScan{s: strings.TrimSpace(sql)}
	verb := strings.ToUpper(sc.word())
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

func (g *schemaCatalog) unknown(schema, table string) bool {
	if g == nil || g.bad == nil {
		return false
	}
	_, ok := g.bad[generatedKey(schema, table)]
	return ok
}

func generatedKey(schema, table string) string {
	return schema + "\x00" + table
}

func (g *schemaCatalog) ensure() {
	if g.cols == nil {
		g.cols = map[string]map[string]struct{}{}
	}
	if g.bad == nil {
		g.bad = map[string]refuseReason{}
	}
}

func (g *schemaCatalog) noteCreate(session string, sc *sqlScan) {
	if strings.EqualFold(sc.word(), "TEMPORARY") {
		// consumed
	} else {
		sc.unreadWord()
	}
	if !sc.wordIs("TABLE") {
		return
	}
	if sc.wordIs("IF") {
		sc.wordIs("NOT")
		sc.wordIs("EXISTS")
	}
	schema, table, ok := sc.qualified(session)
	if !ok {
		return
	}
	key := generatedKey(schema, table)
	g.ensure()
	if sc.wordIs("LIKE") {
		srcSchema, src, ok := sc.qualified(session)
		if !ok {
			g.markBad(key, refuseLikeSource)
			return
		}
		if srcSchema == "" {
			if p := g.pending[src]; p != nil && !p.abandoned && !p.bad {
				g.installCreate(schema, table, cloneSchemaCols(p.cols), true)
				return
			}
			if schema == "" {
				g.putPendingBad(table, refuseLikeSource)
				return
			}
			g.markBad(key, refuseLikeSource)
			return
		}
		srcKey := generatedKey(srcSchema, src)
		if _, bad := g.bad[srcKey]; bad {
			g.markBad(key, refuseLikeSource)
			return
		}
		if cols, known := g.defs[srcKey]; known {
			g.installCreate(schema, table, cols, g.fromFile[srcKey] || g.readingFile)
			return
		}
		g.markBad(key, refuseLikeSource)
		return
	}
	if sc.wordIs("AS") || sc.wordIs("SELECT") {
		if schema == "" {
			g.putPendingBad(table, refuseCreateSelect)
			return
		}
		g.markBad(key, refuseCreateSelect)
		return
	}
	body, ok := sc.parenBody()
	if !ok {
		if schema == "" {
			g.putPendingBad(table, refuseUnreadableCreate)
			return
		}
		g.markBad(key, refuseUnreadableCreate)
		return
	}
	cols, ok := parseSchemaColumns(body)
	if !ok {
		if schema == "" {
			g.putPendingBad(table, refuseUnreadableCreate)
			return
		}
		g.markBad(key, refuseUnreadableCreate)
		return
	}
	g.installCreate(schema, table, applyTableCharset(cols, sc.rest()), g.readingFile)
}

func (g *schemaCatalog) installCreate(schema, table string, cols []schemaCol, fromFile bool) {
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

func (g *schemaCatalog) noteAlter(session string, sc *sqlScan) {
	if !sc.wordIs("TABLE") {
		return
	}
	schema, table, ok := sc.qualified(session)
	if !ok {
		return
	}
	if schema == "" {
		g.alterPending(table, sc.rest())
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
	for _, action := range splitComma(sc.rest()) {
		g.applyAlter(key, action)
	}
}

func (g *schemaCatalog) alterPending(table, rest string) {
	g.sawUnqualified = true
	if g.ambiguousDB {
		return
	}
	p := g.pending[table]
	if p == nil || p.abandoned {
		g.putPendingBad(table, refuseAlterUnknown)
		return
	}
	if p.bad {
		return
	}
	for _, action := range splitComma(rest) {
		if !alterSlice(&p.cols, action) {
			p.bad = true
			p.why = refuseAlterUnreadable
			p.cols = nil
			return
		}
	}
}

func (g *schemaCatalog) applyAlter(key, action string) {
	if alterNoColumnChange(action) {
		return
	}
	cols, ok := g.defs[key]
	if !ok {
		g.markBad(key, refuseAlterUnknown)
		return
	}
	next := cloneSchemaCols(cols)
	if !alterSlice(&next, action) {
		g.markBad(key, refuseAlterUnreadable)
		return
	}
	g.setDef(key, next, g.fromFile[key])
}

func (g *schemaCatalog) known(key string) bool {
	_, ok := g.cols[key]
	return ok
}

func (g *schemaCatalog) noteDrop(session string, sc *sqlScan) {
	if !sc.wordIs("TABLE") {
		return
	}
	sc.wordIs("IF")
	if strings.EqualFold(sc.peekWord(), "EXISTS") {
		sc.word()
	}
	for {
		schema, table, ok := sc.qualified(session)
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
		if !sc.wordIs(",") && sc.peekByte() != ',' {
			sc.skip()
			if sc.i < len(sc.s) && sc.s[sc.i] == ',' {
				sc.i++
				continue
			}
			return
		}
	}
}

func (g *schemaCatalog) noteRename(session string, sc *sqlScan) {
	if !sc.wordIs("TABLE") {
		return
	}
	for {
		srcSchema, src, ok := sc.qualified(session)
		if !ok || !sc.wordIs("TO") {
			return
		}
		dstSchema, dst, ok := sc.qualified(session)
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
		if !sc.wordIs(",") && sc.peekByte() != ',' {
			return
		}
		sc.skip()
		if sc.i < len(sc.s) && sc.s[sc.i] == ',' {
			sc.i++
		}
	}
}

type pendingDef struct {
	table       string
	cols        []schemaCol
	bad         bool
	abandoned   bool
	boundSchema string
	why         refuseReason
}

type abandonedTable struct {
	table   string
	schemas []string
}

func (g *schemaCatalog) setDef(key string, cols []schemaCol, fromFile bool) {
	g.ensure()
	if g.defs == nil {
		g.defs = map[string][]schemaCol{}
	}
	copied := cloneSchemaCols(cols)
	g.defs[key] = copied
	g.cols[key] = generatedNameSet(copied)
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

func (g *schemaCatalog) markBad(key string, why refuseReason) {
	g.ensure()
	g.bad[key] = why
	delete(g.cols, key)
	delete(g.defs, key)
	delete(g.fromFile, key)
	delete(g.pendingBound, key)
}

func (g *schemaCatalog) putPending(table string, cols []schemaCol) {
	if g.pending == nil {
		g.pending = map[string]*pendingDef{}
	}
	g.pending[table] = &pendingDef{table: table, cols: cloneSchemaCols(cols)}
}

func (g *schemaCatalog) putPendingBad(table string, why refuseReason) {
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
func (g *schemaCatalog) observe(schema, table string) bool {
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

func (g *schemaCatalog) bindPending(schema, table string) {
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

func (g *schemaCatalog) abandon(table string) bool {
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
func (g *schemaCatalog) foldCase(schema, table string) {
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
		g.foldNotes = append(g.foldNotes, newFlashWarning(WarnSchemaFileCaseFolded, "warning.flashbackSchemaFold", map[string]any{
			"Binlog": binlog,
			"File":   names[0],
		}))
		return
	}
	if g.foldMiss == nil {
		g.foldMiss = map[string][]string{}
	}
	g.foldMiss[key] = names
	g.foldNotes = append(g.foldNotes, newFlashWarning(WarnSchemaFileCaseMismatch, "warning.flashbackSchemaFoldMiss", map[string]any{
		"Binlog": binlog,
		"File":   strings.Join(names, ", "),
	}))
}

func (g *schemaCatalog) schemaWarnings() []FlashbackWarning {
	if g == nil {
		return nil
	}
	var out []FlashbackWarning
	out = append(out, g.foldNotes...)
	if g.ambiguousDB && g.sawUnqualified {
		out = append(out, newFlashWarning(WarnSchemaFileDBMismatch, "warning.flashbackSchemaDB", map[string]any{
			"Header": g.headerDB,
			"Flag":   g.flagDB,
		}))
	}
	for _, item := range g.abandoned {
		out = append(out, newFlashWarning(WarnSchemaFileTableAmbiguous, "warning.flashbackSchemaTable", map[string]any{
			"Table":   item.table,
			"Schemas": strings.Join(item.schemas, ", "),
		}))
	}
	return out
}
