package guard

import (
	"bufio"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
)

// Piece is one unit of a flashback script. Comment lines are not digested.
// Stmt is prepared from the same string that is hashed. Raw is executed as
// written; the bytes are also the digest payload, because XA START, the
// finish procedure, and the guard header cannot be prepared.
type PieceKind int

const (
	PieceComment PieceKind = iota
	PieceStmt
	PieceRaw
)

// Piece is one comment, prepared statement, or raw section.
type Piece struct {
	Kind PieceKind
	Text string
}

const (
	digestDomain = "binlogviz-flashback-v1"
	trailerMark  = "binlogviz-trailer-v1"

	digestInitSQL = "SET @binlogviz_digest = SHA2('binlogviz-flashback-v1', 256), @binlogviz_applied = 1;"
	digestStepSQL = "SET @binlogviz_digest = SHA2(CONCAT(@binlogviz_digest, @binlogviz_sql), 256);"
	// appliedCheckSQL records the previous statement. A later success must not
	// clear it: @@error_count is only the last statement.
	appliedCheckSQL = "SET @binlogviz_applied = IF(@binlogviz_applied <=> 1 AND @@error_count = 0, 1, 0);"

	markerExec    = "-- binlogviz-exec"
	markerRaw     = "-- binlogviz-raw"
	markerTrailer = "-- binlogviz-trailer"

	stmtPrepare = "PREPARE binlogviz_fb_stmt_x9q FROM @binlogviz_sql;"
	stmtExecute = "EXECUTE binlogviz_fb_stmt_x9q;"
	stmtDealloc = "DEALLOCATE PREPARE binlogviz_fb_stmt_x9q;"

	finishPrepare = "PREPARE binlogviz_fb_finish_x9q FROM @binlogviz_finish;"
	finishExecute = "EXECUTE binlogviz_fb_finish_x9q;"
	finishDealloc = "DEALLOCATE PREPARE binlogviz_fb_finish_x9q;"

	// noCommitSQL is the last line of a published script. It errors so a
	// client that skips verify cannot treat the run as success. The XA is
	// already rolled back by then. verify replaces the finish through this line.
	noCommitSQL = "SET SESSION sql_mode = 'binlogviz: flashback script has no commit trailer; run binlogviz flashback verify';"

	trailerStepSQL = "SET @binlogviz_digest = SHA2(CONCAT(@binlogviz_digest, 'binlogviz-trailer-v1'), 256);"
)

// VerifyError is a script verify will not commit. Reason is the stable
// details.reason value.
type VerifyError struct {
	Reason string
}

func (e *VerifyError) Error() string {
	if e == nil {
		return ""
	}
	return e.Reason
}

// Result is a script whose digest matches its trailer.
type Result struct {
	Statements int
	Digest     string
}

// Write seals a script. render is called twice and must emit the same pieces
// in the same order. The published script does not commit. The token names
// the XA id and the finish procedure. It is the guard session token when the
// script has a guard.
func (g Guard) Write(w io.Writer, render func(emit func(Piece) error) error) error {
	h := sha256.New()
	if err := render(func(p Piece) error {
		if p.Kind == PieceComment {
			return nil
		}
		_, _ = h.Write([]byte(p.Text))
		_, _ = h.Write([]byte{0})
		return nil
	}); err != nil {
		return err
	}
	token := hex.EncodeToString(h.Sum(nil)[:8])
	sub := func(s string) string {
		return strings.ReplaceAll(s, guardTokenMark, token)
	}

	bw := bufio.NewWriter(w)
	digest := sha256hex(digestDomain)
	started := false
	write := func(s string) error {
		_, err := bw.WriteString(s)
		return err
	}
	ensure := func() error {
		if started {
			return nil
		}
		started = true
		return write(digestInitSQL + "\n")
	}
	err := render(func(p Piece) error {
		switch p.Kind {
		case PieceComment:
			text := p.Text
			if text == "" {
				return nil
			}
			if !strings.HasSuffix(text, "\n") {
				text += "\n"
			}
			return write(text)
		case PieceStmt:
			if err := ensure(); err != nil {
				return err
			}
			stmt := strings.TrimSpace(sub(p.Text))
			stmt = strings.TrimSuffix(stmt, ";")
			return writeExec(write, &digest, stmt)
		case PieceRaw:
			if err := ensure(); err != nil {
				return err
			}
			raw := sub(p.Text)
			if !strings.HasSuffix(raw, "\n") {
				raw += "\n"
			}
			return writeRaw(write, &digest, raw)
		default:
			return fmt.Errorf("guard: unknown piece")
		}
	})
	if err != nil {
		return err
	}
	if !started {
		if err := ensure(); err != nil {
			return err
		}
	}
	if err := writeTrailer(write, digest, token); err != nil {
		return err
	}
	return bw.Flush()
}

// ProcedureSQL creates the finish procedure before the undo transaction.
// Write substitutes guardTokenMark. The stock mysql client is required
// because the body uses DELIMITER. The procedure is schema-qualified so the
// session needs no default database.
func ProcedureSQL() string {
	name := "binlogviz.binlogviz_fb_end_" + guardTokenMark
	return "CREATE DATABASE IF NOT EXISTS binlogviz;\n" +
		"DROP PROCEDURE IF EXISTS " + name + ";\n" +
		"DELIMITER $$\n" +
		"CREATE PROCEDURE " + name + "()\n" +
		"SQL SECURITY INVOKER\n" +
		"BEGIN\n" +
		"  DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN RESIGNAL; END;\n" +
		"  IF @binlogviz_sealed <=> 1 AND @binlogviz_applied <=> 1 AND @binlogviz_want <=> 1 THEN\n" +
		"    XA END '" + guardTokenMark + "';\n" +
		"    XA COMMIT '" + guardTokenMark + "' ONE PHASE;\n" +
		"  ELSE\n" +
		"    XA END '" + guardTokenMark + "';\n" +
		"    XA ROLLBACK '" + guardTokenMark + "';\n" +
		"  END IF;\n" +
		"END$$\n" +
		"DELIMITER ;\n"
}

// UndoOpenSQL opens the one undo transaction. If the open fails, the session
// is read-only, so a client that continues cannot autocommit the undo.
func UndoOpenSQL() string {
	return "XA START '" + guardTokenMark + "';\n" +
		appliedCheckSQL + "\n" +
		"SET @binlogviz_hold = IF(@binlogviz_applied <=> 1, 'DO 0', 'SET SESSION TRANSACTION READ ONLY');\n" +
		"PREPARE binlogviz_fb_open_x9q FROM @binlogviz_hold;\n" +
		"EXECUTE binlogviz_fb_open_x9q;\n" +
		"DEALLOCATE PREPARE binlogviz_fb_open_x9q;\n"
}

func procedureName(token string) string {
	return "binlogviz.binlogviz_fb_end_" + token
}

func finishBody(token string, want int) string {
	name := procedureName(token)
	return fmt.Sprintf("SET @binlogviz_want = %d;\nCALL %s();\nDROP PROCEDURE IF EXISTS %s;\n", want, name, name)
}

func publishedSuffix(token string) string {
	return finishBody(token, 0) + noCommitSQL + "\n"
}

func applySuffix(token, nonce string) string {
	name := procedureName(token) + "_" + nonce
	return fmt.Sprintf("SET @binlogviz_want = 1;\nCALL %s();\nDROP PROCEDURE IF EXISTS %s;\n", name, name)
}

// newApplyNonce is 16 hex characters, one per verify. Tests pin it.
var newApplyNonce = func() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func suffixLen() int {
	return len(publishedSuffix(strings.Repeat("0", 16)))
}

func parsePublishedSuffix(s string) (string, bool) {
	const head = "SET @binlogviz_want = 0;\nCALL binlogviz.binlogviz_fb_end_"
	if !strings.HasPrefix(s, head) || len(s) != suffixLen() {
		return "", false
	}
	token := s[len(head) : len(head)+16]
	if strings.Trim(token, "0123456789abcdef") != "" {
		return "", false
	}
	rest := "();\nDROP PROCEDURE IF EXISTS " + procedureName(token) + ";\n" + noCommitSQL + "\n"
	if s[len(head)+16:] != rest {
		return "", false
	}
	return token, true
}

func writeExec(write func(string) error, digest *string, stmt string) error {
	if err := write(markerExec + "\n"); err != nil {
		return err
	}
	if err := write("SET @binlogviz_sql = " + payloadQuote(stmt) + ";\n"); err != nil {
		return err
	}
	if err := write(digestStepSQL + "\n"); err != nil {
		return err
	}
	*digest = sha256hex(*digest + stmt)
	for _, line := range []string{stmtPrepare, appliedCheckSQL, stmtExecute, appliedCheckSQL, stmtDealloc} {
		if err := write(line + "\n"); err != nil {
			return err
		}
	}
	return nil
}

func writeRaw(write func(string) error, digest *string, raw string) error {
	if err := write(markerRaw + "\n"); err != nil {
		return err
	}
	if err := write("SET @binlogviz_sql = " + payloadQuote(raw) + ";\n"); err != nil {
		return err
	}
	if err := write(digestStepSQL + "\n"); err != nil {
		return err
	}
	*digest = sha256hex(*digest + raw)
	if err := write(raw); err != nil {
		return err
	}
	return write(appliedCheckSQL + "\n")
}

func writeTrailer(write func(string) error, digest, token string) error {
	final := sha256hex(digest + trailerMark)
	sealed := "SET @binlogviz_sealed = IF(@binlogviz_digest = '" + final + "' AND NOT (@binlogviz_bad <=> 1), 1, 0);"
	for _, line := range []string{markerTrailer, trailerStepSQL, sealed} {
		if err := write(line + "\n"); err != nil {
			return err
		}
	}
	return write(publishedSuffix(token))
}

func sha256hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// Verify recomputes the apply digest from r. It reads the script once
// and does not buffer it. It does not connect to MySQL.
func Verify(r io.Reader) (Result, error) {
	return verifyStream(r)
}

// ApplySQL is the script verify prints. The file is checked first. The finish
// may commit, and the procedure name is unique to this call so a leftover
// routine is not dropped. The XA id stays the session token.
func ApplySQL(script string) (string, error) {
	if _, err := verifyStream(strings.NewReader(script)); err != nil {
		return "", err
	}
	n := suffixLen()
	if len(script) < n {
		return "", &VerifyError{Reason: "missing_trailer"}
	}
	token, ok := parsePublishedSuffix(script[len(script)-n:])
	if !ok {
		return "", &VerifyError{Reason: "missing_trailer"}
	}
	nonce, err := newApplyNonce()
	if err != nil {
		return "", err
	}
	var b strings.Builder
	if err := rewriteApply(&b, strings.NewReader(script), token, nonce); err != nil {
		return "", err
	}
	return b.String(), nil
}

// CopyApply streams a verified script to w. It writes nothing until the
// no-commit suffix matches, then renames the finish procedure the same way
// ApplySQL does. n is the byte length of r.
func CopyApply(w io.Writer, r io.ReadSeeker, n int64) error {
	nSuffix := int64(suffixLen())
	if n < nSuffix {
		return &VerifyError{Reason: "missing_trailer"}
	}
	if _, err := r.Seek(n-nSuffix, io.SeekStart); err != nil {
		return err
	}
	tail := make([]byte, nSuffix)
	if _, err := io.ReadFull(r, tail); err != nil {
		return err
	}
	token, ok := parsePublishedSuffix(string(tail))
	if !ok {
		return &VerifyError{Reason: "missing_trailer"}
	}
	nonce, err := newApplyNonce()
	if err != nil {
		return err
	}
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return rewriteApply(w, io.LimitReader(r, n), token, nonce)
}

// rewriteApply copies a verified script. The finish procedure payload is
// renamed to procedureName(token)_nonce and the trailer hex is recomputed
// from that payload. Every other payload is copied unchanged. The XA id is
// the token and is not renamed.
func rewriteApply(w io.Writer, r io.Reader, token, nonce string) error {
	br := bufio.NewReaderSize(r, 64<<10)
	bw := bufio.NewWriter(w)
	digest := sha256hex(digestDomain)
	for {
		line, err := readScriptLine(br)
		if err != nil {
			return err
		}
		switch line {
		case markerExec:
			if err := writeScriptLine(bw, line); err != nil {
				return err
			}
			payload, err := copyExecPiece(bw, br)
			if err != nil {
				return err
			}
			digest = chainStep(digest, payload)
		case markerRaw:
			payload, err := rewriteRawPiece(bw, br, token, nonce)
			if err != nil {
				return err
			}
			digest = chainStep(digest, payload)
		case markerTrailer:
			if err := writeApplyTrailer(bw, digest, token, nonce); err != nil {
				return err
			}
			return bw.Flush()
		default:
			if err := writeScriptLine(bw, line); err != nil {
				return err
			}
		}
	}
}

func readScriptLine(br *bufio.Reader) (string, error) {
	s, err := br.ReadString('\n')
	if len(s) == 0 && err != nil {
		return "", err
	}
	if err != nil && err != io.EOF {
		return "", err
	}
	if !strings.HasSuffix(s, "\n") {
		return "", io.ErrUnexpectedEOF
	}
	return strings.TrimSuffix(s, "\n"), nil
}

func writeScriptLine(w io.Writer, line string) error {
	_, err := io.WriteString(w, line+"\n")
	return err
}

func unquoteSQLLine(line string) (string, error) {
	const pfx = "SET @binlogviz_sql = "
	if !strings.HasPrefix(line, pfx) || !strings.HasSuffix(line, ";") {
		return "", fmt.Errorf("guard: apply payload")
	}
	return sqlUnquote(strings.TrimSuffix(strings.TrimPrefix(line, pfx), ";"))
}

func copyExecPiece(w io.Writer, br *bufio.Reader) (string, error) {
	sqlLine, err := readScriptLine(br)
	if err != nil {
		return "", err
	}
	if err := writeScriptLine(w, sqlLine); err != nil {
		return "", err
	}
	payload, err := unquoteSQLLine(sqlLine)
	if err != nil {
		return "", err
	}
	step, err := readScriptLine(br)
	if err != nil {
		return "", err
	}
	if step != digestStepSQL {
		return "", fmt.Errorf("guard: apply digest step")
	}
	if err := writeScriptLine(w, step); err != nil {
		return "", err
	}
	for _, want := range []string{stmtPrepare, appliedCheckSQL, stmtExecute, appliedCheckSQL, stmtDealloc} {
		line, err := readScriptLine(br)
		if err != nil {
			return "", err
		}
		if line != want {
			return "", fmt.Errorf("guard: apply exec piece")
		}
		if err := writeScriptLine(w, line); err != nil {
			return "", err
		}
	}
	return payload, nil
}

func rewriteRawPiece(w io.Writer, br *bufio.Reader, token, nonce string) (string, error) {
	if err := writeScriptLine(w, markerRaw); err != nil {
		return "", err
	}
	sqlLine, err := readScriptLine(br)
	if err != nil {
		return "", err
	}
	payload, err := unquoteSQLLine(sqlLine)
	if err != nil {
		return "", err
	}
	step, err := readScriptLine(br)
	if err != nil {
		return "", err
	}
	if step != digestStepSQL {
		return "", fmt.Errorf("guard: apply digest step")
	}
	name := procedureName(token)
	rewritten := payload
	if strings.HasPrefix(payload, "CREATE DATABASE IF NOT EXISTS binlogviz;\n") && strings.Contains(payload, "CREATE PROCEDURE "+name+"()") {
		rewritten = strings.ReplaceAll(payload, name, name+"_"+nonce)
	}
	if rewritten != payload {
		if err := writeScriptLine(w, "SET @binlogviz_sql = "+payloadQuote(rewritten)+";"); err != nil {
			return "", err
		}
	} else if err := writeScriptLine(w, sqlLine); err != nil {
		return "", err
	}
	if err := writeScriptLine(w, step); err != nil {
		return "", err
	}
	if rewritten != payload {
		if _, err := io.WriteString(w, rewritten); err != nil {
			return "", err
		}
		if _, err := io.CopyN(io.Discard, br, int64(len(payload))); err != nil {
			return "", err
		}
	} else if _, err := io.CopyN(w, br, int64(len(payload))); err != nil {
		return "", err
	}
	applied, err := readScriptLine(br)
	if err != nil {
		return "", err
	}
	if applied != appliedCheckSQL {
		return "", fmt.Errorf("guard: apply raw piece")
	}
	if err := writeScriptLine(w, applied); err != nil {
		return "", err
	}
	return rewritten, nil
}

func writeApplyTrailer(w io.Writer, digest, token, nonce string) error {
	final := chainStep(digest, trailerMark)
	sealed := "SET @binlogviz_sealed = IF(@binlogviz_digest = '" + final + "' AND NOT (@binlogviz_bad <=> 1), 1, 0);"
	for _, line := range []string{markerTrailer, trailerStepSQL, sealed} {
		if err := writeScriptLine(w, line); err != nil {
			return err
		}
	}
	_, err := io.WriteString(w, applySuffix(token, nonce))
	return err
}

func verifyStream(r io.Reader) (Result, error) {
	p := &scrReader{br: bufio.NewReaderSize(r, 64<<10), armed: true}
	found, err := p.seekInit()
	if err != nil {
		return Result{}, err
	}
	if !found {
		if err := p.drain(); err != nil {
			return Result{}, err
		}
		if p.hint {
			return Result{}, &VerifyError{Reason: "truncated"}
		}
		return Result{}, &VerifyError{Reason: "not_flashback"}
	}
	p.armed = false
	p.carry = ""
	digest := sha256hex(digestDomain)
	n := 0
	for {
		if err := p.skipGaps(); err != nil {
			return Result{}, err
		}
		if !p.lined {
			return Result{}, &VerifyError{Reason: "missing_trailer"}
		}
		ok, err := p.hasLine(markerTrailer)
		if err != nil {
			return Result{}, err
		}
		if ok {
			hexDigest, err := p.trailer()
			if err != nil {
				return Result{}, err
			}
			if err := p.skipGaps(); err != nil {
				return Result{}, err
			}
			ok, err = p.hasLine(noCommitSQL)
			if err != nil {
				return Result{}, err
			}
			if !ok {
				return Result{}, &VerifyError{Reason: "missing_trailer"}
			}
			if err := p.skipGaps(); err != nil {
				return Result{}, err
			}
			if p.lined {
				return Result{}, &VerifyError{Reason: "appended"}
			}
			got := chainStep(digest, trailerMark)
			if got != hexDigest {
				return Result{}, &VerifyError{Reason: "digest_mismatch"}
			}
			return Result{Statements: n, Digest: got}, nil
		}
		ok, err = p.hasLine(markerExec)
		if err != nil {
			return Result{}, err
		}
		if ok {
			payload, err := p.execPiece()
			if err != nil {
				return Result{}, err
			}
			digest = chainStep(digest, payload)
			n++
			continue
		}
		ok, err = p.hasLine(markerRaw)
		if err != nil {
			return Result{}, err
		}
		if ok {
			payload, err := p.rawPiece()
			if err != nil {
				return Result{}, err
			}
			digest = chainStep(digest, payload)
			n++
			continue
		}
		return Result{}, &VerifyError{Reason: "appended"}
	}
}

func chainStep(prev, payload string) string {
	h := sha256.New()
	_, _ = io.WriteString(h, prev)
	_, _ = io.WriteString(h, payload)
	return hex.EncodeToString(h.Sum(nil))
}

// scrReader walks a script forward. armed notes a few marker strings until
// the digest init line is found, so a non-script is not buffered to decide
// truncated versus not_flashback.
type scrReader struct {
	br      *bufio.Reader
	line    string
	lined   bool
	eof     bool
	readErr error
	armed   bool
	hint    bool
	carry   string
}

func (p *scrReader) note(s string) {
	if !p.armed || p.hint || s == "" {
		return
	}
	const keep = 32
	head := s
	if len(head) > keep {
		head = s[:keep]
	}
	buf := p.carry + head
	for _, n := range []string{"binlogviz-digest", markerExec, "-- flashback reverses"} {
		if strings.Contains(s, n) || strings.Contains(buf, n) {
			p.hint = true
			p.carry = ""
			return
		}
	}
	if len(s) >= keep {
		p.carry = s[len(s)-keep:]
		return
	}
	buf = p.carry + s
	if len(buf) > keep {
		p.carry = buf[len(buf)-keep:]
	} else {
		p.carry = buf
	}
}

func (p *scrReader) readRaw() (string, error) {
	if p.readErr != nil {
		return "", p.readErr
	}
	s, err := p.br.ReadString('\n')
	if len(s) > 0 {
		p.note(s)
	}
	if err == io.EOF {
		if len(s) == 0 {
			p.eof = true
			return "", io.EOF
		}
		return s, nil
	}
	if err != nil {
		p.readErr = err
		return "", err
	}
	return s, nil
}

func (p *scrReader) fill() error {
	if p.lined || p.eof {
		return nil
	}
	raw, err := p.readRaw()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}
	p.line = strings.TrimSuffix(raw, "\n")
	p.lined = true
	return nil
}

func (p *scrReader) drain() error {
	p.lined = false
	p.line = ""
	for {
		_, err := p.readRaw()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (p *scrReader) hasLine(want string) (bool, error) {
	if err := p.fill(); err != nil {
		return false, err
	}
	if !p.lined || strings.TrimRight(p.line, "\r") != want {
		return false, nil
	}
	p.lined = false
	p.line = ""
	return true, nil
}

func (p *scrReader) skipGaps() error {
	for {
		if err := p.fill(); err != nil {
			return err
		}
		if !p.lined {
			return nil
		}
		p.line = strings.TrimLeft(p.line, " \t\r")
		if p.line == "" {
			p.lined = false
			continue
		}
		trim := strings.TrimSpace(p.line)
		if strings.HasPrefix(trim, "--") && trim != markerExec && trim != markerRaw && trim != markerTrailer {
			p.lined = false
			p.line = ""
			continue
		}
		return nil
	}
}

func (p *scrReader) seekInit() (bool, error) {
	if err := p.skipGaps(); err != nil {
		return false, err
	}
	return p.hasLine(digestInitSQL)
}

func (p *scrReader) execPiece() (string, error) {
	payload, err := p.sqlPayload()
	if err != nil {
		return "", err
	}
	for _, want := range []string{stmtPrepare, appliedCheckSQL, stmtExecute, appliedCheckSQL, stmtDealloc} {
		ok, err := p.hasLine(want)
		if err != nil {
			return "", err
		}
		if !ok {
			return "", &VerifyError{Reason: "truncated"}
		}
	}
	return payload, nil
}

func (p *scrReader) rawPiece() (string, error) {
	payload, err := p.sqlPayload()
	if err != nil {
		return "", err
	}
	buf := make([]byte, len(payload))
	if _, err := io.ReadFull(p.br, buf); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return "", &VerifyError{Reason: "truncated"}
		}
		p.readErr = err
		return "", err
	}
	if string(buf) != payload {
		return "", &VerifyError{Reason: "digest_mismatch"}
	}
	ok, err := p.hasLine(appliedCheckSQL)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", &VerifyError{Reason: "truncated"}
	}
	return payload, nil
}

func (p *scrReader) sqlPayload() (string, error) {
	if err := p.fill(); err != nil {
		return "", err
	}
	if !p.lined || !strings.HasPrefix(p.line, "SET @binlogviz_sql = ") {
		return "", &VerifyError{Reason: "truncated"}
	}
	lit := strings.TrimSuffix(strings.TrimPrefix(p.line, "SET @binlogviz_sql = "), ";")
	payload, err := sqlUnquote(lit)
	if err != nil {
		return "", &VerifyError{Reason: "truncated"}
	}
	p.lined = false
	p.line = ""
	ok, err := p.hasLine(digestStepSQL)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", &VerifyError{Reason: "truncated"}
	}
	return payload, nil
}

func (p *scrReader) trailer() (string, error) {
	ok, err := p.hasLine(trailerStepSQL)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", &VerifyError{Reason: "truncated"}
	}
	if err := p.fill(); err != nil {
		return "", err
	}
	if !p.lined {
		return "", &VerifyError{Reason: "truncated"}
	}
	line := p.line
	const prefix = "SET @binlogviz_sealed = IF(@binlogviz_digest = '"
	const suffix = "' AND NOT (@binlogviz_bad <=> 1), 1, 0);"
	if !strings.HasPrefix(line, prefix) || !strings.HasSuffix(line, suffix) {
		return "", &VerifyError{Reason: "truncated"}
	}
	hexDigest := strings.TrimSuffix(strings.TrimPrefix(line, prefix), suffix)
	if len(hexDigest) != 64 || strings.Trim(hexDigest, "0123456789abcdef") != "" {
		return "", &VerifyError{Reason: "digest_mismatch"}
	}
	p.lined = false
	p.line = ""
	if err := p.publishedFinish(); err != nil {
		return "", err
	}
	return hexDigest, nil
}

func (p *scrReader) publishedFinish() error {
	ok, err := p.hasLine("SET @binlogviz_want = 0;")
	if err != nil {
		return err
	}
	if !ok {
		return &VerifyError{Reason: "truncated"}
	}
	if err := p.fill(); err != nil {
		return err
	}
	if !p.lined {
		return &VerifyError{Reason: "truncated"}
	}
	const prefix = "CALL binlogviz.binlogviz_fb_end_"
	const suffix = "();"
	if !strings.HasPrefix(p.line, prefix) || !strings.HasSuffix(p.line, suffix) {
		return &VerifyError{Reason: "truncated"}
	}
	token := strings.TrimSuffix(strings.TrimPrefix(p.line, prefix), suffix)
	if len(token) != 16 || strings.Trim(token, "0123456789abcdef") != "" {
		return &VerifyError{Reason: "truncated"}
	}
	p.lined = false
	p.line = ""
	ok, err = p.hasLine("DROP PROCEDURE IF EXISTS " + procedureName(token) + ";")
	if err != nil {
		return err
	}
	if !ok {
		return &VerifyError{Reason: "truncated"}
	}
	return nil
}

func chain(payloads []string) string {
	d := sha256hex(digestDomain)
	for _, payload := range payloads {
		d = chainStep(d, payload)
	}
	return chainStep(d, trailerMark)
}

// payloadQuote is safe for the client to parse before the script has changed
// sql_mode. A value with no backslash, newline, or NUL uses doubled quotes,
// which MySQL accepts with NO_BACKSLASH_ESCAPES on or off. Anything else uses
// backslash escapes and must run after the script has removed that mode.
func payloadQuote(s string) string {
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\', '\n', '\r', 0:
			return sqlQuote(s)
		}
	}
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('\'')
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			b.WriteByte('\'')
		}
		b.WriteByte(s[i])
	}
	b.WriteByte('\'')
	return b.String()
}

func sqlUnquote(lit string) (string, error) {
	if len(lit) < 2 || lit[0] != '\'' || lit[len(lit)-1] != '\'' {
		return "", fmt.Errorf("guard: bad string")
	}
	var b strings.Builder
	for i := 1; i < len(lit)-1; i++ {
		if lit[i] == '\'' && i+1 < len(lit)-1 && lit[i+1] == '\'' {
			b.WriteByte('\'')
			i++
			continue
		}
		if lit[i] != '\\' {
			b.WriteByte(lit[i])
			continue
		}
		if i+1 >= len(lit)-1 {
			return "", fmt.Errorf("guard: bad escape")
		}
		i++
		switch lit[i] {
		case '\\', '\'', '"':
			b.WriteByte(lit[i])
		case 'n':
			b.WriteByte('\n')
		case 'r':
			b.WriteByte('\r')
		case '0':
			b.WriteByte(0)
		default:
			b.WriteByte(lit[i])
		}
	}
	return b.String(), nil
}
