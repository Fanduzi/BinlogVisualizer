package guard

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
)

// Piece is one unit of a flashback script. Comment lines are not digested.
// Stmt is prepared from the same string that is hashed. Raw is executed as
// written; the bytes are also the digest payload, because START TRANSACTION
// and the guard header cannot be prepared.
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

	digestInitSQL = "SET @binlogviz_digest = SHA2('binlogviz-flashback-v1', 256);"
	digestStepSQL = "SET @binlogviz_digest = SHA2(CONCAT(@binlogviz_digest, @binlogviz_sql), 256);"

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
	// client that skips verify cannot commit. verify replaces it.
	noCommitSQL = "SET SESSION sql_mode = 'binlogviz: flashback script has no commit trailer; run binlogviz flashback verify';"

	finishMismatch = "SET @binlogviz_finish = IF(@binlogviz_sealed <=> 1, 'DO 0', 'SET SESSION sql_mode = ''binlogviz: flashback script was modified or truncated or its guard token does not match''');"
	trailerStepSQL = "SET @binlogviz_digest = SHA2(CONCAT(@binlogviz_digest, 'binlogviz-trailer-v1'), 256);"

	commitBlock = "SET @binlogviz_commit = IF(@binlogviz_sealed <=> 1, 'COMMIT', 'ROLLBACK');\n" +
		"PREPARE binlogviz_fb_commit_x9q FROM @binlogviz_commit;\n" +
		"EXECUTE binlogviz_fb_commit_x9q;\n" +
		"DEALLOCATE PREPARE binlogviz_fb_commit_x9q;\n"
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
// in the same order. The published script does not commit.
func (g Guard) Write(w io.Writer, render func(emit func(Piece) error) error) error {
	var token string
	if g.header != "" {
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
		sum := h.Sum(nil)
		token = hex.EncodeToString(sum[:8])
	}
	sub := func(s string) string {
		if token == "" {
			return s
		}
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
	if err := writeTrailer(write, digest); err != nil {
		return err
	}
	return bw.Flush()
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
	for _, line := range []string{stmtPrepare, stmtExecute, stmtDealloc} {
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
	return write(raw)
}

func writeTrailer(write func(string) error, digest string) error {
	final := sha256hex(digest + trailerMark)
	sealed := "SET @binlogviz_sealed = IF(@binlogviz_digest = '" + final + "' AND NOT (@binlogviz_bad <=> 1), 1, 0);"
	for _, line := range []string{
		markerTrailer,
		trailerStepSQL,
		sealed,
		finishMismatch,
		finishPrepare,
		finishExecute,
		finishDealloc,
		noCommitSQL,
	} {
		if err := write(line + "\n"); err != nil {
			return err
		}
	}
	return nil
}

func sha256hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// Verify recomputes the apply digest. It does not connect to MySQL.
func Verify(r io.Reader) (Result, error) {
	body, err := io.ReadAll(r)
	if err != nil {
		return Result{}, err
	}
	return verifyText(string(body))
}

// ApplySQL is the script verify prints: the same bytes, with the no-commit
// line replaced by the conditional COMMIT.
func ApplySQL(script string) (string, error) {
	if _, err := verifyText(script); err != nil {
		return "", err
	}
	const line = noCommitSQL + "\n"
	if !strings.HasSuffix(script, line) {
		return "", &VerifyError{Reason: "missing_trailer"}
	}
	return strings.TrimSuffix(script, line) + commitBlock, nil
}

func verifyText(script string) (Result, error) {
	if strings.TrimSpace(script) == "" {
		return Result{}, &VerifyError{Reason: "not_flashback"}
	}
	p := &scrParser{s: script}
	if !p.seekInit() {
		if strings.Contains(script, "binlogviz-digest") || strings.Contains(script, markerExec) || strings.Contains(script, "-- flashback reverses") {
			return Result{}, &VerifyError{Reason: "truncated"}
		}
		return Result{}, &VerifyError{Reason: "not_flashback"}
	}
	var payloads []string
	for {
		p.skipGaps()
		if p.eof() {
			return Result{}, &VerifyError{Reason: "missing_trailer"}
		}
		switch {
		case p.hasLine(markerTrailer):
			hexDigest, err := p.trailer()
			if err != nil {
				return Result{}, err
			}
			p.skipGaps()
			if !p.hasLine(noCommitSQL) {
				return Result{}, &VerifyError{Reason: "missing_trailer"}
			}
			p.skipGaps()
			if !p.eof() {
				return Result{}, &VerifyError{Reason: "appended"}
			}
			got := chain(payloads)
			if got != hexDigest {
				return Result{}, &VerifyError{Reason: "digest_mismatch"}
			}
			return Result{Statements: len(payloads), Digest: got}, nil
		case p.hasLine(markerExec):
			payload, err := p.execPiece()
			if err != nil {
				return Result{}, err
			}
			payloads = append(payloads, payload)
		case p.hasLine(markerRaw):
			payload, err := p.rawPiece()
			if err != nil {
				return Result{}, err
			}
			payloads = append(payloads, payload)
		default:
			return Result{}, &VerifyError{Reason: "appended"}
		}
	}
}

func chain(payloads []string) string {
	d := sha256hex(digestDomain)
	for _, payload := range payloads {
		d = sha256hex(d + payload)
	}
	return sha256hex(d + trailerMark)
}

type scrParser struct {
	s string
	i int
}

func (p *scrParser) rest() string { return p.s[p.i:] }

func (p *scrParser) eof() bool { return p.i >= len(p.s) }

func (p *scrParser) skipGaps() {
	for {
		p.skipSpaceLines()
		if p.eof() {
			return
		}
		line, ok := p.peekLine()
		if !ok {
			return
		}
		trim := strings.TrimSpace(line)
		if strings.HasPrefix(trim, "--") && trim != markerExec && trim != markerRaw && trim != markerTrailer {
			p.skipLine()
			continue
		}
		return
	}
}

func (p *scrParser) skipSpaceLines() {
	for p.i < len(p.s) {
		if p.s[p.i] == ' ' || p.s[p.i] == '\t' || p.s[p.i] == '\r' {
			p.i++
			continue
		}
		if p.s[p.i] == '\n' {
			p.i++
			continue
		}
		// a line of only spaces is consumed by the loop; a content line stops
		// before its first non-space only if we haven't started the line.
		// Restart at the line start if the line has content.
		start := p.i
		for p.i < len(p.s) && p.s[p.i] != '\n' && (p.s[p.i] == ' ' || p.s[p.i] == '\t' || p.s[p.i] == '\r') {
			p.i++
		}
		if p.i < len(p.s) && p.s[p.i] != '\n' {
			p.i = start
			return
		}
	}
}

func (p *scrParser) peekLine() (string, bool) {
	if p.eof() {
		return "", false
	}
	rest := p.rest()
	n := strings.IndexByte(rest, '\n')
	if n < 0 {
		return rest, true
	}
	return rest[:n], true
}

func (p *scrParser) skipLine() {
	rest := p.rest()
	n := strings.IndexByte(rest, '\n')
	if n < 0 {
		p.i = len(p.s)
		return
	}
	p.i += n + 1
}

func (p *scrParser) hasLine(want string) bool {
	line, ok := p.peekLine()
	if !ok || strings.TrimRight(line, "\r") != want {
		return false
	}
	p.skipLine()
	return true
}

func (p *scrParser) seekInit() bool {
	for !p.eof() {
		p.skipGaps()
		if p.hasLine(digestInitSQL) {
			return true
		}
		// skipGaps stopped on a non-comment. Not the init line.
		return false
	}
	return false
}

func (p *scrParser) execPiece() (string, error) {
	payload, err := p.sqlPayload()
	if err != nil {
		return "", err
	}
	for _, want := range []string{stmtPrepare, stmtExecute, stmtDealloc} {
		if !p.hasLine(want) {
			return "", &VerifyError{Reason: "truncated"}
		}
	}
	return payload, nil
}

func (p *scrParser) rawPiece() (string, error) {
	payload, err := p.sqlPayload()
	if err != nil {
		return "", err
	}
	if len(p.rest()) < len(payload) {
		return "", &VerifyError{Reason: "truncated"}
	}
	if p.rest()[:len(payload)] != payload {
		return "", &VerifyError{Reason: "digest_mismatch"}
	}
	p.i += len(payload)
	return payload, nil
}

func (p *scrParser) sqlPayload() (string, error) {
	line, ok := p.peekLine()
	if !ok || !strings.HasPrefix(line, "SET @binlogviz_sql = ") {
		return "", &VerifyError{Reason: "truncated"}
	}
	lit := strings.TrimSuffix(strings.TrimPrefix(line, "SET @binlogviz_sql = "), ";")
	payload, err := sqlUnquote(lit)
	if err != nil {
		return "", &VerifyError{Reason: "truncated"}
	}
	p.skipLine()
	if !p.hasLine(digestStepSQL) {
		return "", &VerifyError{Reason: "truncated"}
	}
	return payload, nil
}

func (p *scrParser) trailer() (string, error) {
	if !p.hasLine(trailerStepSQL) {
		return "", &VerifyError{Reason: "truncated"}
	}
	line, ok := p.peekLine()
	const prefix = "SET @binlogviz_sealed = IF(@binlogviz_digest = '"
	const suffix = "' AND NOT (@binlogviz_bad <=> 1), 1, 0);"
	if !ok || !strings.HasPrefix(line, prefix) || !strings.HasSuffix(line, suffix) {
		return "", &VerifyError{Reason: "truncated"}
	}
	hexDigest := strings.TrimSuffix(strings.TrimPrefix(line, prefix), suffix)
	if len(hexDigest) != 64 || strings.Trim(hexDigest, "0123456789abcdef") != "" {
		return "", &VerifyError{Reason: "digest_mismatch"}
	}
	p.skipLine()
	for _, want := range []string{finishMismatch, finishPrepare, finishExecute, finishDealloc} {
		if !p.hasLine(want) {
			return "", &VerifyError{Reason: "truncated"}
		}
	}
	return hexDigest, nil
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
