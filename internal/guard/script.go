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

// Verify recomputes the apply digest from r. It reads the script once
// and does not buffer it. It does not connect to MySQL.
func Verify(r io.Reader) (Result, error) {
	return verifyStream(r)
}

// ApplySQL is the script verify prints: the same bytes, with the no-commit
// line replaced by the conditional COMMIT.
func ApplySQL(script string) (string, error) {
	if _, err := verifyStream(strings.NewReader(script)); err != nil {
		return "", err
	}
	const line = noCommitSQL + "\n"
	if !strings.HasSuffix(script, line) {
		return "", &VerifyError{Reason: "missing_trailer"}
	}
	return script[:len(script)-len(line)] + commitBlock, nil
}

// CopyApply streams a verified script to w, replacing the no-commit line.
// It writes nothing until that line is the suffix. n is the byte length of r.
func CopyApply(w io.Writer, r io.ReadSeeker, n int64) error {
	const line = noCommitSQL + "\n"
	if n < int64(len(line)) {
		return &VerifyError{Reason: "missing_trailer"}
	}
	if _, err := r.Seek(n-int64(len(line)), io.SeekStart); err != nil {
		return err
	}
	tail := make([]byte, len(line))
	if _, err := io.ReadFull(r, tail); err != nil {
		return err
	}
	if string(tail) != line {
		return &VerifyError{Reason: "missing_trailer"}
	}
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err := io.CopyN(w, r, n-int64(len(line))); err != nil {
		return err
	}
	_, err := io.WriteString(w, commitBlock)
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
	for _, want := range []string{stmtPrepare, stmtExecute, stmtDealloc} {
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
	for _, want := range []string{finishMismatch, finishPrepare, finishExecute, finishDealloc} {
		ok, err := p.hasLine(want)
		if err != nil {
			return "", err
		}
		if !ok {
			return "", &VerifyError{Reason: "truncated"}
		}
	}
	return hexDigest, nil
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
