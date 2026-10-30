// input: the cobra command tree, every non-test Go file, the repository binlog fixtures, and the flashback refusal fixtures.
// output: fails if a user-facing command has no runnable Example, if an example does not run, if a command prompts or blocks on stdin, or if flashback --plan on exit 6 omits the refusal.
// pos: agent usage contract for help examples and non-interactive commands (production-readiness D3, D4).
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"binlogviz/internal/guard"
)

// userFacingCommands is every command a person can run: not hidden, and
// either runnable or a group of runnable commands. Help is runnable, so it
// is included. The walk is the list; callers do not name commands.
func userFacingCommands(root *cobra.Command) []*cobra.Command {
	var out []*cobra.Command
	var walk func(*cobra.Command)
	walk = func(c *cobra.Command) {
		if !c.Hidden && c.Deprecated == "" && (c.Runnable() || c.HasAvailableSubCommands()) {
			out = append(out, c)
		}
		for _, child := range c.Commands() {
			walk(child)
		}
	}
	walk(root)
	return out
}

func exampleInvocations(example string) ([][]string, error) {
	var out [][]string
	for _, line := range strings.Split(example, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		args, err := splitExampleLine(line)
		if err != nil {
			return nil, err
		}
		if len(args) == 0 || args[0] != "binlogviz" {
			return nil, errExampleLine(line)
		}
		out = append(out, args[1:])
	}
	return out, nil
}

type exampleLineError string

func (e exampleLineError) Error() string { return string(e) }

func errExampleLine(line string) error {
	return exampleLineError("example line is not a binlogviz command: " + line)
}

func splitExampleLine(line string) ([]string, error) {
	var args []string
	var b strings.Builder
	var quote rune
	for _, r := range line {
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
				continue
			}
			b.WriteRune(r)
		case r == '\'' || r == '"':
			quote = r
		case r == ' ' || r == '\t':
			if b.Len() > 0 {
				args = append(args, b.String())
				b.Reset()
			}
		default:
			b.WriteRune(r)
		}
	}
	if quote != 0 {
		return nil, exampleLineError("unclosed quote: " + line)
	}
	if b.Len() > 0 {
		args = append(args, b.String())
	}
	return args, nil
}

func invocationPath(args []string) string {
	return strings.TrimSpace("binlogviz " + strings.Join(args, " "))
}

func invokesCommand(args []string, path string) bool {
	got := invocationPath(args)
	return got == path || strings.HasPrefix(got, path+" ")
}

func TestEveryCommandHasExample(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	root := NewRootCommand()
	cmds := userFacingCommands(root)
	if len(cmds) < 2 {
		t.Fatalf("command walk found %d commands", len(cmds))
	}
	for _, c := range cmds {
		path := c.CommandPath()
		t.Run(path, func(t *testing.T) {
			invocations, err := exampleInvocations(c.Example)
			if err != nil {
				t.Fatal(err)
			}
			if len(invocations) == 0 {
				t.Fatalf("%s has no Example", path)
			}
			found := false
			for _, args := range invocations {
				if invokesCommand(args, path) {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("%s example never runs %q:\n%s", path, path, c.Example)
			}
			var buf strings.Builder
			c.SetOut(&buf)
			c.SetErr(&buf)
			if err := c.Help(); err != nil {
				t.Fatal(err)
			}
			help := buf.String()
			for _, line := range strings.Split(c.Example, "\n") {
				line = strings.TrimSpace(line)
				if line == "" || strings.HasPrefix(line, "#") {
					continue
				}
				if !strings.Contains(help, line) {
					t.Fatalf("%s --help does not show %q:\n%s", path, line, help)
				}
			}
		})
	}
}

func TestCommandExamplesRun(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	base := buildExampleSandbox(t)
	root := NewRootCommand()
	var workflow, other []*cobra.Command
	for _, c := range userFacingCommands(root) {
		if c.CommandPath() == "binlogviz workflow" || strings.HasPrefix(c.CommandPath(), "binlogviz workflow ") {
			workflow = append(workflow, c)
			continue
		}
		other = append(other, c)
	}
	// Parent first so incident-out exists for status, resume, clean, and export.
	ordered := make([]*cobra.Command, 0, len(workflow))
	for _, c := range workflow {
		if c.CommandPath() == "binlogviz workflow" {
			ordered = append(ordered, c)
		}
	}
	for _, c := range workflow {
		if c.CommandPath() != "binlogviz workflow" {
			ordered = append(ordered, c)
		}
	}
	for _, c := range other {
		dir := copyTree(t, base)
		runCommandExamples(t, c, dir)
	}
	for _, c := range ordered {
		runCommandExamples(t, c, base)
	}
}

func runCommandExamples(t *testing.T, c *cobra.Command, dir string) {
	t.Helper()
	invocations, err := exampleInvocations(c.Example)
	if err != nil {
		t.Fatal(err)
	}
	t.Run(c.CommandPath(), func(t *testing.T) {
		inDir(t, dir)
		for _, args := range invocations {
			code, stdout, stderr := runRoot(t, args...)
			if !exampleExitOK(args, code) {
				t.Fatalf("%s\nexit %d\nstdout:\n%s\nstderr:\n%s", invocationPath(args), code, stdout, stderr)
			}
			if !containsArg(args, "--plan") {
				continue
			}
			var doc map[string]any
			if err := json.Unmarshal([]byte(stdout), &doc); err != nil {
				t.Fatalf("%s plan is not JSON: %v\n%s", invocationPath(args), err, stdout)
			}
			if doc["plan_version"] != float64(flashbackPlanVersion()) {
				t.Fatalf("%s plan_version = %v\n%s", invocationPath(args), doc["plan_version"], stdout)
			}
			if code == 6 || code == 7 {
				refusal, _ := doc["refusal"].(map[string]any)
				msg, _ := refusal["message"].(string)
				if doc["would_refuse"] != true || msg == "" {
					t.Fatalf("%s refusal plan:\n%s", invocationPath(args), stdout)
				}
			}
		}
	})
}

func flashbackPlanVersion() float64 { return 2 }

func exampleExitOK(args []string, code int) bool {
	switch code {
	case 0, 2:
		return true
	case 6:
		// A refusal is the expected outcome for commands whose emit surface
		// refuses the minimal-metadata fixture (undo names, redo names).
		return containsArg(args, "flashback") || containsArg(args, "redo")
	case 7:
		return containsArg(args, "flashback")
	default:
		return false
	}
}

func containsArg(args []string, want string) bool {
	for _, arg := range args {
		if arg == want {
			return true
		}
	}
	return false
}

func inDir(t *testing.T, dir string) {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(wd); err != nil {
			t.Errorf("restore cwd: %v", err)
		}
	})
}

func buildExampleSandbox(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	root := repoRoot(t)
	binlog := filepath.Join(root, "cmd", "binlogviz", "testdata", "minimal.binlog")
	body, err := os.ReadFile(binlog)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"mysql-bin.000001", "mysql-bin.000002"} {
		if err := os.WriteFile(filepath.Join(dir, name), body, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "incident.yaml"), []byte(exampleIncidentPlan), 0o644); err != nil {
		t.Fatal(err)
	}
	var sealed strings.Builder
	if err := guard.New(nil).Write(&sealed, func(emit func(guard.Piece) error) error {
		return emit(guard.Piece{Kind: guard.PieceStmt, Text: "DO 0"})
	}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "undo.sql"), []byte(sealed.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if chdirErr := os.Chdir(wd); chdirErr != nil {
			t.Errorf("restore cwd: %v", chdirErr)
		}
	}()
	code, stdout, stderr := runRoot(t, "analyze", "mysql-bin.000001", "--format", "json", "-q", "--snapshot-name", "incident-current", "--snapshot-dir", "snaps")
	if code != 0 {
		t.Fatalf("seed analyze exit %d\n%s\n%s", code, stdout, stderr)
	}
	if err := os.WriteFile(filepath.Join(dir, "analyze.json"), []byte(stdout), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

const exampleIncidentPlan = `version: 1
workflow:
  name: incident
  output_dir: incident-out
defaults:
  input:
    from_dir: .
    prefix: mysql-bin.
  analyze:
    format: json
  snapshot:
    save: false
windows:
  - name: week1
    start: 1970-01-01T00:00:00Z
    end: 2099-12-31T23:59:59Z
  - name: week2
    start: 1970-01-01T00:00:00Z
    end: 2099-12-31T23:59:59Z
compare:
  - name: week2_vs_week1
    current: week2
    baseline: week1
    formats: [json]
trend:
  - name: weekly_series
    snapshots: [week1, week2]
    formats: [json]
`

func copyTree(t *testing.T, src string) string {
	t.Helper()
	dst := t.TempDir()
	err := filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, body, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
	return dst
}

func TestCommandsDoNotPrompt(t *testing.T) {
	if os.Getenv("BINLOGVIZ_STDIN_GUARD") == "1" {
		runStdinGuardChild()
	}
	forceEnglishRuntimeOutput(t)
	root := NewRootCommand()
	for _, c := range userFacingCommands(root) {
		args := strings.Fields(c.CommandPath())
		if len(args) > 0 && args[0] == "binlogviz" {
			args = args[1:]
		}
		t.Run(c.CommandPath(), func(t *testing.T) {
			out, err := runStdinGuard(t, args, true)
			if err != nil {
				t.Fatalf("blocked stdin: %v\n%s", err, out)
			}
			if phrase := promptPhrase(out); phrase != "" {
				t.Fatalf("prompt %q\n%s", phrase, out)
			}
		})
		if !strings.Contains(c.Use, "| -]") {
			continue
		}
		t.Run(c.CommandPath()+"/-", func(t *testing.T) {
			out, err := runStdinGuard(t, append(append([]string{}, args...), "-"), false)
			if err != nil {
				t.Fatalf("closed stdin: %v\n%s", err, out)
			}
			if !strings.Contains(out, "stdin has no data") {
				t.Fatalf("closed stdin did not report empty input:\n%s", out)
			}
			if phrase := promptPhrase(out); phrase != "" {
				t.Fatalf("prompt %q\n%s", phrase, out)
			}
		})
	}
}

func promptPhrase(out string) string {
	for _, phrase := range []string{"[y/N]", "[Y/n]", "(y/n)", "(yes/no)", "Are you sure", "are you sure", "password:", "Password:"} {
		if strings.Contains(out, phrase) {
			return phrase
		}
	}
	return ""
}

// runStdinGuard runs one command in a subprocess. blocked keeps the stdin
// pipe open and empty so a read waits; otherwise the pipe is closed and
// reads return EOF. The process is not a terminal.
func runStdinGuard(t *testing.T, args []string, blocked bool) (string, error) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if !blocked {
		w.Close()
	} else {
		defer w.Close()
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestCommandsDoNotPrompt$", "-test.count=1")
	cmd.Env = append(os.Environ(), "BINLOGVIZ_STDIN_GUARD=1", "BINLOGVIZ_STDIN_ARGS="+strings.Join(args, "\x1e"))
	cmd.Stdin = r
	var out strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &out
	timer := time.AfterFunc(5*time.Second, func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	})
	defer timer.Stop()
	err = cmd.Run()
	if blocked && cmd.ProcessState != nil && !cmd.ProcessState.Exited() {
		return out.String(), err
	}
	// A killed process did not finish on its own.
	if cmd.ProcessState != nil && cmd.ProcessState.ExitCode() == -1 {
		return out.String(), err
	}
	return out.String(), nil
}

func runStdinGuardChild() {
	var args []string
	if raw := os.Getenv("BINLOGVIZ_STDIN_ARGS"); raw != "" {
		args = strings.Split(raw, "\x1e")
	}
	cmd := NewRootCommand()
	cmd.SetArgs(args)
	cmd.SetOut(os.Stdout)
	cmd.SetErr(os.Stderr)
	err := cmd.Execute()
	if err != nil {
		PrintCommandError(os.Stderr, err)
		os.Exit(ExitCode(err))
	}
	os.Exit(0)
}

func TestFlashbackPlanRefusalPrintsReasons(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	cases := []struct {
		name   string
		args   []string
		reason string
		msg    string
	}{
		{
			name:   "row image",
			args:   []string{"flashback", mustFixturePath(t, "mysql-8.0.46-flashback-minimal-image.binlog"), "--plan"},
			reason: "row_image",
			msg:    "row image is not FULL",
		},
		{
			name:   "row metadata",
			args:   []string{"flashback", mustFixturePath(t, "mysql-8.0.46-dml-minimal.binlog"), "--include-gtids", "f4d3a60a-c1de-11f1-a30a-822b383dbcd0:4", "--plan"},
			reason: "row_metadata",
			msg:    "column names are unavailable",
		},
		{
			name:   "ddl",
			args:   []string{"flashback", mustFixturePath(t, "mysql-8.0.46-index-ddl.binlog"), "--plan"},
			reason: "ddl_in_range",
			msg:    "DDL in the selected range",
		},
		{
			name:   "sql context",
			args:   []string{"flashback", mustFixturePath(t, "mysql-8.0.46-flashback-full.binlog"), "--sql-context", "off", "--plan"},
			reason: "sql_context_off",
			msg:    "--sql-context off refuses that",
		},
	}
	for _, format := range []string{"text", "json"} {
		for _, tc := range cases {
			t.Run(format+"/"+tc.name, func(t *testing.T) {
				code, stdout, stderr := runRoot(t, append([]string{"--error-format", format}, tc.args...)...)
				if code != 6 {
					t.Fatalf("exit %d\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
				}
				var plan struct {
					PlanVersion int  `json:"plan_version"`
					WouldRefuse bool `json:"would_refuse"`
					Refusal     struct {
						Kind    string `json:"kind"`
						Message string `json:"message"`
						Reason  string `json:"reason"`
					} `json:"refusal"`
				}
				if err := json.Unmarshal([]byte(stdout), &plan); err != nil {
					t.Fatalf("plan json: %v\n%s", err, stdout)
				}
				if plan.PlanVersion != 2 || !plan.WouldRefuse || plan.Refusal.Kind != "refused" || plan.Refusal.Reason != tc.reason || !strings.Contains(plan.Refusal.Message, tc.msg) {
					t.Fatalf("plan: %+v\n%s", plan, stdout)
				}
				if !strings.Contains(stderr, tc.msg) {
					t.Fatalf("stderr missing refusal:\n%s", stderr)
				}
				if format == "json" {
					var body struct {
						Error struct {
							Kind    string         `json:"kind"`
							Message string         `json:"message"`
							Details map[string]any `json:"details"`
						} `json:"error"`
					}
					line := stderr
					if i := strings.Index(stderr, "{\"error\":"); i >= 0 {
						line = stderr[i:]
					}
					if err := json.Unmarshal([]byte(strings.TrimSpace(line)), &body); err != nil {
						t.Fatalf("error json: %v\n%s", err, stderr)
					}
					if body.Error.Kind != "refused" || body.Error.Details["reason"] != tc.reason || !strings.Contains(body.Error.Message, tc.msg) {
						t.Fatalf("error: %+v", body.Error)
					}
				} else if !strings.Contains(stderr, "Error:") {
					t.Fatalf("text error missing:\n%s", stderr)
				}
			})
		}
	}
}

func TestNoInteractivePromptAPI(t *testing.T) {
	root := repoRoot(t)
	fset := token.NewFileSet()
	var hits []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			base := d.Name()
			if base == ".git" || base == "vendor" || base == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		hits = append(hits, promptAPISites(path, f)...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 0 {
		t.Fatalf("interactive prompt API:\n%s", strings.Join(hits, "\n"))
	}
}

func TestPromptAPIScannerCatchesMutants(t *testing.T) {
	fset := token.NewFileSet()
	for name, src := range map[string]string{
		"scan":     `package p; import "fmt"; func f() { fmt.Scan() }`,
		"password": `package p; import "golang.org/x/term"; func f() { term.ReadPassword(0) }`,
		"stdin":    `package p; import "os"; func f() { os.Stdin.Read(nil) }`,
		"survey":   `package p; import "github.com/AlecAivazis/survey/v2"; func f() { survey.Ask(nil, nil) }`,
	} {
		f, err := parser.ParseFile(fset, "other.go", src, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(promptAPISites("other.go", f)) == 0 {
			t.Errorf("scanner missed %s", name)
		}
	}
	fset = token.NewFileSet()
	ok, err := parser.ParseFile(fset, "analyze_stdin.go", `package p; import ("fmt"; "os"; "golang.org/x/term"); func f() { _ = os.Stdin; _ = term.IsTerminal(0); fmt.Println() }`, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := promptAPISites("analyze_stdin.go", ok); len(got) != 0 {
		t.Errorf("false positive: %v", got)
	}
}

func promptAPISites(path string, f *ast.File) []string {
	bannedImport := map[string]bool{
		"github.com/AlecAivazis/survey/v2": true,
		"github.com/manifoldco/promptui":   true,
		"github.com/charmbracelet/huh":     true,
		"github.com/chzyer/readline":       true,
		"github.com/peterh/liner":          true,
	}
	scan := map[string]bool{"Scan": true, "Scanf": true, "Scanln": true, "Fscan": true, "Fscanf": true, "Fscanln": true}
	base := filepath.Base(path)
	var hits []string
	for _, imp := range f.Imports {
		p, err := strconvUnquote(imp.Path.Value)
		if err != nil {
			continue
		}
		if bannedImport[p] || strings.Contains(p, "promptui") || strings.Contains(p, "/survey") {
			hits = append(hits, path+": import "+p)
		}
	}
	ast.Inspect(f, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		id, ok := sel.X.(*ast.Ident)
		if !ok {
			return true
		}
		switch {
		case id.Name == "fmt" && scan[sel.Sel.Name]:
			hits = append(hits, path+": fmt."+sel.Sel.Name)
		case sel.Sel.Name == "ReadPassword":
			hits = append(hits, path+": "+id.Name+".ReadPassword")
		case id.Name == "os" && sel.Sel.Name == "Stdin" && base != "analyze_stdin.go":
			hits = append(hits, path+": os.Stdin")
		}
		return true
	})
	return hits
}

func strconvUnquote(s string) (string, error) {
	return strconv.Unquote(s)
}
