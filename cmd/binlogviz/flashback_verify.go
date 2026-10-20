package binlogviz

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"binlogviz/internal/clierr"
	"binlogviz/internal/flashback"
	"binlogviz/internal/guard"
	"binlogviz/internal/i18n"
	"binlogviz/internal/jsonout"
)

const flashbackVerifySchemaVersion = 1

type flashbackVerifyJSON struct {
	Command    string `json:"command"`
	OK         bool   `json:"ok"`
	Statements int    `json:"statements"`
	Digest     string `json:"digest"`
}

func newFlashbackVerifyCommand() *cobra.Command {
	var format string
	cmd := &cobra.Command{
		Use:   i18n.T("cmd.flashback.verify.use"),
		Short: i18n.T("cmd.flashback.verify.short"),
		Long:  i18n.T("cmd.flashback.verify.long"),
		Example: `  binlogviz flashback verify undo.sql
  binlogviz flashback verify undo.sql --format json`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			switch strings.ToLower(strings.TrimSpace(format)) {
			case "sql", "json":
			default:
				return clierr.Errorf(clierr.KindUsage, "%s", i18n.Tf("error.flashbackVerifyFormat", map[string]any{"Value": format}))
			}
			f, err := os.Open(args[0])
			if err != nil {
				return clierr.Wrap(clierr.KindInput, clierr.Errorf(clierr.KindInput, "%s", i18n.Tf("error.flashbackVerifyOpen", map[string]any{
					"Path":  args[0],
					"Error": err.Error(),
				})))
			}
			defer f.Close()

			// Hash while reading. SQL output copies the bytes to a temp file
			// and emits them only after the digest matches, so a refusal
			// writes nothing and the script is not held in memory.
			src := &verifyProbe{r: &verifyBudget{r: f}}
			var read io.Reader = src
			var tmp *os.File
			if !strings.EqualFold(format, "json") {
				tmp, err = os.CreateTemp("", "binlogviz-flashback-verify-*")
				if err != nil {
					return clierr.Wrap(clierr.KindOutput, err)
				}
				defer os.Remove(tmp.Name())
				defer tmp.Close()
				read = io.TeeReader(src, verifyTemp{tmp})
			}
			res, err := guard.Verify(read)
			if err != nil {
				return verifyFailure(err, src.nonspace)
			}
			out := cmd.OutOrStdout()
			if strings.EqualFold(format, "json") {
				data, err := jsonout.Marshal(flashbackVerifySchemaVersion, flashbackVerifyJSON{
					Command:    "flashback verify",
					OK:         true,
					Statements: res.Statements,
					Digest:     res.Digest,
				})
				if err != nil {
					return clierr.Errorf(clierr.KindOutput, "encode flashback verify output: %w", err)
				}
				_, err = fmt.Fprintln(out, string(data))
				return err
			}
			st, err := tmp.Stat()
			if err != nil {
				return clierr.Wrap(clierr.KindOutput, err)
			}
			if err := guard.CopyApply(out, tmp, st.Size()); err != nil {
				var ve *guard.VerifyError
				if errors.As(err, &ve) {
					return verifyFailure(err, true)
				}
				return clierr.Wrap(clierr.KindOutput, err)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&format, "format", "sql", i18n.T("cmd.flashback.verify.flag.format"))
	return cmd
}

func verifyFailure(err error, nonspace bool) error {
	var ve *guard.VerifyError
	if errors.As(err, &ve) {
		if ve.Reason == "not_flashback" && !nonspace {
			return clierr.Errorf(clierr.KindCorrupt, "%s", i18n.T("error.flashbackVerifyEmpty"))
		}
		if ve.Reason == "old_format" {
			version := ve.Version
			if version == "" {
				version = "an older release"
			}
			return clierr.WithDetails(clierr.KindRefused, clierr.Errorf(clierr.KindRefused, "%s", i18n.Tf("error.flashbackVerifyOld", map[string]any{"Version": version})), map[string]any{"reason": "old_format"})
		}
		return clierr.WithDetails(clierr.KindRefused, clierr.Errorf(clierr.KindRefused, "%s", i18n.Tf("error.flashbackVerify", map[string]any{"Reason": ve.Reason})), map[string]any{"reason": ve.Reason})
	}
	var resource *flashback.ResourceError
	if errors.As(err, &resource) {
		return flashbackExit(err)
	}
	if kind, _ := clierr.Of(err); kind == clierr.KindUsage || kind == clierr.KindOutput || kind == clierr.KindResource {
		return err
	}
	var wrote *verifyWriteError
	if errors.As(err, &wrote) {
		return clierr.Wrap(clierr.KindOutput, err)
	}
	return clierr.Wrap(clierr.KindInput, err)
}

// verifyBudget checks the flashback heap ceiling as bytes arrive so a limit
// the process can see exits 10, not 2.
type verifyBudget struct {
	r io.Reader
	n int64
}

func (b *verifyBudget) Read(p []byte) (int, error) {
	if b.n == 0 {
		if err := flashback.CheckMemory(); err != nil {
			return 0, err
		}
	}
	n, err := b.r.Read(p)
	b.n += int64(n)
	if b.n >= 1<<20 {
		b.n = 0
	}
	return n, err
}

// verifyProbe records whether the file has any non-whitespace byte. An empty
// or all-whitespace file is corrupt (exit 5), not not_flashback.
type verifyProbe struct {
	r        io.Reader
	nonspace bool
}

func (p *verifyProbe) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	if !p.nonspace && len(bytes.TrimSpace(b[:n])) > 0 {
		p.nonspace = true
	}
	return n, err
}

type verifyTemp struct{ f *os.File }

func (t verifyTemp) Write(p []byte) (int, error) {
	n, err := t.f.Write(p)
	if err != nil {
		return n, &verifyWriteError{err: err}
	}
	return n, nil
}

type verifyWriteError struct{ err error }

func (e *verifyWriteError) Error() string {
	if e == nil || e.err == nil {
		return ""
	}
	return e.err.Error()
}

func (e *verifyWriteError) Unwrap() error { return e.err }
