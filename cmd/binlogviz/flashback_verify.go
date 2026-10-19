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
		Use:           i18n.T("cmd.flashback.verify.use"),
		Short:         i18n.T("cmd.flashback.verify.short"),
		Long:          i18n.T("cmd.flashback.verify.long"),
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
			body, err := io.ReadAll(f)
			if err != nil {
				return clierr.Wrap(clierr.KindInput, clierr.Errorf(clierr.KindInput, "%s", i18n.Tf("error.flashbackVerifyOpen", map[string]any{
					"Path":  args[0],
					"Error": err.Error(),
				})))
			}
			if len(bytes.TrimSpace(body)) == 0 {
				return clierr.Errorf(clierr.KindCorrupt, "%s", i18n.T("error.flashbackVerifyEmpty"))
			}
			res, err := guard.Verify(bytes.NewReader(body))
			if err != nil {
				var ve *guard.VerifyError
				if errors.As(err, &ve) {
					return clierr.WithDetails(clierr.KindRefused, clierr.Errorf(clierr.KindRefused, "%s", i18n.Tf("error.flashbackVerify", map[string]any{"Reason": ve.Reason})), map[string]any{"reason": ve.Reason})
				}
				return clierr.Wrap(clierr.KindInput, err)
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
			sql, err := guard.ApplySQL(string(body))
			if err != nil {
				var ve *guard.VerifyError
				if errors.As(err, &ve) {
					return clierr.WithDetails(clierr.KindRefused, clierr.Errorf(clierr.KindRefused, "%s", i18n.Tf("error.flashbackVerify", map[string]any{"Reason": ve.Reason})), map[string]any{"reason": ve.Reason})
				}
				return clierr.Wrap(clierr.KindError, err)
			}
			_, err = io.WriteString(out, sql)
			return err
		},
	}
	cmd.Flags().StringVar(&format, "format", "sql", i18n.T("cmd.flashback.verify.flag.format"))
	return cmd
}
