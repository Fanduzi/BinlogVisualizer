package binlogviz

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/term"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

const stdinAlias = "stdin"

// materializeAnalyzePaths copies non-seekable inputs, including "-", to a
// temporary file. The parser reads binlogs by seeking. Regular files are unchanged.
func materializeAnalyzePaths(paths []string) ([]string, map[string]string, func(), error) {
	if len(paths) == 0 {
		return paths, nil, func() {}, nil
	}
	var dirs []string
	cleanup := func() {
		for _, dir := range dirs {
			releaseStdinTemp(dir)
		}
	}
	out := make([]string, len(paths))
	aliases := map[string]string{}
	seenStdin := false
	for i, path := range paths {
		resolved, dir, alias, err := ensureSeekableBinlog(path, &seenStdin)
		if err != nil {
			cleanup()
			return nil, nil, func() {}, err
		}
		if dir != "" {
			dirs = append(dirs, dir)
		}
		out[i] = resolved
		if alias != "" && alias != resolved {
			aliases[resolved] = alias
		}
	}
	if len(aliases) == 0 {
		aliases = nil
	}
	return out, aliases, cleanup, nil
}

func ensureSeekableBinlog(path string, seenStdin *bool) (string, string, string, error) {
	if path == "-" {
		if seenStdin != nil && *seenStdin {
			return "", "", "", fmt.Errorf("%s", i18n.T("error.stdinOnce"))
		}
		if seenStdin != nil {
			*seenStdin = true
		}
		if isTerminal(os.Stdin) {
			return "", "", "", fmt.Errorf("%s", i18n.T("error.stdinTerminal"))
		}
		resolved, dir, err := spoolReader(os.Stdin, stdinAlias)
		return resolved, dir, stdinAlias, err
	}

	f, err := os.Open(path)
	if err != nil {
		return path, "", "", nil
	}
	info, statErr := f.Stat()
	if statErr == nil && info.IsDir() {
		_ = f.Close()
		return path, "", "", nil
	}
	if statErr == nil && isTerminal(f) && (path == "/dev/stdin" || filepath.Base(path) == stdinAlias) {
		_ = f.Close()
		return "", "", "", fmt.Errorf("%s", i18n.T("error.stdinTerminal"))
	}
	if !mustSpool(info, f) {
		_ = f.Close()
		return path, "", "", nil
	}
	resolved, dir, err := spoolReader(f, stdinAlias)
	_ = f.Close()
	if err != nil {
		return "", "", "", err
	}
	return resolved, dir, stdinAlias, nil
}

func mustSpool(info os.FileInfo, f *os.File) bool {
	if info != nil {
		mode := info.Mode()
		if mode&os.ModeNamedPipe != 0 || mode&os.ModeSocket != 0 || mode&os.ModeCharDevice != 0 {
			return true
		}
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return true
	}
	return false
}

func isTerminal(f *os.File) bool {
	return f != nil && term.IsTerminal(int(f.Fd()))
}

func spoolReader(r io.Reader, name string) (string, string, error) {
	if name == "" {
		name = "stdin"
	}
	dir, err := os.MkdirTemp("", "binlogviz-stdin-")
	if err != nil {
		return "", "", err
	}
	trackStdinTemp(dir)
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	if err != nil {
		releaseStdinTemp(dir)
		return "", "", err
	}
	n, copyErr := io.Copy(f, r)
	closeErr := f.Close()
	if copyErr != nil {
		releaseStdinTemp(dir)
		return "", "", copyErr
	}
	if closeErr != nil {
		releaseStdinTemp(dir)
		return "", "", closeErr
	}
	if n == 0 {
		releaseStdinTemp(dir)
		return "", "", fmt.Errorf("%s", i18n.T("error.stdinEmpty"))
	}
	return path, dir, nil
}

func displayPaths(paths []string, aliases map[string]string) []string {
	if len(aliases) == 0 {
		return paths
	}
	out := make([]string, len(paths))
	for i, path := range paths {
		out[i] = aliasPath(path, aliases)
	}
	return out
}

func aliasPath(path string, aliases map[string]string) string {
	if path == "" || len(aliases) == 0 {
		return path
	}
	if aliased, ok := aliases[path]; ok {
		return aliased
	}
	return path
}

func aliasAnalysisPaths(result *model.AnalysisResult, aliases map[string]string) {
	if result == nil || len(aliases) == 0 {
		return
	}
	for i := range result.Transactions {
		aliasTxn(&result.Transactions[i], aliases)
	}
	aliasTxnSlice(result.Diagnostics.LargestTransactions, aliases)
	aliasTxnSlice(result.Diagnostics.LongestTransactions, aliases)
	aliasTxnSlice(result.Diagnostics.WidestTransactions, aliases)
	aliasTxnSlice(result.Diagnostics.LargestByteTransactions, aliases)
	for i := range result.Diagnostics.DDLEvents {
		result.Diagnostics.DDLEvents[i].BinlogPath = aliasPath(result.Diagnostics.DDLEvents[i].BinlogPath, aliases)
	}
	for i := range result.Diagnostics.OpenDMLGroups {
		group := &result.Diagnostics.OpenDMLGroups[i]
		group.BinlogPathStart = aliasPath(group.BinlogPathStart, aliases)
		group.BinlogPathEnd = aliasPath(group.BinlogPathEnd, aliases)
	}
	for i := range result.Diagnostics.FileCoverage.Selected {
		item := &result.Diagnostics.FileCoverage.Selected[i]
		item.BinlogPath = aliasPath(item.BinlogPath, aliases)
	}
	for i := range result.Diagnostics.FileCoverage.Skipped {
		item := &result.Diagnostics.FileCoverage.Skipped[i]
		item.BinlogPath = aliasPath(item.BinlogPath, aliases)
	}
	if result.Snapshot != nil {
		result.Snapshot.Input.Files = displayPaths(result.Snapshot.Input.Files, aliases)
	}
	aliasAlerts(result.Alerts, aliases)
}

func aliasTxnSlice(txns []model.Transaction, aliases map[string]string) {
	for i := range txns {
		aliasTxn(&txns[i], aliases)
	}
}

func aliasTxn(txn *model.Transaction, aliases map[string]string) {
	if aliasedToStdin(txn.BinlogPathStart, aliases) || aliasedToStdin(txn.BinlogPathEnd, aliases) {
		txn.StdinInput = true
	}
	txn.BinlogPathStart = aliasPath(txn.BinlogPathStart, aliases)
	txn.BinlogPathEnd = aliasPath(txn.BinlogPathEnd, aliases)
	if txn.FullReplaySpan != nil {
		if aliasedToStdin(txn.FullReplaySpan.BinlogPathStart, aliases) || aliasedToStdin(txn.FullReplaySpan.BinlogPathEnd, aliases) {
			txn.StdinInput = true
		}
		txn.FullReplaySpan.BinlogPathStart = aliasPath(txn.FullReplaySpan.BinlogPathStart, aliases)
		txn.FullReplaySpan.BinlogPathEnd = aliasPath(txn.FullReplaySpan.BinlogPathEnd, aliases)
	}
}

func aliasedToStdin(path string, aliases map[string]string) bool {
	aliased, ok := aliases[path]
	return ok && aliased == stdinAlias
}

func aliasAlerts(alerts []model.Alert, aliases map[string]string) {
	for i := range alerts {
		if aliasAlertDetails(alerts[i].Details, aliases) {
			alerts[i].Details["replay_available"] = false
		}
	}
}

func aliasAlertDetails(details map[string]any, aliases map[string]string) bool {
	if len(details) == 0 {
		return false
	}
	stdin := false
	var walk func(map[string]any)
	walk = func(m map[string]any) {
		for key, value := range m {
			switch typed := value.(type) {
			case string:
				if aliased, ok := aliases[typed]; ok {
					m[key] = aliased
					if aliased == stdinAlias {
						stdin = true
					}
				}
			case map[string]any:
				walk(typed)
			}
		}
	}
	walk(details)
	return stdin
}
