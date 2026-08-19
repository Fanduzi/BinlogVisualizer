package binlogviz

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"binlogviz/internal/i18n"
)

const (
	htmlDefaultName = "binlogviz-report.html"
	stdoutOutput    = "-"
)

type outputDestination struct {
	Path     string
	IsFile   bool
	IsStdout bool
}

func resolveOutputDestination(paths []string, discovered bool, requested string, format string) (outputDestination, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return outputDestination{}, fmt.Errorf("get working directory: %w", err)
	}
	return resolveOutputDestinationWithCwd(paths, discovered, requested, format, cwd)
}

func resolveOutputDestinationWithCwd(paths []string, discovered bool, requested string, format string, cwd string) (outputDestination, error) {
	if format != "html" {
		if requested != "" {
			return outputDestination{}, fmt.Errorf("--output is only supported with --format html")
		}
		return outputDestination{IsStdout: true}, nil
	}

	if requested == stdoutOutput {
		return outputDestination{IsStdout: true}, nil
	}

	if requested != "" {
		return outputDestination{Path: requested, IsFile: true}, nil
	}

	name := deriveHTMLFilename(paths, discovered)
	resolved := findAvailablePath(filepath.Join(cwd, name))
	return outputDestination{Path: resolved, IsFile: true}, nil
}

func deriveHTMLFilename(paths []string, discovered bool) string {
	if discovered || len(paths) > 1 {
		return htmlDefaultName
	}
	base := filepath.Base(paths[0])
	return base + ".html"
}

func findAvailablePath(desired string) string {
	if _, err := os.Stat(desired); os.IsNotExist(err) {
		return desired
	}

	ext := filepath.Ext(desired)
	base := strings.TrimSuffix(desired, ext)
	for i := 1; ; i++ {
		candidate := fmt.Sprintf("%s.%d%s", base, i, ext)
		if _, err := os.Stat(candidate); os.IsNotExist(err) {
			return candidate
		}
	}
}

func writeHTMLAtomically(dest outputDestination, content string) error {
	if dest.IsStdout {
		_, err := fmt.Print(content)
		return err
	}

	dir := filepath.Dir(dest.Path)
	tmp, err := os.CreateTemp(dir, ".binlogviz-html-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmp.Name()

	defer func() {
		_ = os.Remove(tmpPath)
	}()

	if _, err := tmp.WriteString(content); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}

	if err := os.Rename(tmpPath, dest.Path); err != nil {
		return fmt.Errorf("rename to destination: %w", err)
	}

	return nil
}

func printHTMLSaveConfirmation(path string) {
	printHTMLSaveConfirmationTo(os.Stderr, path)
}

func printHTMLSaveConfirmationTo(out io.Writer, path string) {
	abs, err := filepath.Abs(path)
	if err != nil {
		abs = path
	}
	_, _ = fmt.Fprintln(out, i18n.Tf("progress.htmlSaved", map[string]any{"Path": abs}))
}
