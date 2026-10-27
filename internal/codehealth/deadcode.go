package codehealth

import (
	"bytes"
	"fmt"
	"os/exec"
	"path"
	"strings"
)

// DeadcodeTool is the pinned deadcode command RunDeadcode runs. Without
// -test it treats code reached only from tests as unreachable.
const DeadcodeTool = "golang.org/x/tools/cmd/deadcode@v0.52.0"

// RunDeadcode runs the pinned deadcode tool on root, generated files
// included, and returns what ParseDeadcode makes of its output minus the
// functions in the generated files listed in the baseline, which are
// proven by regeneration instead.
func RunDeadcode(root string, generated map[string]string) (map[string]string, error) {
	cmd := exec.Command("go", "run", DeadcodeTool, "-generated", "./...")
	cmd.Dir = root
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("deadcode: %w: %s", err, stderr.String())
	}
	return withoutGenerated(ParseDeadcode(string(out)), generated), nil
}

func withoutGenerated(dead map[string]string, generated map[string]string) map[string]string {
	for key, pos := range dead {
		file, _, _ := strings.Cut(pos, ":")
		if _, ok := generated[file]; ok {
			delete(dead, key)
		}
	}
	return dead
}

// ParseDeadcode reads deadcode's "file:line:col: unreachable func: Name"
// lines into "pkgdir:Name" -> "file:line:col". Other lines are ignored.
func ParseDeadcode(out string) map[string]string {
	dead := map[string]string{}
	for _, line := range strings.Split(out, "\n") {
		pos, name, ok := strings.Cut(line, ": unreachable func: ")
		if !ok {
			continue
		}
		file, _, _ := strings.Cut(pos, ":")
		dead[path.Dir(file)+":"+strings.TrimSpace(name)] = pos
	}
	return dead
}
