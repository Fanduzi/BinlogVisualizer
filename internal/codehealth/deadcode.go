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

// RunDeadcode runs the pinned deadcode tool on root and returns what
// ParseDeadcode makes of its output.
func RunDeadcode(root string) (map[string]string, error) {
	cmd := exec.Command("go", "run", DeadcodeTool, "./...")
	cmd.Dir = root
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("deadcode: %w: %s", err, stderr.String())
	}
	return ParseDeadcode(string(out)), nil
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
