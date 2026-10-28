package binlogviz

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// e1MemoryTests are the checks behind production-readiness items E1
// (analyze, flashback generate and verify memory bounds) and E2 (the
// large-input baseline). Some of them skip
// under -race. That is only safe while CI runs them in a non-race job, so
// this test fails if CI's verify job stops running plain `go test ./...`, if
// one of them moves behind a race build tag (left out of the build without
// a SKIP line), or if its race skip loses its t.Skip reason.
var e1MemoryTests = map[string]string{
	"TestFlashbackSpillLimitOnFixture":       "cmd/binlogviz/flashback_resource_test.go",
	"TestFlashbackVerifyMemoryCeiling":       "cmd/binlogviz/flashback_verify_test.go",
	"TestAnalyzeRetainedHeap":                "cmd/binlogviz/analyze_memory_test.go",
	"TestAnalyzeGTIDFilterRetainedHeap":      "cmd/binlogviz/analyze_gtid_memory_test.go",
	"TestAnalyzeMatchedGTIDSetHeap":          "cmd/binlogviz/analyze_gtid_memory_test.go",
	"TestGeneratedReviewRSS":                 "internal/flashback/genmem_test.go",
	"TestVerifyDoesNotBufferTheScript":       "internal/guard/script_mem_test.go",
	"TestLargeInputBaseline":                 "cmd/binlogviz/large_input_baseline_test.go",
	"TestWideActivityHeap":                   "cmd/binlogviz/wide_activity_heap_test.go",
	"TestAlternatingMinuteSpillStaysBounded": "internal/analyzer/activity_spill_test.go",
}

func TestEveryMemoryTestRunsInCIVerify(t *testing.T) {
	root := filepath.Join("..", "..")
	ci, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	verify := ciJob(t, string(ci), "verify")
	plain := false
	for _, line := range strings.Split(verify, "\n") {
		line = strings.TrimSpace(line)
		if line == "run: go test ./..." || line == "run: go test -count=1 ./..." {
			plain = true
		}
	}
	if !plain {
		t.Fatalf("CI's verify job no longer runs plain `go test [-count=1] ./...` (no -race, -run, -skip or -short); the E1 memory tests skip under -race and need a non-race job:\n%s", verify)
	}

	buildTag := regexp.MustCompile(`(?m)^//go:build .*\brace\b`)
	for name, rel := range e1MemoryTests {
		body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		src := string(body)
		if !strings.Contains(src, "func "+name+"(t *testing.T) {") {
			t.Fatalf("%s is no longer declared in %s", name, rel)
		}
		if buildTag.MatchString(src) {
			t.Fatalf("%s has a race build tag, so a -race build leaves %s out without a SKIP line; skip with t.Skip instead", rel, name)
		}
		fn := src[strings.Index(src, "func "+name+"("):]
		if end := strings.Index(fn, "\n}\n"); end >= 0 {
			fn = fn[:end]
		}
		if strings.Contains(fn, "Race") || strings.Contains(fn, "raceBuild") {
			if !regexp.MustCompile(`t\.Skip\("-race build: [^"]+CI's verify job`).MatchString(fn) {
				t.Fatalf("%s skips under -race without a t.Skip reason naming CI's verify job", name)
			}
		}
	}
}

// ciJob returns the lines of one top-level job in a workflow file.
func ciJob(t *testing.T, workflow, job string) string {
	t.Helper()
	lines := strings.Split(workflow, "\n")
	start := -1
	for i, line := range lines {
		if line == "  "+job+":" {
			start = i
			break
		}
	}
	if start < 0 {
		t.Fatalf("ci.yml has no %q job", job)
	}
	end := len(lines)
	for i := start + 1; i < len(lines); i++ {
		l := lines[i]
		if strings.HasPrefix(l, "  ") && !strings.HasPrefix(l, "   ") && strings.TrimSpace(l) != "" {
			end = i
			break
		}
	}
	return strings.Join(lines[start:end], "\n")
}
