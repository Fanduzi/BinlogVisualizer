# Coding-standards gate (docs/development/coding-standards.md).
.PHONY: lint codehealth codehealth-baseline
LINT_BASE ?= $(shell git merge-base origin/main HEAD)

# golangci-lint, only issues on lines changed since LINT_BASE (default: merge base with origin/main).
lint:
	golangci-lint run --new-from-rev=$(LINT_BASE) ./...

# Fail if file size, complexity, issue refs or deadcode are worse than internal/codehealth/baseline.json.
codehealth:
	go run ./internal/codehealth/cmd/codehealth -deadcode check

# Lower internal/codehealth/baseline.json to today's numbers; refuses to raise anything.
codehealth-baseline:
	go run ./internal/codehealth/cmd/codehealth -deadcode update
