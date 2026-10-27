.PHONY: help release-check release-check-test

help:
	@echo "Targets:"
	@echo "  make release-check VERSION=vX.Y.Z [REF=<git-ref>] # before tagging: notes (EN+zh), CHANGELOG, README pins, version-free landing page"
	@echo "  make release-check-test         # self-test release-check against fake complete and incomplete releases"

# The release-preparation commit (notes, CHANGELOG) is what gets
# tagged; release.yml runs the same check on the tag before any build starts.
release-check:
	@if [ -z "$(VERSION)" ]; then \
		echo "usage: make release-check VERSION=vX.Y.Z [REF=<git-ref>]"; \
		exit 2; \
	fi
	./scripts/check-release-prep.sh "$(VERSION)" $(REF)

release-check-test:
	./scripts/check-release-prep-test.sh

# Coding-standards gate (docs/development/coding-standards.md). Kept as its
# own block so it does not overlap the release targets above.
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
