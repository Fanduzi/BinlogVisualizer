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
.PHONY: test
# -count=1 because TestRepoWithinBaseline reads the worktree and the git index,
# neither of which invalidates go test's result cache (#454 QA N1).
test:
	go test -count=1 ./...

.PHONY: lint codehealth codehealth-baseline codehealth-compare codehealth-regen
LINT_BASE ?= $(shell git merge-base origin/main HEAD)

# golangci-lint findings that the tree at LINT_BASE (default: merge base with origin/main) does not have.
# Code that moved or was renamed keeps its findings' identity; see docs/development/coding-standards.md.
lint:
	go run ./internal/codehealth/cmd/codehealth -base $(LINT_BASE) lint

# Fail unless the tree holds against internal/codehealth/baseline.json (file size, function complexity and length, issue refs, deadcode).
codehealth:
	go run ./internal/codehealth/cmd/codehealth -deadcode check

# Lower internal/codehealth/baseline.json to today's numbers and add entries for split or renamed code;
# refuses to raise anything. CI checks by content that each new entry came from an old one.
codehealth-baseline:
	go run ./internal/codehealth/cmd/codehealth -deadcode update

# What CI's baseline comparison says about this change: run codehealth-baseline, then compare the
# result with the baseline at LINT_BASE (default: merge base with origin/main), paydown included.
# A new file or function over a limit passes only if its content came from a baseline entry.
codehealth-compare: codehealth-baseline
	go run ./internal/codehealth/cmd/codehealth -base $(LINT_BASE) -paydown compare

# Regenerate the files in generated_files and fail if any differs from what is committed.
codehealth-regen:
	go run ./internal/codehealth/cmd/codehealth regen
	git diff --exit-code
