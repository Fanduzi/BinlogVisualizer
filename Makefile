.PHONY: help release-check release-check-test

help:
	@echo "Targets:"
	@echo "  make release-check VERSION=vX.Y.Z [REF=<git-ref>] # before tagging: notes (EN+zh), CHANGELOG, README pins, landing for the version"
	@echo "  make release-check-test         # self-test release-check against fake complete and incomplete releases"

# The release-preparation commit (notes, CHANGELOG, landing) is what gets
# tagged; release.yml runs the same check on the tag before any build starts.
release-check:
	@if [ -z "$(VERSION)" ]; then \
		echo "usage: make release-check VERSION=vX.Y.Z [REF=<git-ref>]"; \
		exit 2; \
	fi
	./scripts/check-release-prep.sh "$(VERSION)" $(REF)

release-check-test:
	./scripts/check-release-prep-test.sh
