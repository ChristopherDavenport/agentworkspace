GO ?= go
STATICCHECK ?= $(GO) run honnef.co/go/tools/cmd/staticcheck@latest
GOVULNCHECK ?= $(GO) run golang.org/x/vuln/cmd/govulncheck@latest

.PHONY: build deps test vet fmt tidy tidy-check lint vuln check \
	guard-test release-guard release clean

build:
	$(GO) build ./...

# The module must build from the standard library alone.
deps:
	@deps=$$($(GO) list -deps -f '{{if not .Standard}}{{.ImportPath}}{{end}}' ./... \
	  | grep -v '^github.com/ChristopherDavenport/agentworkspace' || true); \
	  test -z "$$deps" || { echo "module depends on: $$deps"; exit 1; }

# Race detector on, as CI runs it.
test:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

tidy:
	$(GO) mod tidy

# Fails when go mod tidy would change go.mod or go.sum, without writing,
# so a stray dependency shows up in make check and not only in CI's diff.
tidy-check:
	$(GO) mod tidy -diff

fmt:
	gofmt -l . && test -z "$$(gofmt -l .)"

lint:
	$(STATICCHECK) ./...

vuln:
	$(GOVULNCHECK) ./...

# Everything CI runs. The go.work repositories in this workspace need
# GOWORK=off GOFLAGS=-mod=readonly in front of it.
check: fmt tidy-check vet deps lint vuln test guard-test

MODULE := $(shell $(GO) list -m)

# The release guard's own tests, against throwaway repositories.
guard-test:
	@scripts/release-guard-test.sh

# Checks one tag is safe to push, before it is pushed. A pushed tag is
# permanent: the proxy and the checksum database keep the version
# forever, so this is the last point at which a mistake is free:
#   make release-guard TAG=v0.1.0
release-guard:
	@test -n "$(TAG)" || { echo "usage: make release-guard TAG=<tag>"; exit 1; }
	@scripts/release-guard.sh "$(TAG)"

# Cut a release: the changelog's Unreleased section is dated, everything
# is checked, one commit is made, the tag is guarded and then written
# with the changelog section as its message, and the branch and tag are
# pushed. TRAILER, when set, is appended to the commit message.
#
# The guard runs after the commit and before the tag, which is the last
# moment everything is still local: if it refuses, undo with git reset
# --hard HEAD~1. Nothing is public until the push. --atomic lands the
# branch and the tag in one transaction.
#
# The changelog is dated through a temp file rather than sed -i, which is
# a GNU-ism: BSD sed reads the argument after -i as a backup suffix, so
# the GNU spelling fails outright on macOS, where these releases are cut.
# The temp file is removed if sed dies, so a failed run leaves nothing
# untracked behind for the clean-tree gate to trip over next time.
release:
	@test -n "$(VERSION)" || { echo "usage: make release VERSION=vX.Y.Z"; exit 1; }
	@grep -q '^## Unreleased$$' CHANGELOG.md || { echo "CHANGELOG.md has no Unreleased section"; exit 1; }
	@test -z "$$(git status --porcelain)" || { echo "working tree is not clean"; exit 1; }
	sed 's/^## Unreleased$$/## $(VERSION) - '"$$(date +%F)"'/' CHANGELOG.md > CHANGELOG.md.tmp \
	  && mv CHANGELOG.md.tmp CHANGELOG.md \
	  || { rm -f CHANGELOG.md.tmp; exit 1; }
	$(MAKE) tidy
	$(MAKE) check
	git add -A && git commit -q -m "Release $(VERSION)" $(if $(TRAILER),-m "$(TRAILER)")
	@scripts/release-guard.sh "$(VERSION)"
	@notes="$$(scripts/release-notes.sh $(VERSION))" || exit 1; \
	 git tag -a $(VERSION) -m "$$notes"
	git push origin --atomic HEAD $(VERSION)

clean:
	rm -rf .cache
