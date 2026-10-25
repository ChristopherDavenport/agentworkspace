GO ?= go
STATICCHECK ?= $(GO) run honnef.co/go/tools/cmd/staticcheck@latest
GOVULNCHECK ?= $(GO) run golang.org/x/vuln/cmd/govulncheck@latest
# The module path of the root, which every first-party require and
# replace is written against. go list -m reports every module in the
# workspace, so the root has to be asked for outside it.
MODULE := $(shell GOWORK=off $(GO) list -m)

# Nested modules that are tested alongside the root but keep their own
# dependencies out of it. Each requires the root at exactly the version
# the whole repository is released at, and carries a replace pointing
# at the tree; see replaces below.
SUBMODULES = mcpworkspace

.PHONY: build deps replaces test vet fmt tidy tidy-check lint vuln check \
	extracted guard-test release-guard release release-commit clean

build:
	$(GO) build ./...
	@for m in $(SUBMODULES); do (cd $$m && $(GO) build ./...) || exit 1; done

# The root module must build from the standard library alone, and so
# must mcpworkspace/wire, which a program that only relays the methods
# imports without the MCP SDK.
deps:
	@deps=$$($(GO) list -deps -f '{{if not .Standard}}{{.ImportPath}}{{end}}' ./... \
	  | grep -v '^github.com/ChristopherDavenport/agentworkspace' || true); \
	  test -z "$$deps" || { echo "root module depends on: $$deps"; exit 1; }
	@deps=$$(cd mcpworkspace && $(GO) list -deps -f '{{if not .Standard}}{{.ImportPath}}{{end}}' ./wire \
	  | grep -v '^github.com/ChristopherDavenport/agentworkspace/mcpworkspace/wire$$' || true); \
	  test -z "$$deps" || { echo "mcpworkspace/wire depends on: $$deps"; exit 1; }

# Every first-party module a nested module requires must also be
# replaced, at a path that exists: go mod tidy ignores go.work, so a
# require naming the version being released resolves from the proxy,
# where that version does not exist until the tag is pushed. The
# replace is what lets a release name its own version. Consumers ignore
# a replace and get the require, which names the commit the module is
# tagged from; release-guard is what proves it.
replaces:
	@scripts/check-replaces.sh $(SUBMODULES)

# Race detector on, as CI runs it.
test:
	$(GO) test -race ./...
	@for m in $(SUBMODULES); do (cd $$m && $(GO) test -race ./...) || exit 1; done

vet:
	$(GO) vet ./...
	@for m in $(SUBMODULES); do (cd $$m && $(GO) vet ./...) || exit 1; done

tidy:
	$(GO) mod tidy
	@for m in $(SUBMODULES); do (cd $$m && $(GO) mod tidy) || exit 1; done

# Fails when go mod tidy would change any go.mod or go.sum, without
# writing, so a stray dependency shows up in make check and not only in
# CI's diff.
tidy-check:
	$(GO) mod tidy -diff
	@for m in $(SUBMODULES); do (cd $$m && $(GO) mod tidy -diff) || exit 1; done

fmt:
	gofmt -l . && test -z "$$(gofmt -l .)"

lint:
	$(STATICCHECK) ./...
	@for m in $(SUBMODULES); do (cd $$m && $(STATICCHECK) ./...) || exit 1; done

vuln:
	$(GOVULNCHECK) ./...
	@for m in $(SUBMODULES); do (cd $$m && $(GOVULNCHECK) ./...) || exit 1; done

# Everything CI runs. The go.work repositories in this workspace need
# GOWORK=off GOFLAGS=-mod=readonly in front of it.
check: fmt tidy-check vet deps replaces lint vuln test guard-test

# Builds, vets and tests each nested module the way a consumer gets it:
# extracted to a directory with no parent go.mod, with the in-tree
# replace dropped, so its require lines are answered by the proxy (see
# scripts/check-extracted.sh). Needs the network, so it is not part of
# check; CI runs it.
extracted:
	@scripts/check-extracted.sh $(SUBMODULES)

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

# Every tag a release writes: the root and one per nested module, all at
# the same version, all from the one commit below.
RELEASE_TAGS = $(VERSION) $(patsubst %,%/$(VERSION),$(SUBMODULES))

# Cut a release:
#
#   make release VERSION=v0.1.0
#
# Every module is released at one version, from one commit, and a nested
# module requires the root at exactly that version, so a consumer who
# takes only mcpworkspace at vX.Y.Z gets root vX.Y.Z, the commit it was
# built and tested against. The first thing this does is point every
# nested module at VERSION, a version that does not exist yet; the
# replace makes that resolve to the tree.
#
# The guard runs after the commit and before each tag, which is the last
# moment everything is still local: if it refuses, undo with git reset
# --hard HEAD~1 and git tag -d the tags written. The root is guarded and
# tagged first, then each nested module, because a nested module's guard
# proves the root tag of that version names this commit. --atomic lands
# the branch and every tag in one transaction.
release: release-commit
	@scripts/release-guard.sh "$(VERSION)"
	@notes="$$(scripts/release-notes.sh $(VERSION))" || exit 1; \
	 git tag -a $(VERSION) -m "$$notes"
	@notes="$$(scripts/release-notes.sh $(VERSION))" || exit 1; \
	 for m in $(SUBMODULES); do \
	   scripts/release-guard.sh "$$m/$(VERSION)" || exit 1; \
	   git tag -a $$m/$(VERSION) -m "$$notes" || exit 1; \
	 done
	git push origin --atomic HEAD $(RELEASE_TAGS)

# Bump every first-party requirement to VERSION, date the changelog,
# check everything, commit. Nothing here is pushed, so a failure costs a
# git reset and no more. TRAILER, when set, is appended to the commit
# message. go mod tidy is free to move a requirement the bump just set,
# so what landed is read back and asserted before anything is committed.
#
# The changelog is dated through a temp file rather than sed -i, which is
# a GNU-ism: BSD sed reads the argument after -i as a backup suffix, so
# the GNU spelling fails outright on macOS, where these releases are cut.
# The temp file is removed if sed dies, so a failed run leaves nothing
# untracked behind for the clean-tree gate to trip over next time.
#
# GitHub creates no events for a push of more than three tags, so the
# release workflow would silently never run; a third nested module means
# choosing between the atomic push and creating the releases with gh.
release-commit:
	@test -n "$(VERSION)" || { echo "usage: make release VERSION=vX.Y.Z"; exit 1; }
	@test $(words $(RELEASE_TAGS)) -le 3 || { echo "$(words $(RELEASE_TAGS)) tags would be pushed at once, and GitHub creates no events for a push of more than three"; exit 1; }
	@test "$(origin SUBMODULES)" = file || { echo "do not override SUBMODULES here: the override would reach the bump, tidy and check, so a module would be tagged having checked a subset."; exit 1; }
	@grep -q '^## Unreleased$$' CHANGELOG.md || { echo "CHANGELOG.md has no Unreleased section"; exit 1; }
	@test -z "$$(git status --porcelain)" || { echo "working tree is not clean"; exit 1; }
	@scripts/versions.sh set $(VERSION) $(SUBMODULES)
	sed 's/^## Unreleased$$/## $(VERSION) - '"$$(date +%F)"'/' CHANGELOG.md > CHANGELOG.md.tmp \
	  && mv CHANGELOG.md.tmp CHANGELOG.md \
	  || { rm -f CHANGELOG.md.tmp; exit 1; }
	$(MAKE) tidy
	$(MAKE) check
	@scripts/versions.sh check $(VERSION) $(SUBMODULES)
	git add -A && git commit -q -m "Release $(VERSION)" $(if $(TRAILER),-m "$(TRAILER)")

clean:
	rm -rf .cache
