#!/usr/bin/env bash
# Checks a tag is safe to push before it is pushed. A pushed tag is
# permanent: the module proxy and the checksum database keep the version
# forever, so a mistake can only be retracted, never withdrawn.
#
#   scripts/release-guard.sh v0.0.2
#   scripts/release-guard.sh mcpworkspace/v0.0.2
#
# Exits non-zero, with the reason, if the tree is dirty, the tag already
# exists, or the version does not sort above the current release. When no
# plain vX.Y.Z tag is published yet, any vX.Y.Z of the module's major
# version is the first release. Prerelease versions are refused.
#
# Run it before the tag is written; make release does. Nothing is public
# until the push, so a refusal costs a git reset --hard HEAD~1 and a git
# tag -d at worst.
#
# A nested module's tag, <dir>/vX.Y.Z, is held to the shared version
# line: at or above the root's release, above the module's own, its
# go.mod requiring the root at that very version, and the root tag of
# that version naming this commit. It is only checkable once that root
# tag exists locally, so run it in the order make release does: guard
# the root, tag it, then guard and tag each nested module. This half is
# agenttool's script's, which holds the same line.

set -euo pipefail

TAG="${1:-}"

die() { echo "release-guard: $*" >&2; exit 1; }
ok()  { echo "  ok  $*"; }

[ -n "$TAG" ] || die "usage: $0 <tag>"

# Newest version wins a version sort; -V orders v0.0.9 before v0.0.10.
newest() { sort -V | tail -1; }

echo "release-guard: $TAG"

# --- Repository state -------------------------------------------------
[ -z "$(git status --porcelain)" ] || die "working tree is dirty; commit or stash first"
ok "working tree clean"

# --- What origin has published ----------------------------------------
# The version floor has to come from what is published, not from what
# this checkout happens to know. A clone that has not fetched recently
# carries a stale floor, and a version that sorts below one already on
# the proxy is the one mistake with no remedy: proxy.golang.org and
# sum.golang.org serve both forever and nobody can supersede the older
# content. Reading only local tags made this script approve exactly that.
#
# git ls-remote is read-only, so unlike a git fetch --tags at the top of
# a check it does not mutate the caller's tag state as a side effect.
#
# It fails closed. If origin cannot be reached then the push could not
# have succeeded either, so refusing costs a release nothing, while a
# silent fallback to local tags would reinstate the stale floor on
# precisely the day the network is unreliable.
REMOTE_LS="$(git ls-remote --tags origin 2>&1)" \
  || die "cannot read the published tags from origin:
            $REMOTE_LS
            The floor is what origin has published, so there is no safe answer
            without it, and a push could not have succeeded either."

# ls-remote returns the peeled ^{} refs alongside the tags; drop them.
REMOTE_TAGS="$(printf '%s\n' "$REMOTE_LS" | sed -e 's|.*refs/tags/||' -e '/\^{}$/d')"

# The floor is the union of local and remote. Remote alone would break
# make release: it writes the root tag locally and does not push until
# the end, so the checks below still have to see local tags.
ALL_TAGS="$( { git tag -l; printf '%s\n' "$REMOTE_TAGS"; } | sort -u )"

# Tags matching a pattern, from that union. grep exits 1 on no match,
# which errexit would take as a failure, so the empty case is explicit.
matching() { printf '%s\n' "$ALL_TAGS" | grep -E "$1" || true; }
ok "read the published tags from origin"

git rev-parse -q --verify "refs/tags/$TAG" >/dev/null \
  && die "tag $TAG already exists locally"
printf '%s\n' "$REMOTE_TAGS" | grep -qxF -- "$TAG" \
  && die "tag $TAG already exists on origin"
ok "tag is new"

# --- Version moves forward --------------------------------------------
# A version that sorts below one already published is the one mistake
# this cannot be undone from: the proxy serves both forever, and nobody
# can supersede the older content.
# The remote listing above succeeded, so an empty floor means nothing has
# been published: this is the first release, which has nothing to sort
# above.
#
# Only plain vX.Y.Z tags make the floor. sort -V puts v0.0.7-rc1 above
# v0.0.7 and vnext above every number, so counting them would refuse the
# release of a version that is not behind anything.
LATEST="$(matching '^v[0-9]+\.[0-9]+\.[0-9]+$' | newest)"

case "$TAG" in
  v*)
    printf '%s\n' "$TAG" | grep -qE '^v[0-9]+\.[0-9]+\.[0-9]+$' \
      || die "$TAG is not a plain vX.Y.Z version; prereleases and other forms are not released by this guard"
    # Go's rule: a module whose path has no /vN suffix is v0 or v1; /vN is
    # vN. A v2.0.0 tag on the unsuffixed path cannot be withdrawn and no
    # consumer can require it as the module.
    MODPATH="$(sed -n 's/^module[[:space:]]*//p' go.mod | head -1)"
    MAJOR="${TAG#v}"; MAJOR="${MAJOR%%.*}"
    if printf '%s\n' "$MODPATH" | grep -qE '/v[0-9]+$'; then
      WANT="${MODPATH##*/v}"
      [ "$MAJOR" = "$WANT" ] \
        || die "$TAG does not match the module path $MODPATH, whose major version is $WANT"
    else
      [ "$MAJOR" = 0 ] || [ "$MAJOR" = 1 ] \
        || die "$TAG is major version $MAJOR, but the module path $MODPATH has no /v$MAJOR suffix; a v2 or later needs the path to say so (and go.mod changed first)"
    fi
    ok "major version $MAJOR matches the module path"
    if [ -z "$LATEST" ]; then
      ok "$TAG is the first release"
    else
      [ "$(printf '%s\n%s\n' "$LATEST" "$TAG" | newest)" = "$TAG" ] \
        || die "$TAG does not sort above the current release $LATEST"
      ok "$TAG is newer than $LATEST"
    fi
    ;;

  */v*)
    DIR="${TAG%/v*}"                 # mcpworkspace
    VERSION="v${TAG##*/v}"           # v0.0.7
    [ -f "$DIR/go.mod" ] || die "no module at $DIR (expected $DIR/go.mod)"

    # Every published module shares one version line. A nested module
    # numbered below the newest root drags consumers' root module
    # backwards: they select the highest nested version available, and
    # its go.mod then pins the root, through minimal version selection.
    [ "$(printf '%s\n%s\n' "$LATEST" "$VERSION" | newest)" = "$VERSION" ] \
      || die "$VERSION sorts below the current root release $LATEST;
            a nested version under the root silently downgrades consumers.
            Tag $LATEST or later."
    ok "$VERSION is at or above the root release $LATEST"

    # And it has to move that module forward too, or the proxy keeps
    # serving the older content under a version nobody can supersede.
    DIR_LATEST="$(matching "^$DIR/v" | sed "s|^$DIR/||" | newest)"
    if [ -n "$DIR_LATEST" ]; then
      [ "$(printf '%s\n%s\n' "$DIR_LATEST" "$VERSION" | newest)" = "$VERSION" ] \
        || die "$VERSION does not sort above $DIR's current release $DIR_LATEST"
      ok "$VERSION is newer than $DIR/$DIR_LATEST"
    fi

    # The whole invariant, in two checks.
    #
    # First: every first-party version this module requires is the
    # version being tagged. A consumer who takes only this module gets
    # exactly those versions, so this is what makes the shared version
    # line mean something rather than being decoration.
    scripts/versions.sh check "$VERSION" "$DIR" \
      || die "$DIR does not require its first-party siblings at $VERSION"
    ok "requires every first-party sibling at $VERSION"

    # Second: the root tag of that version names this very commit. The
    # require above claims this module was built against root $VERSION;
    # this is what makes the claim true rather than merely asserted. It
    # is why there is no build against a published root here — there is
    # no older root in the picture to drift from.
    ROOT_TAG_COMMIT="$(git rev-list -n1 "$VERSION" 2>/dev/null || true)"
    [ -n "$ROOT_TAG_COMMIT" ] \
      || die "$VERSION is not tagged; the root and its nested modules are tagged from one commit,
            so tag the root in the same run (make release does this)"
    [ "$ROOT_TAG_COMMIT" = "$(git rev-parse HEAD)" ] \
      || die "root $VERSION points at $ROOT_TAG_COMMIT, not HEAD;
            $DIR would claim to be built against a root it was not built against"
    ok "root $VERSION is this commit"

    # Build, vet and test it the way a consumer gets it: extracted, with
    # the replace dropped, so the require line above is resolved from the
    # proxy rather than from the directory next door.
    #
    # This line used to read (cd "$DIR" && GOWORK=off go build ./...),
    # described as proof the module "is buildable as published". It was
    # that, once. A nested go.mod carrying replace <root> => .. makes
    # GOWORK=off resolve the root from the tree, so it became a build of
    # the tree against itself — make build, run twice — while still
    # printing ok. check-extracted.sh restores what the line claimed.
    #
    # It can only run once the root version is on the proxy, and during
    # make release it is not: the root tag is written locally and pushed
    # on the last line. Nothing is lost by skipping it there. The two
    # checks immediately above have already established that every
    # first-party require names $VERSION and that root $VERSION is this
    # commit, so root $VERSION *is* this tree, and make check compiled
    # $DIR against this tree before the release commit was written. The
    # extracted build would re-derive that through the proxy. Where it
    # earns its keep is on a require naming an earlier release, which is
    # every run outside make release: CI on pull requests and on main,
    # and make release-guard on a tag whose root is already published.
    # go list -m reports every module in the workspace, so the root has
    # to be asked for outside it.
    MODULE="$(GOWORK=off go list -m)"
    if [ -z "$(GOWORK=off go list -m -e -f '{{with .Error}}{{.Err}}{{end}}' \
                 "$MODULE@$VERSION")" ]; then
      scripts/check-extracted.sh "$DIR" \
        || die "$DIR does not build against $MODULE@$VERSION as a consumer gets it"
    else
      echo "  --  $MODULE@$VERSION is not on the proxy yet, so $DIR cannot be"
      echo "      built the way a consumer gets it. Root $VERSION is this commit"
      echo "      and make check compiled $DIR against it, so nothing is unproven;"
      echo "      the extracted build runs in CI once the tags are pushed."
    fi
    ;;

  *)
    die "unrecognised tag shape: $TAG (expected vX.Y.Z or <dir>/vX.Y.Z)"
    ;;
esac

echo "release-guard: $TAG is safe to push"
