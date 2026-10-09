#!/usr/bin/env bash
# Checks a tag is safe to push before it is pushed. A pushed tag is
# permanent: the module proxy and the checksum database keep the version
# forever, so a mistake can only be retracted, never withdrawn.
#
#   scripts/release-guard.sh v0.0.1
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
# This repository is a single module, so every tag is a root tag. The
# sibling repositories with nested modules carry a longer version of this
# script that also holds a <dir>/vX.Y.Z tag to its module's go.mod and to
# the root tag of the same version. If a nested module is ever added
# here, that is the part to bring over.

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
    die "$TAG names a nested module, and this repository has a single module.
            Release it as vX.Y.Z, or bring over the nested-module checks
            from a sibling repository's release-guard.sh first."
    ;;

  *)
    die "unrecognised tag shape: $TAG (expected vX.Y.Z)"
    ;;
esac

echo "release-guard: $TAG is safe to push"
