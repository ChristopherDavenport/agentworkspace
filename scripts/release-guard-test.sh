#!/usr/bin/env bash
# Tests release-guard.sh against throwaway repositories with a local bare
# origin: each case names a module path, the tag to guard and the tags
# already published, and whether the guard must accept.
#
#   scripts/release-guard-test.sh

set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
GUARD="${GUARD:-$HERE/release-guard.sh}"
fails=0

# case <accept|refuse> <module path> <tag> [published tags...]
case_() {
  want="$1"; mod="$2"; tag="$3"; shift 3
  d="$(mktemp -d)"
  git init -q --bare "$d/origin.git"
  git init -q "$d/work"
  (
    cd "$d/work" || exit 1
    printf 'module %s\n\ngo 1.25\n' "$mod" > go.mod
    git add . && git -c user.email=t@t -c user.name=t commit -qm init
    git remote add origin "$d/origin.git"
    for t in "$@"; do git tag "$t"; done
    git push -q origin --tags 2>/dev/null
    bash "$GUARD" "$tag" >/dev/null 2>&1
  )
  rc=$?
  rm -rf "$d"
  got=refuse; [ "$rc" = 0 ] && got=accept
  if [ "$got" != "$want" ]; then
    echo "FAIL: $mod guard $tag with [$*]: want $want, got $got"
    fails=$((fails + 1))
  fi
}

M=example.com/m

# First release, and the major version against the module path.
case_ accept "$M" v0.0.1
case_ accept "$M" v1.0.0
case_ refuse "$M" v2.0.0
case_ refuse "$M" v2.0.0 v1.4.0
case_ accept "$M" v1.5.0 v1.4.0
case_ accept "$M/v2" v2.0.0
case_ refuse "$M/v2" v1.0.0
case_ refuse "$M/v2" v3.0.0 v2.0.0

# The floor is the plain vX.Y.Z tags: a prerelease or a stray v* tag
# neither raises it nor blocks the release of that version.
case_ accept "$M" v0.0.7 v0.0.6 v0.0.7-rc1
case_ accept "$M" v0.0.7 v0.0.6 vnext
case_ refuse "$M" v0.0.5 v0.0.6 vnext
case_ refuse "$M" v0.0.6 v0.0.6
case_ accept "$M" v0.0.10 v0.0.9

# A prerelease or a malformed version is refused.
case_ refuse "$M" v0.1.0-rc1 v0.0.6
case_ refuse "$M" v0.1.0-rc1
case_ refuse "$M" v0.1 v0.0.6
case_ refuse "$M" 0.1.0

[ "$fails" = 0 ] && echo "release-guard tests passed"
exit "$fails"
