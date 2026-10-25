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

# nested <accept|refuse> <tag> <root version sub/go.mod requires>
#        <where the root tag of the tag's version is: head, behind or none>
#        [published tags...]
# A repository with a nested module at sub/, requiring the root through
# an in-tree replace, as mcpworkspace does. The guard's version check
# runs from the repository's scripts/, so they are copied in. GOPROXY=off
# keeps the extracted build from reaching for a proxy: the root version
# is then "not on the proxy yet", which is what make release sees.
nested() {
  want="$1"; tag="$2"; req="$3"; root="$4"; shift 4
  d="$(mktemp -d)"
  git init -q --bare "$d/origin.git"
  git init -q "$d/work"
  (
    cd "$d/work" || exit 1
    mkdir -p scripts sub
    cp "$HERE/versions.sh" "$HERE/check-extracted.sh" scripts/
    printf 'module %s\n\ngo 1.25\n' "$M" > go.mod
    printf 'module %s/sub\n\ngo 1.25\n\nrequire %s %s\n\nreplace %s => ..\n' "$M" "$M" "$req" "$M" > sub/go.mod
    git add . && git -c user.email=t@t -c user.name=t commit -qm init
    git remote add origin "$d/origin.git"
    for t in "$@"; do git tag "$t"; done
    git push -q origin --tags 2>/dev/null
    v="v${tag##*/v}"
    case "$root" in
      head) git tag "$v" ;;
      behind)
        git tag "$v"
        echo x > README && git add README && git -c user.email=t@t -c user.name=t commit -qm later ;;
    esac
    GOPROXY=off GOWORK=off bash "$GUARD" "$tag" >/dev/null 2>&1
  )
  rc=$?
  rm -rf "$d"
  got=refuse; [ "$rc" = 0 ] && got=accept
  if [ "$got" != "$want" ]; then
    echo "FAIL: nested guard $tag (requires $req, root tag $root) with [$*]: want $want, got $got"
    fails=$((fails + 1))
  fi
}

# A nested tag: the root tag of its version at this commit, required at
# that version, and the version at or above the root's and its own.
nested accept sub/v0.0.2 v0.0.2 head v0.0.1
nested accept sub/v0.0.3 v0.0.3 head v0.0.2 sub/v0.0.2
nested refuse sub/v0.0.2 v0.0.1 head v0.0.1
nested refuse sub/v0.0.2 v0.0.2 none v0.0.1
nested refuse sub/v0.0.2 v0.0.2 behind v0.0.1
nested refuse sub/v0.0.2 v0.0.2 head v0.0.3
nested refuse sub/v0.0.2 v0.0.2 head v0.0.1 sub/v0.0.2
nested refuse other/v0.0.2 v0.0.2 head v0.0.1

[ "$fails" = 0 ] && echo "release-guard tests passed"
exit "$fails"
