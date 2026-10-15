#!/usr/bin/env bash
# Record the golden fixtures of the link rewrite with the real Obsidian app (OBS-024 task 8.1,
# workspace spec `notes-link-rewrite`). CI never runs this script: it replays the fixtures in
# services/notes/tests/link_rewrite_golden.rs.
#
# usage: tools/record-link-fixtures.sh [case ...]
#
# With no case, every fixture under services/notes/tests/fixtures/link-rewrite/ is recorded again.
# A fixture is a folder with two inputs, written by hand:
#
#   case.json   the rename (`from`, `to`), the setting `newLinkFormat`, and the hard cases it covers
#   before/     the whole vault before the rename
#
# and three outputs, written only by this script:
#
#   after/          the whole vault after Obsidian renamed the file and updated the links
#   recording.json  the Obsidian version, the installer version and the vault settings
#   SHA256SUMS      the SHA-256 of every other file of the fixture
#
# The script opens Obsidian with an isolated profile (`--user-data-dir`) whose only vault is a new,
# empty recording vault in a temporary folder. It never opens another vault or the default profile.
# The recording plugin tools/link-fixtures/main.js does the work inside the app; the script waits for
# it, stops the app, and writes SHA256SUMS.
#
# Environment:
#   OBSIDIAN_ASAR    required: the app package to record with, a file named obsidian-<version>.asar.
#                    The script copies it into the profile, and Obsidian loads it with updates off.
#   OBSIDIAN_APP     the app binary; default /Applications/Obsidian.app/Contents/MacOS/Obsidian
#   RECORD_PROFILE   the isolated profile; default ${TMPDIR:-/tmp}/ratatoskr-link-fixtures/profile
#   RECORD_VAULT_ID  the id of the recording vault in that profile; default 5261746174736b72
#   RECORD_TIMEOUT   seconds to wait for the plugin; default 600
#
# On the first run in a new profile, Obsidian asks whether you trust the author of the vault. Choose
# to trust it: that enables the recording plugin, and the profile keeps the answer for the vault id.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
fixtures="$root/services/notes/tests/fixtures/link-rewrite"
plugin="$root/tools/link-fixtures"
app=${OBSIDIAN_APP:-/Applications/Obsidian.app/Contents/MacOS/Obsidian}
profile=${RECORD_PROFILE:-${TMPDIR:-/tmp}/ratatoskr-link-fixtures/profile}
vault_id=${RECORD_VAULT_ID:-5261746174736b72}
timeout=${RECORD_TIMEOUT:-600}

fail() { echo "record-link-fixtures: $*" >&2; exit 1; }

[ -n "${OBSIDIAN_ASAR:-}" ] || fail "set OBSIDIAN_ASAR to an obsidian-<version>.asar file"
[ -f "$OBSIDIAN_ASAR" ] || fail "$OBSIDIAN_ASAR is not a file"
asar_name=$(basename "$OBSIDIAN_ASAR")
[[ "$asar_name" =~ ^obsidian-[0-9]+(\.[0-9]+)*\.asar$ ]] || fail "$asar_name is not named obsidian-<version>.asar"
[ -x "$app" ] || fail "$app is not an executable Obsidian app"
[[ "$vault_id" =~ ^[0-9a-f]{16}$ ]] || fail "RECORD_VAULT_ID must be 16 lowercase hex digits"

cases=("$@")
for name in ${cases[@]+"${cases[@]}"}; do
  [ -f "$fixtures/$name/case.json" ] || fail "no fixture $name"
done

work=$(mktemp -d "${TMPDIR:-/tmp}/ratatoskr-link-fixtures.XXXXXX")
vault="$work/vault"
for value in "$fixtures" "$work"; do
  case "$value" in *'"'* | *'\'*) fail "the path $value holds a quote or a backslash" ;; esac
done

pid=''
cleanup() {
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT

recorder="$vault/.obsidian/plugins/ratatoskr-link-recorder"
mkdir -p "$recorder"
cp "$plugin/main.js" "$plugin/manifest.json" "$recorder/"
printf '{"alwaysUpdateLinks": true}\n' > "$vault/.obsidian/app.json"
printf '["ratatoskr-link-recorder"]\n' > "$vault/.obsidian/community-plugins.json"
only=''
for name in ${cases[@]+"${cases[@]}"}; do
  only="${only:+$only, }\"$name\""
done
printf '{"fixtures": "%s", "done": "%s", "only": [%s]}\n' "$fixtures" "$work/DONE" "$only" \
  > "$recorder/job.json"

mkdir -p "$profile"
cp "$OBSIDIAN_ASAR" "$profile/$asar_name"
printf '{"vaults": {"%s": {"path": "%s", "ts": 0, "open": true}}, "updateDisabled": true}\n' \
  "$vault_id" "$vault" > "$profile/obsidian.json"

echo "record-link-fixtures: recording with $asar_name in $profile"
"$app" --user-data-dir="$profile" > "$work/obsidian.log" 2>&1 &
pid=$!

waited=0
until [ -f "$work/DONE" ]; do
  kill -0 "$pid" 2>/dev/null || { cat "$work/obsidian.log" >&2; fail "Obsidian stopped before the recording ended"; }
  [ "$waited" -lt "$timeout" ] || fail "no recording after $timeout s; did Obsidian ask to trust the vault?"
  sleep 1
  waited=$((waited + 1))
done
kill "$pid" 2>/dev/null || true
wait "$pid" 2>/dev/null || true
pid=''

cat "$work/DONE"
if grep -q '"error' "$work/DONE"; then
  fail "a fixture was not recorded"
fi

for dir in "$fixtures"/*/; do
  name=$(basename "$dir")
  if [ "${#cases[@]}" -gt 0 ] && [[ ! " ${cases[*]+${cases[*]}} " =~ " $name " ]]; then
    continue
  fi
  (
    cd "$dir"
    find . -type f ! -name SHA256SUMS -print0 | LC_ALL=C sort -z | xargs -0 shasum -a 256 \
      | sed 's#  \./#  #' > SHA256SUMS
  )
  echo "record-link-fixtures: $name recorded"
done
