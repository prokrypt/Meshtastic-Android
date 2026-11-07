#!/usr/bin/env bash
# prokrypt fork only: prints Markdown release notes for <ref> (default HEAD): fork changes and
# upstream commits since the previous v*-pk.* tag. Needs full history and upstream main at <upstream>.
set -euo pipefail
ref=${1:-HEAD}
upstream=${2:-upstream/main}
prev=$(git tag --list 'v*-pk.*' --merged "$ref^" --sort=-v:refname | head -n1)
range=${prev:+$prev..}$ref
base=$(git merge-base "$ref" "$upstream")

echo "Fork build of $(git rev-parse --short=8 "$ref")${prev:+, changes since $prev}."
echo
echo "meshtashtic-google: Google Maps and Play services. meshtashtic-fdroid: no Google services."
echo
echo "## Fork changes"
fork=$(git log --no-merges --format='- %s (%h)' "$range" --not "$upstream")
echo "${fork:-- None}"
echo
echo "## Upstream changes"
if [ -z "$prev" ]; then
  echo "- Based on upstream main at $(git rev-parse --short=8 "$base")"
  exit 0
fi
up=$(git log --no-merges --format='- %s (%h)' "$prev..$base" \
  | sed -E 's/\(#([0-9]+)\)/(meshtastic\/Meshtastic-Android#\1)/g')
echo "${up:-- None}"
