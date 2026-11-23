#!/bin/bash
# [T5] archive_command: archive_wal.sh %p %f
# Copy a finished WAL segment into the shared archive atomically. After a failover the new
# primary may try to archive a segment the old primary already archived: succeed if the
# archived copy is byte-identical, fail (and keep retrying) if it differs.
set -euo pipefail
src="$1"; dst="/wal_archive/$2"
if [ -f "$dst" ]; then
  exec cmp -s "$src" "$dst"
fi
tmp="$dst.tmp.$$"
cp "$src" "$tmp"
mv "$tmp" "$dst"
