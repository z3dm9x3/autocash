#!/usr/bin/env bash
# Periodic logical dump of the Autocash database to another disk/server (NFT-14).
# Run from a systemd timer or cron as the postgres or autocash user, e.g. daily:
#   deploy/backup/pg_dump.sh
# Settings (environment or /etc/autocash/backup.env):
#   AUTOCASH_BACKUP_DIR   target directory on ANOTHER disk or a mounted remote share
#   AUTOCASH_DB           database name (default: autocash)
#   AUTOCASH_KEEP         number of dumps to keep (default: 7)
# The dump itself is written first under a temporary name and renamed only when complete,
# so a crash never leaves a half-written file that looks valid.
set -euo pipefail

[ -f /etc/autocash/backup.env ] && . /etc/autocash/backup.env
DIR="${AUTOCASH_BACKUP_DIR:?AUTOCASH_BACKUP_DIR is not set}"
DB="${AUTOCASH_DB:-autocash}"
KEEP="${AUTOCASH_KEEP:-7}"

mkdir -p "$DIR/dumps"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
tmp="$DIR/dumps/.${DB}_${stamp}.dump.part"
out="$DIR/dumps/${DB}_${stamp}.dump"

# Custom format: compressed, restorable table by table with pg_restore.
pg_dump --format=custom --compress=6 --no-owner --file="$tmp" "$DB"
sync "$tmp"
mv "$tmp" "$out"
echo "dump written: $out ($(du -h "$out" | cut -f1))"

# Keep the newest $KEEP dumps: delete the oldest first.
mapfile -t dumps < <(ls -1t "$DIR"/dumps/"${DB}"_*.dump 2>/dev/null)
for old in "${dumps[@]:$KEEP}"; do
    rm -f -- "$old"
    echo "old dump removed: $old"
done
