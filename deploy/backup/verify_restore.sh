#!/usr/bin/env bash
# Regular restore check (NFT-14): restores the newest dump into a throw-away database,
# checks that the core tables are readable and not empty, then drops the database.
# Run weekly from a timer; a non-zero exit code means the backup cannot be trusted.
set -euo pipefail

[ -f /etc/autocash/backup.env ] && . /etc/autocash/backup.env
DIR="${AUTOCASH_BACKUP_DIR:?AUTOCASH_BACKUP_DIR is not set}"
DB="${AUTOCASH_DB:-autocash}"
CHECK_DB="${DB}_restore_check"

dump="$(ls -1t "$DIR"/dumps/"${DB}"_*.dump 2>/dev/null | head -n 1 || true)"
if [ -z "$dump" ]; then
    echo "verify_restore: no dumps in $DIR/dumps" >&2
    exit 1
fi

cleanup() { dropdb --if-exists "$CHECK_DB" >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup
createdb "$CHECK_DB"
pg_restore --exit-on-error --no-owner --dbname="$CHECK_DB" "$dump"

q() { psql -X -A -t -v ON_ERROR_STOP=1 -d "$CHECK_DB" -c "$1"; }
migrations="$(q 'SELECT count(*) FROM schema_migrations')"
messages="$(q 'SELECT count(*) FROM messages')"
traces="$(q 'SELECT count(*) FROM trace_inbound')"
reasons="$(q 'SELECT count(*) FROM reason_codes')"

echo "restored $dump: migrations=$migrations messages=$messages traces=$traces reason_codes=$reasons"
if [ "$migrations" -eq 0 ] || [ "$reasons" -eq 0 ]; then
    echo "verify_restore: schema is incomplete" >&2
    exit 1
fi
echo "verify_restore: OK"
