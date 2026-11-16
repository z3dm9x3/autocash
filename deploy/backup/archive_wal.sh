#!/usr/bin/env bash
# WAL archiving for point-in-time recovery (NFT-14). Used as PostgreSQL archive_command:
#
#   archive_mode = on
#   archive_command = '/opt/autocash/deploy/backup/archive_wal.sh %p %f'
#   archive_timeout = 300
#
# PostgreSQL retries a segment until this script exits 0, so it must fail loudly and never
# overwrite an existing archived segment with different contents.
set -euo pipefail

src="$1"   # %p: path of the segment, relative to the data directory
name="$2"  # %f: file name of the segment

[ -f /etc/autocash/backup.env ] && . /etc/autocash/backup.env
DIR="${AUTOCASH_BACKUP_DIR:?AUTOCASH_BACKUP_DIR is not set}/wal"
mkdir -p "$DIR"
dst="$DIR/$name"

if [ -e "$dst" ]; then
    # Already archived (a retry after a crash): fine only if it is the same file.
    cmp -s "$src" "$dst" && exit 0
    echo "archive_wal: $dst exists with different contents" >&2
    exit 1
fi

cp -- "$src" "$dst.part"
sync "$dst.part"
mv -- "$dst.part" "$dst"
