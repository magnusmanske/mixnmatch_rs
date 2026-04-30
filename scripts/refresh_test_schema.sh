#!/usr/bin/env bash
#
# Refresh tests/fixtures/schema.sql from the live Mix'n'match production DB.
#
# Read-only: uses --no-data, --skip-lock-tables, --single-transaction.
# Connection details are read from config.json::mixnmatch.url, so the DB
# never lives in the script. Requires the SSH tunnels in
# `connect_test_db.sh` to be active before this is run.
#
# Output:
#   tests/fixtures/schema.sql  (DDL only — no rows, no AUTO_INCREMENT
#                               counters, no DEFINER lines)
#
set -euo pipefail

cd "$(dirname "$0")/.."

CONFIG="${CONFIG:-config.json}"
OUT="tests/fixtures/schema.sql"

if [[ ! -f "$CONFIG" ]]; then
  echo "config.json not found at $CONFIG" >&2
  exit 1
fi

URL=$(python3 -c "import json,sys; print(json.load(open('$CONFIG'))['mixnmatch']['url'])")

# Parse mysql://user:pass@host:port/db
re='^mysql://([^:]+):([^@]+)@([^:/]+):?([0-9]*)/(.+)$'
if [[ ! "$URL" =~ $re ]]; then
  echo "Could not parse mixnmatch.url: $URL" >&2
  exit 1
fi
USER="${BASH_REMATCH[1]}"
PASS="${BASH_REMATCH[2]}"
HOST="${BASH_REMATCH[3]}"
PORT="${BASH_REMATCH[4]:-3306}"
DB="${BASH_REMATCH[5]}"

mkdir -p "$(dirname "$OUT")"

# Probe connection so we fail fast with a clear message instead of a
# half-written dump.
if ! mysql -h "$HOST" -P "$PORT" -u "$USER" -p"$PASS" -D "$DB" \
     -N -e "SELECT 1" >/dev/null 2>&1; then
  echo "Cannot reach $HOST:$PORT/$DB — is the tunnel up? (./connect_test_db.sh)" >&2
  exit 1
fi

# Read-only flags:
#   --no-data            structure only
#   --skip-lock-tables   user has no LOCK TABLES on the shared replica
#   --single-transaction safe consistent read
#   --no-tablespaces     PROCESS privilege not granted
#   --skip-comments      drop the version-stamped header (cleaner diffs)
#   --skip-set-charset   we want portable DDL
#
# Stripping AUTO_INCREMENT=N and DEFINER=... keeps the dump
# deterministic across refreshes.
mysqldump \
  -h "$HOST" -P "$PORT" -u "$USER" -p"$PASS" \
  --no-data \
  --skip-lock-tables \
  --single-transaction \
  --no-tablespaces \
  --skip-add-drop-table \
  --skip-comments \
  --skip-set-charset \
  "$DB" \
  | sed -E \
      -e 's/ AUTO_INCREMENT=[0-9]+//g' \
      -e 's/DEFINER=[^ ]+ //g' \
      -e '/^\/\*M!999999\\- /d' \
  > "$OUT.tmp"

mv "$OUT.tmp" "$OUT"

echo "Wrote $OUT ($(wc -l < "$OUT") lines, $(grep -c '^CREATE TABLE' "$OUT") tables)"
