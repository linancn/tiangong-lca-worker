#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "usage: $0 <sql-file> [<sql-file> ...]" >&2
  exit 2
fi

for f in "$@"; do
  echo "[check] $f"
  [ -f "$f" ] || { echo "missing file: $f" >&2; exit 1; }

  # Hard-block destructive statements.
  if rg -n -i "\b(drop\s+table|drop\s+schema|truncate\s+table|delete\s+from|update\s+public\.)" "$f" >/dev/null; then
    echo "destructive pattern detected in $f" >&2
    rg -n -i "\b(drop\s+table|drop\s+schema|truncate\s+table|delete\s+from|update\s+public\.)" "$f" >&2
    exit 1
  fi

  # ALTER TABLE must stay within the migration's explicitly declared schema.
  if rg -n -i "alter\s+table\s+public\." "$f" >/tmp/alter_hits.txt; then
    if rg -n -i -v "alter\s+table\s+public\.lca_" /tmp/alter_hits.txt >/tmp/alter_bad.txt; then
      echo "non-lca ALTER TABLE detected in $f" >&2
      cat /tmp/alter_bad.txt >&2
      rm -f /tmp/alter_hits.txt /tmp/alter_bad.txt
      exit 1
    fi
    rm -f /tmp/alter_hits.txt /tmp/alter_bad.txt
  fi

  # Soft warning if INSERT appears (migration should stay DDL-only for phase1).
  if rg -n -i "\binsert\s+into\s+public\." "$f" >/dev/null; then
    echo "warning: INSERT found in $f (review carefully)" >&2
  fi

done

echo "additive migration checks passed"
