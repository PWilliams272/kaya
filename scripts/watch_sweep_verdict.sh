#!/usr/bin/env bash
# Write the sweep verdict the moment the noise floor lands.
#
# report_noise_floor.py answers the two questions the whole height section
# rests on, but only once v10_lin_marg_s2 exists and has an elpd. That happens
# in the middle of the night. Rather than leave the answer waiting for someone
# to run a command, this writes it to runs/logs/overnight/sweep-verdict.log as
# soon as it is computable, and re-writes it after each later fit lands so the
# file always reflects everything scored so far.
#
# Costs nothing: it sleeps, and the report is a few JSON reads. It does NOT
# compete for cores, which is why it is a separate waiter rather than a step in
# run_night_queue.sh -- adding steps to a running bash script is how you corrupt
# one mid-execution.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
OUT=runs/logs/overnight/sweep-verdict.log
export PYTHONPATH="$PWD/src"

has_loo() {
  .venv/bin/python -c "
import json,sys
from pathlib import Path
p = Path('runs/results/result_$1.json')
sys.exit(0 if p.exists() and (json.loads(p.read_text()).get('loo') or {}).get('elpd_loo') else 1)
" 2>/dev/null
}

while ! has_loo v10_lin_marg_s2; do sleep 120; done

# Re-report as later fits land, so the file is never a stale first draft.
for _ in $(seq 1 200); do
  {
    echo "=== $(date '+%Y-%m-%d %H:%M:%S') ==="
    .venv/bin/python scripts/report_noise_floor.py
    echo
  } > "$OUT.tmp" 2>&1
  mv "$OUT.tmp" "$OUT"
  sleep 600
done
