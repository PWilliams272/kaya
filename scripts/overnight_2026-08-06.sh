#!/usr/bin/env bash
# The night of 2026-08-06.
#
# Two probes are already running when this starts:
#   v8_lin_centered   -- does the centered parameterisation help?      (4 cores)
#   v9_probe_margall  -- does the quadrature finish its trajectories?  (4 cores)
#
# Both gate the plan in scripts/run_overnight.py, so this waits for their result
# files before handing over. Waiting rather than starting immediately is the
# point: launching the scheduler now would evaluate both gates against files
# that do not exist yet and skip every job in the plan within seconds.
#
# The wait is bounded. If a probe dies or hangs, the scheduler still starts and
# the gates fail honestly -- a night that runs the ungated work is worth more
# than a night that runs nothing.
set -u

cd "$(dirname "$0")/.." || exit 1
RESULTS=runs/results
DEADLINE=$(( $(date +%s) + 5400 ))          # 90 minutes

echo "[$(date +%H:%M:%S)] waiting for the two probes"
while :; do
  have_cen=0; have_quad=0
  [ -f "$RESULTS/result_v8_lin_centered.json" ] && have_cen=1
  [ -f "$RESULTS/result_v9_probe_margall.json" ] && have_quad=1
  # macOS pgrep has no -c, and no alternation support worth relying on:
  # count the two patterns separately and add them.
  running=$(( $(pgrep -f 'v8_lin_centered' | wc -l) + $(pgrep -f 'v9_probe_margall' | wc -l) ))

  if [ "$have_cen" = 1 ] && [ "$have_quad" = 1 ]; then
    echo "[$(date +%H:%M:%S)] both probes finished"
    break
  fi
  if [ "$running" = 0 ]; then
    # Neither is running any more, so any missing result is never arriving.
    echo "[$(date +%H:%M:%S)] probes no longer running (centered=$have_cen quad=$have_quad)"
    break
  fi
  if [ "$(date +%s)" -ge "$DEADLINE" ]; then
    echo "[$(date +%H:%M:%S)] deadline reached; starting anyway"
    break
  fi
  sleep 60
done

echo "[$(date +%H:%M:%S)] gate inputs:"
for f in result_v8_lin_centered result_v9_probe_margall; do
  if [ -f "$RESULTS/$f.json" ]; then
    .venv/bin/python - "$RESULTS/$f.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
td = d.get('tree_depth') or {}
print(f"   {d['name']}: R-hat {d.get('max_rhat')}, ESS {d.get('min_ess')}, "
      f"{d.get('elapsed_min', 0):.0f} min, "
      f"{100 * td.get('frac_at_limit', float('nan')):.0f}% at depth limit "
      f"{td.get('limit')}, step {td.get('step_size')}")
PY
  else
    echo "   $f: MISSING — its gate will fail"
  fi
done

echo "[$(date +%H:%M:%S)] starting the gated plan"
exec .venv/bin/python scripts/run_overnight.py --cores 8
