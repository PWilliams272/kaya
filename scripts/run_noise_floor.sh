#!/usr/bin/env bash
# The noise-floor replicate, launched when the 2026-08-07 scheduler frees its cores.
#
# Why a waiter rather than a job in the plan: run_overnight.py builds its job
# list once at start. The plan in build_jobs was corrected on 2026-08-07 to run
# this replicate in place of a retired centered arm, but the scheduler launched
# at 07:17 had already loaded the old plan, and restarting it would have thrown
# away two fits with hours of work in them. So the source is right for next
# time, and this script closes the gap for this run.
#
# What it measures: v10_lin_marg refit at an identical configuration and a
# different seed. The elpd gap between the two IS the noise floor -- the amount
# a leave-one-climber-out score moves for reasons that have nothing to do with
# the model. The seven-form sweep has to clear that gap to be a ranking rather
# than a rearrangement, and the v7 sweep is the cautionary case: 32.7 elpd of
# spread against 31.1 elpd of floor, which is no ranking at all.
#
# The v7 floor does not carry over. It was measured per OBSERVATION on chains
# that never mixed; these fits are per CLIMBER on chains that did. Neither the
# unit nor the geometry transfers.
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1
SCHED_PID="${1:-}"
LOG_DIR=runs/logs/overnight
mkdir -p "$LOG_DIR"
say() { echo "[$(date '+%H:%M:%S')] $*" | tee -a "$LOG_DIR/noise-floor.log"; }

if [ -n "$SCHED_PID" ]; then
  say "waiting for scheduler pid $SCHED_PID to finish"
  while kill -0 "$SCHED_PID" 2>/dev/null; do sleep 120; done
  say "scheduler exited"
fi

# Belt and braces: never start while fits are still holding cores. The core
# budget is the hard constraint -- three concurrent fits was measured at 370
# minutes each against ~85 for two.
while [ "$(pgrep -f 'run_fit.py --name' | wc -l | tr -d ' ')" != "0" ]; do
  say "fits still running, holding"
  sleep 120
done

if [ -f runs/results/result_v10_lin_marg_s2.json ]; then
  say "already done, nothing to do"; exit 0
fi

say "starting v10_lin_marg_s2 (linear, seed 20261007 -- twin of v10_lin_marg)"
export PYTHONPATH="$PWD/src"
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 \
       VECLIB_MAXIMUM_THREADS=1 NUMEXPR_NUM_THREADS=1

.venv/bin/python scripts/run_fit.py --name v10_lin_marg_s2 \
  --network net50 --name-filter confident \
  --fixed-sigma-link --marginalize-singles \
  --height-form linear --tune 1500 --draws 1500 --chains 4 \
  --seed 20261007 --marginalize-all --n-quad 21 \
  > "$LOG_DIR/v10_lin_marg_s2.log" 2>&1
rc=$?
say "v10_lin_marg_s2 exited rc=$rc"

if [ "$rc" = 0 ]; then
  say "recovering LOO for every marginalized fit and reporting the floor"
  .venv/bin/python scripts/recover_marg_loo.py >> "$LOG_DIR/noise-floor.log" 2>&1
  .venv/bin/python scripts/report_noise_floor.py >> "$LOG_DIR/noise-floor.log" 2>&1
fi
say "done"
