#!/usr/bin/env bash
# The night of 2026-08-07: everything left, run TWO AT A TIME.
#
# SUPERSEDES run_noise_floor.sh + run_fit_queue.sh, whose waiter shells were
# killed to install this. No fit was interrupted -- both were asleep in poll
# loops with nothing running under them.
#
# WHY IT REPLACED THEM. Between them those two scripts had five fits left and
# ran them strictly ONE at a time: run_fit_queue.sh's wait_for_cores blocks
# until *zero* fits are running, then starts a single fit and waits for it. One
# fit is 4 chains on a 4 P-core machine, so half the box sat idle while five
# fits queued behind each other -- ~20 hours of wall clock for ~12 hours of
# work. docs/run-plan.md has said "10 cores that fit two concurrently" the whole
# time, and the pair running tonight (sat + vtx) is that configuration working.
# Three concurrent is the thing that is actually bad: measured at 370 min each
# against ~85 for two.
#
# THE ORDER CHANGED TOO, because the sweep's own numbers reordered it. The three
# clean v10 fits land at elpd -35521.13 (linear), -35521.39 (linear x gender)
# and -35518.38 (quadratic): a spread of 3.0 elpd across the whole height
# question. The noise floor has not been measured on this geometry yet, and 3.0
# is small enough that it will very likely swallow the ranking whole. So:
#
#   1. v10_lin_marg_s2   the noise floor. Nothing else can be READ until this
#                        lands, so it goes first and never waits again.
#   2. v10_zero_marg_r2  no height at all. The current zero fit is unusable --
#                        chain 3 froze, R-hat 1.53, ESS 7, p_loo 19,228, and
#                        its elpd of -55,363 is arithmetic on a dead chain, not
#                        evidence that height matters. A clean zero fit is the
#                        "does height do anything measurable" baseline, which
#                        given a 3.0 elpd spread is the most decision-relevant
#                        number left on the page. It was LAST in the old queue.
#   3. v11_lin_adv       the advancement arm: new information, not confirmation.
#   4. v10_conf_marg_r2  quadratic x gender on four clean chains. Best-scoring
#                        form once the frozen chain is dropped, but that verdict
#                        currently rests on three.
#   5. v11_quad_adv      the second advancement arm.
#
# Reruns come after the baselines because they confirm a ranking that the noise
# floor may be about to make unreadable. If it does, 4 and 5 are the fits to
# reconsider in the morning -- not to have already spent the night on.
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1
LOG_DIR=runs/logs/overnight
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/night-queue.log"
say() { echo "[$(date '+%H:%M:%S')] $*" | tee -a "$LOG"; }

export PYTHONPATH="$PWD/src"
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 \
       VECLIB_MAXIMUM_THREADS=1 NUMEXPR_NUM_THREADS=1

BASE=(--network net50 --name-filter confident
      --fixed-sigma-link --marginalize-singles
      --tune 1500 --draws 1500 --chains 4
      --marginalize-all --n-quad 21)

# Wait until nothing is sampling. Matches the scheduler's fits as well as our
# own, which is the point: the core budget is shared, not per-script.
wait_for_idle() {
  while [ "$(pgrep -f 'run_fit.py --name' | wc -l | tr -d ' ')" != "0" ]; do
    sleep 60
  done
}

# Start one fit in the background. Returns immediately; the caller waits.
start_one() {
  local name="$1" form="$2" seed="$3"; shift 3
  if [ -f "runs/results/result_${name}.json" ]; then
    say "$name already done, skipping"
    return 0
  fi
  say "starting $name ($form${*:+ $*})"
  .venv/bin/python scripts/run_fit.py --name "$name" \
    --height-form "$form" --seed "$seed" "${BASE[@]}" "$@" \
    > "$LOG_DIR/${name}.log" 2>&1 &
}

# A wave is at most two fits. Both must finish before the next wave starts:
# letting a third slip in beside a straggler is exactly the 370-minute case.
wave() {
  say "--- wave: $* ---"
  wait
  say "wave done"
}

say "waiting for the running fits (sat, vtx) and the scheduler to finish"
wait_for_idle
say "cores are free"

# 1 + 2
start_one v10_lin_marg_s2  linear             20261007
start_one v10_zero_marg_r2 zero               20261202
wave "noise floor + clean zero baseline"

# The two fits above are what the morning read depends on, so their elpd is
# assembled the moment they land rather than at the end of the night.
say "recovering LOO for anything missing it"
.venv/bin/python scripts/recover_marg_loo.py >> "$LOG" 2>&1
say "LOO pass done"

# 3 + 4
start_one v11_lin_adv      linear             20261101 --advancement
start_one v10_conf_marg_r2 quadratic_x_gender 20261201
wave "advancement arm + quad x gender rerun"

# 5
start_one v11_quad_adv     quadratic          20261102 --advancement
wave "second advancement arm"

say "recovering LOO for every marginalized fit"
.venv/bin/python scripts/recover_marg_loo.py >> "$LOG" 2>&1
say "night queue done"
