#!/usr/bin/env bash
# One ordered queue for everything waiting on the 2026-08-07 scheduler's cores.
#
# Why one script and not three waiters: each waiter polls for "no fits
# running", so several of them fire in the same instant the machine drains and
# then fight for cores. Three concurrent fits was measured at 370 minutes each
# against ~85 for two. Serialising them here is the whole point.
#
# Order, highest value first:
#
#   1. The frozen-chain reruns. v10_conf_marg and v10_zero_marg each reported
#      R-hat 1.53 / ESS 7 / 1500 divergences -- identical numbers from two
#      models that share no height parameters, which is the tell. Chain 3 of
#      each adapted its step size to exactly 0.0 during warmup and never moved
#      from its initial point: beta0 standard deviation 0.0000 across 3,000
#      draws. The other three chains were healthy and agreed to three decimals.
#      Dropping the frozen chain, quadratic_x_gender is the BEST-scoring form,
#      not a failure. That verdict currently rests on three chains, so it gets
#      four clean ones.
#
#   2. The advancement arm. Paired against the v10 twins, identical but for
#      the fixed offset and the seed.
#
# The noise floor (run_noise_floor.sh) still runs first -- it is what makes any
# of these elpd differences readable -- and this queue waits for its output.
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1
LOG_DIR=runs/logs/overnight
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/fit-queue.log"
say() { echo "[$(date '+%H:%M:%S')] $*" | tee -a "$LOG"; }

export PYTHONPATH="$PWD/src"
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 \
       VECLIB_MAXIMUM_THREADS=1 NUMEXPR_NUM_THREADS=1

say "waiting for the noise-floor replicate to land"
while [ ! -f runs/results/result_v10_lin_marg_s2.json ]; do sleep 120; done
say "noise floor is in"

wait_for_cores() {
  while [ "$(pgrep -f 'run_fit.py --name' | wc -l | tr -d ' ')" != "0" ]; do
    sleep 120
  done
}

# name form seed [extra flags...]
run_one() {
  local name="$1" form="$2" seed="$3"; shift 3
  if [ -f "runs/results/result_${name}.json" ]; then
    say "$name already done, skipping"; return 0
  fi
  wait_for_cores
  say "starting $name ($form${*:+ $*})"
  .venv/bin/python scripts/run_fit.py --name "$name" \
    --network net50 --name-filter confident \
    --fixed-sigma-link --marginalize-singles \
    --height-form "$form" --tune 1500 --draws 1500 --chains 4 \
    --seed "$seed" --marginalize-all --n-quad 21 "$@" \
    > "$LOG_DIR/${name}.log" 2>&1
  say "$name exited rc=$?"
}

run_one v10_conf_marg_r2 quadratic_x_gender 20261201
run_one v10_zero_marg_r2 zero               20261202
run_one v11_lin_adv      linear             20261101 --advancement
run_one v11_quad_adv     quadratic          20261102 --advancement

wait_for_cores
say "recovering LOO for every marginalized fit"
.venv/bin/python scripts/recover_marg_loo.py >> "$LOG" 2>&1
say "done"
