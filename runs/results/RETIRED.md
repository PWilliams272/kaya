# Retired result files

## `result_v8_lin_centered.json.retired-2026-08-07`

Moved aside 2026-08-07, deliberately, to retire the centered arm of the
`run_overnight.py` plan **from a scheduler that was already running**.

The scheduler builds its job list once at start and evaluates each gate lazily,
on first use, by reading a result file from this directory. The plan was edited
in source (see `build_jobs`, which now runs a noise-floor replicate in place of
the two centered fits), but the process launched at 07:17 had already loaded the
old plan and the old gate function, so a source edit could not reach it. Moving
the gate's evidence out from under it was the only lever that did not require
killing two fits with four and one hours of work in them.

**The scheduler will therefore log `centering_helped: FAIL — no result file --
the centered probe did not finish`. That reason is wrong.** The probe finished
fine; its result is in this file. The real reason is that the arm was superseded:

| | centered (`v8_lin_centered`) | quadrature (`v10_lin_marg`) |
| --- | --- | --- |
| R-hat (lower is better) | 1.030 | **1.0000** |
| Effective sample size (higher is better) | 194 | **762** |
| Iterations at the tree-depth limit | 100% | **0%** |
| Step size | 0.0029 | **0.121** |

The centered arm existed as insurance against the quadrature proving too slow.
It did not prove too slow. Spending ~10 core-hours reproducing a strictly worse
parameterisation to insure against an outcome that has already resolved is not
insurance.

Nothing else reads this file — `grep -rn v8_lin_centered` finds only
`run_overnight.py`'s gate and the historical launcher `overnight_2026-08-06.sh`,
which has already run. Restore with:

    mv result_v8_lin_centered.json.retired-2026-08-07 result_v8_lin_centered.json
