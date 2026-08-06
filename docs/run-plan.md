# Run plan

Every fit costs 60–90 minutes at current settings and 5–6 hours at long ones,
on 10 cores that fit two concurrently. So the plan is **decision-driven**: each
phase exists to settle one question, and nothing downstream of an unsettled
question gets run. Stop when the question is answered, not when the batch is.

Status as of 2026-08-05.

## The questions, in dependency order

| # | question | what settles it | blocks |
|---|---|---|---|
| Q1 | Does the zero-sum prior fix hold up under an independent sampler? | emcee `lin2` | nothing — it is a cross-check |
| Q2 | Is the R-hat problem underadapted warm-up? | `v5_lin_marg_long` | Q3's settings |
| Q3 | Does orthogonalising the design fix the height block? | `v6_conf_orth` vs `v3_conf_marg` | Q4 |
| Q4 | Which height form is best, measured properly? | 7-form sweep + noise floor | the page's primary |
| Q5 | Can the model predict a climber it has never seen? | grouped k-fold | nothing — it is the honest headline |
| Q6 | What are the Bayes factors between forms? | nested / bridge sampling | nothing |

## Phase 0 — in flight

| run | settings | cost | status |
|---|---|---|---|
| emcee `lin2` | 128 walkers × 20,000 steps, DE moves | 7 h | running |
| `v5_lin_marg_long` | linear, marginalized, tune 2000 / draws 2000 | 5–6 h | queued behind emcee |

**Q1** is answered by whether `sigma_gym` comes back near PyMC's 0.301 rather
than the pre-fix run's 0.193.
**Q2** is answered by whether max R-hat drops below 1.01 from the 1.040
baseline. If it does, the fix is a settings change and Phase 1 gets cheaper.

## Phase 1 — settle the parameterisation (1 fit)

`v6_conf_orth`: quadratic × gender, marginalized, **orthogonalised height and
ape blocks**, at the *baseline* settings (tune 600 / draws 500) so it is
directly comparable to `v3_conf_marg`.

Baseline to beat: max R-hat **1.069**, min ESS **48**, with `gamma1_x` 1.056,
`gamma2_x` 1.051, `gamma1` 1.050.

Two checks, and the second is the correctness gate:

* R-hat and ESS on the gamma block should improve. The design-block condition
  number goes 36.0 → 1.00, and a diagonal mass matrix cannot represent a
  rotation no matter how long it tunes, so this has no ceiling that warm-up has.
* **The fitted height curve must be unchanged.** Orthogonalising re-expresses
  the same function space; if the curve moves, the transform or the prior
  rescaling is wrong.

Cost: ~85 min. **Decision point.** If it works, everything after this uses the
orthogonal parameterisation and the earlier fits become the "before" arm.

## Phase 2 — the height-form sweep (7 fits)

Only if Phase 1 succeeds. Re-run all seven height forms, marginalized,
orthogonalised, at whatever settings Phase 0 established:

`zero, linear, quadratic, linear × gender, quadratic × gender, saturating,
vertex quadratic`

Cost: 7 fits, 2 concurrent → **~5 h** at baseline settings, ~20 h at long ones.

## Phase 3 — the noise floor (3 fits)

**Non-optional, and the most under-valued phase.** Three refits of the chosen
primary, identical except for the seed. The spread among them is the noise
floor, and *any gap in Phase 2 smaller than it is not a result*.

This project has already been burned here: two fits of the identical model
scored 31.1 elpd apart, which was larger than most of the differences being
compared.

Cost: 3 fits → ~2 h at baseline. Run concurrently with Phase 2's tail.

## Phase 4 — grouped k-fold (20–35 fits)

The honest validation: hold out whole **climbers**, refit, predict all their
rows. Leave-one-out asks "given this climber's other sends, predict one more",
which is the easy question. This asks "can you predict somebody you have never
seen", which is the model's actual job whenever it makes a claim about a gym.

5 folds × the forms worth comparing. Four forms → 20 fits ≈ **28 h local**.

**This is the phase to move to cloud compute** if any of them is. The launcher
exists (`scripts/run_batch.py --cloud`), blocked only on an IAM policy. ~2 h
and a few dollars on a spot instance against 28 h of laptop.

Cut it to 3 folds × 3 forms (9 fits, ~13 h) if that stays blocked.

## Phase 5 — evidence and Bayes factors

Independent of everything above; scripts are written and unrun.

* **Nested sampling** — computes the evidence directly, with a `--prior-scale`
  sweep to show how much the Bayes factors depend on the priors. Note it
  explores the whole prior and has already found a numerical fragility NUTS
  never reaches (`inf - inf` in the ExGaussian when `nu <= 0.05*sigma`).
* **Bridge sampling** — the evidence from the MCMC draws already on disk.
  Cheapest of the two by far; do it first.

## What is deliberately not queued

* **Re-running the unmarginalized arm.** It is kept as the evidence that the
  change was needed, not as a live comparison. Do not spend compute on it.
* **`v3_all`, `v3_zsu`, `v4_lin_apex`, `v4_lin_apelin`.** Side experiments,
  each already answered. Zero-sum users was measured and is worse (R-hat 1.180
  / ESS 17 against 1.090 / 37).
* **More chains.** R-hat compares between chains; more of them measure that
  comparison better, they do not improve mixing.
* **Dropping `a²`.** `delta2` never exceeds 1.6 standard errors from zero in
  any standard fit, so the ape curve is effectively linear — but removing it is
  a modelling change, not a conditioning fix, and it belongs to the height-form
  comparison rather than to this work.

## Open decision, unrelated to compute

The page's primary fit is still `v3_conf` (unmarginalized, quadratic × gender).
Switching it to a marginalized fit is a one-flag rebuild: correlation 0.9999
across the 29 gyms, largest shift 0.012 grades, **0 of 29 rank changes**. Only
the height figures change shape. Phase 1 may make the choice for us.
