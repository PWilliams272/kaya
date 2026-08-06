# Run plan

Every fit costs 60–90 minutes at current settings and 5–6 hours at long ones,
on 10 cores that fit two concurrently. So the plan is **decision-driven**: each
phase exists to settle one question, and nothing downstream of an unsettled
question gets run. Stop when the question is answered, not when the batch is.

Status as of 2026-08-05. Q2, Q3 and Q4 are queued as one unattended chain —
`scripts/run_overnight.py`, armed behind the emcee run, status in
`runs/logs/overnight/STATUS.md`.

## The questions, in dependency order

| # | question | what settles it | blocks |
|---|---|---|---|
| Q1 | Does the zero-sum prior fix hold up under an independent sampler? | emcee `lin2` | nothing — it is a cross-check |
| Q2 | Is the R-hat problem underadapted warm-up? | `v5_conf_marg_long` | Q3's settings |
| Q3 | Does orthogonalising the design fix the height block? | `v6_conf_orth` vs `v3_conf_marg` | Q4 |
| Q4 | Which height form is best, measured properly? | 7-form sweep + noise floor | the page's primary |
| Q5 | Can the model predict a climber it has never seen? | grouped k-fold | nothing — it is the honest headline |
| Q6 | What are the Bayes factors between forms? | nested / bridge sampling | nothing |

## Phase 0 — in flight

| run | settings | cost | status |
|---|---|---|---|
| emcee `lin2` | 128 walkers × 20,000 steps, DE moves | 7 h | running |
| `v5_conf_marg_long` | quad × gender, marginalized, tune 2000 / draws 2000 | 5–6 h | queued behind emcee |
| `v6_conf_orth` | quad × gender, marginalized, orthogonal, tune 600 / draws 500 | ~1.5 h | queued, runs beside Q2 |

**Q1** is answered by whether `sigma_gym` comes back near PyMC's 0.301 rather
than the pre-fix run's 0.193.
**Q2** is answered by whether max R-hat drops below 1.01 from `v3_conf_marg`'s
**1.069**. If it does, the fix is a settings change and Phase 1 gets cheaper.

Q2 runs on **quadratic × gender, not linear**. Linear is the best-conditioned
form in the set (worst pair 0.52, condition number 12) and has the least room
to improve, so a good result there would not have transferred. Quadratic ×
gender is the page's primary *and* the worst conditioned (0.80, condition
number 44), and it is the cell the 2×2 below actually needs. `v3_lin_marg` is
already an adequate PyMC reference for the emcee comparison, so nothing is lost.

### The 2×2 this sets up

Q2 and Q3 are one factorial experiment on the same model, not two unrelated
runs. Three of the four cells are cheap and one is already on disk:

| | short warm-up (600) | long warm-up (2000) |
|---|---|---|
| **raw basis** | `v3_conf_marg` ✓ on disk — R-hat 1.069, ESS 48 | `v5_conf_marg_long` (Q2, queued) |
| **orthogonal basis** | `v6_conf_orth` (Q3, Phase 1) | `v7_conf_orth_long` — only if both help |

The fourth cell is deliberately conditional: run it only if the first three
show both levers doing something, otherwise it answers nothing.

**Do not add orthogonalisation to Q2.** Each cell changes one thing. A run that
moved both would improve R-hat without saying which change did it, and its
baseline is a raw-basis fit.

## Phase 1 — settle the parameterisation (1 fit)

`v6_conf_orth`: quadratic × gender, marginalized, **orthogonalised height and
ape blocks**, at the *baseline* settings (tune 600 / draws 500) so it is
directly comparable to `v3_conf_marg` — the short-warm-up row of the 2×2 above.

Baseline to beat: max R-hat **1.069**, min ESS **48**, with `gamma1_x` 1.056,
`gamma2_x` 1.051, `gamma1` 1.050.

### How it is implemented, and what it is not

`--orthogonal-design` on `run_fit.py`. The sampler moves coefficients `θ` on a
Gram-Schmidt basis; the model reads `β = Tθ` on the raw columns. Because the
substitution happens at the coefficient vector, every line downstream —
including `ability_for`, which rebuilds gender columns individually per
marginalization branch and could not have taken a column-space transform —
is untouched. The raw-basis names stay `pm.Deterministic`s, so no consumer of
a trace has to know this happened, and `max_rhat` stays comparable to every
raw-basis fit on disk. The sampled basis is reported separately under `orth`
in the result JSON.

Verified before queueing:

| check | result |
| --- | --- |
| design-block condition number, quad × gender | 52.4 → **1.0000** |
| worst off-diagonal correlation | 0.899 → **1.7×10⁻¹⁴** |
| PyMC data log-probability, raw vs orthogonal at matched points | identical, **0.0** |
| `T @ θ` recovers the raw coefficients | 2.2×10⁻¹⁶ |
| PyMC ↔ NumPy cross-check, both bases | **1.3×10⁻⁹** (unchanged) |
| all seven height forms build with the flag | yes |

**It is not a pure reparameterisation, and the earlier note here that the
fitted curve must come back unchanged was too strong.** Independent priors on
an orthogonal basis imply a *correlated* prior `T diag(sd²) Tᵀ` on the raw
coefficients. Rescaling each orthogonalised column back to its original norm
was chosen precisely to control what that costs, and it works on the quantity
that matters:

| quantity | raw basis | orthogonal | ratio |
| --- | --- | --- | --- |
| prior SD of the fitted linear predictor (mean over climbers) | 1.614 | 1.612 | **0.999×** |
| the same, height block alone | 0.821 | 0.820 | 0.998× |
| implied prior SD on `gamma1_x` | 0.500 | 1.258 | 2.52× |
| implied prior SD on `gamma2_x` | 0.150 | 0.432 | 2.88× |

So the prior on the *curve* is preserved to 0.1%, while the prior on
individual interaction coefficients loosens ~2.5–2.9×. That widening is the
implicit tightening being removed: independent priors on columns correlated at
−0.899 put most of their mass on near-cancelling combinations, which is a
constraint on the curve nobody wrote down. Two checks, revised accordingly:

* R-hat and ESS on the gamma block should improve. A diagonal mass matrix
  cannot represent a rotation no matter how long it tunes, so unlike warm-up
  this has no ceiling.
* **The fitted height curve should move by less than its own credible band.**
  A visible shift is not automatically a bug now — it could be the loosened
  prior on the collinear direction — but it needs explaining before the
  parameterisation is adopted.

One caveat for reading Q4: on `saturating` and `vertex_quadratic` the height
terms are nonlinear in the parameters, so they never enter the design matrix
and cannot be rotated. Those two forms go 4.4 → 1.0 on a block that was
already well conditioned; expect the flag to do essentially nothing for them.

Cost: ~85 min. **Decision point.** If it works, everything after this uses the
orthogonal parameterisation and the earlier fits become the "before" arm.

## Phase 2 — the height-form sweep (6 fits)

Only if Phase 1 succeeds. Re-run the other six height forms, marginalized,
orthogonalised — `v6_conf_orth` already supplies the seventh:

`zero, linear, quadratic, linear × gender, saturating, vertex quadratic`

At **baseline** settings (tune 600 / draws 500), not Q2's long ones, even
though Q2 may be about to argue for the long ones. Every fit on disk is at
baseline so the comparison stays like-for-like; seven long fits is ~35 hours
rather than a night; and if Q2 says warm-up matters, this sweep says which
forms are worth paying it for. It is a screen, not the final measurement.

Cost: 6 fits, 2 concurrent → **~4.5 h** at baseline settings.

## How the three actually run — `scripts/run_overnight.py`

The three questions are not a straight line, so the chain is a
dependency-aware scheduler over a fixed core budget rather than a script of
`&&`s. Q2 and Q3 are the two independent levers of the 2×2 and start together;
Q4 is downstream of Q3 only. Q3 finishes in ~85 minutes and Q2 runs ~5.5 h, so
serialising them would idle half the machine for four hours.

```
t=0      Q2 v5_conf_marg_long  (raw, long)     ─────────────────────► ~5.5 h
t=0      Q3 v6_conf_orth       (orth, short)   ──► ~1.5 h
t~1.5h   gate on Q3 ──► pass: Q4 sweep, 6 fits, filling slots as they free
                    └─► fail: Phase 3 noise floor, 3 refits, raw basis
```

Ten cores, `--cores 8`, four per fit, so **two fits at a time**. Three was
measured on this machine and it is not a mild penalty: `v4_lin_b_marg`,
`v4_lin_c_marg` and `v4_linxg_marg` took 370 minutes each against ~85 for the
same model run two at a time.

**The gate.** Q4 is most of a night and only makes sense on a parameterisation
shown to help, so it is conditional on Q3 clearing the `v3_conf_marg`
baseline: `max_rhat ≤ 1.069` **or** `min_ess ≥ 48`. An OR, not an AND — both
statistics are noisy at 500 draws, and demanding an improvement in both would
reject a real improvement about as often as it would catch a real regression.

**The fallback is not idle time.** A failing gate runs the Phase 3 noise floor
instead, which is non-optional work worth a night on its own. It also could
not simply have been run alongside: the noise floor has to be measured in
whichever parameterisation the fits it calibrates were run in, and that is
exactly what Q3 decides. If Q3 passes, the noise floor becomes three refits of
`v6_conf_orth` on a later night.

Progress is rewritten to `runs/logs/overnight/STATUS.md` after every state
change, so the morning check is one `cat`. Per-fit logs sit beside it.
`kill` the `run_overnight.py` pid to stop scheduling (fits already running
continue).

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
