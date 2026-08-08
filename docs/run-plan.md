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
| Q1 | Does the zero-sum prior fix hold up under an independent sampler? | emcee `lin2` | **answered — and it opened Q1b** |
| Q1b | Which sampler is right about `sigma_gym`? | `v5_conf_marg_long` + 9 more fits | **answered — emcee is the outlier** |
| Q2 | Is the R-hat problem underadapted warm-up? | `v5_conf_marg_long` | Q3's settings |
| Q3 | Does orthogonalising the design fix the height block? | 7 paired fits, raw vs orthogonal | **answered — no. Dropped.** |
| Q4 | Which height form is best, measured properly? | `v7_*` sweep, raw basis, 8 chains | the page's primary |
| Q5 | Can the model predict a climber it has never seen? | grouped k-fold | nothing — it is the honest headline |
| Q6 | What are the Bayes factors between forms? | nested / bridge sampling | nothing |

## Phase 0 — in flight

| run | settings | cost | status |
|---|---|---|---|
| emcee `lin2` | 128 walkers × 20,000 steps, DE moves | 7.1 h | **done** |
| `v5_conf_marg_long` | quad × gender, marginalized, tune 2000 / draws 2000 | 5–6 h | **running** (also answers Q1b) |
| `v6_conf_orth` | quad × gender, marginalized, orthogonal, tune 600 / draws 500 | ~1.5 h | **running** beside Q2 |

**Q1 is answered, and the answer was not clean.** The zero-sum fix held: eleven
of the twelve parameters emcee and `v3_lin_marg` share agree to within **0.24
posterior standard deviations**, which is a strong statement that the PyTensor
graph and the NumPy reference describe the same posterior.

**`sigma_gym` did not.** emcee puts it at **0.390**, PyMC at **0.304** — a gap of
1.86 posterior sd, on the parameter that *is* the page's headline. Three
explanations were eliminated by `scripts/check_sigma_gym.py`:

| candidate | measurement | verdict |
|---|---|---|
| different prior | log-priors profiled across a `sigma_gym` grid differ by a constant to 9.1e-13 | same shape exactly |
| different likelihood | 1.3e-9 relative at matched parameters (`check_pymc_marginal.py`) | same function |
| under-resolved quadrature | 31 → 201 Gauss-Hermite nodes moves the gap by 4.9e-6 | 31 already resolves it |

So it is a **sampling** difference, and the diagnostics point at PyMC: it drew
this parameter with **ESS 109 and R-hat 1.02** — its worst-mixed parameter and
the only one over threshold — while emcee drew it with **ESS 4,045** and a
running mean that moves 0.0003 between the halves of the kept chain. A
hierarchical scale under-explored by a short-adaptation gradient sampler is
biased *downward*, which is the direction seen, and four chains failing the same
way is that failure's expected signature rather than evidence against it.

**Q1b is answered, and it went against emcee.** `v5_conf_marg_long` ran at tune
2,000 / draws 2,000 — 2.6× the effective sample size of the short fit — and
`sigma_gym` came back at **0.303**, exactly where it started. It did not climb.

Nor is it one fit against one fit. **Ten PyMC fits report `sigma_gym`, and all
ten land between 0.298 and 0.309** — across seven height forms, both the raw and
the orthogonalised design basis, and warm-ups differing by 4×. emcee's 0.390 is
outside that band entirely.

So the outlier is emcee, and the mechanism is a documented weakness of ensemble
samplers at ~40 dimensions: walkers propose from each other's positions, so
unlike independently started chains they can contract onto a subspace
*together*, and a flat running mean is not the proof of convergence for an
ensemble that it is for independent chains. emcee declined to certify the run
itself — its rule wants 50×τ and this chain is 31×.

`sigma_gym ≈ 0.30` is therefore **the number, not a lower bound**. The page said
lower bound for one day and has been corrected. The remaining check is a
different *kind* of algorithm rather than a longer chain — see Q6.
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


## 2026-08-06 — what changed after the overnight run

### Q3 answered: orthogonalisation is dropped

Both arms are now fitted for all seven height forms, so the comparison is
paired rather than anecdotal. Effective sample size on the reported quantities,
orthogonal ÷ raw:

| | |
|---|---|
| geometric mean | **1.01×** |
| median | 1.01× |
| range | 0.09× to 5.00× |
| wins for orthogonal | 4 of 7 — P(≥4 of 7 by chance) = **0.50** |

A coin flip. And **both** bases produced a catastrophic failure — linear
collapsed to ESS 9 orthogonalised, linear × gender to ESS 13 on the raw basis —
which places the blow-ups on the 600-iteration warm-up rather than the basis.

Dropped, for three reasons beyond the null result: it is **not
prior-preserving** (independent priors on the orthogonal basis imply correlated
priors on the raw coefficients); it requires the transform to be stored with the
fit and the *training* fold's version applied to held-out climbers, which is a
live cross-validation leak that needs an explicit guard; and it buys nothing.

The code stays — it is tested and this is a clean negative result worth being
able to point at. `build_jobs()` no longer sets the flag, and a test now asserts
it does not come back by accident.

### Diagnostics are noisier than they were being read

Measured, not assumed. Under **perfect** mixing (4 chains × 500 genuinely
independent draws, 2,000 simulated replicates), R-hat reaches 1.0035 at the 99th
percentile and 1.0066 at worst. So the 1.01 threshold is well calibrated and
anything above it is real signal — but on actual chains R-hat swings hard: the
*same* fit's four 500-draw windows gave `sigma_gym` R-hat **1.009 / 1.041 /
1.075 / 1.057** while its window means agreed to within 2.4 × Monte Carlo error.

The answer is stable; the diagnostic is not. Consequences for the plan:

* **More chains is the fix, bought incrementally.** R-hat's between-chain term
  is estimated from the chain count, so 4 is where the noise comes from. But
  chains merge *exactly* — splitting a real 4-chain fit into halves and merging
  them back reproduces R-hat and ESS to four decimal places — so the sweep runs
  at 4 chains, two fits at a time, and only the forms whose diagnostics warrant
  it get topped up with `scripts/merge_chains.py`. Committing all seven to 8
  chains up front would spend the compute whether it was needed or not.
* **Every fit carries an explicit `--seed`** (new on `run_fit.py`, stamped into
  the trace as well as the result JSON). Without one PyMC draws a fresh seed
  per run, which makes fits irreproducible and a top-up indistinguishable from
  a repeat. `merge_chains.py` refuses to merge two runs that shared a seed:
  those are the same chains twice, so between-chain variance is zero and R-hat
  would read 1.000 no matter how badly the sampler mixed.
* **Stop leading with `max_rhat`.** It is a maximum over thousands of
  parameters, most of which appear on no page. The useful summary is R-hat and
  the ESS floor on the *reported* quantities.

### Priors audited, and three were doing real work

`scripts/check_priors.py` (new) varies each parameter alone and requires the
PyMC and NumPy log-priors to differ by a constant. All 18 directions agree to
**4.5e-13**, including the gym block — which had never been compared, and is
exactly where the original zero-sum bug lived, because that bug was invisible to
a likelihood-only check.

Sensibility was the other half, and it found three problems. Prior SD ÷
posterior SD, where large means the data dominates:

| parameter | was | posterior | z | ratio | now |
|---|---|---|---|---|---|
| `vq_peak` | N(0, 1.5) | −2.553 ± 0.528 | **1.70** | 2.8× | **N(0, 3.0)** |
| `sat_amp` | N(0, 1.0) | −1.172 ± 0.393 | 1.17 | 2.5× | **N(0, 2.0)** |
| `sat_h0` | N(0, 1.5) | 1.068 ± 0.555 | 0.71 | 2.7× | **N(0, 3.0)** |
| `sat_scale` | HalfNormal(1.0) | 1.014 ± 0.379 | 0.36 | 1.6× | **HalfNormal(2.0)** |

`vq_peak` is the serious one: it is the *best height*, in SDs from the median,
and the vertex form exists precisely to estimate it directly instead of deriving
−γ₁/2γ₂ and propagating error through a ratio. A prior contributing ~12% of its
posterior precision and dragging the estimate from about −2.9 to −2.55 defeats
the point of the form. Widened priors leave all four at 3–10% of the posterior
precision.

This also confounded Q4 directly: comparing height forms whose priors constrain
them to *different degrees* is not a comparison of the forms.

**Not changed:** `gamma2_x` (N(0, 0.15), z = 1.09, ratio 4×, ~8% shrinkage). It
is the parameter the gender question turns on, and the tight prior there is
deliberate regularisation — but it should be stated on the page rather than
left silent.

**Core priors are fine.** The data dominates by 17–88× on every shared
parameter, and no posterior sits more than 1 prior SD from its prior mean. Prior
predictive is loose but harmless: 17% of prior mass on negative grades, killed
instantly by 20,014 observations.

### The queued sweep

Seven forms, **raw basis**, tune 2,000 / draws 1,000, **4 chains**, seeds
20260806–20260812, two fits at a time (4 waves). ~11 hours. Nothing is gated,
because nothing is left to gate on.

Then read the diagnostics and top up only what needs it:

```bash
scripts/run_fit.py --name v7_v3_lin_b --seed 20260906 ...   # 4 more chains
scripts/merge_chains.py --out v7_v3_lin_merged v7_v3_lin v7_v3_lin_b
```
