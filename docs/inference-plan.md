# Inference plan: marginalization, and four ways to fit the same model

Status as of 2026-08-04. Owner: this repo. Written because the work spans
several hours of compute and needs to survive a context reset.

## The problem being solved

The v2 grading model gives every climber their own latent ability offset:
10,357 of them against 20,014 observations. 59% of climbers contribute exactly
one observation, so their offset can absorb that observation completely.

Three measured symptoms, all the same cause:

| symptom | value |
|---|---|
| effective parameters reported by cross-validation (`p_loo`) | 8,400 on 20,014 rows |
| rows whose leave-one-out estimate is flagged unreliable (Pareto k > 0.7) | 17% |
| score gap between **two fits of the identical model**, different seed only | 31.1 elpd |

That last number is the damning one: it is larger than every difference the
height-form comparison was being used to adjudicate. The comparison was
ranking noise.

The diagnosis was confirmed rather than assumed. Cross-validation already
produces a score per observation, so the totals can be re-added over climbers
with several rows without refitting anything. Doing that collapses the
identical-model gap from **31.1 to 0.45**, and the unreliable-row share from
17% to 4%. The single-observation rows are the cause, not a correlate.

## Stage 1 — marginalize (DONE)

Integrate the offsets out instead of estimating them. They cannot be dropped:
gym corrections are identified by one climber appearing at two gyms, so the
climber term is the entire method.

**Single-observation climbers — exact, no approximation.** The offset enters
the ceiling additively and is Gaussian; the observation noise is Gaussian; two
Gaussians convolve:

```
-m ~ ExGaussian(-c, sqrt(sigma_link^2 + sigma_user^2), nu)
```

Same family, one wider spread parameter. 6,156 parameters vanish.

**Multi-observation climbers — one-dimensional quadrature.** They share one
scalar, so the marginal is a 1-D integral against a Gaussian weight. Plain
Gauss-Hermite spreads nodes over the *prior*, which is the wrong scale — a
climber with k rows has an integrand ~1/sqrt(k) as wide, and at k=21 that
needed 161 nodes and 466 ms per evaluation. Adaptive quadrature (Newton to
each climber's mode, Laplace width) converges at 21–31 nodes in 88–134 ms.

### What exists

| file | what |
|---|---|
| `src/kaya/marginal_v2.py` | the reference implementation, plain NumPy. 40 parameters. Used by emcee, nested sampling and bridge sampling. |
| `src/kaya/grading_model_v2.py` | `marginalize_singles=True` — the exact single-observation half only, in PyTensor, so PyMC can still use gradients. 10,397 → 4,241 parameters. |
| `scripts/check_marginal.py` | closed form vs dense numerical integration (1.2e-13); quadrature convergence; the model's own node placement vs dense integration on the 25 hardest climbers (8.9e-13). |
| `scripts/check_pymc_marginal.py` | PyTensor logp vs NumPy logp at the same point: 1.3e-9 relative. |

### Three traps already paid for

* `model['x']` compiles the **random variable** — evaluating it draws from the
  prior and ignores the point entirely. Use `replace_rvs_by_values`. This
  produced a fake 4,535-unit "disagreement" between implementations.
* `ZeroSumNormal`'s transform is an orthogonal projection, not "append minus
  the sum". Its free coordinates cannot be reconstructed by hand. **This was
  written down here and then implemented wrong anyway**: `marginal_v2.unpack`
  sampled 28 gym corrections and set the 29th to minus their sum, giving that
  one gym a prior variance of 28.02 against everyone else's 1.00 (PyMC's is a
  uniform 0.966). It survived for weeks because `check_pymc_marginal.py`
  compares `model.logp(vars=model.observed_RVs)` — the **likelihood only**, and
  both schemes hand the likelihood a valid zero-sum vector. A likelihood check
  cannot see a prior bug. Fixed with `zero_sum_basis()` (QR of `[1 | I]`,
  columns 1:), guarded by `tests/test_marginal_prior.py`, which checks the
  prior directly and includes a test that the broken scheme *fails* it.
* The ExGaussian log-density is `inf - inf` when `nu <= 0.05*sigma` — three
  terms individually infinite that cancel exactly. NUTS never wandered there;
  nested sampling starts from the whole prior and hit it in 2 of 5 random
  draws. Switch to the Normal limit.

## Stage 2 — refit everything, keep both arms (DONE)

`batch_marg.sh`: seven height forms plus two refits of one of them, on the
marginalized model, with settings otherwise identical to the original batch
(net50/confident, 500 draws, 600 tune, 4 chains, fixed sigma_link). Three at a
time; more thrashes ~10 cores.

**Both arms are kept.** The original is not obsolete — it is the evidence the
change was needed, and a claim that a fix worked is empty without the thing it
fixed. Traces live in `runs/traces/` with a `_marg` suffix for the new arm.

**Do not compare the two arms' scores to each other.** They answer different
questions. The original predicts an observation *given* a climber ability that
was fitted using that same observation; the marginalized version averages over
an unknown ability. The lower number is the honest one, not the worse model.

### What to report

* noise floor (spread across refits) — old vs new
* unreliable-row share — old vs new
* R-hat and effective sample size — old vs new
* the height-form ranking, and whether it survives

## Stage 3 — emcee (READY, NOT RUN)

`scripts/run_emcee.py`. Affine-invariant ensemble sampler, no gradients, on
the NumPy likelihood.

**Purpose is validation, not speed.** Two implementations of a quadrature
nested inside an ExGaussian is exactly the kind of code that computes a
plausible wrong number silently. If emcee (NumPy) and PyMC (PyTensor) land on
the same posterior, the graph is right.

Sizing: 128 walkers × 4,000 steps ≈ 512k likelihood evaluations. At 21
quadrature nodes (~88 ms) across 8 processes that is roughly 1.5–2 hours.
Needs `walkers > 2 × 40 parameters`. Check the integrated autocorrelation time
before believing anything — emcee raises rather than quietly returning a
number it doesn't trust.

## Stage 4 — nested sampling (READY, NOT RUN)

`scripts/run_nested.py`, using dynesty with `sample='rslice'` (at 40
dimensions, ellipsoid proposals reject almost everything).

**Purpose: a route to the model comparison that shares none of the failing
machinery.** Nested sampling computes the evidence `Z` directly, and the ratio
of two models' Z is the Bayes factor. No held-out points, no importance
weights, no Pareto k.

**The catch, which must be reported alongside any result.** Z is
prior-sensitive in a way cross-validation is not. The Bayes factor against a
curved height term depends on the prior width on the curvature coefficient,
which was a reasonable default rather than an elicited belief. Widen it and
the curved model looks worse with nothing about the data having changed
(Jeffreys–Lindley). Hence `--prior-scale`: run the sweep, and **a Bayes factor
that flips across the sweep is not a result.**

## Stage 5 — bridge sampling (READY, NOT RUN)

`scripts/run_bridge.py`. Estimates the same Z from posterior draws already in
hand, in minutes rather than hours, using a completely different estimator.

Weaker than nested sampling — it assumes a fitted Gaussian proposal overlaps
the posterior, safe here for a unimodal 40-parameter posterior and not safe in
general. Its value is as a cross-check: agreement with nested sampling is
evidence for both, disagreement means neither should be quoted.

Splits the draws so the half that fits the proposal is not the half that
enters the estimator — fitting and evaluating on the same draws biases Z
upward invisibly.

## Stage 6 — write it up

Both methodologies, both result sets, and the comparison, in the Grading Model
v2 page. Already in place: the noise-floor section, the arm toggle, the
side-by-side table, and the explicit warning against cross-arm score
comparison. Still to add once the runs land: the sampler comparison
(PyMC / emcee / nested / bridge), and the evidence-versus-cross-validation
discussion.

## Open questions this does not address

* **Grouped k-fold** — hold out entire *climbers*, refit, predict all their
  rows. Answers "can this predict someone it has never seen", which is the
  honest test for a model separating climber ability from gym grading. Costs a
  refit per fold (5 folds × 7 models ≈ 35 fits). Now looks *more* worth doing,
  not less: see the Stage 2 result below.

* **More refits.** Two converged refits per arm is not enough to estimate a
  noise floor, and the floor is the yardstick everything else is measured
  against. Three or four more per arm would cost ~6 hours and would firm up
  the one number the whole comparison rests on.
* **The multi-observation offsets in PyMC.** Stage 1 marginalizes only the
  single-observation half there. Full quadrature in PyTensor would need
  `stop_gradient` on the node placement. Only worth doing if Stage 2 shows
  4,241 parameters still sampling badly.


## Stage 2 result (2026-08-05)

Nine marginalized fits, matching the original nine settings for settings.

**Sampling quality improved.** Minimum effective sample size roughly tripled
(23–53 → 104–128), max R-hat moved toward 1 (1.07–1.14 → 1.03–1.05), and
`p_loo` halved from ~8,400 to ~4,200 — exactly the parameters removed. Same
wall-clock time.

**The answers did not move.** Across five height forms with both versions
fitted, gym corrections correlate 0.9999, the largest single gym shifts 0.013
grades, and no parameter moves by even half its own standard deviation.
`sigma_user` — predicted to be the most-affected parameter — is the least.
The marginalization is a repair to model *comparison*, not a change of finding.

**The noise floor: better where it mattered, unresolved elsewhere.**

| scored on | original | marginalized |
|---|---|---|
| all climbers | 31.1 | 2.9 |
| ≥2 sends | 0.7 | 3.2 |
| ≥3 sends | 0.5 | 3.3 |
| ≥5 sends | 2.2 | 1.9 |

The all-climbers column — the one the fix targeted — improves 10×. The
well-observed columns are low single digits in both arms and cannot be ranked
with two refits each. Restricting the score and marginalizing address the same
defect; doing both is not measurably better than doing either.

**One refit in three failed to converge** (`v4_lin_c_marg`, max R-hat 1.44,
min ESS 8). Excluded from the floor via `RHAT_GATE = 1.2` and named in the
write-up. The marginalized model is not uniformly well-behaved.

**Two changes to how the comparison is read**, both in
`scripts/build_v2_vs_null.py`:

* Score against a **fixed** reference (no height term) rather than gap-from-
  best. The winner is selected on the data it is scored on, so it is biased
  upward, and it changes between columns, which moves the zero point.
* Compute the **error on the difference** by differencing per observation
  before summing. The two models find the same rows hard and that cancels:
  ±145 on a total becomes ±8–19 on a difference. This was missing entirely.

With those in place: height matters at 2–3 standard errors (not decisively);
the functional form is **not** resolved; and `v3_conf`, the model used
elsewhere on the page, is the weakest height form in both arms.
