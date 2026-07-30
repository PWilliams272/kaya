# Bayesian Grading Model — Design

Written for both human and agent readers picking this up later. Covers the
full model design worked out in conversation, before any implementation
existed. If code and this doc ever disagree, the code's docstrings/comments
are more likely to be current — update this doc when the design changes
rather than letting it drift.

## Motivation

Two things already in the viewer are informal, empirical approximations of
what this model estimates properly:

- **Gym Comparison** shows raw `comp − ref` grade-delta histograms per user,
  per gym pair — no correction for how many times a user visited each gym,
  how completely they log, or any structure shared across gyms/users.
- **Height and Wingspan** fits a GAM of max grade sent vs. height/ape index
  — descriptive only, doesn't account for gym grading bias or per-user
  logging behavior confounding the result.

This model replaces both with one coherent, jointly-fit estimate, and adds
capabilities neither tab has: per-climb difficulty diagnostics, and
(optionally, later) climb-specific morphology sensitivity.

## Framework

This is an **Item Response Theory (IRT) / Rasch-style model** — the standard
psychometrics framework for jointly estimating item difficulty and
respondent ability from pass/fail outcomes (originally: which test questions
are hard, and how skilled is each test-taker), without knowing either one in
advance. Climb → item, climber → respondent, sent → correct answer. The core
functional form,

```
P(send) = logistic(α · (ability − difficulty))
```

is the same one already sketched in `notebooks/Writeup.ipynb`.

## Scope: full separation by discipline

Bouldering and routes are fit as **two completely independent models** — no
shared parameters, no partial pooling between them, anywhere: not the
ability regression, not gym/climb corrections, not the reliability signal.
Someone's logging behavior or ability in one discipline says nothing about
the other in this design.

## Notation (per discipline — everything below is implicitly duplicated)

- `u = 1…U` — users with ≥1 send in this discipline
- `g = 1…G` — gyms
- `c = 1…C` — climbs (each belongs to exactly one gym)
- For each (user, gym) pair with a logged send: `c*(u,g)` = the specific
  climb behind that user's *hardest* send at that gym; `m(u,g)` = that
  climb's labeled grade (numeric, via the existing `grade_to_num`
  conversions)
- `n(u,g)` = distinct visit-days logged by u at g (already computed by the
  existing gym-comparison pipeline)
- `r(u)` = a per-discipline logging-reliability signal, reusing/extending
  the existing Active/Inactive segmentation criteria

## 1. Ability sub-model

```
A(u) = β₀ + β_gender·gender(u) + f_height(height(u)) + f_ape(ape(u)) + ε(u)

f_height(h) = γ₁·(h − h̄) + γ₂·(h − h̄)²
f_ape(a)    = δ₁·(a − ā) + δ₂·(a − ā)²

ε(u) ~ Normal(0, σ_user)
```

`h̄`/`ā` are population median height/ape-index (centering keeps `γ₁`/`δ₁`
interpretable as the effect at a typical body size). Quadratic, not a free
smoothing spline — deliberately: height/ape effects are expected to be
simple (linear, or one turn — an "elbow" or gentle reversal), not
multi-modal. A quadratic can represent that while being structurally
incapable of the kind of spurious wiggling a flexible spline could fit to a
single outlier (the exact failure mode hit earlier with the pygam fit).
`γ₂`/`δ₂` get a *tighter* prior than `γ₁`/`δ₁` (see §5) — curvature has to
be earned by the data, not assumed.

`ε(u)` — the per-user residual after covariates — is the "individual
inherent ability, for well-sampled users" output. Free byproduct of this
regression, not separate machinery.

## 2. Gym/climb correction sub-model (hierarchical, average-anchored)

```
gym_correction(g) ~ Normal(0, σ_gym)
climb_correction(c) ~ Normal(gym_correction[gym(c)], σ_climb)
```

Zero-mean prior on `gym_correction` is the identifiability anchor — see §6.
`climb_correction` partial-pools toward its own gym's baseline: a climb with
few sends leans on the gym average, one with lots of sends can stand apart.
This is the "is a specific climb hard or soft" output.

## 3. Observation model / likelihood

The central data problem: sends are never paired with fails, so a per-send
Bernoulli likelihood isn't identifiable without knowing what was attempted.
Instead, each user's *hardest send at each gym* is modeled as a one-sided,
shrinking-bias estimate of their true ceiling there:

```
ceiling(u,g) = A(u) + climb_correction[c*(u,g)]

m(u,g) = ceiling(u,g) − gap(u,g)
gap(u,g) ~ Exponential(rate = λ(u,g))
λ(u,g) = λ₀ · (1 + κ·n(u,g)) · (1 + ρ·r(u))
```

`gap` must be non-negative — the max of a set of attempts can only fall
short of (or match) the true ceiling, essentially never exceed it.
Exponential is the simplest strictly-non-negative choice with a single rate.
More visits (`n`) or higher reliability (`r`) raises the rate, shrinking the
*expected* gap toward zero — this is the model's version of "someone with
50 visits has probably found their ceiling; someone with 2 visits probably
hasn't," and it's a real bias correction, not just a precision weight.

Routing through `climb_correction[c*(u,g)]` (the specific climb behind the
max, not just the gym average) is what lets one user's single data point
inform gym-, climb-, and ability-level estimates simultaneously without
needing per-send data — just each user's existing per-gym max.

## 4. Deferred extension: climb-specific morphology sensitivity

Not part of v1. If/when revisited, it's a random-slope extension of §2/§3,
structurally consistent with everything above:

```
ceiling(u,g) = A(u) + climb_correction[c*(u,g)]
             + climb_height_slope[c*(u,g)]·(height(u) − h̄)
             + climb_ape_slope[c*(u,g)]·(ape(u) − ā)

climb_height_slope(c) ~ Normal(0, σ_climb_height)
climb_ape_slope(c) ~ Normal(0, σ_climb_ape)
```

Zero-mean again: the default assumption for any climb is "no unusual
morphology sensitivity beyond the population-wide effect already in
`f_height`/`f_ape`." Reason it's deferred: estimating a climb-specific slope
needs sends from that *same* climb across a real spread of heights, which
most individual climbs won't have — it'll only be genuinely informative for
a handful of high-traffic, long-standing climbs, and gets correctly shrunk
to ~0 for everything else. `ape_slope` matters alongside `height_slope`
specifically because "reachy" is arguably more about ape index (a reach
proxy) than raw height.

## 5. Priors

| Parameter | Prior | Rationale |
|---|---|---|
| `β₀` | `Normal(0, wide)` | weakly informative baseline |
| `β_gender` | `Normal(0, moderate)` | no directional assumption |
| `γ₁`, `δ₁` | `Normal(0, moderate)` | standard weakly-informative |
| `γ₂`, `δ₂` | `Normal(0, tighter)` | curvature must be earned by data |
| `σ_user`, `σ_gym`, `σ_climb` | `HalfNormal` | positive-only; `σ_gym` scale calibratable against the spread already visible in today's empirical gym-comparison histograms |
| `λ₀`, `κ`, `ρ` | `HalfNormal` or `Gamma` | calibrated so 1 visit → expected gap of roughly a grade, shrinking meaningfully by ~10 visits |
| `α` (logistic steepness) | fixed at 2 (matches the notebook), not estimated | avoids confounding with the scale of everything else in v1; revisit later |

Exact numeric scales need prior-predictive checking against real data before
finalizing — the table above gives the *shape*, not final numbers.

## 6. Identifiability & anchoring

IRT-style models are identified only up to an additive shift (add k to every
ability, subtract k from every correction — likelihood unchanged). The
zero-mean prior on `gym_correction` (§2) anchors to "the average gym" rather
than pinning one specific gym — more robust, since pinning to one gym would
make every other gym's estimate inherit that one gym's data quality/quirks.

This doesn't conflict with the app's "Reference gym" picker: once fit,
showing results relative to any specific gym (e.g. Touchstone Cliffs of Id)
is just subtracting that gym's `gym_correction` from every other gym's value
at display time — a re-centering, not a refit.

## 7. Reliability signal `r(u)`

Reuses/extends the existing Active/Inactive segmentation criteria
(`n_sends`, `n_sesh`, `n_sends_per_sesh`), computed **per discipline**
(matches §"Scope" — someone active in bouldering may not be active in
routes). Encodes "how completely does this user log their climbing" as a
second, independent shrinkage factor alongside raw visit count `n(u,g)`.

## 8. Methodology

- **Data prep**: extend the existing `build_gym_comparison_base()`-style
  pipeline (already computes per-user, per-gym max grade + visit days) to
  also carry the climb_id behind each max, plus height/ape/gender/reliability
  per user.
- **Fitting**: PyMC, NUTS sampler. New dependency — added to
  `pyproject.toml`/`requirements.txt` as part of this implementation.
- **Functional-form check** (linear vs. quadratic vs. hinge for
  `f_height`/`f_ape`): fit the quadratic version first; check whether the
  posterior credible interval on `γ₂`/`δ₂` excludes zero. Formal comparison
  via `arviz`'s LOO (preferred over WAIC — more robust, better failure
  diagnostics) if the quick check is ambiguous or a hinge form is proposed
  later.
- **Diagnostics**: R-hat, effective sample size, divergence count on every
  fit before trusting point estimates.
- **Posterior predictive check**: simulate `m(u,g)` from the fitted model,
  compare its distribution to the real data — sanity-checks that the
  gap/reliability mechanism captures real shape, not just matching averages.

## 9. Mapping back to the stated goals

| Goal | Source |
|---|---|
| How gyms grade (hard/soft) | `gym_correction(g)` |
| Gender/height/ape → ability | `β_gender`, `f_height`, `f_ape` — already gym-bias- and logging-behavior-adjusted |
| (optional) individual inherent ability | `ε(u)` |
| (optional) is a climb hard/soft | `climb_correction(c)` |
| (optional) climb × morphology interaction | §4, deferred |

## Status

Design complete, agreed in conversation. Implementation starting now — see
git history / commit messages for what's actually been built vs. this plan.
