# Choosing and diagnosing a sampler

A portable reference: what the convergence diagnostics actually measure, which
sampler families exist and when each one applies, and which fix a given
diagnostic signature is pointing at.

Written while fitting the Kaya grading model, so the worked examples are that
model's own numbers — a hierarchical Bayesian model with 10,397 parameters,
one ability offset per climber. They are labelled as examples throughout; the
argument does not depend on them, and this document is meant to travel.

---

## 1. R-hat

Run `m` chains from different random starting points. If they have all
converged on the same distribution, how far apart the chains sit should match
how much each one wanders on its own. R-hat is the ratio of those two things.

With `n` draws per chain and `θ̄ⱼ` the mean of chain `j`:

```
B = n/(m-1) · Σⱼ (θ̄ⱼ - θ̄)²          between-chain variance
W = (1/m) · Σⱼ sⱼ²                    within-chain variance

var⁺ = (n-1)/n · W + B/n
R̂    = √(var⁺ / W)
```

**Lower is better.** Under perfect mixing `B = W` and `R̂ = 1`.

### Read it in the other direction

R-hat divides the interesting part by `n` and then square-roots it, which
compresses a large number into one that looks like it is hovering near 1.
Rearranged:

```
B/W = n(R̂² - 1) + 1
```

`B/W` is approximately the **autocorrelation factor** — how many draws the
sampler needs to produce one draw's worth of genuinely new information. At 500
draws per chain:

| R̂ | B/W | effective draws per chain (of 500) |
|---|---|---|
| 1.000 | 1 | 500 |
| 1.005 | 6 | 83 |
| 1.010 | 11 | 45 |
| 1.020 | 21 | 24 |
| 1.050 | 52 | 10 |

The conventional 1.01 threshold is not "almost perfect". At this run length it
means **91% of your draws are redundant**.

### Three things people get wrong

**1.00 is not a floor.** If chains land closer together than their own
wandering predicts, `B < W` and R-hat comes out below 1. *Example: across every
trace in the Kaya project, 16 of 312 scalar parameters have classic R-hat under
1.0, lowest 0.99918.* A value a hair under 1 is ordinary noise.

**R-hat is relative to run length.** Since `R̂ ≈ 1 + (τ-1)/2n`, doubling the
draws roughly halves `R̂ - 1` with no improvement in mixing whatsoever. R-hat
alone can be bought with runtime, which is why it must always be quoted
alongside effective sample size.

**Most libraries report a stricter variant.** ArviZ and Stan report
**rank-normalized split R-hat**: each chain is cut in half first (so four
chains of 500 become eight half-chains of 250), catching a chain that drifts
*within itself*; and draws are replaced by normal scores of their ranks, so it
behaves on heavy-tailed posteriors. The gap can be large. *Example: `sigma_gym`
reads 0.9997 classic — the four chain means agree essentially perfectly — and
1.0223 split, because each individual chain's first half differs from its
second. The classic statistic is structurally blind to this.*

The 1.01 line is a convention calibrated by simulation (Vehtari et al. 2021),
not a theorem. Older references say 1.1, which is now considered far too loose.

## 2. Effective sample size

Consecutive MCMC draws are correlated, because each one starts where the last
ended. ESS is how many **independent** draws they are worth.

```
ESS ≈ (total draws) / (autocorrelation factor)
```

which is the same quantity as `B/W`, read the other way round.

The reason it matters more than the raw draw count: **the Monte Carlo error on
anything you compute scales with √ESS, not with √(number of draws).** 2,000
draws with ESS 100 give you exactly the error bars of 100 perfect independent
draws. **Higher is better; ≥ 400 is the usual working target.**

## 3. Which sampler

The model class picks the family. The diagnostics then pick the fix — that
second part is iterative, and that is the method, not a failure of it.

### The family

| question | answer → tool |
|---|---|
| **What do you want?** | posterior summaries → MCMC. Evidence / Bayes factors → nested or bridge sampling. Cheap approximate predictions → variational inference |
| **Do you have gradients?** | yes → HMC/NUTS. No (discrete parameters, black-box or non-differentiable likelihood) → ensemble samplers or nested sampling |
| **How many parameters?** | <20: anything works. 20–50: ensemble/nested viable. >100: NUTS effectively required. >1,000: NUTS, and the geometry needs to be decent |

**NUTS** (No-U-Turn Sampler) is the default whenever the model is
differentiable. It simulates a trajectory across the probability surface using
the gradient, which is what lets it scale to thousands of dimensions.

**Ensemble samplers** (emcee) need no gradients. They move a population of
walkers by proposing points on the line between pairs of them.

**Nested sampling** (dynesty, MultiNest, PolyChord) is built to compute the
**evidence** `Z = ∫ L(θ)π(θ)dθ` — the quantity Bayes factors need. Posterior
samples fall out as a by-product. It starts with live points drawn from the
prior and repeatedly replaces the worst by a fresh draw from the prior
*constrained to beat that likelihood*, so the threshold ratchets up and the
enclosed volume shrinks geometrically.

### The trade-offs that actually decide it

**Affine invariance.** A sampler is affine invariant if its performance is
unchanged by any stretch, rotation, shear or shift of the parameters. Ensemble
samplers get this structurally: their proposal is built from *differences
between walkers*, and the walkers are distributed like the target, so the
proposal inherits the target's shape for free — no tuning, no pilot run.

NUTS is **not** affine invariant. It approximates the same thing with a **mass
matrix** learned during warm-up. A diagonal mass matrix handles axis-aligned
scale differences but not rotations; a dense one handles correlation too but
costs O(d²) to store and O(d³) to factor, which rules it out past a few hundred
parameters.

**Affine invariance only buys immunity to *linear* correlation.** This is the
single most important limitation in this document, and it applies equally to a
dense mass matrix and to a PCA rotation of the parameters. Affine maps send
straight lines to straight lines:

```
straight ridge                    banana
●━━━━━━━━━━━━━━━●                 ●╲            ╱●
                                    ╲╲________╱╱
line through two walkers             ╲  chord  ╱   ← proposal lands
runs ALONG it → accepted              ╲______╱        outside → rejected
```

On a straight ridge, the line joining two walkers lies along the ridge and
every point on it is plausible: long steps, high acceptance. On a curved
target, that line is a **chord** cutting across the inside of the bend, through
empty space. The sampler's only escape is to pick walkers close together, so
short chords hug the curve — which means small steps and slow diffusion. It
does not break; it crawls.

A **funnel** — where the distribution's width varies along its length — is
worse than a banana, because a chord from the wide mouth to the narrow neck
lies almost entirely outside the distribution.

**Dimension.** Ensemble samplers degrade badly with dimension: a point on the
line between two walkers is almost always somewhere improbable in high
dimensions, so acceptance collapses. Rules of thumb are ~10–20 parameters with
the default stretch move, perhaps 50 with differential-evolution moves, and you
need more walkers than parameters (a common floor is 2×). Nested sampling has
its own version of the same wall: drawing from the prior above a likelihood
threshold becomes hopeless when that region is a vanishing sliver, giving a
practical ceiling around 20–50 for ellipsoidal methods and a few hundred for
slice-based ones.

**What nested sampling is genuinely best at** is **multimodality**. Because it
works with likelihood level sets rather than following a trajectory, it can
track several separated peaks at once. NUTS gets stuck in whichever mode it
started near, and R-hat will not tell you the others exist. NS also needs no
initialization and no warm-up tuning.

The corresponding liability: NS explores the *whole prior*, including regions
your MCMC never visits, so it finds numerical fragility that nothing else does.
*Example: this model's ExGaussian log-density evaluates to `inf - inf` when
`ν ≤ 0.05σ` — three individually infinite terms that cancel exactly. NUTS never
went there in any run. Nested sampling hit it in 2 of 5 random draws.*

## 4. Reading the diagnostic signature

This is the part that is not guesswork. Each pattern names its own fix.

| signature | what it means | fix |
|---|---|---|
| **divergences** | sharp curvature; the trajectory simulation is breaking down | reparameterise (non-centre), raise target acceptance |
| **high R-hat, zero divergences, tree depth pinned at the cap** | elongated and correlated, not broken — the sampler stays on the surface but has to inch along it | better metric, longer warm-up, fewer dimensions |
| **chains converge to different answers, R-hat large** | multimodality | nested sampling, parallel tempering |
| **everything clean but slow** | nothing is wrong | more compute, or marginalize |

Note the second row: **raising target acceptance is the most commonly given
advice and it is the wrong medicine there.** Target acceptance treats
divergences. If there are none, it buys nothing.

## 5. The geometry fixes

### Non-centring

Write an offset as `σ × z` with `z ~ Normal(0,1)`, rather than drawing it from
a distribution whose width is itself being fitted. This breaks the dependence
between an offset and its own scale — the funnel shape samplers handle worst.
Cheap, standard, and should generally be done before anything else is tried.

### Marginalization

Integrate a parameter out analytically instead of sampling it. Every parameter
removed is a dimension the sampler never has to explore, and if the removed
parameter was the one causing a funnel, the funnel goes with it.

This is exact, not an approximation: the integral *is* the likelihood written
without that parameter in it. Sampling and then ignoring converges to the same
answer given infinitely many draws; doing the integral removes the "given
infinitely many draws".

*Example: integrating out the offsets of single-observation climbers took the
Kaya model from 10,397 parameters to 4,241 and improved minimum ESS in 7 of 8
height forms — up to 15× on the worst-mixing one, from ESS 10 to 153.*

### Orthogonal polynomials

**The problem.** The posterior correlation between two coefficients is driven
by the correlation between their **design columns**. For a linear model the
posterior covariance of the coefficients is proportional to `(XᵀX)⁻¹` — so
correlated columns produce correlated coefficients, and the sampler is handed a
ridge it did not need to be given.

Note the sign: inverting a 2×2 correlation matrix with off-diagonal `ρ` gives
off-diagonal `-ρ/(1-ρ²)`. **Strongly negatively correlated columns produce
strongly positively correlated coefficients.**

#### What an interaction term is

Without an interaction, a covariate gets one coefficient and therefore one
effect for everybody. `h` alone says "every extra inch of height is worth γ₁,
the same for every climber."

An **interaction** lets that effect depend on something else. Adding a column
`g·h` alongside `h` makes the model:

```
effect of height  =  γ₁ + γ₁ˣ·g
```

so at `g = 0` the slope is `γ₁`, and at `g = 1` it is `γ₁ + γ₁ˣ`. **The
interaction coefficient is the *difference* between the two slopes, not the
second slope itself** — a point that catches people reading the output table.
`γ₁ˣ = 0` means "height works the same way regardless of `g`".

The column is literally the elementwise product of the two variables, which is
where the conditioning problems come from.

*In the worked example `g` is `w_female`: the **probability** that a climber is
female, inferred from their first name and sharpened by their height. It is
continuous on [0, 1], not a binary flag — 68.6% of climbers sit below 0.05 and
28.3% above 0.95, with only 324 of 10,357 (3.1%) genuinely uncertain in
between. Using a probability rather than a hard label means a climber whose
name is ambiguous contributes partially to both groups instead of being guessed
into one.*

#### Where the coefficients come from

Orthogonalising is Gram-Schmidt, and Gram-Schmidt has a one-line description
that makes it obvious:

> **Replace each column by its residual after regressing it on the columns
> before it.**

The coefficient subtracted is exactly the projection

```
c = ⟨a, b⟩ / ⟨b, b⟩
```

which, when both columns are mean-centred, is `cov(a,b)/var(b)` — the ordinary
least-squares slope of `a` on `b`. Nothing more exotic than that.

*Worked: for `p₂ = h² − c·h`,*

```
c = ⟨h², h⟩ / ⟨h, h⟩ = −5,770.4 / 8,707.8 = −0.663
```

*so `p₂ = h² + 0.663·h`. The raw `h²` column leans in the `h` direction because
height is left-skewed (skew −0.547); regressing it on `h` and keeping the
residual removes precisely that lean.*

#### The worked example

*A height model with four gamma coefficients on the columns `h`, `h²`, `g·h`,
`g·h²`. Design-column correlations:*

| | h | h² | g·h | g·h² |
|---|---|---|---|---|
| **h** | 1.000 | −0.363 | +0.718 | −0.654 |
| **h²** | | 1.000 | −0.497 | +0.637 |
| **g·h** | | | 1.000 | **−0.899** |
| **g·h²** | | | | 1.000 |

*Resulting posterior correlations:*

| | γ₁ | γ₂ | γ₁ˣ | γ₂ˣ |
|---|---|---|---|---|
| **γ₁** | 1.000 | −0.027 | −0.199 | +0.098 |
| **γ₂** | | 1.000 | −0.011 | −0.354 |
| **γ₁ˣ** | | | 1.000 | **+0.799** |
| **γ₂ˣ** | | | | 1.000 |

*The −0.899 between the interaction columns comes back as +0.799 between their
coefficients — the sign flip the inverse predicts.*

**Two distinct causes, worth separating:**

*Skew.* Centring `h` does **not** make `h` and `h²` orthogonal. After centring,
`cov(h, h²) = E[h³]`, which is zero only if the distribution is symmetric.
*In the example, standardised height has skew −0.547 and `corr(h, h²) = −0.363`
even though both columns are mean-centred.*

*Subgroup restriction.* This is the bigger effect. The interaction columns
`g·h` and `g·h²` are near zero for every male climber, so they are effectively
the polynomial evaluated on the female subsample alone — `Σg² = 2,987` of
10,357 climbers. That subsample has a **narrower and off-centre** height range:
−2.30 to +0.51 standard deviations, against −2.04 to +1.79 for everyone.

**Over a narrow interval that does not straddle zero, `h` and `h²` are nearly
linearly dependent.** On [−2, +2], `h²` is a clear parabola carrying
information `h` cannot. On [−2.3, +0.5] it is close to a straight line in `h`.
That is why the interaction pair reaches −0.899 while the main-effect pair is
only −0.363, and it is the general reason **interaction terms are usually the
worst-conditioned part of a polynomial model**.

#### The fix

Replace the raw basis with an orthogonal one spanning the same space:
Gram-Schmidt each column against the ones before it — equivalently
QR-decompose the design block and keep `Q`, or use R's `poly(h, 2)`, or a
Legendre basis evaluated at the data.

The resulting basis functions are the raw monomials with the earlier ones
subtracted off. The coefficients are properties of *this* sample, not
universal constants:

```
p₂  = h²   + 0.663·h                              orthogonal quadratic
q₁  = g·h  − 0.436·h  + 0.079·h²                  orthogonal linear interaction
q₂  = g·h² + 0.768·h  − 0.257·h² + 1.513·q₁       orthogonal quadratic interaction
```

*`q₂` needs four terms because `g·h²` overlaps with `h`, with `h²`, and with
`q₁`, so all three come off.*

*Result: every pairwise correlation in the block drops to ~10⁻¹⁵, and the
block's condition number goes from **36.0 to 1.00** — a perfectly round
posterior in those coordinates.*

**What it costs statistically: nothing.** The two bases span the same function
space, so the fitted curve, the predictions and the model comparison are
identical. Only the coordinates change, and with them the conditioning the
sampler sees.

**What it costs in interpretation:** `γ₁` stops meaning "grades per SD of
height" and becomes the coefficient on an abstract basis function. Transform
back for reporting. If the write-up already reports fitted *curves* rather than
raw coefficients, this cost is close to zero.

**One trap.** Orthogonalising `h²` against `h` on the full sample does **not**
orthogonalise `g·h²` against `g·h`. The interaction block lives under a
weighting by `g`, so orthogonality under the unweighted inner product says
nothing about orthogonality under the weighted one — in the example, reusing
the main-effect basis only takes the interaction pair from −0.899 to −0.289,
which looks like progress and is not enough. Gram-Schmidt the whole design
block in one pass and the problem does not arise.

### Dense mass matrix / PCA rotation

Run a pilot, estimate the posterior covariance, rotate into its eigenbasis. A
diagonal metric in the rotated basis is a dense metric in the original. Stan
supports this directly.

The blocker is cost: `d²` storage and `d³` factorisation. At thousands of
parameters it is out of reach — but a **block** approach usually is not. Dense
on the handful of population-level scalars, diagonal on the many offsets, gets
most of the benefit for almost nothing. Support varies by library (NumPyro is
more flexible than PyMC here).

Whether it is worth doing is a property of the specific model, and should be
measured rather than assumed: compute the posterior correlation matrix of the
scalar block and look at its condition number. Near 1 means the posterior is
round and there is nothing to rotate.

*Example: this varies sharply between specifications of the same model. The
linear height form's worst pair correlates at 0.52 (condition number 12) —
nothing to gain. The saturating form reaches 0.82 (condition number 201).*

**As with affine invariance: a rotation straightens lines. It does nothing to a
funnel.**

## 6. The order to try things in

1. **Non-centre** every hierarchical offset. Nearly free, and it is the fix for
   the most common pathology.
2. **Look at the diagnostic signature** before spending compute. Divergences
   and depth-capping call for opposite remedies.
3. **Orthogonalise correlated design columns**, interactions included. Free,
   exact, changes no result.
4. **Lengthen warm-up** before lengthening the run. Warm-up is where the step
   size and mass matrix are estimated; too short and every later proposal is
   badly scaled, which is autocorrelation by another name.
5. **Marginalize** whatever can be integrated out in closed form.
6. **Only then** reach for more draws, a different sampler, or more hardware.

Things that will not help, and are worth ruling out explicitly: more chains
(R-hat compares *between* chains — more of them measure that comparison more
reliably, and can *raise* R-hat by catching a bad chain that fewer would have
hidden); raising target acceptance when there are no divergences; and any
rotation-based fix when the problem is curvature.
