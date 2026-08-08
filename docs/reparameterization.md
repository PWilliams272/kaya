# Reparameterizing the climber offsets

*Written 2026-08-06, after the v7 sweep failed to converge on all seven height forms.*

This is a teaching document. It explains the problem the model hit, the four ways out of it that
were considered, why each one works or doesn't, and — in more detail — the one being implemented
and what the PyTensor code actually does.

---

## 1. The problem, in one picture

The model gives every climber a personal ability offset, `epsilon_u`. Climbers vary, and
`sigma_user` is how much. Written the way the code had it:

```python
sigma_user = pm.HalfNormal('sigma_user', sigma=2)
eps_raw    = pm.Normal('epsilon_raw', 0, 1, dims='user_multi')   # 4,201 of these
eps_multi  = sigma_user * eps_raw
```

That is the **non-centered parameterization**. It does not sample a climber's offset directly; it
samples a standardized version, `z_u`, and multiplies by the scale afterwards. `epsilon = sigma * z`.

Here is what the v7 sweep measured, across all seven fits:

| Quantity | Measured | What it should look like |
| --- | --- | --- |
| Divergences | **0** of 4,000 | 0 — good |
| Iterations hitting max tree depth | **100%** | a few percent at most |
| Step size | **0.003** | 0.05–0.5 for a model this size |
| `corr(log sigma_user, spread of epsilon_raw)` | **−0.847** | ≈ 0 |
| Worst R̂ | 1.015 – 1.040 | ≤ 1.01 |

The last row of that table is the diagnosis and the first three are its consequences.

**Why the −0.847 happens.** `epsilon_u` is what the data actually constrains — it's the thing that
enters the likelihood. If the data pins `epsilon_u` tightly, then the coordinate we're *sampling*,
`z_u = epsilon_u / sigma`, is forced to shrink whenever `sigma` grows, purely to keep the product
where the data wants it. The sampler is therefore exploring a curved, stretched surface: a ridge in
`(log sigma, z)` space that gets narrower as you move along it. That shape is a **funnel**.

NUTS reacts to a funnel by shrinking its step size until it can navigate the narrow end — hence
0.003. But NUTS also has a fixed budget of steps per iteration (2¹⁰ = 1,023 by default). With tiny
steps and a fixed budget, it can't travel far enough to cross the posterior, so it gets cut off
mid-trajectory on **every single iteration**. Consecutive draws end up highly correlated, effective
sample size collapses, and R̂ refuses to come down.

Note what is *not* wrong: there are no divergences, so the sampler isn't hitting anything it can't
integrate through. The geometry is bad, not pathological. That distinction matters, because it rules
out the fixes aimed at pathology.

---

## 2. The rule that decides the parameterization

The single most useful fact here: **non-centered is not "the good one."** Which parameterization
samples better depends on how much data each group carries (Betancourt & Girolami, 2015).

Think about what each one asks the sampler to do.

- **Centered** (`epsilon ~ Normal(0, sigma)`) samples the climber's offset directly. The likelihood
  speaks about `epsilon` directly, so if the data is informative the posterior for each `epsilon_u`
  is tight and well-behaved. But the prior links `epsilon` to `sigma`, so when the data is *weak*
  the offsets are pulled around by `sigma` and you get a funnel — the classic Neal's-funnel picture.
- **Non-centered** (`epsilon = sigma * z`) samples `z`, whose prior is a fixed `Normal(0,1)`
  regardless of `sigma`. When the data is weak this is ideal: the posterior for `z` is just its
  prior, a clean sphere, and `sigma` moves freely. But when the data is *strong* it creates exactly
  the coupling described above.

So the two failure modes are mirror images, and the deciding quantity is the **ratio of likelihood
information to prior information** for a typical group:

```
information from the data   ≈  n_u / sigma_link²      (n_u = that climber's observations)
information from the prior  ≈  1 / sigma_user²
```

Measured on `net50/confident`:

```
multi-row climbers        : 4,201    (single-row climbers are already integrated out)
observations per climber  : median 3, mean 3.3, p90 6
likelihood:prior ratio    : p10 21.3x    p50 31.9x    p90 63.8x
climbers where data dominates: 100.0%
```

Every climber is 21–64× more informed by data than by prior. **This is the centered regime, without
exception**, and the model was written non-centered. The −0.847 correlation is not a mystery; it is
what that mismatch looks like.

---

## 3. The four options

### Option 1 — Switch to centered

```python
eps_multi = pm.Normal('epsilon', 0, sigma_user, dims='user_multi')
```

**How it works.** Sample the offset itself instead of a standardized proxy. The likelihood pins each
`epsilon_u` directly, and `sigma_user` is then informed by the spread of 4,201 well-determined
values rather than being entangled with each one.

**Why you'd pick it.** It's two lines and it targets exactly what was measured. The usual objection —
centered funnels as `sigma → 0` — is measurably absent: `sigma_user` is **1.627 ± 0.015**, tightly
determined and nowhere near zero. There is no narrow end for the funnel to have.

**Why you might not.** It's a coordinate change, not a structural one. The 4,201 offsets are still
sampled, so the model is still 4,241-dimensional, traces are still ~1 GB, and every downstream job
(the 35-fit Phase 4 cross-validation) stays expensive. It fixes the geometry without reducing the
size.

**Cost:** one flag, one test fit.

### Option 2 — Integrate the offsets out entirely

Don't sample `epsilon` at all. Replace it with its integral:

```
p(observations of climber u) = ∫ ∏ᵢ ExGauss(mᵢ | epsilon) · φ(epsilon; 0, sigma_user) d epsilon
```

**How it works.** Each climber's observations share one scalar offset. That means the offset can be
integrated away analytically-or-numerically *before* the sampler ever sees it, leaving a likelihood
that depends only on the global parameters. Detail in §4.

**Why you'd pick it.** It removes the cause rather than re-coordinatising around it. No `epsilon`
means no `sigma_user`/`epsilon` coupling, no funnel, and no possibility of the same problem
returning in a different guise. And the size collapse is dramatic: **4,241 parameters → 40**. Traces
drop from ~1 GB to ~30 MB. It is also not speculative — the NumPy version already exists and is what
emcee and the nested samplers use, so there is a reference to check against to machine precision.

**Why you might not.** Numerical integration is not free, and it runs inside every gradient
evaluation. Measured on this model:

| Model | Parameters | Gradient |
| --- | ---: | ---: |
| sampled offsets | 4,241 | **1.9 ms** |
| quadrature, 15 nodes | 40 | **32.1 ms** |
| quadrature, 31 nodes | 40 | **53.4 ms** |

**28× slower per gradient.** Whether that's a win depends entirely on whether the better geometry
lets NUTS finish its trajectories early. The arithmetic:

- sampled offsets: 1,023 steps × 1.9 ms ≈ **1.9 s per iteration** (measured)
- quadrature, if it U-turns at depth 5 (31 steps): 31 × 40 ms ≈ **1.2 s** — a modest win
- quadrature, if it U-turns at depth 3 (7 steps): 7 × 40 ms ≈ **0.3 s** — a large win
- quadrature, if it *still* saturates at depth 10: 1,023 × 40 ms ≈ **41 s** — catastrophic

So this option is a bet on geometry, and it must be probed before being committed to.

**Cost:** a differentiable quadrature in PyTensor, plus the probe.

### Option 3 — Partial (hybrid) centering

```
epsilon_u = sigma^{w_u} · z_u,     w_u ∈ [0, 1]
```

**How it works.** `w = 1` is centered, `w = 0` is non-centered, and intermediate values interpolate.
Choose `w_u` per climber from how much data that climber has, so data-rich climbers get centered
treatment and data-poor ones get non-centered.

**Why you'd pick it.** It's the principled answer when a dataset genuinely spans both regimes —
some groups with 200 observations, some with 2.

**Why not here.** Your *minimum* likelihood:prior ratio is 21×. There are no data-poor climbers to
protect; the single-observation ones are already integrated out. This adds machinery to express a
distinction the data doesn't contain.

**Verdict: skip.**

### Option 4 — Whitening / a dense mass matrix

**How it works.** NUTS carries a "mass matrix" — really a metric — that rescales the parameter space
before simulating. By default it's **diagonal**: one scale per parameter. A dense matrix additionally
captures linear correlations, so a tilted elliptical posterior gets rotated upright.

**Why you'd pick it.** When the posterior is a long thin *tilted* ellipse, this is the correct fix
and it's often dramatic.

**Why not here.** I measured it, expecting to confirm it, and it failed:

```
condition number of the global correlation matrix : 6
max |r| among global parameters                   : 0.564
posterior SD spread                               : 75x
```

A condition number of 6 is nearly spherical. There is no tilted ellipse to rotate. And critically,
**a funnel is curved, and a dense matrix is a linear transformation** — it cannot straighten a shape
whose width changes as you move along it. This was my initial hypothesis and the measurement killed
it.

**Verdict: skip.**

### Summary

| Option | Fixes the funnel? | Reduces size? | Effort | Verdict |
| --- | --- | --- | --- | --- |
| 1 · Centered | Yes, if the regime call is right | No | Trivial | **Test it** |
| 2 · Integrate out | Yes, by removing the cause | 4,241 → 40 | High | **Implement** |
| 3 · Partial centering | Yes | No | Medium | Skip — no weak groups |
| 4 · Dense mass matrix | No — funnels are curved | No | Medium | Skip — measured spherical |

Options 1 and 2 are being run together, because option 1 is nearly free and is the control: if
centered fixes it, that confirms the diagnosis independently of whether the quadrature pays off.

---

## 4. Option 2 in detail: what the PyTensor code does

### The integral

Every observation of climber *u* shares one scalar `epsilon_u`. So their joint likelihood, with the
offset integrated out, is a **one-dimensional integral**:

```
p(m_u) = ∫ ∏ᵢ ExGauss(mᵢ | c_i + epsilon) · φ(epsilon; 0, sigma_user) d epsilon
```

One dimension is the whole point. A 4,201-dimensional integral would be hopeless; 4,201 *separate*
one-dimensional integrals are routine, and they're independent of each other so they vectorize.

For climbers with a **single** observation this integral has a closed form and needs no quadrature
at all. A Normal offset plus Normal noise convolve into one wider Normal, and the density stays in
the same family:

```
-m ~ ExGaussian(-c, sqrt(sigma_link² + sigma_user²), nu)
```

That's the `marginalize_singles` path that already existed — 6,156 parameters removed for free. This
work is about the other 4,201.

### Why naive quadrature fails

**Gauss-Hermite quadrature** approximates `∫ f(x)·e^{-x²/2} dx` as a weighted sum at fixed nodes,
and those nodes are spread out to match the **prior**, width `sigma_user ≈ 1.6`.

But the integrand isn't prior-shaped. A climber with *k* observations has a posterior for their
offset roughly `1/√k` as wide as the prior — the data has already narrowed it. So the nodes sit
spread across a region where the integrand is essentially zero, and only one or two land near the
peak. Measured in the reference implementation: **~161 nodes to converge, 466 ms per evaluation.**
Far too slow to sample with.

### Adaptive Gauss-Hermite: move the nodes to the mass

The fix is to find each climber's peak first and place the nodes around *it*, at the width that
climber's own curvature implies. This is standard — it's what `lme4` does for `nAGQ > 1` — and it
converges at ~15–21 nodes instead of 161.

Finding the peak is Newton's method on

```
h(epsilon) = Σᵢ log ExGauss(mᵢ | epsilon) − epsilon² / (2 sigma_user²)
```

which has analytic first and second derivatives in terms of the inverse Mills ratio
`lam(z) = phi(z)/Phi(z)`:

```
h'  = Σᵢ [ lam(zᵢ)/sigma_link − 1/nuᵢ ] − epsilon/sigma_user²
h'' = Σᵢ [ −lam(zᵢ)(zᵢ + lam(zᵢ)) ] / sigma_link² − 1/sigma_user²
```

`h''` is strictly negative — `lam(z)(z + lam(z))` is the variance of a truncated normal, up to sign
— so `h` is **concave**, and Newton converges from `epsilon = 0` in about four steps with no
safeguarding needed. Then `mode` is the peak and `1/sqrt(-h'')` is the Laplace width.

Measured convergence on the real data, against 121 nodes:

| nodes | error in total log-likelihood |
| ---: | ---: |
| 9 | 0.086 |
| 15 | 0.0043 |
| **21** | **0.0003** |
| 31 | 0.000026 |
| 61 | exact to 1e-8 |

21 nodes is the operating point: error three orders of magnitude below anything that could affect a
model comparison, at two-thirds the cost of 31.

### Doing it in PyTensor, and why that's the hard part

NumPy would be enough if we only needed to *evaluate* the likelihood. NUTS needs its **gradient**, so
every step above has to be expressed as PyTensor operations that automatic differentiation can walk
through. That's what `src/kaya/marginal_pt.py` is.

The translations that needed care:

**Segment sums.** The reference uses `np.bincount(seg, weights=row)` to sum within climber. There's
no differentiable bincount, but `pt.inc_subtensor(zeros[:, seg], values)` accumulates on repeated
indices, which is the same operation and differentiates cleanly. It works on the whole `(nodes ×
observations)` matrix at once, so all 21 nodes are summed in one op.

**The left tail.** `log(Phi(z))` underflows to `-inf` deep in the left tail, silently killing regions
the sampler must walk through. The reference uses `scipy.special.log_ndtr`; here it's PyMC's own
`normal_lcdf`, which has the additional benefit of being the *same* function PyMC's built-in
`ExGaussian` uses — so the quadrature and the closed-form singles branch are numerically consistent
with each other.

**The `inf − inf` switch.** When `nu` is small relative to `sigma`, three terms of the ExGaussian are
individually infinite and cancel exactly, giving `nan`. Both implementations switch to the Normal
limit at the same threshold (`nu ≤ 0.05·sigma`). They must switch at the *same* place or they
disagree in the tail. The switch appears twice — in the density and in Newton's derivatives — and
getting it wrong in the second place wouldn't corrupt the integral, it would just put the nodes in
the wrong spot, which amounts to the same thing.

**Differentiating through the node placement.** Newton's iterations depend on the parameters, so the
node positions do too. The gradient propagates through all of it. That is correct rather than
merely convenient: NUTS needs the gradient of the function it is *actually evaluating* — the
quadrature approximation — not of the exact integral it approximates.

**A constant that cost a debugging round.** `hermegauss` returns weights that sum to `sqrt(2π)`, not
to 1, because they carry the `e^{-z²/2}` factor rather than a normalized Gaussian. Forgetting to
divide it out shifts every climber by exactly `½·log(2π) = 0.9189`. That is invisible inside a
single fit — it's a constant — and it corrupts **every model comparison**, which is the entire point
of the sweep. It's now pinned by a test that asserts `exp(gh_logw).sum() == 1`.

### How it's verified

Nothing here is asserted; it's checked against the reference at two levels
(`tests/test_marginal_pt.py`):

| Check | Result |
| --- | --- |
| Per-climber integral vs `multi_log_integral` | max abs diff **5e-8** |
| Whole model log-likelihood vs `log_likelihood` | **6e-5** on 45,000 |
| Sampled elements | **40**, matching emcee exactly |
| Quadrature weights normalized | asserted directly |
| `sigma_user` still estimated | asserted — it survives inside the integrand |

And for option 1, the centered and non-centered models are proven to be the same model in different
coordinates (`tests/test_centered_offsets.py`): their log densities differ by exactly the Jacobian
`−n·log(sigma)`, **residual 2e-11**.

---

## 5. What is still unknown

The quadrature is correct. Whether it is *fast enough* is a separate question and the measurement
above frames it precisely: it costs 28× more per gradient, so it only wins if the improved geometry
lets NUTS finish trajectories in far fewer steps than the 1,023 it currently burns on every
iteration.

That is what the probe run measures, and it is why the overnight schedule is gated on the answer
rather than committed to it in advance.
