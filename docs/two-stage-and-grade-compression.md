# Two-stage fitting, and grade compression

*Written 2026-08-07, while the v10 quadrature sweep was running.*

Two things came out of the same conversation and they interact, so they are written down together.

**The two-stage plan** splits one hard fit into two easy ones: stage 1 measures how gyms differ from
each other using only climbers who appear at more than one gym, and stage 2 fixes those numbers and
fits the height model on one climb per climber.

**Grade compression** is the observation that a gym's grading offset is probably not one number. A
gym that is soft at V2 and stiff at V9 has *two* corrections, not one, and the model currently fits
one. This turns out to be measurable, and it is measured below.

They interact because stage 1 is where a more complex correction would go — it has the budget for it
that the combined model does not.

---

## Part 1 — The two-stage plan

### 1.1 What it is

The model currently does two jobs at once:

1. **Gym comparison.** How much harder is a V5 at gym A than a V5 at gym B? This is
   `gym_correction_g`, one number per gym, and it is the headline output of the whole project.
2. **The height model.** Does being taller help you climb harder, and by how much? This is the
   `beta_h` / `delta` block, and it is what the seven "height forms" in the sweep disagree about.

Doing both at once is what makes the fit hard. The proposal is to separate them:

| | Stage 1 | Stage 2 |
| --- | --- | --- |
| **Question** | how do gyms differ? | does height matter? |
| **Climbers used** | only those at 2+ gyms (4,201) | all, but **one climb each** |
| **Height terms** | none at all | the full block |
| **Gym corrections** | free parameters | **fixed** at stage 1's answer |

The reason this is allowed is a piece of algebra. For a climber `u` who sends at gyms A and B, the
model says

```
m_A = ceiling_u + correction_A + noise
m_B = ceiling_u + correction_B + noise
```

so the difference

```
m_A - m_B = correction_A - correction_B + noise
```

**cancels `ceiling_u` exactly.** Everything about that climber — their ability, their height, their
gender, their ape index — is gone. The difference is a statement about the two gyms and nothing
else. Height cannot confound stage 1 because height is not in the equation any more.

### 1.2 Why it should work here — measured, not assumed

Two things were checked before proposing this.

**The gym network is connected.** Gym corrections are only identified if gyms are linked by shared
climbers. Measured on `net50`:

| Quantity | Measured |
| --- | --- |
| Gym pairs with at least one shared climber | 395 of 406 |
| Median shared climbers per linked pair | 19 |
| Climbers at 2+ gyms | 4,201 |
| Climbers at 3+ gyms | 2,430 |

**The gym corrections barely move when the height model changes.** This is the load-bearing
assumption — that stage 1's answer does not depend on which height form stage 2 eventually picks.
Across all seven height forms in the v7 sweep:

| Quantity | Measured |
| --- | --- |
| Largest shift in any gym correction | **0.019 grades** |
| Correlation between any two forms' corrections | 0.9999 |
| Largest shift as a fraction of that correction's posterior SD | 0.36× |

A gym correction moves by at most a third of its own uncertainty when the entire height model is
swapped out. Stage 1 is genuinely separable.

### 1.3 The two things that must be handled

Neither of these kills the plan, but skipping either one produces a confidently wrong answer.

**(a) Stage 1's uncertainty has to be carried into stage 2.**

This is the classic *generated regressor* problem. If stage 2 treats the gym corrections as known
constants when they are actually estimates, every standard error downstream is too small — stage 2
gets credit for certainty that stage 1 never had. The fix is a **cut posterior**: draw a full set of
gym corrections from stage 1's posterior, run stage 2 conditional on that draw, repeat, and pool.
The resulting spread includes stage 1's uncertainty by construction.

"Cut" is the standard name for this — it is a deliberately *modular* posterior, where information
flows stage 1 → stage 2 but not back. That one-way flow is the point: it is what stops a
misspecified height model from contaminating the gym comparison. It is also why the result is not
the same as the full joint posterior, and that should be said plainly wherever the numbers are
published.

**(b) Stage 1's climbers are not a random sample of climbers.**

Multi-gym climbers differ from single-gym climbers, measured:

| | Multi-gym | Single-gym |
| --- | --- | --- |
| Mean send grade | **5.42** | 4.60 |
| Mean height | 0.73″ shorter | — |
| P(female) | 0.333 | 0.279 |

The 0.82-grade gap is the one that matters. If a gym's correction genuinely depends on ability —
which Part 2 shows it does — then stage 1 measures the correction **at the multi-gym climbers'
ability level**, not at the average climber's. Applying that number to a V2 climber at stage 2 is
then a small extrapolation.

This is not a reason to abandon the plan. It is a reason to fit the compression term in stage 1, so
that stage 2 receives a correction *function* rather than a correction *number*, and the
extrapolation becomes explicit instead of hidden.

---

## Part 2 — Grade compression

### 2.1 The claim

The model fits one number per gym. Reality plausibly has more structure: a gym can be soft on its
beginner problems and stiff on its hard ones, or the reverse. Gym setters often compress the top of
their range — the V8s and V9s all end up feeling like V8s, because there are three of them and
nobody wants to grade the outlier.

If that happens, one number per gym is the *average* correction across that gym's grade range, and
it is wrong at both ends.

### 2.2 How it was measured, without a model

The paired-difference identity from §1.1 does all the work. For a climber at two gyms:

```
m_A - m_B = correction_A - correction_B + noise
```

If corrections are constant, that difference does not depend on who the climber is. **If they are
grade-dependent, it does** — a strong climber and a weak climber at the same two gyms will report
different differences.

So: for each gym pair, regress the difference `m_A - m_B` on the climber's ability, and look at the
slope. A nonzero slope means the two gyms' corrections diverge as ability rises. This needs no
model, no sampler, and no priors.

**First pass**, 109 gym pairs with ≥40 shared climbers, 22,257 paired observations:

```
slope of (grade difference) on (climber ability)
  mean    +0.0216      sd 0.1300      median +0.0169
  range   -0.342 to +0.373
  fraction with |slope| > 0.05:  65%
```

### 2.3 The detectability determination

The numbers above are **not yet evidence of anything.** A slope estimated from ~112 noisy
observations has its own error bar, and 109 such estimates will scatter even if every gym's true
slope is identical. The observed sd of 0.1300 is the sum of real variation and estimation noise, and
those have to be separated before the finding means anything.

This is the whole determination, in four steps.

**Step 1 — measure the noise directly.** Every per-pair regression reports its own standard error,
so the noise is not a guess:

| Quantity | Measured |
| --- | --- |
| Gym pairs used (≥40 shared climbers) | 109 |
| Median climbers per pair | 112 |
| Median ability span within a pair | 9.0 grades |
| Per-pair slope SE — median | **0.0740** |
| Per-pair slope SE — best pair | 0.0225 |
| Per-pair slope SE — worst pair | 0.1957 |

The median SE (0.0740) is more than half the observed spread (0.1300). Most of what the eye reads as
"gyms differ" in §2.2 is noise. This is exactly why the raw spread cannot be quoted as a result.

**Step 2 — test whether anything is left over.** Cochran's Q is the standard tool: it sums the
squared deviations of each estimate from the pooled mean, weighted by each estimate's own precision.
Under the null hypothesis that every gym pair shares one true slope, **Q follows a chi-square
distribution with k−1 degrees of freedom, so its expected value is just k−1.** Q much larger than
its df means the estimates disagree by more than their error bars allow.

```
pooled slope    +0.0178 +/- 0.0058 grades per grade
Q = 268.3  on 108 df       (expected 108 under "no real variation")
critical value at alpha=0.05:  133.3
I^2 = 60%        tau = 0.0740
```

`I²` is the fraction of the observed variance that is real rather than sampling noise — **higher
means more real signal**; 60% means most of the scatter survives the noise correction. `tau` is that
real between-pair spread expressed back in the original units.

**Step 3 — establish the floor, so a null result would have meant something.** Q = 268.3 clears its
critical value of 133.3 by a wide margin, but "we detected it" is only half an answer without "how
small an effect would we have missed?" Simulating the test at 2,000 replicates per level, using the
actual per-pair standard errors:

| True τ | Power to detect it |
| --- | --- |
| 0.010 | 7% |
| 0.020 | 21% |
| 0.030 | 50% |
| **0.040** | **85%** |
| 0.050 | 97% |
| 0.075 | 100% |

The design reaches 80% power at about **τ = 0.039**. The measured τ = 0.074 is roughly **twice the
detection floor** — comfortably inside the range this data can resolve, and not a marginal call. It
also means a null result would have been informative: compression smaller than ~0.03 would have been
invisible here, so the honest conclusion from a null would have been "smaller than 0.03", not "zero".

**Step 4 — rule out the artifact.** The ability proxy in §2.2 is the climber's mean grade across
their gyms, which *includes the two rows whose difference is the outcome*. Since

```
cov(m_A - m_B,  (m_A + m_B)/2)  =  [var(m_A) - var(m_B)] / 2
```

a slope appears by construction whenever the two gyms differ in noise variance. That alone could
manufacture heterogeneity.

The fix: restrict to the 2,430 climbers who appear at **3 or more** gyms, and measure ability from
the gyms *not* in the pair. The proxy then shares no noise with the outcome and the artifact is
impossible.

| | Contaminated proxy | Clean proxy (3+ gyms) |
| --- | --- | --- |
| Gym pairs | 109 | 135 |
| Pooled slope | +0.0178 ± 0.0058 | **+0.0160 ± 0.0058** |
| Q / df | 268.3 / 108 | **302.2 / 134** |
| I² | 60% | **56%** |
| **τ** | **0.0740** | **0.0753** |

The two agree to within a rounding error. The heterogeneity is not an artifact of the proxy.

**Verdict: grade compression is real, and it is a genuine per-gym effect, not a single global one.**

### 2.4 How big is it, in grades

τ = 0.074 is the spread of *pairwise* slopes. A pairwise slope is a difference of two gyms' slopes,
so `var(b_A − b_B) = 2·var(b)`, and the per-gym spread is `τ/√2 = 0.052` grades of correction per
grade of climber ability.

Turning that into something interpretable, over the 10th–90th percentile of ability (V3 to V8, a
5-grade span):

| Effect | Size in grades |
| --- | --- |
| Two gyms 1 sd apart in compression, differential across V3→V8 | **0.52** |
| Full range of the constant gym corrections | ~1.3 |

**Compression is about 40% the size of the effect already being modelled.** That is large enough
that ignoring it biases the constant corrections, and small enough that it is a refinement rather
than a rewrite. It is second-order, not headline — but it is not noise.

There is also a small **global** compression: the pooled slope is +0.016 to +0.018 with an SE of
0.0058, about 3 standard errors from zero. That is a real average tendency, but see §3.2 — the
global level is not separately identifiable from the ability scale, so this number is descriptive
rather than something the model will estimate.

### 2.5 Is there curvature, or only a slope?

This decides how many basis terms §4/Option A needs, so it was measured rather than argued. Same
paired-difference design, one term further: fit `d = q*(a - ā)² + b*(a - ā) + const` per gym pair,
and run the same heterogeneity machinery on the **quadratic** coefficient. 80 gym pairs with ≥60
shared climbers.

| | Linear slope | Quadratic curvature |
| --- | --- | --- |
| Median per-pair SE | 0.0722 | 0.0253 |
| Q / df | 243.9 / 79 | **169.7 / 79** |
| Critical value (α = 0.05) | 100.7 | 100.7 |
| I² | 68% | **53%** |
| **τ** | **0.0851** | **0.0230** |
| 80% power reached at τ ≈ | 0.040 | **0.018** |

**Curvature is real.** Q = 169.7 against a critical value of 100.7, I² = 53%, and the design has 98%
power at τ = 0.020 — so this is a well-powered detection, not a marginal one, with the measured
τ = 0.023 sitting at roughly 1.3× the 80%-power floor.

Magnitude, converting to per-gym (÷√2) and evaluating at ±2.5 grades from centre — about the width
of the well-populated ability range:

| Term | Per-gym contribution at the edge of the common range |
| --- | --- |
| Linear | 0.060 × 2.5 = **0.15 grades** |
| Quadratic | 0.016 × 2.5² = **0.10 grades** |

Curvature is about two-thirds the size of the linear term at the edges. **That is too large to
drop.** It supersedes the earlier "D: not yet, untested" call in §4 — the test has now been run and
it passes.

What is *not* yet known is whether a third term is needed. That question should be answered the same
way before any spline with more than three knots is fitted.

---

## Part 3 — Two things that constrain any model of this

Both were established before the options were drawn up, and both eliminate otherwise-reasonable
designs. They are stated first because they are the reason the options list looks the way it does.

### 3.1 The correction must depend on the *latent* ability, never the observed grade

The tempting formulation is: let the correction depend on the grade of the climb.

```python
m = ceiling + correction[gym, grade_bin_of(m)] + noise     # WRONG
```

This is **endogenous**. `m` is the outcome — the thing the noise is on. Using it to select which
correction applies means the noise decides its own correction, and the estimate is biased regardless
of sample size. It is the same error as regressing a variable on itself.

The correct form makes the correction a function of the **latent ceiling**, which is a parameter and
carries no observation noise:

```python
m = ceiling + correction_g(ceiling) + noise                # right
```

Every option below is written this way. It costs nothing structurally — `ceiling` is already in the
model — but it does mean there is no "grade bin" to index into, because the latent ceiling is
continuous. Anything wanting bins has to bin the *parameter*, which is awkward and is one reason
Option A scores poorly.

### 3.2 Only *relative* compression is identifiable

There are **two independent reasons**, and they are worth separating because they bite at different
places — one is about the model, the other about the measurement.

**Reason 1 — the model absorbs it into the ability scale.**

Suppose every gym compresses identically, a global slope `b` applied everywhere:

```
m = ceiling + b*(ceiling - c0) + correction_g + noise
  = ceiling*(1 + b) - b*c0 + correction_g + noise
```

Now define `ceiling' = ceiling*(1+b) - b*c0`. The equation becomes

```
m = ceiling' + correction_g + noise
```

which is **the original model, exactly**. For that relabelling to be legitimate, everything defining
`ceiling` has to be free to rescale with it — and it all is. `ceiling = beta0 + X·beta + eps` with
`eps ~ Normal(0, sigma_user)`, so the transformation is absorbed by `beta' = beta*(1+b)`,
`sigma_user' = sigma_user*(1+b)`, and a shifted intercept. Every one of those is an estimated
parameter with nothing pinning it down.

The reason nothing pins it down: **`ceiling` is latent and has no external anchor.** The observed
grades `m` are on a fixed V-scale, but `m` is `ceiling + correction + noise` — a *sum*, and the
data only ever sees the sum. Stretching ability while shrinking nothing else is invisible because
the compression term stretches back in compensation. Note the noise does not save you either:
`sigma_link` is on the fixed `m` scale and never needed to change in the derivation above.

Strictly, the *posterior* is not perfectly flat in `b`, because `sigma_user ~ HalfNormal(2)` is not
scale-invariant and would leak a little information. That is the prior talking, not the data, and it
is a bad foundation for a published number.

**Reason 2 — the paired-difference measurement is structurally blind to it.**

This one is specific to how §2.2 measured the effect, and it is the cleaner argument:

```
m_A - m_B = (correction_A - correction_B) + (b_A - b_B)*(c - c_bar) + noise
```

The part of `b` common to every gym **cancels in the difference**, exactly as `ceiling_u` does. The
estimator sees `b_A - b_B` and nothing else. So τ = 0.074 is a statement about *differences between
gyms*, full stop — the design could not have measured a global slope even if the model could
represent one.

**Consequence.** The per-gym slopes need a **zero-sum constraint**, exactly as `correction_g`
already has:

```python
b_g = pm.ZeroSumNormal('compression', sigma=sigma_b, dims='gym')
```

**Is that a loss?** Almost none, and this is the reassuring part. The question being asked is
"is gym A stiffer than gym B, and does that gap widen with grade?" — entirely relative. A global
slope would be a statement that *the V-scale itself* is nonlinear, which is a claim about the
grading system, not about any gym. Answering it needs an anchor outside the data: an objective
difficulty measure, or one physical climb graded at multiple gyms. Neither exists here. This is not
a defect in the model; the information is genuinely absent.

This is also why §2.4 reports the pooled slope as descriptive only. It is not an estimate of global
compression — it is a precision-weighted contrast whose value depends on the arbitrary gym-ID
ordering used to orient each pair, and on which gyms happen to appear as the first member more
often. Do not read it as an effect.

---

## Part 4 — The options

Five candidates, each written as a form for `correction_g(c)` where `c` is the latent ceiling and
`c̄` its mean. Parameter counts are for the 29 gyms in `net50`.

### Option A — per-grade cells

```
correction[g, bin(c)]      # 29 gyms x ~6 bins = 174 parameters
```

**As stated — hard cells with a step between them — no.** Binning a *latent continuous* parameter
(§3.1) means the bin assignment changes between MCMC draws, so the likelihood is a step function of
`c`: the gradient is zero inside a bin and undefined at the boundary. NUTS runs on gradients. That
is close to disqualifying on its own, before the 174-parameter funnel and the ~19-climbers-per-cell
thinness are even counted.

**But interpolating between the cells fixes it, and that is the right idea.** Place a value at each
bin centre and join them up, and the discontinuity is gone: linear interpolation gives a continuous,
piecewise-differentiable function; a cubic spline gives a fully smooth one. NUTS is fine with either
(kinks at the knots are measure-zero; if they cause trouble, a cubic basis removes them).

The important consequence is a change of what is being estimated. Once you interpolate, **you are no
longer estimating cell means — you are estimating basis coefficients**, and the count is the number
of knots, not the number of bins-with-enough-data-in-them. Written out:

```
correction_g(c) = sum_k  a_{g,k} * phi_k(c)
```

where `phi_k` are fixed basis functions ("hat" functions for linear interpolation, B-splines for
smooth). Thin data in one bin no longer produces one unconstrained cell; it produces a coefficient
that neighbouring data still informs through the overlapping basis functions.

**And this reframes the entire option list.** A, B and D are *the same model with different bases*:

| Option | Basis | Terms per gym |
| --- | --- | --- |
| current model | `{1}` | 1 |
| B | `{1, c}` | 2 |
| D | `{1, c, c²}` | 3 |
| A-interpolated (6 knots) | `{1, φ₁…φ₅}` | 6 |
| E | not a basis expansion — a monotone reparameterization of `c` | — |

So the question is not "which option" but **"how many terms does the data support?"** That is an
empirical question, and §2.5 answers it.

**Verdict: A as hard cells, no. A as an interpolated basis, yes — it is the general case, and the
only open question is how many knots.**

### Option B — per-gym linear slope ★

```python
correction_g(c) = a_g + b_g * (c - c_bar)
# a_g: 29 params (already exist)   b_g: 29 new, zero-sum
```

**How it works.** Each gym gets an intercept (what it already has) and a slope (how its correction
changes with ability). `b_g > 0` means the gym gets relatively stiffer as climbs get harder.

**For it.**

- It is exactly what §2.2 measured. The measurement was a per-pair *linear* slope; τ = 0.074 is
  literally the sd of this parameter. The evidence and the model are the same object, which means
  the prior can be set from the data: `sigma_b ~ HalfNormal(0.05)` centres on the measured 0.052.
- 29 parameters on top of 40 is a 1.7× increase in a model that now samples cheaply. Affordable.
- `c` is continuous and the form is smooth, so the gradient is clean — none of Option A's problems.
- Interpretable in one sentence per gym: "gets 0.05 grades stiffer per grade of ability."
- Degrades gracefully. If compression is not real for some gym, `b_g` shrinks to zero and that gym
  is exactly the current model.

**Against it.** Linear is an assumption. Real compression is plausibly a *ceiling* effect —
flat through the middle grades, biting only at the top — which is a curve, not a line. A linear fit
through a curve like that will understate the top-end effect and put a spurious tilt on the bottom.

**Verdict: this is the one to run first.** Not because linear is right, but because it is what has
actually been measured, it is cheap, and it is the necessary baseline for testing anything curved.

### Option C — one global slope plus per-gym intercepts

```python
correction_g(c) = a_g + b * (c - c_bar)     # 1 new parameter
```

**Ruled out by §3.2.** A single global slope is absorbed into the ability scale. It is not a
simplification of Option B; it is a non-identifiable model. Listed only because it is the obvious
next thing to reach for after B, and it does not work.

### Option D — per-gym quadratic or low-order spline

```python
correction_g(c) = a_g + b_g*(c - c_bar) + d_g*(c - c_bar)^2
# 58 new parameters (b_g and d_g, both zero-sum)
```

**How it works.** Option B plus curvature, so a gym can be soft in the middle and stiff at both
ends, or show the top-end ceiling effect that B cannot represent.

**For it.** It is the honest functional form if compression really is a ceiling effect — and §2.5
now shows it is needed: curvature has τ = 0.023 with I² = 53% at 98% power, contributing ~0.10
grades at the edges of the common ability range against the linear term's ~0.15.

**Against it.** 58 new parameters instead of 29, and the curvature coefficients are less precisely
determined per gym, so the hierarchical prior does more of the work. Neither is disqualifying.

**Verdict: yes, but fit B first.** Not because D is in doubt — it is measured — but because B is
nested inside D, is much cheaper, and gives the sanity check that stage 1 reproduces the known gym
corrections before any curvature is added. Going straight to D means debugging two changes at once.

A monotone spline basis (I-splines) is the principled refinement, keeping the correction from
wiggling non-physically at the sparse ends of the range. Worth it if the quadratic's fitted
coefficients turn out to imply non-monotone corrections.

### Option E — monotone latent warping (IRT-style)

```python
c_effective_at_gym_g = w_g(c)     # a monotone warp of the ability axis, per gym
```

**How it works.** Instead of an additive correction, each gym has its own monotone re-mapping of the
ability scale — the fully general statement of "this gym's grades mean something different." This is
the item-response-theory framing, where compression is a discrimination parameter.

**For it.** Theoretically the most correct. Compression genuinely *is* a scale distortion, not an
offset, and this says so directly. It also guarantees monotonicity, which Options B and D do not —
under B, a large enough `b_g` implies a gym where higher ability yields a lower expected grade,
which is nonsense.

**Against it.** It is a different model, not an extension of the current one. Everything downstream
— the gym-correction output, the viewer payloads, the entire published interpretation of "gym A is
0.4 grades stiffer" — assumes an additive correction. Under E there is no single number per gym to
report. That is a large change to make on the strength of a τ = 0.074 effect.

**Verdict: the right end state, the wrong next step.** Worth revisiting if B and D both show the
effect is large and clearly nonlinear. Note that B is a first-order approximation to E, so B's
results will point at whether E is worth the disruption.

### Summary

| Option | New params | Identifiable | Samples cleanly | Supported by the measurement | Verdict |
| --- | --- | --- | --- | --- | --- |
| A — hard cells | 145 | yes | **no** (discontinuous) | — | no |
| A — interpolated basis | 145 | yes, with zero-sum | yes | only 2 terms tested so far | the general case; needs a knot count |
| **B — per-gym linear** | **29** | yes, with zero-sum | yes | **yes, τ = 0.085** | **fit first** |
| C — global slope | 1 | **no** (§3.2) | — | — | no |
| **D — per-gym quadratic** | **58** | yes, with zero-sum | yes | **yes, τ = 0.023 at 98% power** | **the target** |
| E — monotone warp | ~29–58 | yes | probably | not tested | end state, later |

**Net effect of §2.5 on this table: D is the destination, B is the first step toward it, and A's
interpolated form is what you would reach for only if a *third* term also proved necessary.**

---

## Part 5 — Main model or stage 1?

This was the second half of the question, and it has a clear answer: **stage 1.**

### The argument

**Stage 1 has the parameter budget and the main model does not.** The combined model is currently
being rescued from a geometry failure by integrating out 4,201 climber offsets to get down to 40
sampled parameters. Adding 29 more to *that* model means adding them to the fit that just barely
started converging. Stage 1 has no height block at all — it is a much smaller, much better-behaved
problem, and it can absorb 29 parameters without drama.

**Compression is a property of gyms, which is exactly what stage 1 is for.** The split in §1.1 is
"gym comparison" vs "height model". Compression is entirely on the gym side. Putting it in stage 1
is not a compromise for tractability — it is the correct place for it.

**Stage 1 is where the identifying information lives.** The measurement in §2.2 used paired
differences among multi-gym climbers. Those are precisely stage 1's data. The main model has access
to the same information but diluted among single-gym climbers who contribute nothing to it.

**It fixes §1.3(b).** The known weakness of the two-stage plan is that stage 1's climbers send 0.82
grades harder than average, so a constant correction measured on them is measured at the wrong
ability level. With a slope in hand, stage 2 receives `correction_g(c)` and evaluates it at each
climber's own ability. The bias does not need to be assumed away; it is modelled. **Compression and
the two-stage plan fix each other's weaknesses,** which is the strongest argument for doing both.

**It is falsifiable cheaply.** Stage 1 with and without the slope is a fast model comparison on a
small model. Testing the same thing inside the main model means two full quadrature fits.

### The one real cost

Stage 2 must now consume a correction *function* rather than a *number*, and the cut posterior of
§1.3(a) has to carry `(a_g, b_g)` jointly — they are correlated, and drawing them independently
would understate the uncertainty. That is a bookkeeping requirement, not a modelling obstacle.

---

## Part 6 — Should climber advancement be *fitted* rather than fixed?

Asked while the above was being written, and it belongs here because advancement has the same shape
as compression — an effect that varies with latent ability — so the obvious move is to treat them
the same way. **It should not be.** The answer is no, and the reason is identification, not cost.

### 6.1 What is currently done

Climber improvement over time is measured **outside** the model, by `scripts/build_v2_time.py`, from
the raw sends table, and then enters as a **fixed** per-row offset. The measured rate, debiased
across all watching periods rather than any single window:

```
rate(v) = 0.338 - 0.035*v   grades per year
  at V2: 0.268     at V5: 0.163     at V9: 0.023
```

Climbers improve fast when they are new and barely at all near their ceiling, which is why a
constant drift term would be the wrong shape.

### 6.2 Why the confound is worth taking seriously

The grading model has no time in it, so a climber who logged at gym A in 2022 and gym B in 2025 has
three years of their own improvement charged to the gyms. Measured on `net50`, using each gym's mean
**within-climber-centred** send time `t_c` (how early or late in their own history climbers tend to
visit that gym):

| Quantity | Measured |
| --- | --- |
| Correlation between a gym's correction and its mean `t_c` | **r = 0.607** |
| Slope | **0.827 grades per year** |
| Spread of `t_c` across gyms | 0.897 years |
| Spread of the gym corrections | 1.290 grades |
| Correction spread implied by `t_c` alone | **0.742 grades** — 58% of the observed spread |
| Within brand (removes brand-level grading style) | r = 0.551, slope 0.674 |

That is not a small confound. Well over half the apparent spread in gym corrections tracks *when*
people climbed there.

### 6.3 The reason not to fit it

Here is the number that decides it:

| | Grades per year |
| --- | --- |
| Slope the data shows between correction and time | **0.827** |
| Rate climbers actually improve at (measured externally) | **~0.24** |
| Ratio | **3.4×** |

Real climber advancement can account for about **29% of that slope, ~8% in variance terms.** The
other 71% is something else — most plausibly genuine gym-level drift (gyms got stiffer over the
period) or a selection effect (people try a new gym when they are climbing well).

Now the argument. **Inside the model, nothing distinguishes those three stories.** "The climber
improved", "the gym got stiffer", and "climbers visit new gyms while peaking" all make later sends
look harder in exactly the same way. A free advancement parameter is therefore free to absorb the
entire 0.827 — 3.4× its true value — because doing so improves the fit and the likelihood has no
grounds to object. The result would be a confidently estimated advancement rate that is mostly gym
drift, **and** gym corrections with the drift silently removed from them.

Fixing the rate at an externally measured value is what prevents this. It is the same logic as the
cut posterior in §1.3(a): a deliberate restriction on where information is allowed to flow, imposed
because the joint model cannot separate the sources and the marginal one can.

**The external estimate can do the separation because it uses data the model does not have.** It
tracks *the same climber at the same gym* over time, so the gym cancels exactly — the same
paired-difference trick as §1.1, run on the other axis. That identification simply is not available
to a model whose dataset has one row per (climber, gym).

### 6.4 The other reasons, in descending order of importance

**The dataset has no time in it.** `base_bouldering.pkl` is one row per (climber, gym), columns
`user_id, gym_id, climb_id, m, n_visits, n_sends_gym, n_at_max`. There is no date. Fitting
advancement means re-shaping the model's dataset to be time-resolved, which is a data-layer change,
not a model tweak, and it multiplies the row count.

**It would complicate the quadrature — though not fatally.** Worth being precise, because the two
cases differ:

- *A fixed offset costs nothing.* `m = ceiling + adv(v, t) + correction + noise` with `adv` known is
  just a shift of the mean. `marginal_pt`'s integrand keeps its exact structure, `h''` keeps its
  proof of concavity, and the Newton solve is untouched.
- *A fitted rate does cost something.* The rate depends on ability, and ability is the variable being
  integrated over, so the mean becomes `eps*(1 + r'*t) + ...`. The Laplace step in
  `marginal_pt._laplace` assumes `d(mean)/d(eps) = 1`; it would need a per-row Jacobian. Concavity
  survives while the rate is linear in ability (which the measured form is), but the measured form
  is a *decaying* curve fit linearly, and if that were replaced with the real curve the concavity
  argument would have to be redone.

Moderate cost. It is not the reason to say no — §6.3 is — but it is not free either.

### 6.5 How to determine gym drift — and the answer, measured

The question "can we determine gym drift at all?" has a clean answer, and it is the same identity
this whole document runs on, rotated onto a different axis.

**The estimator.** Take one climber, in one 90-day window, who climbed at two gyms:

```
level_A - level_B  =  correction_A - correction_B + noise
```

Because both sides are **the same climber at the same time**, this cancels:

- their ability (same person),
- their improvement (same moment — advancement hits both sides equally),
- any tendency to visit a new gym while peaking (they are in both).

Every confound in §6.3 is gone by construction. So ask: does that difference trend with **calendar
date**? If it does, one gym drifted relative to the other. Note the contrast with §2.2, which
regressed the same difference on *ability*; drift and compression are the same measurement on
perpendicular axes.

**What it cannot see:** drift common to every gym, which cancels in the difference exactly as global
compression does (§3.2). This measures **relative** drift only, and for the same reason — there is
no external anchor.

**Data.** 1,191,431 dated boulder sends at the 29 `net50` gyms, 2020-08-25 to 2026-07-26, 16,133
climbers. Cells of (climber, gym, 90-day window) with ≥3 sends; 11,902 (climber, window) cells
contain two or more gyms. `log(n_sends)` is a control covariate, because a window's max grade rises
with how many sends went into it and a pair whose send counts drift would otherwise fake a grading
drift.

**Result**, 121 gym pairs with ≥25 paired same-window observations:

| Quantity | Measured |
| --- | --- |
| Median paired observations per gym pair | 204 |
| Median calendar span per pair | 1.4 years |
| Median per-pair drift SE | 0.159 grades/yr |
| **Q / df** | **379.7 / 120** (critical value 146.6) |
| **I²** | **68%** |
| **τ (relative drift spread)** | **0.163 grades/yr** |
| 80% power reached at τ ≈ | 0.068 grades/yr |

**Gym drift is real, and it is the largest effect in this document.** Q overshoots its critical value
by 2.6×, and the measured τ is ~2.4× the detection floor.

**Per-gym rates.** Pairwise contrasts were solved into per-gym rates by weighted least squares with a
zero-sum constraint and partial pooling at the measured τ. Two notes on getting this right:

- At a ≥40-observation threshold the gym graph **splits into three disconnected components** (16, 8
  and 3 gyms), and relative rates between components are genuinely unidentifiable — the first
  attempt returned standard errors of ~19,000 and per-gym rates 3× too large. Lowering the threshold
  to 25 connects all 28 gyms into one component and the solve behaves. **Check connectivity before
  solving a contrast network.**
- The solve validates against the pairwise test independently: per-gym sd **0.105** grades/yr vs
  τ/√2 = **0.115** from the paired data. Two different routes, same answer.

| Quantity | Measured |
| --- | --- |
| Per-gym drift rate, sd across gyms | 0.105 grades/yr |
| Range | −0.171 to +0.319 grades/yr |
| Median per-gym SE | 0.052 (so the spread is ~2× the typical error) |
| **Accumulated drift over the 5.9-year span, sd across gyms** | **0.62 grades** |
| For comparison: full spread of the constant gym corrections | 1.29 grades |

Stiffening fastest: Movement Santa Clara (+0.319 ± 0.037), Movement San Francisco (+0.172 ± 0.057),
Bouldering Project Poplar (+0.142 ± 0.068). Softening fastest: Touchstone Team Training Center
(−0.171 ± 0.082), Movement Belmont (−0.128 ± 0.052), Touchstone Dogpatch (−0.110 ± 0.044).

**Reproducible as of 2026-08-07: `scripts/probe_gym_drift.py`.** Until then every number in §6.5
lived only in this prose — the measurement had no script, could not be re-run when gyms were
added, and could not be checked. It now rebuilds from the live mirror and writes
`runs/results/gym_drift.json` plus `runs/time_resolved_cells.pkl`, the (climber, gym, 90-day
window) dataset a drift-carrying model needs and which `base_bouldering.pkl` does not contain.

The paired half reproduces exactly: 1,191,431 dated sends, 16,133 climbers, 11,902 multi-gym
(climber, window) cells, I² 68%, **τ 0.163 grades/yr**. Q/df comes out 381.8/122 against 379.7/120
because two more pairs clear the 25-observation threshold under the rebuilt cells.

**The per-gym solve only reproduces with the pooling made explicit, and that is worth recording.**
"Partial pooling at the measured τ" is not weighting — weights alone leave 123 noisy contrasts
free to absorb their own sampling error into 29 unknowns, and the spread comes out at **0.276
grades/yr against a τ/√2 of 0.115, 2.4× too large**, with a range of −0.47 to +0.68 that is mostly
noise wearing a gym's name. It is the ~19,000-standard-error failure of the first attempt, one
notch milder: there the graph was disconnected, here it is connected but unconstrained. Each rate
needs its own prior, `rate_g ~ N(0, τ²/2)`, as extra rows pulling toward zero — ridge with the
penalty fixed by a measurement rather than chosen. τ/√2 because τ is the spread of *differences*,
and a difference of two independent rates has twice the variance of one. With it in place: sd
**0.101** (table says 0.105), median SE **0.053** (0.052), range −0.170 to +0.305 (−0.171 to
+0.319), accumulated **0.60 grades** (0.62).

**Six years of drift is about half the entire spread the gym corrections are trying to measure.**
A single number per gym is an average over a moving target.

**The surprise, and it matters:** drift does **not** explain the confound that motivated the
question. Removing each gym's accumulated drift from its correction leaves the §6.2 confound
slightly *larger* (slope +0.809 → +0.955, r +0.601 → +0.678), so relative drift is eliminated as its
cause. §6.3 already showed climber advancement accounts for only ~8%. The leading remaining
candidate is **selection** — which climbers choose which gym, and when — a composition effect rather
than a grading one, needing its own design.

**A distinction this table does not make, and Part 7 does:** drift is large *within* a gym over the
full 5.9 years (0.62 grades) but small *between* gyms at the dates their data actually sits (0.063
grades), because the gyms are roughly contemporaneous — their mean dates differ by only 0.59 years.
Both numbers are correct and they answer different questions. Do not quote the first one when
discussing the confound.

> **§6.5 is the compressed version.** **Part 7 teaches the same result from the beginning** — what
> drift is, why the paired-difference design works, what Q / df / I² / τ each mean, what "two
> independent routes agree" refers to, and the connectivity trap. It also corrects a test that was
> reported wrongly here on first pass (§7.9). Read Part 7 if any of the above was terse.

### 6.6 What would change the answer

Fitting advancement becomes the right call if **the confound can be separated by structure rather
than by assumption.** Concretely, that means putting *both* a climber-advancement term and a
gym-drift term in the model, and having enough of the following to tell them apart:

- climbers who visit the **same gym** at widely separated times (identifies climber advancement with
  the gym held fixed);
- climbers who visit **two gyms** at nearly the same time (identifies the gym contrast with time
  held fixed).

Both types exist in the sends table. Neither survives the aggregation to one row per (climber, gym).
So the prerequisite is the same data-layer change as §6.4, and the payoff is different from what the
question assumed: the value is not in fitting advancement, it is in fitting **gym drift**, which is
currently not modelled at all and which §6.3 suggests is the larger of the two effects.

### 6.7 Recommendation

**Keep advancement fixed.** Nothing measured in §6.5 changes that — if anything it strengthens the
case, since drift turns out to be a real effect of its own that a free advancement parameter would
have been free to absorb.

**But gym drift should be modelled.** It is larger than compression (0.62 grades of accumulated
spread over the span, against compression's ~0.26 sd equivalent) and it is now measured with the
same rigour. The obvious form is a per-gym linear drift rate with a zero-sum constraint —
structurally identical to Option B, on the time axis instead of the ability axis:

```python
drift_g = pm.ZeroSumNormal('drift', sigma=sigma_d, dims='gym')   # sigma_d ~ HalfNormal(0.11)
correction_gt = correction_g + drift_g * (t - t_bar)
```

The blocker is the same as everything else in §6.4: **the model's dataset has no dates.** This is
now the strongest reason to build the time-resolved dataset — not to fit advancement, but to fit
drift.

**And the §6.2 confound remains open.** Advancement explains ~8% of it; drift explains ~0%. Report
it as a known, quantified, unexplained limitation wherever the corrections are published. The
leading remaining candidate is selection — who climbs where, and when — which is a composition
effect and needs its own design.

---

## Part 7 — Gym drift, explained slowly

*Part 6.5 states this result compressed into a table. This part is the same material taught from
the beginning, because the compressed version assumed too much. Nothing here is new evidence — it
is the same numbers, unpacked. It also corrects one claim §6.5 got wrong; see §7.9.*

### 7.1 What "gym drift" means

A climbing gym assigns grades to its own problems. **Nothing forces those grades to mean the same
thing in 2026 as they did in 2021.** Setters change, management changes, a gym decides its V6s have
been too generous and tightens up. When that happens, the gym has **drifted**.

Two words used throughout, and it is worth fixing which is which:

- **Stiffening** — grades get *harder to earn*. A climb that would have been called V7 last year is
  called V6 this year. A climber of unchanged ability logs *lower* numbers than before.
- **Softening** — the opposite. Grades get easier to earn, and the same climber logs *higher*
  numbers.

Neither is "good" or "bad" — this is not a quality measure. It matters only because the whole
project is a comparison *between* gyms, and a gym whose meaning of "V6" is moving is a moving
target to compare against.

### 7.2 Why you cannot just look at the grades over time

The naive approach: take gym X, plot the average grade logged there each year, see if it trends.

That fails, because **three completely different things push that line in the same direction**, and
looking at the line cannot tell them apart:

1. **Climbers improve.** Everyone in the data is getting better at climbing. Grades logged
   anywhere go up over time for that reason alone.
2. **The gym drifts.** The thing we actually want.
3. **Different people show up.** The population climbing at gym X in 2026 is not the population
   from 2021. If the gym got popular with stronger climbers, the average rises with no drift at all.

Any of the three produces "grades at gym X went up." This is the entire difficulty.

### 7.3 The trick, in one sentence

> Compare **one climber**, in **one 90-day window**, at **two gyms**, and look at the *difference*.

That is it. Everything below is bookkeeping around that sentence. Written out — `level` means the
hardest grade that climber logged at that gym in that window:

```
level_at_A  =  (their ability)  +  (how gym A grades)  +  noise
level_at_B  =  (their ability)  +  (how gym B grades)  +  noise

level_at_A - level_at_B  =  (how A grades) - (how B grades)  +  noise
```

**The subtraction deletes all three confounds from §7.2 at once:**

| Confound | Why it cancels |
| --- | --- |
| Climbers improve | Both sides are in the *same 90-day window*, so most of any gain is shared and subtracts away. "Same moment" is an approximation — 90 days is not zero — and §7.5b measures what leaks through and shows it is not driving the result. |
| Different people show up | Both sides are the *same person*. There is no population to differ. |
| Their raw ability | Same person again — it appears identically on both lines and subtracts away. |

What survives the subtraction is a statement about **the two gyms and nothing else**, measured at
one point in time.

Then the actual question: **take that difference and watch it across calendar years.** If gym A
stiffens relative to gym B, then `level_at_A − level_at_B` gets more negative as the years pass. The
slope of that difference against the calendar date is the **relative drift rate**, in grades per
year.

This is the same identity used in §1.1 and §2.2. §2.2 asked how the difference varies with the
*climber's ability*; here it varies with the *calendar date*. Same trick, perpendicular axis.

### 7.4 What one gym pair gives you, and why 121 of them is not yet an answer

Run that regression for one pair of gyms and you get **one number**: "gym A drifted +0.09 grades per
year relative to gym B." Do it for every gym pair with enough shared data and you get **121
numbers** — one per pair.

Those 121 numbers are scattered. Here is the trap, and it is the same trap as §2.3: **scatter is not
evidence.** Two things produce it:

1. **Real differences.** Gyms genuinely drift at different rates. This is the finding.
2. **Measurement noise.** Each of the 121 numbers came from a limited amount of data and has its own
   error bar. Even if every gym in the world drifted at exactly the same rate, 121 noisy estimates
   of that one rate would still scatter.

Quoting the raw spread of the 121 numbers would be reporting (1) and (2) added together and calling
it (1). They have to be separated first.

### 7.5 Separating real from noise — where each number comes from

Every one of the 121 regressions reports its own **standard error (SE)** — how uncertain *that
particular* estimate is, derived from how much data went into it and how scattered that data was.
This is the key to the whole method: **the noise is measured, not assumed.** Median SE here is 0.159
grades per year, which is very nearly the whole observed spread — so most of the raw scatter really
is noise, and saying so requires the machinery below.

#### Q — the total disagreement, in units of "error bars"

Start with a single gym pair. Ask: how far is this estimate from the overall average, **counted in
its own error bars?**

```
z  =  (this estimate - the average) / (this estimate's SE)
```

That is a standardized deviation. If this pair's true drift equals the average and the only reason
it differs is noise, then `z` is a standard normal — it lands near 0, occasionally near ±2, and
`z²` **averages exactly 1.**

That "averages exactly 1" is the hinge. Now add up `z²` over all 121 pairs:

```
Q  =  sum over pairs of  z^2  =  sum of (estimate - average)^2 / SE^2
```

If the scatter is *pure noise*, we are adding up 121 things that each average 1, so Q should land
near 121. (One is subtracted because the "average" was itself estimated from the same data, which
uses up one unit of freedom — hence 120, not 121.)

Dividing by `SE²` is what makes this work. A pair that sits far from average but has a huge error bar
contributes almost nothing; a pair that sits far from average with a *tight* error bar contributes a
lot. Q is asking a single question: **do these estimates disagree by more than their own error bars
can account for?**

#### df — the expected value, not a threshold

**`df` (degrees of freedom) = 120 is what Q should equal if there were no real differences at all.**
It is the *expected value* under "pure noise", not a pass mark. This is the point that was unclear
before: `df` is a prediction of where Q lands in the boring case, and the comparison is Q against
that prediction.

Of course Q would not land on exactly 120 even in the boring case — it varies. Its full distribution
under pure noise is chi-square with 120 degrees of freedom, and from that we get:

| | Value |
| --- | --- |
| Expected Q if all scatter is noise (`df`) | **120** |
| Q would exceed this only 5% of the time by chance ("critical value") | **146.6** |
| **Q actually measured** | **379.7** |
| Probability of Q this large by chance | 1 × 10⁻²⁸ |

So the boring explanation predicts ~120, would rarely exceed 146.6, and we observed 379.7. The
estimates disagree roughly **three times more** than their error bars permit. That excess is the
finding.

#### I² — the excess as a percentage

`Q − df = 379.7 − 120 = 259.7` is the disagreement that noise cannot explain. Express it as a share
of the total:

```
I^2  =  (Q - df) / Q  =  (379.7 - 120) / 379.7  =  0.684
```

**I² = 68%.** Read it as: *of all the scatter among the 121 estimates, 68% reflects genuine
differences between gyms and 32% is measurement noise.* **Higher means more real signal**; 0% would
mean the scatter is entirely noise and there is nothing to model.

I² is a proportion, so it is unitless — it tells you *what fraction* is real but not *how much*.
That is what τ is for.

#### τ — the real spread, converted back into grades per year

Q lives in "squared error bars", which is not a unit anyone can interpret. τ converts the excess back
into the original units. The standard estimator (DerSimonian–Laird) is:

```
tau^2  =  (Q - df) / C          where   C = sum(w) - sum(w^2)/sum(w),   w = 1/SE^2
```

**Where `C` comes from.** Work out what Q averages when there *is* real between-gym variation of size
τ². The answer is:

```
E[Q]  =  df  +  tau^2 * C
```

Every term of Q gets inflated in proportion to how much real variance there is, scaled by how precise
the estimates are — and `C` is exactly that scaling factor. It is a pure "exchange rate" between real
variance and chi-square units. Rearranging that one equation for τ² gives the formula above; there is
nothing deeper to it than solving for the unknown.

`w = 1/SE²` is the **precision** (the inverse of variance) — a precise estimate gets a large weight.
With the actual numbers:

| Quantity | Value |
| --- | --- |
| Σw | 10,217.2 |
| Σw² | 4,413,510.3 |
| C = Σw − Σw²/Σw | 9,785.2 |
| τ² = (Q − df) / C = (379.7 − 120) / 9,785.2 | 0.02654 |
| **τ = √0.02654** | **0.1629 grades per year** |

If Q comes out *below* df — which happens when estimates agree more closely than chance would
predict — the formula returns a negative number, and τ² is floored at 0. There is no such thing as
negative real variance.

**Sanity check on the same data:** the median single-pair term `z²` is 1.14. For pure noise, `z²` has
a median of 0.455 (its mean is 1, but the distribution is heavily right-skewed). Observing 1.14 means
the *typical* pair is about 2.5× more deviant than noise predicts — consistent with I² = 68%, and it
confirms the excess is broad-based rather than a handful of outlier pairs dragging the sum.

#### How small an effect could this have caught?

Detecting an effect is only half an answer without knowing what would have been missed. Simulating
the whole test at 1,500 replicates per level, using the actual per-pair standard errors: **80% power
arrives at about τ = 0.068 grades/year.** The measured 0.163 is roughly **2.4× the detection floor**
— comfortably clear, not a borderline call. It also means a null result would have meant "smaller
than about 0.07 grades/year", not "zero".

### 7.5b Does the 90-day window let climber improvement leak in?

**A fair objection, and the answer needed measuring rather than arguing.** §7.3 claimed the window
makes improvement cancel because both sides are "the same moment." That is too glib: 90 days is not a
moment, and beginners improve fastest of all — about 0.27 grades/year at V2. If a climber visits gym
A at the start of a window and gym B at the end, they have genuinely improved in between, and part of
`level_B − level_A` is *their* gain rather than the gyms' difference.

Scale of the worry: the mean absolute date gap between a climber's two gym visits within a window is
**22 days**. At 0.27 grades/year that is 0.016 grades of possible contamination — small, but "small"
is an argument, and this is checkable.

Two checks. **(1)** Add the within-window date gap `(t_A − t_B)` as a control covariate in every
pairwise regression, which absorbs the leakage directly. **(2)** Halve the window to 45 days, which
mechanically halves the gap — if improvement were driving the result, tightening the window should
shrink τ.

| Setup | Pairs | Q | df | I² | **τ** | Mean gap |
| --- | --- | --- | --- | --- | --- | --- |
| 90-day window, no gap control | 121 | 379.7 | 120 | 68% | **0.1629** | 22 days |
| 90-day window, **gap controlled** | 121 | 367.7 | 120 | 67% | **0.1605** | 22 days |
| 45-day window, no gap control | 108 | 450.8 | 107 | 76% | **0.1832** | 12 days |
| 45-day window, **gap controlled** | 108 | 456.6 | 107 | 77% | **0.1854** | 12 days |

**The result survives both.** Controlling the gap moves τ by 0.7% (0.1629 → 0.1605) — negligible.
And halving the window moves τ *up*, not down (0.163 → 0.183), which is the decisive direction: if
within-window improvement were manufacturing the effect, a tighter window would have shrunk it.

The rise at 45 days is most likely a cleaner measurement rather than a stronger effect — a tighter
window means less within-window drift and improvement blurring each observation, so the same signal
is measured with less contamination. Either way, contamination is not what is producing τ, which is
what the check was for.

### 7.6 From pairs to individual gyms — and what "two independent routes agree" meant

**This is the sentence that was unclear, so here it is in full.**

τ = 0.163 describes **pairs of gyms**, not gyms. "A drifted 0.163 grades/yr faster than B" is a
statement about a *difference*. What you actually want is each gym's own rate: how fast did Movement
Santa Clara drift, full stop?

Getting there needs a **conversion factor**. If individual gym rates have standard deviation `σ`,
then the difference between two independently chosen gyms has standard deviation `σ × √2` — the
standard rule that subtracting two independent quantities adds their variances. So:

```
tau  =  sigma * sqrt(2)        =>        sigma  =  tau / sqrt(2)
                                                =  0.163 / 1.414
                                                =  0.115 grades/yr
```

That is **route 1**: never compute a per-gym number at all, just divide τ by √2.

**Route 2** is completely different work. Take the 121 pairwise differences and *solve* for the 28
individual rates that best reproduce them — a linear system, the same way you would reconstruct
people's heights from a list of "Alice is 3 inches taller than Bob" statements. Then take the
standard deviation of the 28 solved rates directly. Result: **0.105 grades/yr.**

**Route 1 says 0.115. Route 2 says 0.105.** They agree to about 9%.

That agreement is a **check, not a finding**. The two routes share the input data but almost nothing
else — route 1 is one division, route 2 is a 28-unknown weighted least-squares solve with a
constraint and a shrinkage prior. If the solve were broken (as it was on the first attempt, §7.7),
or if τ were being computed wrongly, there is no reason the two would land in the same place. They
did, so both are probably right. That is the entire content of "two independent routes agree."

### 7.7 The trap: a network of relative comparisons has to be connected

**My first attempt at route 2 returned standard errors of about 19,000** — a nonsense answer, and
worth explaining because the failure mode is general and easy to walk into.

**The concrete version, with four gyms.** Every measurement we have is a *difference*: "gym A drifted
0.10 grades/yr faster than gym B." No measurement ever gives a gym's rate on its own. So suppose we
have measured:

```
A - B = +0.10          B - C = +0.05
```

These **chain**. From them, `A − C = +0.15`, and now all three gyms sit on one ladder. Fix the ladder
by requiring the rates to average zero (the zero-sum constraint), and every gym gets a number:
A = +0.10, B = 0.00, C = −0.05. Done.

Now suppose instead we measured:

```
A - B = +0.10          C - D = +0.05
```

and **nothing connecting {A, B} to {C, D}.** We know A sits 0.10 above B. We know C sits 0.05 above
D. We have **no information at all** about where the C/D pair sits relative to the A/B pair — slide
{C, D} up by 1.0, or down by 1.0, and every measurement is reproduced exactly as well. The data
simply does not contain that comparison.

That is not a precision problem more data of the same kind would fix. **The two groups are
*disconnected*, and the offset between them is unknowable in principle.**

**What happened here.** Using only gym pairs with **≥40** shared observations, the 27 surviving gyms
broke into **three disconnected groups of 16, 8, and 3** — plenty of comparisons inside each group,
not one comparison bridging them. Least squares does not report "this is unknowable"; it reports it
the way it always does, by returning enormous standard errors (~19,000) on the unknowable directions.
Worse, the unconstrained gaps between groups leaked into the individual estimates, producing per-gym
rates about three times too large (sd 0.294 instead of 0.105) — numbers that looked plausible enough
to publish if the standard errors had not been so obviously broken.

**The fix** was to relax the threshold to **≥25** shared observations. That admits more gym pairs,
which supplies the bridging comparisons, and all 28 gyms collapse into a **single connected network**.
The solve then behaved: standard errors around 0.05, and the answer agreed with route 1.

**The general lesson: before solving any network of relative comparisons for absolute positions,
check that the network is connected.** §1.2 already performs exactly this check for the gym
corrections themselves (395 of 406 pairs linked) — it was simply not repeated here, which is how it
slipped through.

### 7.8 How big is the effect — and a distinction that matters enormously

Here are two numbers that sound contradictory and are not. Both are correct, and confusing them is
what made §6.5's summary misleading.

**Number 1 — drift *within* a gym, over the full period.** Per-gym rate spread is 0.105 grades/yr,
and the data spans 5.9 years. A gym one standard deviation above average accumulates:

```
0.105 grades/yr  x  5.9 yr  =  0.62 grades
```

Against a total gym-correction spread of 1.29 grades, that is **large** — over six years, a typical
gym's grading standard moves by roughly half the entire range the project is trying to measure. A
single number per gym really is an average over a moving target.

**Number 2 — drift *between* gyms, at the dates their data actually sits.** This is the one that
governs whether drift can distort the *comparison* between gyms, and it is much smaller:

| Quantity | Measured |
| --- | --- |
| Spread of gym mean send dates | **0.59 years** (sd), 2.07 years (range) |
| Total data span | 5.92 years |
| Accumulated drift difference between gyms at their own mean dates | **0.063 grades** (sd) |
| Gym correction spread, for comparison | 0.284 grades (sd) |

**The gyms are roughly contemporaneous.** Their mean dates differ by only 0.59 years — about 10% of
the observation window. Drift only distorts a *comparison* to the extent that the two gyms' data
sits at different points on the calendar, and here it barely does. So the between-gym distortion is
0.063 grades: about a fifth of the correction spread, and small.

**Both are true at once.** Drift is a big effect on the axis it acts on (time within a gym) and a
small one on the axis the corrections live on (differences between roughly contemporaneous gyms).
§6.5 quoted only Number 1 and then discussed the confound, which implied Number 2 was also large. It
is not.

### 7.9 Does drift explain the confound? No — and I tested it wrongly the first time

Recall the confound from §6.2, since it is what started this: a gym's **correction** (how stiff it
grades, one number) correlates **r = 0.607** with **when** its climbers logged, at a slope of 0.827
grades per year. That is suspicious, and §6.3 showed climber improvement explains only about 8% of
it. Gym drift was the other obvious suspect.

**The test I first reported was the wrong one.** I quoted `corr(drift rate, correction) = +0.04` and
concluded drift explains nothing. But that correlation asks "*do fast-drifting gyms have unusual
corrections?*" — which is not the question. Drift distorts a correction through **accumulated**
drift (rate × elapsed time), not through the rate alone. A gym could drift fast and still have
accumulated nothing if its data sits early in the window.

**The right test** is direct: compute each gym's accumulated drift by the date its data actually
sits, subtract it from that gym's correction, and see whether the confound shrinks.

| | Slope on time | Correlation r |
| --- | --- | --- |
| Raw correction | +0.809 | +0.601 |
| **Correction minus accumulated drift** | **+0.955** | **+0.678** |

**Removing drift does not shrink the confound — it slightly enlarges it.** So the conclusion in
§6.5 survives, but only because the correct test happens to agree with the incorrect one. Reported
properly: `corr(accumulated drift, correction) = −0.10`, and correcting for drift moves the confound
the wrong way.

**One honest limit, which §6.5 did not state.** This design measures **relative** drift only — drift
common to *every* gym cancels in the subtraction, exactly as global compression does in §3.2, and
for exactly the same reason. So "drift does not explain the confound" means **relative drift does
not**. A uniform industry-wide trend would be invisible to this estimator and is not ruled out. That
would need an external anchor, which does not exist in this data.

### 7.10 Where this leaves things

| Candidate explanation for the r = 0.607 confound | Status |
| --- | --- |
| Climber improvement | **Measured.** Explains ~8%. |
| Relative gym drift | **Measured.** Explains ~0%, and correcting for it makes the confound worse. |
| Global (industry-wide) drift | **Not testable** with this design — cancels in the differences. |
| Selection: who climbs where, and when | **Untested.** Now the leading candidate. |

Selection means a composition effect rather than a grading effect: if the climbers who happen to log
at a gym late in their own history differ systematically from those who log early, the gym's
correction absorbs that difference. It needs its own design and has not been attempted.

**The net position is better than before, not worse.** What was one large unexplained correlation is
now one large unexplained correlation *plus*: two candidate causes quantified and eliminated, one
candidate identified as structurally untestable, a fourth named as the leading suspect, and — found
along the way, because nothing was looking for it — a real and well-measured drift effect that the
model does not currently represent at all.

### 7.11 What to do about drift itself

Independently of the confound, drift is real and should be modelled. The form is structurally
identical to Option B (§4), on the time axis instead of the ability axis:

```python
drift_g = pm.ZeroSumNormal('drift', sigma=sigma_d, dims='gym')   # sigma_d ~ HalfNormal(0.11)
correction_gt = correction_g + drift_g * (t - t_bar)
```

Zero-sum for the same reason as everywhere else: only relative drift is identifiable (§7.9).
`sigma_d`'s prior comes straight from the measured per-gym sd of 0.105.

**The blocker is the same one as everywhere in Part 6: the model's dataset has no dates.**
`base_bouldering.pkl` is one row per (climber, gym) and carries no time at all. This is now the
strongest argument for building the time-resolved dataset — not to fit climber advancement, which
§6.3 argues against, but to fit drift.


---

## Part 8 — What to do, in order

1. **Let the v10 quadrature sweep finish.** It settles the height form and validates that the
   marginalized geometry samples properly. Nothing below should start before it lands, because
   everything below is built on the same likelihood.
2. **Build stage 1 as its own model**: multi-gym climbers, paired structure, no height terms,
   constant corrections only. Confirm its corrections reproduce the v7/v10 corrections to within the
   0.019-grade tolerance already measured. **If they do not, stop** — the whole plan rests on that
   agreement.
3. **Add Option B to stage 1**: `b_g` zero-sum, `sigma_b ~ HalfNormal(0.05)` from the measured 0.052.
   Compare to step 2 by LOO. The prediction, from τ = 0.074 at ~2× the detection floor, is that B
   wins but not enormously.
4. **Check the residuals for curvature** against latent ability. This is the go/no-go for Option D.
   Do not fit D without it.
5. **Build stage 2** with the cut posterior, drawing `(a_g, b_g)` jointly per replicate.
6. **Re-check §1.3(b)** once B is fitted: with the slope in the model, does the multi-gym ability
   gap still bias anything? It should not, and that is a checkable claim rather than a hope.
7. **Separately, and larger than any of the above: the gym-drift question of §6.5.** 58% of the
   spread in gym corrections tracks *when* people climbed, and only about 8% of that is explained.
   Nothing above touches it. It needs a time-resolved dataset before it can even be asked, so it is
   listed last by dependency, not by importance.

## Where the numbers came from

Everything quoted here was computed on `net50` / `confident` from `runs/base_bouldering.pkl`:

- **§1.2** gym-correction stability — cross-tabulated from the seven `idata_v7_*.nc` traces.
- **§1.3(b)** multi- vs single-gym climber differences — direct group means on the observations table.
- **§2.2–2.4** the compression measurement, the Q/I²/τ decomposition, the 2,000-replicate power
  simulation, and the 3+-gym artifact-free replication — all model-free, computed directly from the
  observations table. None of it depends on any fit.
- **§6.1–6.2** the advancement rate and the time/correction confound — read from
  `src/kaya/viewer_static/v2_time.json`, produced by `scripts/build_v2_time.py` from the raw sends
  table (the only place dates exist). The 3.4× ratio in §6.3 is those two numbers divided.
- **§6.5 and Part 7** the gym-drift measurement — computed directly from 1,191,431 dated boulder
  sends at the 29 `net50` gyms (2020-08-25 to 2026-07-26), read via `KayaDataAccessor.read_sends`.
  Model-free: cells of (climber, gym, 90-day window) with ≥3 sends, paired within window, regressed
  on calendar date with log(send count) as a control. Per-gym rates by weighted least squares with a
  zero-sum constraint and shrinkage at the measured τ. Depends on no fit.

Everything in Parts 2, 6 and 7 is a **measurement**. Everything in Parts 3, 4 and 5 is an
**argument** about what to do with those measurements, and Part 4's verdicts are predictions that
steps 3 and 4 of Part 8 will test.
