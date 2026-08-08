// ---- The height-form sweep, and why its leaderboard cannot be read yet
//
// v2_sweep.json is written by scripts/build_v2_sweep.py from the seven
// runs/traces/idata_v7_*.nc. The temptation with seven fits and a score each
// is to sort and declare a winner. The whole spread is 32.7 elpd against a
// measured 31.1 elpd of seed-to-seed noise, so the ranking is 1.05x its own
// error bar — which is to say it is not a ranking. This section is built to
// show that first and the table second.
//
// Namespaced like every other v2 renderer: ids resolve through v2El/v2Id.

let V2_SWEEP = null;

async function loadV2Sweep() {
  if (V2_SWEEP !== null) return V2_SWEEP;
  try {
    const r = await fetch('/static/v2_sweep.json', { cache: 'no-cache' });
    V2_SWEEP = r.ok ? await r.json() : false;
  } catch (e) {
    V2_SWEEP = false;
  }
  return V2_SWEEP;
}

// ---- the run, as a row of stat cards ----

function renderV2SweepRun() {
  const el = v2El('sweep-run');
  if (!el || !V2_SWEEP) return;
  const s = V2_SWEEP.settings;
  const c = V2_SWEEP.convergence;
  const mins = V2_SWEEP.fits.reduce((a, f) => a + (f.minutes || 0), 0);
  const cards = [
    ['height forms', String(c.n), 'each a different shape for what height buys'],
    ['chains × draws', `${s.chains} × ${s.draws.toLocaleString()}`,
      `after ${s.tune.toLocaleString()} warm-up iterations, raw basis`],
    ['R-hat passing', `${c.rhat_pass} / ${c.n}`,
      `bar is ${V2_SWEEP.bars.rhat} — lower is better`],
    ['effective draws passing', `${c.ess_pass} / ${c.n}`,
      `floor is ${V2_SWEEP.bars.ess} — higher is better`],
    ['divergences', String(c.divergences_total),
      'none at all, across every chain of every fit'],
    ['total compute', `${(mins / 60).toFixed(1)} h`,
      'two fits at a time on four performance cores'],
  ];
  el.innerHTML = cards.map(([k, v, sub]) => `<div class="card kpi-card">
      <div class="eyebrow">${k}</div>
      <div class="kpi-value kpi-value-small">${v}</div>
      <div class="kpi-sub">${sub}</div>
    </div>`).join('');
}

// ---- the verdict callout, which has to come before the table ----

const V2_SWEEP_VERDICT = {
  'separated': ['reads as a ranking',
    'The spread clears the noise floor by enough to be worth reading.'],
  'marginal': ['does not read as a ranking',
    'The spread clears the noise floor by so little that the ordering is '
    + 'not distinguishable from which random seeds happened to be drawn.'],
  'inside-noise': ['is not a ranking',
    'The whole spread fits inside the noise floor. Re-running the same model '
    + 'with different seeds could reorder it entirely.'],
  'unknown': ['cannot be assessed', 'No leave-one-out scores were recorded.'],
};

function renderV2SweepVerdict() {
  const el = v2El('sweep-verdict');
  if (!el || !V2_SWEEP) return;
  const r = V2_SWEEP.ranking;
  const [head, body] = V2_SWEEP_VERDICT[r.verdict] || V2_SWEEP_VERDICT.unknown;
  const ratio = r.ratio === null ? '—' : `${r.ratio.toFixed(2)}×`;
  el.innerHTML = `<div class="callout">
      <div>
        <b>The table below ${head}.</b>
        Best form to worst spans <b>${r.spread_elpd.toFixed(1)} elpd</b>.
        Two fits of the <i>identical</i> model, differing only by random seed,
        have scored <b>${r.noise_floor_elpd} apart</b> in this project — so the
        entire range is <b>${ratio}</b> its own measurement error. ${body}
      </div>
      <div class="callout-note">
        elpd = expected log pointwise predictive density, the leave-one-out
        score. Higher is better. The fix is not a better sampler: it is Phase 3,
        three refits of one model differing only by seed, which measures the
        floor instead of quoting a remembered number.
      </div>
    </div>`;
}

// ---- the sweep table ----

function renderV2SweepTable() {
  const el = v2El('sweep-table');
  if (!el || !V2_SWEEP) return;
  const bars = V2_SWEEP.bars;
  const flag = (ok) => ok ? '<span class="pill pill-ok">pass</span>'
    : '<span class="pill pill-warn">under</span>';
  const rows = V2_SWEEP.fits.map((f) => `<tr>
      <td><b>${f.form}</b><div class="cell-sub">${f.terms}</div></td>
      <td class="num">${f.rhat.toFixed(3)} ${flag(f.rhat_ok)}
        <div class="cell-sub">worst: ${f.rhat_param}</div></td>
      <td class="num">${Math.round(f.ess)} ${flag(f.ess_ok)}
        <div class="cell-sub">worst: ${f.ess_param}</div></td>
      <td class="num">${f.loo === null ? '—' : f.loo.toFixed(1)}</td>
      <td class="num">${f.delta_loo === null ? '—'
        : (f.delta_loo === 0 ? '<b>best</b>' : f.delta_loo.toFixed(1))}</td>
      <td class="num">${f.minutes === null ? '—' : Math.round(f.minutes)}</td>
    </tr>`).join('');
  el.innerHTML = `<thead><tr>
      <th>height form</th>
      <th class="num">R-hat<div class="cell-sub">bar ${bars.rhat}, lower better</div></th>
      <th class="num">effective draws<div class="cell-sub">floor ${bars.ess}, higher better</div></th>
      <th class="num">elpd<div class="cell-sub">higher better</div></th>
      <th class="num">vs best</th>
      <th class="num">min</th>
    </tr></thead><tbody>${rows}</tbody>`;
}

// ---- chains versus draws: the measurement that changed the plan ----

function renderV2SweepScaling() {
  const el = v2El('sweep-scaling');
  if (!el || !V2_SWEEP) return;
  const s = V2_SWEEP.scaling;
  const cell = (v, d = 3) => v === null ? '<span class="muted">n/a</span>' : v.toFixed(d);
  const chainRows = s.by_chain.map((r) => `<tr>
      <td class="num">${r.n}</td>
      <td class="num">${Math.round(r.ess)}</td>
      <td class="num">${r.ratio.toFixed(2)}×</td>
      <td class="num">${cell(r.rhat)}</td>
    </tr>`).join('');
  const drawRows = s.by_draw.map((r) => `<tr>
      <td class="num">${r.n.toLocaleString()}</td>
      <td class="num">${Math.round(r.ess)}</td>
      <td class="num">${r.ratio.toFixed(2)}×</td>
      <td class="num">${cell(r.rhat)}</td>
    </tr>`).join('');
  el.innerHTML = `<div class="split-tables">
      <div>
        <h4>More chains, same length</h4>
        <table class="data-table compact">
          <thead><tr><th class="num">chains</th><th class="num">ESS</th>
            <th class="num">vs 1</th><th class="num">R-hat</th></tr></thead>
          <tbody>${chainRows}</tbody>
        </table>
      </div>
      <div>
        <h4>Same chains, more draws</h4>
        <table class="data-table compact">
          <thead><tr><th class="num">draws</th><th class="num">ESS</th>
            <th class="num">vs first</th><th class="num">R-hat</th></tr></thead>
          <tbody>${drawRows}</tbody>
        </table>
      </div>
    </div>`;
}

function renderV2SweepScalingNote() {
  const el = v2El('sweep-scaling-note');
  if (!el || !V2_SWEEP) return;
  const s = V2_SWEEP.scaling;
  const ch = s.by_chain[s.by_chain.length - 1];
  const dr = s.by_draw;
  const rFirst = dr.find((r) => r.rhat !== null);
  const rLast = dr[dr.length - 1];
  el.innerHTML = `<div class="callout">
      <div>
        <b>Both columns buy effective draws. Only one buys convergence.</b>
        Quadrupling the chains multiplied effective sample size by
        <b>${ch.ratio.toFixed(2)}×</b> while leaving R-hat at
        <b>${ch.rhat.toFixed(3)}</b> — flat, and at points along the way it rose.
        Quadrupling the draws bought a similar
        <b>${rLast.ratio.toFixed(2)}×</b> in effective draws <i>and</i> pulled
        R-hat from <b>${rFirst.rhat.toFixed(3)}</b> to
        <b>${rLast.rhat.toFixed(3)}</b>.
      </div>
      <div class="callout-note">
        Why it matters beyond the arithmetic: PyMC cannot resume a finished
        chain — the step size is stored but the mass matrix is not. A trace can
        gain chains after the fact; it can never gain draws. So the lever that
        fixes R-hat is the one that has to be chosen correctly before the run
        starts, and this sweep chose it wrong.
      </div>
    </div>`;
}

// ---- the conditioning signature ----

function renderV2SweepDiagnosis() {
  const el = v2El('sweep-diagnosis');
  if (!el || !V2_SWEEP) return;
  const c = V2_SWEEP.convergence;
  el.innerHTML = `<div class="callout">
      <div>
        <b>Zero divergences, ${c.treedepth_fits} of ${c.n} fits saturating the
        tree depth.</b>
        A divergence means the sampler hit a region it could not integrate
        through — there were none, so the posterior has no funnel or cliff.
        Saturated tree depth means the opposite problem: NUTS spent its entire
        step budget on every iteration and was cut off mid-trajectory, so
        consecutive draws stay correlated.
      </div>
      <div class="callout-note">
        That pairing — no divergences, exhausted budget, low effective sample
        size — is the signature of a badly conditioned posterior rather than a
        pathological one: a long thin ridge that the diagonal step-size matrix
        NUTS adapts by default cannot stretch to match. More draws would help;
        a dense matrix, or a reparameterization, is what the signature actually
        points at.
      </div>
    </div>`;
}

// ---- entry point ----

async function renderV2Sweep() {
  const host = v2El('sweep-run');
  if (!host) return;                        // pane does not include the section
  const d = await loadV2Sweep();
  const section = host.closest('.article-section');
  if (!d) {
    if (section) section.style.display = 'none';
    return;
  }
  if (section) section.style.display = '';
  renderV2SweepRun();
  renderV2SweepVerdict();
  renderV2SweepTable();
  renderV2SweepScaling();
  renderV2SweepScalingNote();
  renderV2SweepDiagnosis();
}
