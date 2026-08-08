// ---- The factor ledger: every effect that could distort a gym correction
//
// Renders tabs/grading-findings/factors.html from v2_factors.json, which
// scripts/build_v2_factors.py assembles. The prose lives in the template
// because it is interpretation; everything numeric is a <span data-fx="key">
// filled here, so a number cannot be stale in one place and current in another.
//
// NOT namespaced through v2El/v2Id. Those exist because three panes mount the
// same shared renderers and element ids must be unique per document. This
// section is mounted by exactly one pane and owns its `gf-fx-` ids outright,
// so going through v2El would resolve them against whichever namespace was set
// last. Same reasoning as 14-runlog.js.
//
// Hides its own section if the payload is missing, rather than leaving a page
// of empty tables behind — a half-rendered ledger reads as "these effects were
// measured at zero", which is the opposite of what a missing file means.

let V2_FACTORS = null;

const fxEl = (name) => document.getElementById(`gf-fx-${name}`);

async function loadV2Factors() {
  if (V2_FACTORS !== null) return V2_FACTORS;
  try {
    const r = await fetch('/static/v2_factors.json', { cache: 'no-cache' });
    V2_FACTORS = r.ok ? await r.json() : false;
  } catch (e) {
    V2_FACTORS = false;
  }
  return V2_FACTORS;
}

// The ledger itself. Order is by how much each effect could move the headline
// number, which is deliberately NOT the order the sections appear in — the
// prose runs in a teaching order (what is handled, then what is not), the
// table runs in a triage order.
function fxLedger(F) {
  const sd = F.correction_sd;
  const pct = (v) => `${Math.round((100 * v) / sd)}%`;
  return [
    { name: 'Ceiling attainment',
      what: 'how many days they climbed at that gym',
      size: F.exposure.contrast, note: '3 days vs 100 days',
      state: 'in', how: 'ex-Gaussian shortfall, <code>n_visits</code>' },
    { name: 'Grade compression',
      what: 'the correction changes with the climber&rsquo;s ability',
      size: F.compression.differential,
      note: `across V${F.compression.ability_p10}–V${F.compression.ability_p90}`,
      state: 'measured', how: 'per-gym quadratic in latent ability' },
    { name: 'Height and arm span',
      what: 'body dimensions, curving differently by gender',
      size: Math.max(...F.height.curves.map((c) => c.span)),
      note: 'across ±8in of height', state: 'in',
      how: 'quadratic × gender' },
    { name: 'Climber advancement',
      what: 'they improved between the two visits',
      size: F.advancement.timing.bias_max,
      note: `worst gym pair; median ${F.advancement.timing.bias_median}`,
      state: 'measured', how: 'fixed offset, needs only the send date' },
    { name: 'Gym grading drift',
      what: 'the gym&rsquo;s own standards moved over time',
      size: F.drift.accum_sd,
      note: `between gyms; ${F.drift.accumulated_span} accumulated overall`,
      state: 'measured', how: 'time-varying correction, needs windowed rows' },
    { name: 'Grade quantization',
      what: 'V-grades are integers and the max is often a single send',
      size: null, note: 'absorbed, not isolated', state: 'partial',
      how: '<code>n_at_max</code> exists but is unused in the fitted model' },
    { name: 'Gender coding',
      what: 'inferred from a first name, not recorded',
      size: null, note: 'attenuates the effect it acts on', state: 'partial',
      how: 'probability weight + confident-names-only subset' },
    { name: 'Selection',
      what: 'who logs, when, and at which gym',
      size: null, note: 'not yet quantified', state: 'open',
      how: 'no approach settled' },
  ].map((r) => ({ ...r, pct: r.size === null ? null : pct(r.size) }));
}

const FX_STATE = {
  in: ['pill-ok', 'in the model'],
  measured: ['pill-warn', 'measured, not modelled'],
  partial: ['pill-warn', 'partly handled'],
  open: ['pill-warn', 'open'],
};

function renderFxTable(F) {
  const el = document.getElementById('gf-factors-table');
  if (!el) return;
  const rows = fxLedger(F).sort((a, b) => (b.size ?? -1) - (a.size ?? -1));
  el.innerHTML = `<thead><tr>
      <th>factor</th><th>what it is</th>
      <th class="num">grades<div class="cell-sub">bigger = worse</div></th>
      <th class="num">vs the signal<div class="cell-sub">of ${F.correction_sd} sd</div></th>
      <th>status</th><th>how it is or would be handled</th>
    </tr></thead><tbody>${rows.map((r) => {
      const [cls, label] = FX_STATE[r.state];
      return `<tr>
        <td><b>${r.name}</b></td>
        <td>${r.what}</td>
        <td class="num">${r.size === null ? '—' : r.size.toFixed(3)}
          <div class="cell-sub">${r.note}</div></td>
        <td class="num">${r.pct ?? '—'}</td>
        <td><span class="pill ${cls}">${label}</span></td>
        <td>${r.how}</td></tr>`;
    }).join('')}</tbody>`;
}

function renderFxAdvPairs(F) {
  const el = fxEl('adv-pairs');
  if (!el) return;
  const t = F.advancement.timing;
  el.innerHTML = `<thead><tr>
      <th>gym pair</th>
      <th class="num">climbers at both</th>
      <th class="num">mean date gap<div class="cell-sub">years</div></th>
      <th class="num">grades misread as grading<div class="cell-sub">lower is better</div></th>
    </tr></thead><tbody>${t.worst.map((w) => `<tr>
        <td>${w.a} <span class="cell-sub">vs</span> ${w.b}</td>
        <td class="num">${w.n}</td>
        <td class="num">${w.gap >= 0 ? '+' : ''}${w.gap.toFixed(2)}</td>
        <td class="num"><b>${w.bias.toFixed(3)}</b></td></tr>`).join('')}
      <tr><td colspan="3"><i>median across all ${t.n_gym_pairs} pairs</i></td>
        <td class="num">${t.bias_median.toFixed(3)}</td></tr>
    </tbody>`;
}

function renderFxHeightTable(F) {
  const el = fxEl('height');
  if (!el) return;
  el.innerHTML = `<thead><tr>
      <th>question</th>
      <th class="num">median &sigma;</th>
      <th class="num">range over ${F.height.n_seeds} seeds</th>
      <th class="num">clears 2&sigma;</th>
    </tr></thead><tbody>${F.height.comparisons.map((c) => {
      const ok = c.median > 2;
      return `<tr>
        <td>${c.label}</td>
        <td class="num"><b>${c.median >= 0 ? '+' : ''}${c.median.toFixed(2)}</b></td>
        <td class="num">${c.min.toFixed(2)} to ${c.max.toFixed(2)}</td>
        <td class="num">${c.clears}/${F.height.n_seeds}
          <span class="pill ${ok ? 'pill-ok' : 'pill-warn'}">${
            ok ? 'real' : 'not detectable'}</span></td></tr>`;
    }).join('')}</tbody>`;
}

function renderFxExposureChart(F) {
  const el = fxEl('exposure');
  if (!el || typeof Plotly === 'undefined') return;
  const c = F.exposure.curve;
  const layout = chartLayout('days climbed at that gym');
  layout.height = 320;
  layout.margin = { l: 66, r: 20, t: 12, b: 60 };
  layout.xaxis = { ...layout.xaxis, type: 'log', automargin: false,
    title: { text: 'days climbed at that gym (log scale)', standoff: 10 } };
  layout.yaxis = { ...layout.yaxis,
    title: { text: 'assumed shortfall (grades)', standoff: 10 } };
  layout.showlegend = false;
  Plotly.newPlot(el, [{
    type: 'scatter', mode: 'lines+markers',
    x: c.map((p) => p.days), y: c.map((p) => p.shortfall),
    line: { color: cssVar('--lg-cat-1'), width: 2.6 },
    marker: { size: 7 },
    hovertemplate: '%{x} days: %{y:.3f} grades below ceiling<extra></extra>',
  }], layout, { displayModeBar: false, responsive: true });

  const note = fxEl('exposure-note');
  if (note) {
    note.innerHTML = `How far below their true ceiling the model assumes a `
      + `climber logged, given how many days they climbed there. Read off the `
      + `fitted posterior of <code>${F.exposure.fit}</code>. The gap between `
      + `3 days and 100 days is <b>${F.exposure.contrast.toFixed(2)} grades</b> `
      + `— larger than any other single effect on this page, which is why it `
      + `is the one the model was built around first.`;
  }
}

function renderFxHeightChart(F) {
  const el = fxEl('height-chart');
  if (!el || typeof Plotly === 'undefined') return;
  const colors = [cssVar('--lg-cat-1'), cssVar('--lg-cat-3')];
  const traces = F.height.curves.map((c, i) => ({
    type: 'scatter', mode: 'lines', x: c.x, y: c.y, name: c.label,
    line: { color: colors[i], width: 2.6 },
    hovertemplate: `%{x:+.0f}in: %{y:+.2f} grades<extra>${c.label}</extra>`,
  }));
  const layout = chartLayout('height, inches from the median');
  layout.height = 340;
  layout.margin = { l: 66, r: 20, t: 12, b: 76 };
  layout.xaxis = { ...layout.xaxis, automargin: false, dtick: 2,
    title: { text: `inches from the median (${F.height.median_height}in)`, standoff: 10 } };
  layout.yaxis = { ...layout.yaxis,
    title: { text: 'ability (grades, centred)', standoff: 10 } };
  Plotly.newPlot(el, traces, layout, { displayModeBar: false, responsive: true });

  const note = fxEl('height-note');
  if (note) {
    const [m, f] = F.height.curves;
    note.innerHTML = `The fitted <code>${F.height.winner}</code> curve, with `
      + `each gym's grading already removed. <b>The curvatures have opposite `
      + `signs</b> — ${m.label} ${m.curv.toFixed(5)}/in² (concave, an optimum), `
      + `${f.label} ${f.curv >= 0 ? '+' : ''}${f.curv.toFixed(5)}/in² (convex). `
      + `That is why a shared bend measures nothing: averaged, they cancel.`;
  }
}

// The decisions, ordered by movement-per-unit-of-work. Hand-maintained, like
// the run log's plan — "what should we do about it" is not recoverable from a
// tau.
function renderFxPlan(F) {
  const el = fxEl('plan');
  if (!el) return;
  const steps = [
    { title: 'Dated base snapshot <i>(done)</i>',
      why: `The aggregate every fit reads now carries max_send_date, `
        + `first_send and last_send, and <code>build_base_snapshot.py</code> `
        + `rebuilds it — previously nothing in the repo wrote that file at `
        + `all. Coverage is ${F.coverage.pct}% of modelled pairs `
        + `(${F.coverage.date_min} to ${F.coverage.date_max}), and the rebuild `
        + `reproduced the old snapshot row for row, so no fitted result moved. `
        + `This was the blocker under the two items below.`,
      state: 'done' },
    { title: 'Add climber advancement as a fixed offset',
      why: `The best ratio of correction to effort on this page. It needs only `
        + `the send date carried through an aggregation that currently discards `
        + `it — no new rows, no re-derived dataset — and it removes up to `
        + `${F.advancement.timing.bias_max.toFixed(3)} grades of bias from the `
        + `worst gym pair. Fixed, never fitted: a free parameter absorbs 3.4× `
        + `its true value.`,
      state: 'next' },
    { title: 'Put <code>n_at_max</code> into the marginalized likelihood',
      why: `A port, not a design question — it is already in the data and `
        + `already in the PyMC path, just absent from the likelihood the `
        + `current fits sample. It strengthens the effect that is already `
        + `doing the most work.`,
      state: 'next' },
    { title: 'Add per-gym grade compression, quadratic',
      why: `${F.compression.differential} grades across the ability range, `
        + `confirmed artifact-free, with real curvature (τ = `
        + `${F.compression.curvature.tau}) so a straight line is the wrong `
        + `shape. Needs the zero-sum constraint on the slopes and must key off `
        + `latent ability, never the observed grade.`,
      state: 'queued' },
    { title: 'Build the time-resolved dataset, then gym drift',
      why: `The only item here that requires rebuilding the unit of `
        + `observation to (climber, gym, 90-day window). Worth `
        + `${F.drift.accum_sd} grades between gyms — the smallest confirmed `
        + `effect — which is why it is last despite having the largest raw τ.`,
      state: 'queued' },
    { title: 'Quantify selection',
      why: `No approach settled. It is the residual after advancement (~8%), `
        + `relative drift (none) and brand composition (part), and it is the `
        + `only item on this list where the next step is a question rather `
        + `than a task.`,
      state: 'open' },
  ];
  const pills = { done: ['pill-ok', 'done'], next: ['pill-ok', 'do next'],
    queued: ['pill-warn', 'queued'],
    open: ['pill-warn', 'needs a decision'] };
  el.innerHTML = steps.map((s, i) => {
    const [cls, label] = pills[s.state];
    return `<div class="card runlog-card">
      <div class="runlog-card-head">
        <span class="runlog-step">${i + 1}</span>
        <h4>${s.title}</h4>
        <span class="pill ${cls}">${label}</span>
      </div>
      <p class="prose">${s.why}</p></div>`;
  }).join('');
}

// ---- the numeric slots ----
function renderFxInline(F) {
  const c = F.compression;
  const d = F.drift;
  const t = F.advancement.timing;
  const e = F.exposure;
  const h = F.height;
  const q = F.quantization;
  const vals = {
    corr_sd: F.correction_sd, corr_range: F.correction_range,
    exp_pct_le5: e.pct_le5, exp_pct_ge100: e.pct_ge100,
    exp_ratio_median: e.ratio_median, exp_ratio_p90: e.ratio_p90,
    exp_fit: e.fit, exp_ll0: e.log_lambda0.toFixed(3),
    exp_kappa: e.kappa.toFixed(3),
    adv_gap_median: t.gap_median, adv_gap_1y: t.gap_over_1y,
    adv_n_pairs: t.n_gym_pairs, adv_min_shared: t.min_shared,
    adv_bias_median: t.bias_median.toFixed(3), adv_bias_max: t.bias_max.toFixed(3),
    adv_bias_max_pct: Math.round((100 * t.bias_max) / F.correction_sd),
    adv_rate: t.rate_used,
    drift_k: d.het.k, drift_i2: d.het.i2, drift_tau: d.het.tau,
    drift_floor: d.het.floor, drift_per_gym: d.per_gym_sd, drift_route1: d.route1,
    drift_span: d.span_years, drift_accum: d.accumulated_span,
    drift_date_sd: d.mean_date_sd, drift_accum_sd: d.accum_sd,
    comp_k: c.linear.k, comp_i2: c.linear.i2, comp_tau: c.linear.tau,
    comp_floor: c.linear.floor, comp_clean_tau: c.clean.tau,
    curv_tau: c.curvature.tau, curv_i2: c.curvature.i2,
    comp_p10: c.ability_p10, comp_p90: c.ability_p90,
    comp_diff: c.differential,
    comp_diff_pct: Math.round((100 * c.differential) / F.correction_range),
    h_n: h.n.toLocaleString(), h_seeds: h.n_seeds, h_winner: h.winner,
    h_winner_count: h.winner_count,
    g_conf_male: F.gender.confident_male, g_conf_female: F.gender.confident_female,
    g_ambiguous: F.gender.ambiguous,
    q_pct_used: q.pct_sends_used, q_n_sends: q.n_sends.toLocaleString(),
    q_pct_once: q.pct_once, q_pct_10: q.pct_10plus,
    cov_pct: F.coverage.pct, cov_dated: F.coverage.dated_pairs.toLocaleString(),
    cov_model: F.coverage.model_pairs.toLocaleString(),
    cov_missing: F.coverage.missing.toLocaleString(),
    cov_min: F.coverage.date_min, cov_max: F.coverage.date_max,
    conf_r: d.confound.raw_r.toFixed(2),
    conf_raw_slope: d.confound.raw_slope.toFixed(3),
    conf_adj_slope: d.confound.adj_slope.toFixed(3),
  };
  document.querySelectorAll('[data-fx]').forEach((node) => {
    const v = vals[node.dataset.fx];
    if (v !== undefined) node.textContent = v;
  });
}

async function renderV2Factors() {
  const F = await loadV2Factors();
  const section = document.getElementById('gf-factors');
  if (!F) {
    if (section) section.style.display = 'none';
    return;
  }
  renderFxInline(F);
  renderFxTable(F);
  renderFxAdvPairs(F);
  renderFxHeightTable(F);
  renderFxExposureChart(F);
  renderFxHeightChart(F);
  renderFxPlan(F);
  if (typeof v2Typeset === 'function') v2Typeset(section);
}
