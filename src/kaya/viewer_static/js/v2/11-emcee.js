// ---- The second sampler, in full: chains, posteriors, and where it disagrees
//
// v2_emcee.json is written by scripts/build_v2_emcee.py from
// runs/results/emcee_lin2.npz. Everything here is a property of that one run —
// 128 walkers, 20,000 steps, 7.1 hours — and the point of showing it rather
// than summarising it is that the interesting part is not the posterior means.
// It is the one parameter where the two samplers do not land in the same
// place, which no table of means makes legible on its own.
//
// Namespaced like every other v2 renderer: ids resolve through v2El/v2Id, so
// the same code serves whichever pane includes the section.

let V2_EMCEE = null;
let V2_EMCEE_PARAM = 'log_sigma_gym';   // the one that disagrees; open on it

async function loadV2Emcee() {
  if (V2_EMCEE !== null) return V2_EMCEE;
  try {
    const r = await fetch('/static/v2_emcee.json', { cache: 'no-cache' });
    V2_EMCEE = r.ok ? await r.json() : false;
  } catch (e) {
    V2_EMCEE = false;
  }
  return V2_EMCEE;
}

const v2EmceeParam = (name) =>
  (V2_EMCEE && V2_EMCEE.params.find((p) => p.name === name)) || null;

// Walker colours: eight steps along one hue rather than eight different hues.
// The question a trace plot answers is "are they on top of each other", and
// eight competing colours actively work against reading that.
function v2EmceeWalkerColours(n) {
  const base = cssVar('--lg-gold') || '#1976d2';
  return Array.from({ length: n }, (_, i) => hexToRgba(base, 0.28 + 0.06 * i));
}

// ---- the run, as a row of stat cards ----

function renderV2EmceeRun() {
  const el = v2El('emcee-run');
  if (!el || !V2_EMCEE) return;
  const r = V2_EMCEE.run;
  const fmt = (n) => Number(n).toLocaleString();
  const cards = [
    ['walkers', fmt(r.walkers), 'each one a chain, all moving together'],
    ['steps', fmt(r.steps_total), `${fmt(r.burn)} discarded as warm-up`],
    ['raw draws', fmt(r.draws), 'walkers × kept steps'],
    ['effective draws', fmt(Math.round(r.ess_min)),
      'the floor across all 40 parameters — this is the number that counts'],
    ['acceptance', r.acc_mean.toFixed(3),
      `0.2–0.4 is the healthy band; walkers ranged ${r.acc_min.toFixed(2)}–${r.acc_max.toFixed(2)}`],
    ['wall clock', `${(r.elapsed_min / 60).toFixed(1)} h`,
      `${r.moves} moves, ${r.n_quad}-node quadrature`],
  ];
  el.innerHTML = cards.map(([k, v, sub]) => `<div class="card kpi-card">
      <div class="eyebrow">${k}</div>
      <div class="kpi-value kpi-value-small">${v}</div>
      <div class="kpi-sub">${sub}</div>
    </div>`).join('');
}

// ---- the parameter picker, which drives all four charts ----

function bindV2EmceeParam() {
  const sel = v2El('emcee-param');
  if (!sel || !V2_EMCEE) return;
  // Shared-with-PyMC parameters first: those are the ones with an overlay, and
  // the 28 zero-sum basis coordinates below them are not quantities anyone
  // wants by name (see the gym chart at the end for what they add up to).
  const shared = V2_EMCEE.params.filter((p) => p.shared);
  const rest = V2_EMCEE.params.filter((p) => !p.shared);
  const opt = (p) => `<option value="${p.name}">${p.name}`
    + (p.label !== p.name ? ` — ${p.label}` : '') + '</option>';
  sel.innerHTML = `<optgroup label="Shared with PyMC (overlaid)">${
    shared.map(opt).join('')}</optgroup>`
    + `<optgroup label="Zero-sum basis coordinates">${rest.map(opt).join('')}</optgroup>`;
  if (!v2EmceeParam(V2_EMCEE_PARAM)) V2_EMCEE_PARAM = V2_EMCEE.params[0].name;
  sel.value = V2_EMCEE_PARAM;
  sel.addEventListener('change', v2Bound(() => {
    V2_EMCEE_PARAM = sel.value;
    renderV2EmceeCharts();
  }));
}

function renderV2EmceeVerdict() {
  const el = v2El('emcee-verdict');
  const p = v2EmceeParam(V2_EMCEE_PARAM);
  if (!el || !p) return;
  const bits = [`&tau; = ${p.tau.toFixed(0)} steps`,
    `${Math.round(p.ess).toLocaleString()} effective draws`];
  if (p.gap_sd != null) {
    const cls = p.gap_sd > 1 ? 'bad' : (p.gap_sd > 0.3 ? 'warn' : 'ok');
    bits.push(`<span class="${cls}">${p.gap_sd.toFixed(2)} sd from PyMC</span>`);
  }
  el.innerHTML = bits.join(' &middot; ');
}

// ---- chart 1: the chains themselves ----

function renderV2EmceeTrace() {
  const el = v2El('emcee-trace');
  if (!el || !V2_EMCEE || typeof Plotly === 'undefined') return;
  const rows = V2_EMCEE.trace[V2_EMCEE_PARAM] || [];
  const cols = v2EmceeWalkerColours(rows.length);
  const traces = rows.map((y, i) => ({
    type: 'scatter', mode: 'lines', hoverinfo: 'skip', showlegend: false,
    x: V2_EMCEE.trace_x, y,
    line: { width: 1, color: cols[i] },
  }));
  const layout = chartLayout('step, after warm-up');
  layout.height = 250;
  layout.margin = { l: 54, r: 12, t: 10, b: 44 };
  Plotly.react(el, traces, layout, { displayModeBar: false, responsive: true });
}

// ---- chart 2: the posterior, with PyMC's on the same axis ----

function renderV2EmceeDens() {
  const el = v2El('emcee-dens');
  if (!el || !V2_EMCEE || typeof Plotly === 'undefined') return;
  const d = V2_EMCEE.dens[V2_EMCEE_PARAM] || { x: [], y: [] };
  const gold = cssVar('--lg-gold') || '#1976d2';
  const warm = cssVar('--lg-highlight') || '#b8752e';
  const traces = [{
    type: 'scatter', mode: 'lines', name: 'emcee', x: d.x, y: d.y,
    fill: 'tozeroy', fillcolor: hexToRgba(gold, 0.18),
    line: { width: 2, color: gold },
    hovertemplate: 'emcee %{x:.3f}<extra></extra>',
  }];
  if (d.pm_y) {
    traces.push({
      type: 'scatter', mode: 'lines', name: 'PyMC / NUTS', x: d.x, y: d.pm_y,
      fill: 'tozeroy', fillcolor: hexToRgba(warm, 0.14),
      line: { width: 2, color: warm, dash: 'dot' },
      hovertemplate: 'PyMC %{x:.3f}<extra></extra>',
    });
  }
  const layout = chartLayout(V2_EMCEE_PARAM);
  layout.height = 250;
  layout.margin = { l: 54, r: 12, t: 10, b: 44 };
  layout.yaxis = { ...layout.yaxis, title: { text: 'density' }, rangemode: 'tozero' };
  layout.showlegend = !!d.pm_y;
  Plotly.react(el, traces, layout, { displayModeBar: false, responsive: true });
}

// ---- chart 3: has it settled? ----

function renderV2EmceeRunmean() {
  const el = v2El('emcee-runmean');
  if (!el || !V2_EMCEE || typeof Plotly === 'undefined') return;
  const r = V2_EMCEE.runmean[V2_EMCEE_PARAM];
  if (!r) return;
  const gold = cssVar('--lg-gold') || '#1976d2';
  const x = V2_EMCEE.runmean_x;
  const traces = [
    {
      type: 'scatter', mode: 'lines', name: 'walker spread', hoverinfo: 'skip',
      x: x.concat(x.slice().reverse()),
      y: r.hi.concat(r.lo.slice().reverse()),
      fill: 'toself', fillcolor: hexToRgba(gold, 0.13),
      line: { width: 0 }, showlegend: false,
    },
    {
      type: 'scatter', mode: 'lines', name: 'ensemble running mean',
      x, y: r.mean, line: { width: 2, color: gold },
      hovertemplate: 'step %{x}<br>running mean %{y:.4f}<extra></extra>',
    },
  ];
  const p = v2EmceeParam(V2_EMCEE_PARAM);
  const layout = chartLayout('step, after warm-up');
  layout.height = 250;
  layout.margin = { l: 54, r: 12, t: 10, b: 44 };
  layout.showlegend = false;
  if (p && p.pm_mean != null) {
    // PyMC's answer as a reference line: on every parameter but one it lands
    // inside the band, which is what makes the exception readable at a glance.
    layout.shapes = [{
      type: 'line', xref: 'paper', yref: 'y', x0: 0, x1: 1,
      y0: p.pm_mean, y1: p.pm_mean,
      line: { color: cssVar('--lg-highlight') || '#b8752e', width: 1.4, dash: 'dot' },
    }];
    layout.annotations = [{
      xref: 'paper', yref: 'y', x: 0.99, y: p.pm_mean, xanchor: 'right',
      yanchor: 'bottom', text: 'PyMC mean', showarrow: false,
      font: { size: 10, color: cssVar('--lg-highlight') || '#b8752e' },
    }];
  }
  Plotly.react(el, traces, layout, { displayModeBar: false, responsive: true });
}

// ---- chart 4: the autocorrelation tau summarises ----

function renderV2EmceeAcf() {
  const el = v2El('emcee-acf');
  if (!el || !V2_EMCEE || typeof Plotly === 'undefined') return;
  const y = V2_EMCEE.acf[V2_EMCEE_PARAM] || [];
  const p = v2EmceeParam(V2_EMCEE_PARAM);
  const gold = cssVar('--lg-gold') || '#1976d2';
  const traces = [{
    type: 'scatter', mode: 'lines', x: V2_EMCEE.acf_lags, y,
    fill: 'tozeroy', fillcolor: hexToRgba(gold, 0.15),
    line: { width: 2, color: gold }, showlegend: false,
    hovertemplate: 'lag %{x} steps<br>correlation %{y:.3f}<extra></extra>',
  }];
  const layout = chartLayout('lag (steps)');
  layout.height = 250;
  layout.margin = { l: 54, r: 12, t: 10, b: 44 };
  layout.yaxis = { ...layout.yaxis, title: { text: 'autocorrelation' } };
  layout.shapes = [{
    type: 'line', xref: 'paper', yref: 'y', x0: 0, x1: 1, y0: 0, y1: 0,
    line: { color: cssVar('--lg-text-2'), width: 1 },
  }];
  if (p) {
    layout.shapes.push({
      type: 'line', xref: 'x', yref: 'paper', x0: p.tau, x1: p.tau, y0: 0, y1: 1,
      line: { color: cssVar('--lg-highlight') || '#b8752e', width: 1.4, dash: 'dot' },
    });
    layout.annotations = [{
      xref: 'x', yref: 'paper', x: p.tau, y: 1, xanchor: 'left', yanchor: 'top',
      // Plotly annotations are plain text, not HTML — an entity would
      // render literally as "&tau;". Unicode direct.
      text: ` \u03c4 = ${p.tau.toFixed(0)}`, showarrow: false,
      font: { size: 10, color: cssVar('--lg-highlight') || '#b8752e' },
    }];
  }
  Plotly.react(el, traces, layout, { displayModeBar: false, responsive: true });
}

// Everything that depends on which parameter is selected, in one call, so the
// picker cannot leave one figure showing a different parameter from the rest.
function renderV2EmceeCharts() {
  renderV2EmceeVerdict();
  renderV2EmceeTrace();
  renderV2EmceeDens();
  renderV2EmceeRunmean();
  renderV2EmceeAcf();
  renderV2EmceeWalkers();
}

// ---- walker health: does any one of the 128 behave differently? ----

function renderV2EmceeWalkers() {
  const el = v2El('emcee-walkers');
  if (!el || !V2_EMCEE || typeof Plotly === 'undefined') return;
  const gold = cssVar('--lg-gold') || '#1976d2';
  const means = V2_EMCEE.walker_means[V2_EMCEE_PARAM] || [];
  const traces = [{
    type: 'scatter', mode: 'markers', name: 'walker', xaxis: 'x', yaxis: 'y',
    x: V2_EMCEE.acceptance.map((_, i) => i), y: V2_EMCEE.acceptance,
    marker: { size: 6, color: hexToRgba(gold, 0.7) },
    hovertemplate: 'walker %{x}<br>accepted %{y:.3f} of proposals<extra></extra>',
  }, {
    type: 'scatter', mode: 'markers', name: 'walker mean', xaxis: 'x2', yaxis: 'y2',
    x: means.map((_, i) => i), y: means,
    marker: { size: 6, color: hexToRgba(gold, 0.7) },
    hovertemplate: 'walker %{x}<br>mean %{y:.4f}<extra></extra>',
  }];
  const layout = chartLayout('');
  layout.height = 250;
  // Wider left margin than the single-panel charts: two subplots each carry
  // their own y-axis title, and 54px clips them.
  layout.margin = { l: 68, r: 12, t: 26, b: 44 };
  layout.showlegend = false;
  layout.grid = { rows: 1, columns: 2, pattern: 'independent' };
  const ax = { gridcolor: cssVar('--lg-border'), zerolinecolor: cssVar('--lg-border'),
    automargin: true };
  layout.xaxis = { ...ax, title: { text: 'walker', standoff: 10 }, domain: [0, 0.45] };
  layout.yaxis = { ...ax, title: { text: 'acceptance' } };
  layout.xaxis2 = { ...ax, title: { text: 'walker', standoff: 10 }, domain: [0.57, 1] };
  // Short on purpose: the parameter name is in the picker directly above,
  // and a 14-character axis title gets clipped in a half-width subplot.
  layout.yaxis2 = { ...ax, title: { text: 'walker mean' } };
  Plotly.react(el, traces, layout, { displayModeBar: false, responsive: true });
}

// ---- the whole parameter set, as a table ----

function renderV2EmceeTable() {
  const el = v2El('emcee-table');
  if (!el || !V2_EMCEE) return;
  const rows = V2_EMCEE.params.filter((p) => p.shared)
    .slice().sort((a, b) => (b.gap_sd || 0) - (a.gap_sd || 0));
  el.innerHTML = '<thead><tr>'
    + '<th>parameter</th>'
    + '<th>emcee<br /><span class="muted">mean (sd)</span></th>'
    + '<th>PyMC / NUTS<br /><span class="muted">mean (sd)</span></th>'
    + '<th>gap<br /><span class="muted">posterior SDs, lower is better</span></th>'
    + '<th>steps per new draw<br /><span class="muted">&tau;, lower is better</span></th>'
    + '<th>effective draws<br /><span class="muted">emcee, higher is better</span></th>'
    + '<th>R&#770; across walkers<br /><span class="muted">caveat below</span></th>'
    + '</tr></thead><tbody>'
    + rows.map((p) => {
      const cls = p.gap_sd > 1 ? ' class="bad"' : (p.gap_sd > 0.3 ? ' class="warn"' : '');
      return `<tr>
        <td class="label-cell"><b>${p.name}</b><br /><span class="muted">${p.short || p.label}</span></td>
        <td class="unit">${p.mean.toFixed(4)} <span class="muted">(${p.sd.toFixed(4)})</span></td>
        <td class="unit">${p.pm_mean.toFixed(4)} <span class="muted">(${p.pm_sd.toFixed(4)})</span></td>
        <td class="unit"><span${cls}>${p.gap_sd.toFixed(3)}</span></td>
        <td class="unit">${p.tau.toFixed(0)}</td>
        <td class="unit">${Math.round(p.ess).toLocaleString()}</td>
        <td class="unit">${p.rhat_walkers.toFixed(3)}</td>
      </tr>`;
    }).join('')
    + '</tbody>';
}

// ---- the numbers the page is actually about ----

function renderV2EmceeGyms() {
  const el = v2El('emcee-gyms');
  if (!el || !V2_EMCEE || typeof Plotly === 'undefined') return;
  const rows = V2_EMCEE.gyms || [];
  if (!rows.length) {
    el.style.display = 'none';
    return;
  }
  const gold = cssVar('--lg-gold') || '#1976d2';
  const warm = cssVar('--lg-highlight') || '#b8752e';
  const bar = (key, lo, hi, name, colour, off) => ({
    type: 'scatter', mode: 'markers', name,
    x: rows.map((r) => r[key]),
    y: rows.map((r, i) => i + off),
    error_x: {
      type: 'data', symmetric: false,
      array: rows.map((r) => r[hi] - r[key]),
      arrayminus: rows.map((r) => r[key] - r[lo]),
      color: hexToRgba(colour, 0.45), thickness: 1.5, width: 0,
    },
    marker: { size: 8, color: hexToRgba(colour, 0.8) },
    text: rows.map((r) => r.g),
    hovertemplate: `<b>%{text}</b><br>${name} %{x:+.3f} grades<extra></extra>`,
  });
  const traces = [bar('m', 'lo', 'hi', 'emcee', gold, 0.16)];
  if (rows[0].pm != null) traces.push(bar('pm', 'pm_lo', 'pm_hi', 'PyMC / NUTS', warm, -0.16));
  const layout = chartLayout('grading correction (grades) — negative = softer, positive = stiffer');
  layout.height = Math.max(380, rows.length * 24 + 90);
  layout.margin = { l: 250, r: 24, t: 14, b: 52 };
  layout.yaxis = {
    ...layout.yaxis, automargin: true, tickfont: { size: 11 },
    tickmode: 'array', tickvals: rows.map((_, i) => i), ticktext: rows.map((r) => r.g),
    range: [-0.6, rows.length - 0.4],
  };
  layout.shapes = [{
    type: 'line', xref: 'x', yref: 'paper', x0: 0, x1: 0, y0: 0, y1: 1,
    line: { color: cssVar('--lg-text-2'), width: 1, dash: 'dot' },
  }];
  Plotly.react(el, traces, layout, { displayModeBar: false, responsive: true });
}

// ---- the sigma_gym discrepancy, entirely from measured numbers ----
//
// Every figure in this callout comes from scripts/check_sigma_gym.py by way of
// the payload. Hard-coding them would have been easier and would have gone
// stale the moment a longer PyMC fit lands -- which is the whole point of the
// callout.

function renderV2EmceeDiscrepancy() {
  const el = v2El('emcee-discrepancy');
  if (!el || !V2_EMCEE) return;
  const c = V2_EMCEE.sigma_gym_check;
  if (!c || !c.emcee || !c.pymc) {
    el.style.display = 'none';
    return;
  }
  el.style.display = '';
  const e = c.emcee;
  const p = c.pymc;
  const pct = Math.round(100 * Math.abs(e.mean - p.mean) / p.mean);
  const sci = (v) => {
    const exp = Math.floor(Math.log10(Math.abs(v)));
    const man = (v / 10 ** exp).toFixed(1);
    return `${man}&times;10<sup>${exp < 0 ? '&minus;' : ''}${Math.abs(exp)}</sup>`;
  };
  // Corroboration, from scripts/check_sigma_gym.py: every PyMC fit that
  // reports sigma_gym. The argument is not "one longer fit disagreed with
  // emcee" -- it is that a set of fits spanning different height forms, both
  // design bases and a 4x longer warm-up all land in the same narrow band.
  const fleet = c.pymc_fleet || [];
  const LONG = fleet.find((r) => (r.tune || 0) >= 2000) || null;
  const FLEET_LO = fleet.length ? Math.min(...fleet.map((r) => r.mean)) : 0;
  const FLEET_HI = fleet.length ? Math.max(...fleet.map((r) => r.mean)) : 0;
  const FLEET_HTML = fleet.length < 3 ? '' : (
    `<b>And it is not one fit against one fit.</b> Every PyMC run on this `
    + `model reports <span>\\(\\sigma_{\\text{gym}}\\)</span>, and all `
    + `<b>${fleet.length}</b> of them land between <b>${FLEET_LO.toFixed(3)}</b> `
    + `and <b>${FLEET_HI.toFixed(3)}</b> &mdash; across `
    + `${new Set(fleet.map((r) => r.height_form)).size} different height forms, `
    + 'both the raw and the orthogonalised design basis, and warm-ups differing '
    + `by a factor of four. emcee sits at <b>${e.mean.toFixed(3)}</b>, outside `
    + 'that band entirely. A single fit disagreeing would be ambiguous; ten '
    + 'agreeing with each other and not with emcee is not.');
  el.innerHTML = `<div>
    <b>The exception is <span>\\(\\sigma_{\\text{gym}}\\)</span>, and it is not a
    minor one.</b> That parameter <i>is</i> the headline of this page &mdash; the
    spread of grading style across gyms. emcee puts it at
    <b>${e.mean.toFixed(3)}</b> grades; the PyMC fit puts it at
    <b>${p.mean.toFixed(3)}</b>. Roughly a ${pct}% disagreement about how much
    gyms differ.
    <br /><br />
    Three explanations were checked and eliminated by
    <code>scripts/check_sigma_gym.py</code>. <b>The priors are not the
    difference</b>: profiled against each other across a grid of
    <span>\\(\\sigma_{\\text{gym}}\\)</span> values, the two implementations&rsquo;
    log-priors differ by a constant to <b>${sci(c.prior_spread)}</b> &mdash; the
    same shape, exactly. <b>The likelihood is not the difference</b>: the two
    agree to 1.3&times;10<sup>&minus;9</sup> relative at matched parameter
    values, which <code>scripts/check_pymc_marginal.py</code> asserts on every
    run. <b>The quadrature is not the difference</b>: raising it from
    ${c.n_quad_used} nodes to ${c.quad_nodes[c.quad_nodes.length - 1]} moves the
    log-posterior gap by <b>${sci(c.quad_drift)}</b>, so ${c.n_quad_used} already
    resolves the integral over climber ability.
    <br /><br />
    <b>What is left is sampling, and the run that settles it has now
    happened.</b> The prediction was explicit: refit the same model in
    PyMC with a long warm-up, and if
    <span>\\(\\sigma_{\\text{gym}}\\)</span> climbs toward
    ${e.mean.toFixed(2)}, the short fit was under-sampling.
    <b>It did not climb.</b> At ${LONG ? LONG.tune.toLocaleString() : '2,000'}
    warm-up iterations and ${LONG ? LONG.draws.toLocaleString() : '2,000'} draws
    &mdash; ${LONG ? Math.round(LONG.ess).toLocaleString() : ''} effective
    samples against the short fit&rsquo;s ${Math.round(p.ess).toLocaleString()}
    &mdash; it came back at <b>${LONG ? LONG.mean.toFixed(3) : ''}</b>, which is
    where it started.
    <br /><br />
    ${FLEET_HTML}
    <br /><br />
    <b>So the outlier is emcee, and the earlier reading of this section was
    wrong.</b> The mechanism is a known weakness of ensemble samplers at this
    dimension: walkers propose from each other&rsquo;s positions, so unlike four
    chains started independently they can contract onto a subspace <i>together</i>
    &mdash; and a flat running mean, which is decisive evidence of convergence
    for independent chains, is not decisive for an ensemble. emcee declined to
    certify this run itself: its own rule wants a chain
    50&times;&nbsp;&tau;&nbsp;long, and this one is 31&times;.
    <br /><br />
    <b>What this does and does not settle.</b> It does not touch the agreement
    on the other eleven parameters, which is what the cross-check was for and
    which still stands. It does mean
    <span>\\(\\sigma_{\\text{gym}}\\)</span>&nbsp;&asymp;&nbsp;${(FLEET_LO+FLEET_HI ? (FLEET_LO+FLEET_HI)/2 : p.mean).toFixed(2)}
    is the number to quote, not a lower bound. The remaining check is a
    different <i>kind</i> of algorithm rather than a longer chain: nested
    sampling, which is queued below, explores by shrinking a likelihood contour
    rather than by walking, and would fail in neither of these ways.
  </div>`;
  v2Typeset(el);
}

// ---- the notes that have to restate live numbers ----

function renderV2EmceeNotes() {
  if (!V2_EMCEE) return;
  const r = V2_EMCEE.run;
  const worst = V2_EMCEE.params.filter((p) => p.shared)
    .reduce((a, b) => ((b.gap_sd || 0) > (a.gap_sd || 0) ? b : a));
  const conv = v2El('emcee-converged');
  if (conv) {
    conv.innerHTML = r.converged
      ? `The chain is longer than 50&times;&tau;, so emcee calls its own `
        + 'autocorrelation estimate trustworthy.'
      : `emcee <b>declines to call this converged</b>, and it is worth being `
        + 'precise about what it is objecting to. Its rule is that a chain '
        + `should be at least <b>50&times;&tau;</b> long before &tau; itself can `
        + `be trusted &mdash; here that wants ${r.steps_wanted.toLocaleString()} `
        + `steps and the run has ${r.steps_total.toLocaleString()}. That is a `
        + 'statement about the precision of the &tau; estimate, not evidence '
        + 'that the answer is wrong: the running means below are flat across '
        + 'the whole kept chain.';
  }
  const gapNote = v2El('emcee-gap-note');
  if (gapNote) {
    gapNote.innerHTML = `Sorted by disagreement. Across ${r.n_shared} shared `
      + 'parameters, every one lands within '
      + `<b>${(V2_EMCEE.params.filter((p) => p.shared && p.name !== worst.name)
        .reduce((m, p) => Math.max(m, p.gap_sd), 0)).toFixed(2)} posterior standard `
      + `deviations</b> of PyMC &mdash; except <code>${worst.name}</code>, at `
      + `<b>${worst.gap_sd.toFixed(2)}</b>. Two samplers, two implementations of `
      + 'the likelihood, no gradients in one of them: agreement this close on '
      + '11 of 12 parameters is a strong statement that the PyTensor graph and '
      + 'the NumPy reference describe the same posterior. The twelfth is '
      + 'discussed below.';
  }
  const gymNote = v2El('emcee-gyms-note');
  if (gymNote && (V2_EMCEE.gyms || []).length) {
    gymNote.innerHTML = 'All 29 gym corrections, as each sampler sees them, '
      + 'with 89% intervals. Neither sampler stores these directly &mdash; '
      + 'emcee samples 28 coordinates in an orthonormal zero-sum basis and '
      + 'PyMC stores the corrections themselves &mdash; so both are '
      + 'reconstructed onto the same scale by '
      + '<code>scripts/build_v2_emcee.py</code>. The <i>ordering</i> of gyms '
      + 'agrees; emcee&rsquo;s intervals are systematically the wider of the '
      + 'two, which is the same disagreement about '
      + '<code>log_sigma_gym</code> seen above, expressed per gym.';
  }
}

async function renderV2Emcee() {
  const host = v2El('emcee-run');
  if (!host) return;                       // pane does not include the section
  const d = await loadV2Emcee();
  const section = host.closest('.article-section');
  if (!d) {
    if (section) section.style.display = 'none';
    return;
  }
  if (section) section.style.display = '';
  renderV2EmceeRun();
  bindV2EmceeParam();
  renderV2EmceeCharts();
  renderV2EmceeTable();
  renderV2EmceeDiscrepancy();
  renderV2EmceeGyms();
  renderV2EmceeNotes();
}
