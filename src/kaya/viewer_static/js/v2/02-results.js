// ---- fitted results (gyms, height-form comparison) ----

async function loadV2Results() {
  if (V2_RESULTS) return V2_RESULTS;
  try {
    const r = await fetch('/static/v2_results.json');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    V2_RESULTS = await r.json();
    V2_GYMS = V2_RESULTS.gyms;
  } catch (e) {
    V2_RESULTS = null;
    return null;
  }
  return V2_RESULTS;
}

// LOO ranks the height forms. Reported as a difference from the best form,
// because the absolute elpd is meaningless on its own and only the gaps
// between models on identical data carry information.
function renderV2FormsLoo() {
  const el = v2El('loo-table');
  const note = v2El('loo-note');
  if (!el || !V2_RESULTS) return;
  const forms = V2_RESULTS.forms || [];
  if (!forms.length) { el.innerHTML = ''; return; }

  const hasDse = forms.some((f) => f.dse !== undefined);
  el.innerHTML = '<thead><tr><th>height form</th>'
    + '<th>height params<br /><span class="muted">free parameters, fewer is simpler</span></th>'
    + '<th>LOO elpd<br /><span class="muted">leave-one-out score, higher is better</span></th>'
    + '<th>&Delta; vs best<br /><span class="muted">elpd below the top row, 0 is best</span></th>'
    + (hasDse ? '<th>SE of &Delta;<br /><span class="muted">standard error of that gap</span></th>' : '')
    + '<th>max R&#770;<br /><span class="muted">chain agreement, &le; 1.01 wanted</span></th>'
    + '<th>min ESS<br /><span class="muted">effective sample size, &ge; 400 wanted</span></th>'
    + '</tr></thead><tbody>'
    + forms.map((f, i) => {
      const conv = f.rhat <= 1.01;
      // A gap inside one SE of the difference is not a ranking, it is noise.
      const real = f.dse !== undefined && Math.abs(f.d_elpd) > 2 * f.dse;
      return `<tr${i === 0 ? ' class="row-best"' : ''}>`
        + `<td><b>${f.label}</b>${i === 0 ? ' <span class="pill-best">best</span>' : ''}</td>`
        + `<td class="unit">${f.k ?? '&mdash;'}</td>`
        + `<td class="unit">${f.elpd.toFixed(1)}</td>`
        + `<td class="unit${i && !real ? ' muted' : ''}">${f.d_elpd === 0 ? '&mdash;' : f.d_elpd.toFixed(1)}</td>`
        + (hasDse ? `<td class="unit">${i === 0 ? '&mdash;' : `&plusmn;${f.dse?.toFixed(1) ?? '?'}`}</td>` : '')
        + `<td class="unit ${conv ? '' : 'bad'}">${f.rhat.toFixed(2)}</td>`
        + `<td class="unit ${f.ess >= 400 ? '' : 'bad'}">${f.ess}</td></tr>`;
    }).join('') + '</tbody>';

  if (note) {
    const pending = V2_RESULTS.pending || [];
    const best = forms[0], second = forms[1];
    let txt = `<b>${forms.length} of 6 height forms fitted.</b> `;
    if (best && second) {
      const gap = Math.abs(second.d_elpd);
      txt += `${best.label} leads ${second.label} by ${gap.toFixed(1)} elpd`;
      if (best.k !== null && second.k !== null && best.k < second.k) {
        txt += ` <b>with ${second.k - best.k} fewer height parameters</b>`;
      }
      txt += '. ';
      if (second.dse !== undefined) {
        const sep = forms.filter((f, i) => i && Math.abs(f.d_elpd) > 2 * f.dse);
        txt += `The <b>SE of &Delta;</b> column is the honest one: it is the `
          + 'standard error of the <i>difference</i>, from paired pointwise LOO, '
          + 'which accounts for every model being scored on the same '
          + 'observations. ';
        txt += sep.length
          ? `Only ${sep.map((f) => f.label.toLowerCase()).join(', ')} `
            + `${sep.length === 1 ? 'is' : 'are'} separated from the leader by `
            + 'more than two of those SEs. Every other gap in this table is '
            + 'inside the noise &mdash; the ranking is an ordering, not a result. '
          : '<b>No gap in this table exceeds two of those SEs.</b> The ranking '
            + 'is an ordering, not a result: on this data LOO cannot tell these '
            + 'height forms apart. ';
      } else if (gap < 10) {
        txt += 'That gap is small relative to the standard errors quoted per '
          + 'model, so it is a lead, not a verdict &mdash; a paired comparison '
          + 'on pointwise LOO is needed before ranking these confidently. ';
      }
    }
    if (pending.length) {
      txt += `Still running or queued: <code>${pending.join('</code>, <code>')}</code>. `;
    }
    txt += 'Built by <code>scripts/build_v2_results.py</code> from the per-fit '
      + 'results JSON under <code>runs/</code>.';
    note.innerHTML = txt;
  }
}

// What has held steady across every fit. Replication across different height
// forms is the strongest evidence available here that a number is real and
// not an artefact of one model specification.
function renderV2Replication() {
  const el = v2El('repl-table');
  if (!el || !V2_RESULTS) return;
  const rep = V2_RESULTS.replication || {};
  const SHOW = [
    ['sigma_gym', '\\(\\sigma_{\\text{gym}}\\)', 'spread of gym grading'],
    ['kappa', '\\(\\kappa\\)', 'gap rate per visit'],
    ['rho', '\\(\\rho\\)', 'gap rate per logging completeness'],
    ['sigma_user', '\\(\\sigma_{\\text{user}}\\)', 'spread of climber ability'],
    ['beta_gender', '\\(\\beta_{\\text{gender}}\\)', 'female-user ability shift'],
    ['delta1', '\\(\\delta_1\\)', 'ape-index slope'],
  ];
  // Only the arms that are genuine like-for-like refits.
  const fits = (V2_RESULTS.generated_from || []).filter((f) => f !== 'v3_all');
  el.innerHTML = '<thead><tr><th>parameter</th><th>meaning</th>'
    + fits.map((f) => `<th>${f.replace('v3_', '')}</th>`).join('')
    + '<th>verdict</th></tr></thead><tbody>'
    + SHOW.filter(([k]) => rep[k]).map(([k, tex, meaning]) => {
      const vals = fits.map((f) => rep[k][f]);
      const present = vals.filter(Boolean).map((v) => v.m);
      const rng = present.length > 1 ? Math.max(...present) - Math.min(...present) : 0;
      const tight = present.length > 1 && rng < 0.05;
      return `<tr><td class="sym">${tex}</td><td>${meaning}</td>`
        + vals.map((v) => `<td class="unit">${v ? v.m.toFixed(3) : '&mdash;'}</td>`).join('')
        + `<td>${tight ? '<span class="ok">stable</span>' : (present.length > 1
            ? `varies by ${rng.toFixed(2)}` : '&mdash;')}</td></tr>`;
    }).join('') + '</tbody>';
  if (typeof window.renderMathInElement === 'function') {
    window.renderMathInElement(el, {
      delimiters: [{ left: '\\(', right: '\\)', display: false }], throwOnError: false });
  }
}

// ---- priors, posteriors, corner plots and sampler diagnostics ----
//
// v2_posterior.json holds every fit: 4 chains x 150 thinned draws per
// parameter, plus prior draws. Thinning uses one step per fit, so draw i of
// one parameter matches draw i of another -- that is what makes the corner
// plots real joint samples rather than a scatter of unrelated numbers.

let V2_POST = null;

const V2_PARAM_TEX = {
  beta0: '\\beta_0', beta_gender: '\\beta_{\\text{gender}}',
  gamma1: '\\gamma_1', gamma2: '\\gamma_2',
  gamma1_x: '\\gamma_1^{\\times}', gamma2_x: '\\gamma_2^{\\times}',
  delta1: '\\delta_1', delta2: '\\delta_2',
  delta1_x: '\\delta_1^{\\times}', delta2_x: '\\delta_2^{\\times}',
  sigma_user: '\\sigma_{\\text{user}}', sigma_gym: '\\sigma_{\\text{gym}}',
  log_lambda0: '\\log\\lambda_0', kappa: '\\kappa', rho: '\\rho',
  beta_h_missing: '\\beta_{h\\text{-miss}}', beta_a_missing: '\\beta_{a\\text{-miss}}',
  sat_amp: 'A', sat_h0: 'h_0', sat_scale: 's',
  vq_curv: '\\kappa_h', vq_peak: 'p',
};
const V2_PARAM_BLURB = {
  beta0: 'baseline ability at an average gym',
  beta_gender: 'female-user shift in ability',
  gamma1: 'height slope, male users', gamma2: 'height curvature, male users',
  gamma1_x: 'how the female height slope differs',
  gamma2_x: 'how the female height curvature differs',
  delta1: 'ape-index slope', delta2: 'ape-index curvature',
  delta1_x: 'extra ape slope for female users',
  delta2_x: 'extra ape curvature for female users',
  sigma_user: 'spread of ability between climbers',
  sigma_gym: 'spread of grading style across gyms',
  log_lambda0: 'baseline gap rate (log)', kappa: 'gap rate per extra visit',
  rho: 'gap rate per unit logging completeness',
  beta_h_missing: 'ability shift for users with no height on file',
  beta_a_missing: 'ability shift for users with no ape index on file',
  sat_amp: 'saturating: total worth of reach', sat_h0: 'saturating: where it levels off',
  sat_scale: 'saturating: how sharply it levels',
  vq_curv: 'vertex form: how sharp the optimum is', vq_peak: 'vertex form: the best height',
};
const V2_FIT_LABEL = {
  v3_conf: 'quad × gender', v3_all: 'quad × gender (all names)',
  v3_lin: 'linear', v3_sat: 'saturating', v3_zero: 'zero',
  v3_quad: 'quadratic', v3_vtx: 'vertex quadratic',
  v4_linxg: 'linear × gender',
  v4_lin_b: 'linear, refit #2', v4_lin_c: 'linear, refit #3',
  v4_lin_d: 'linear, refit #4', v4_lin_e: 'linear, refit #5',
  v4_lin_apex: 'linear + ape×gender', v4_lin_apelin: 'linear, linear ape',
  v3_apex: 'quad × gender + ape×gender', v3_zsu: 'quad × gender (zero-sum users)',
};
// Ten distinct hues -- there are nine fits, and the status tokens only give
// five usable colours (two of which are near-identical blues).
const V2_FIT_HUES = ['--lg-cat-1', '--lg-cat-2', '--lg-cat-3', '--lg-cat-4',
                     '--lg-cat-5', '--lg-cat-6', '--lg-cat-7', '--lg-cat-8',
                     '--lg-cat-9', '--lg-cat-10'];

const V2_CORNER_GROUPS = {
  height: { label: 'Height block', of: ['beta0', 'gamma1', 'gamma2', 'gamma1_x', 'gamma2_x'] },
  gap: { label: 'Gap model', of: ['log_lambda0', 'kappa', 'rho'] },
  spread: { label: 'Variance components', of: ['beta0', 'sigma_user', 'sigma_gym'] },
  body: { label: 'Body covariates', of: ['gamma1', 'delta1', 'beta_h_missing', 'beta_a_missing'] },
};

const v2Fit = (n) => V2_POST?.fits?.[n];
const v2FitNames = () => Object.keys(V2_POST?.fits || {});
// The fit every page quotes when no picker has been touched. Named once
// because two panes set it -- the detailed page seeds its dropdown with it,
// and the presentation page has no dropdown at all and would otherwise fall
// through to whatever sorts first in the payload. Two pages quoting different
// primaries is the drift this constant exists to prevent.
const V2_PRIMARY_FIT = 'v3_conf';

const v2SelectedFit = () => {
  const picked = v2El('fit-pick')?.value;
  if (picked) return picked;
  const names = v2FitNames();
  return names.includes(V2_PRIMARY_FIT) ? V2_PRIMARY_FIT : names[0];
};

// ---- which fits the multi-model figures draw -------------------------------
//
// There are 22 fitted models. Overlaying all of them produces a legend taller
// than the plot and 22 curves in 10 colours, which answers no question anyone
// asked. Most of them are not alternatives to each other either: four are the
// same model refitted with a different seed (they exist to measure the noise
// floor, and overlaying them only shows that the sampler is stochastic), and
// several change something other than the height form, so their posteriors are
// not comparable to the rest.
//
// The default is therefore the seven height forms of one arm. The scope is per
// pane: the archived page is the full working record and shows everything,
// while the current page starts narrow.
//
// The presentation page (`gf-`) takes the SAME scope as the detailed page on
// purpose. It is a shorter read, not a different measurement -- two pages
// quoting different primaries would be worse than either choice on its own.
// What makes it shorter is which sections exist, not which fits back them.
const V2_SCOPES = new Map();
const V2_SCOPE_DEFAULT = {
  'gm-': { arm: 'unmarginalized', extras: false },
  'gf-': { arm: 'unmarginalized', extras: false },
};

function v2Scope() {
  if (!V2_SCOPES.has(V2_NS)) {
    V2_SCOPES.set(V2_NS, { ...(V2_SCOPE_DEFAULT[V2_NS] || { arm: 'both', extras: true }) });
  }
  return V2_SCOPES.get(V2_NS);
}

function v2ScopedFitNames() {
  const s = v2Scope();
  const keep = v2FitNames().filter((n) => {
    const f = v2Fit(n);
    if (!f) return false;
    if (!s.extras && (f.role || 'form') !== 'form') return false;
    return s.arm === 'both' || (f.arm || 'unmarginalized') === s.arm;
  });
  // A filter that empties the figure is worse than no filter. This only bites
  // on a payload built before `role` existed, where nothing is a 'form'.
  return keep.length ? keep : v2FitNames();
}

// Colour has to be stable as the scope changes, or narrowing the selection
// silently recolours every curve and the reader thinks the fits moved.
const v2FitHue = (n) => cssVar(V2_FIT_HUES[v2FitNames().indexOf(n) % V2_FIT_HUES.length]);

// A fit's display name: the height form, plus which version of the model it
// is. Two things vary across the fits and the label has to carry both, or a
// legend entry cannot be placed. "offsets" alone was ambiguous -- they are the
// per-climber ability offsets, so the label says climber offsets.
function v2FitLabel(fn) {
  if (V2_FIT_LABEL[fn]) return V2_FIT_LABEL[fn];
  const f = v2Fit(fn);
  const base = (f && f.base) || (fn.endsWith('_marg') ? fn.slice(0, -5) : fn);
  const stem = V2_FIT_LABEL[base] || (f && f.height_form) || base;
  return fn.endsWith('_marg') ? `${stem} · climber offsets integrated out` : stem;
}

// Gaussian KDE on a fixed grid. Silverman bandwidth; these posteriors are
// unimodal, so nothing fancier earns its keep.
function v2Kde(samples, grid) {
  const n = samples.length;
  if (!n) return grid.map(() => 0);
  const mean = samples.reduce((a, b) => a + b, 0) / n;
  const sd = Math.sqrt(samples.reduce((a, b) => a + (b - mean) ** 2, 0) / Math.max(1, n - 1)) || 1e-6;
  const sorted = [...samples].sort((a, b) => a - b);
  const q = (p) => sorted[Math.min(n - 1, Math.max(0, Math.floor(p * n)))];
  const iqr = q(0.75) - q(0.25);
  const bw = 0.9 * Math.min(sd, iqr / 1.349 || sd) * Math.pow(n, -0.2) || sd * 0.1;
  return grid.map((x) => {
    let acc = 0;
    for (const v of samples) acc += Math.exp(-0.5 * ((x - v) / bw) ** 2);
    return acc / (n * bw * Math.sqrt(2 * Math.PI));
  });
}

function v2Grid(lo, hi, n = 140) {
  const step = (hi - lo) / (n - 1), g = [];
  for (let i = 0; i < n; i++) g.push(lo + i * step);
  return g;
}

function v2SharedRange(post, prior) {
  const all = prior ? post.concat(prior) : post;
  const sorted = [...all].sort((a, b) => a - b);
  const q = (p) => sorted[Math.min(sorted.length - 1, Math.floor(p * sorted.length))];
  let lo = q(0.005), hi = q(0.995);
  lo = Math.min(lo, Math.min(...post)); hi = Math.max(hi, Math.max(...post));
  const pad = (hi - lo) * 0.06 || 0.1;
  return [lo - pad, hi + pad];
}

// ---- overview grid: every parameter of the selected fit ----

function renderV2PostGrid() {
  const host = v2El('post-grid');
  const fit = v2Fit(v2SelectedFit());
  if (!host || !fit) return;
  const names = Object.keys(fit.params);
  host.innerHTML = names.map((n) => `
    <button type="button" class="post-tile" data-param="${n}">
      <span class="post-tile-name">\\(${V2_PARAM_TEX[n] || n}\\)</span>
      <span class="post-tile-chart" id="${v2Id(`pt-${n}`)}"></span>
      <span class="post-tile-rhat ${fit.params[n].rhat > 1.01 ? 'bad' : 'ok'}">R&#770; ${fit.params[n].rhat.toFixed(2)}</span>
    </button>`).join('');

  names.forEach((n) => {
    const el = document.getElementById(v2Id(`pt-${n}`));
    if (!el || typeof Plotly === 'undefined') return;
    const p = fit.params[n];
    const post = p.chains.flat();
    const pri = fit.prior?.[n];
    const [lo, hi] = v2SharedRange(post, pri);
    const grid = v2Grid(lo, hi, 70);
    const traces = [];
    if (pri) {
      traces.push({ type: 'scatter', mode: 'lines', x: grid, y: v2Kde(pri, grid),
        line: { color: cssVar('--lg-text-2'), width: 1 }, fill: 'tozeroy',
        fillcolor: `color-mix(in srgb, ${cssVar('--lg-text-2')} 16%, transparent)`,
        hoverinfo: 'skip' });
    }
    traces.push({ type: 'scatter', mode: 'lines', x: grid, y: v2Kde(post, grid),
      line: { color: cssVar('--lg-info'), width: 1.6 }, fill: 'tozeroy',
      fillcolor: `color-mix(in srgb, ${cssVar('--lg-info')} 24%, transparent)`,
      hoverinfo: 'skip' });
    Plotly.react(el, traces, {
      height: 62, margin: { l: 2, r: 2, t: 2, b: 2 }, showlegend: false,
      paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)',
      xaxis: { visible: false, range: [lo, hi] }, yaxis: { visible: false },
    }, { displayModeBar: false, staticPlot: true, responsive: true });
  });

  host.querySelectorAll('.post-tile').forEach((b) => {
    b.addEventListener('click', () => {
      const sel = v2El('param-pick');
      if (sel) sel.value = b.dataset.param;
      renderV2ParamDetail(b.dataset.param);
      document.querySelector('.param-detail')?.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
    });
  });
  if (typeof window.renderMathInElement === 'function') {
    window.renderMathInElement(host, {
      delimiters: [{ left: '\\(', right: '\\)', display: false }], throwOnError: false });
  }
}

// ---- one parameter, in detail ----

function renderV2ParamDetail(name) {
  const fitName = v2SelectedFit();
  const fit = v2Fit(fitName);
  if (!fit) return;
  const p = fit.params[name];
  if (!p) return;
  const post = p.chains.flat();

  // prior vs posterior
  const dens = v2El('param-dens');
  if (dens && typeof Plotly !== 'undefined') {
    const wide = v2El('param-wide')?.checked;
    const pri = fit.prior?.[name];
    const [lo, hi] = wide && pri
      ? v2SharedRange(post, pri)
      : [p.mean - 6 * p.sd, p.mean + 6 * p.sd];
    const grid = v2Grid(lo, hi, 160);
    const traces = [];
    if (pri) {
      traces.push({ type: 'scatter', mode: 'lines', name: 'prior', x: grid, y: v2Kde(pri, grid),
        line: { color: cssVar('--lg-text-2'), width: 1.5 }, fill: 'tozeroy',
        fillcolor: `color-mix(in srgb, ${cssVar('--lg-text-2')} 14%, transparent)`,
        hovertemplate: 'prior<br>%{x:.3f}<extra></extra>' });
    }
    traces.push({ type: 'scatter', mode: 'lines', name: 'posterior', x: grid, y: v2Kde(post, grid),
      line: { color: cssVar('--lg-info'), width: 2.2 }, fill: 'tozeroy',
      fillcolor: `color-mix(in srgb, ${cssVar('--lg-info')} 22%, transparent)`,
      hovertemplate: 'posterior<br>%{x:.3f}<extra></extra>' });
    const layout = chartLayout('value');
    layout.height = 280;
    layout.xaxis = { ...layout.xaxis, title: { text: 'value', standoff: 8 }, range: [lo, hi] };
    layout.yaxis = { ...layout.yaxis, title: { text: 'density', standoff: 6 }, showticklabels: false };
    layout.margin = { l: 48, r: 16, t: 10, b: 76 };
    layout.legend = { ...layout.legend, orientation: 'h', y: -0.34, x: 0 };
    layout.shapes = [p.lo, p.hi].map((x) => ({ type: 'line', x0: x, x1: x, y0: 0, y1: 1,
      yref: 'paper', line: { color: cssVar('--lg-info'), width: 1, dash: 'dot' } }));
    Plotly.react(dens, traces, layout, { displayModeBar: false, responsive: true });
  }

  // chains
  const tr = v2El('param-trace');
  if (tr && typeof Plotly !== 'undefined') {
    const hues = ['--lg-info', '--lg-highlight', '--lg-success', '--lg-danger'];
    const mode = v2El('trace-mode')?.value || 'trace';
    const layout = chartLayout('');
    layout.height = 280;
    layout.margin = { l: 56, r: 16, t: 10, b: 76 };
    layout.legend = { ...layout.legend, orientation: 'h', y: -0.34, x: 0 };
    let traces;

    if (mode === 'trace') {
      const nWarm = p.warmup ? p.warmup[0].length : 0;
      traces = p.chains.map((ch, i) => {
        const series = nWarm ? p.warmup[i].concat(ch) : ch;
        return { type: 'scatter', mode: 'lines', name: `chain ${i}`,
          x: series.map((_, j) => j - nWarm), y: series,
          line: { color: cssVar(hues[i % hues.length]), width: 0.9 },
          hovertemplate: `chain ${i}<br>%{y:.3f}<extra></extra>` };
      });
      layout.shapes = p.chains.map((ch, i) => ({
        type: 'line', xref: 'paper', x0: 0, x1: 1,
        y0: ch.reduce((a, b) => a + b, 0) / ch.length,
        y1: ch.reduce((a, b) => a + b, 0) / ch.length,
        line: { color: cssVar(hues[i % hues.length]), width: 1.2, dash: 'dash' },
      }));
      layout.annotations = [];
      if (nWarm) {
        layout.shapes.push({ type: 'rect', xref: 'x', yref: 'paper',
          x0: -nWarm, x1: 0, y0: 0, y1: 1,
          fillcolor: cssVar('--lg-text-2'), opacity: 0.08, line: { width: 0 } });
        layout.annotations.push({ x: -nWarm / 2, y: 1, yref: 'paper', yanchor: 'bottom',
          showarrow: false, text: 'warm-up (discarded)',
          font: { size: 10, color: cssVar('--lg-text-2') } });
        layout.margin.t = 22;
      }
      if (!nWarm) {
        // Only fits run after discard_tuned_samples=False went in have their
        // warm-up; say which those are rather than leaving a silent absence.
        const withWarm = v2FitNames().filter((f) => v2Fit(f)?.n_warmup)
          .map((f) => v2FitLabel(f));
        layout.annotations.push({
          xref: 'paper', x: 0.5, yref: 'paper', y: 1, yanchor: 'bottom',
          showarrow: false, font: { size: 10, color: cssVar('--lg-text-2') },
          text: withWarm.length
            ? `warm-up not kept for this fit — see ${withWarm[withWarm.length - 1]}`
            : 'warm-up not kept for this fit',
        });
        layout.margin.t = 22;
      }
      layout.xaxis = { ...layout.xaxis, title: {
        text: nWarm ? 'draw (thinned; 0 = end of warm-up)' : 'draw (thinned, post-warm-up)',
        standoff: 8 } };
      layout.yaxis = { ...layout.yaxis, title: { text: 'value', standoff: 6 } };
    } else {
      const flat = [];
      p.chains.forEach((ch, ci) => ch.forEach((v) => flat.push([v, ci])));
      flat.sort((a, b) => a[0] - b[0]);
      const nBins = 12, total = flat.length;
      const counts = p.chains.map(() => new Array(nBins).fill(0));
      flat.forEach(([, ci], rank) => {
        counts[ci][Math.min(nBins - 1, Math.floor((rank / total) * nBins))] += 1;
      });
      const centres = [...Array(nBins)].map((_, b) => (b + 0.5) / nBins);
      traces = counts.map((c, i) => ({
        type: 'bar', name: `chain ${i}`, x: centres, y: c,
        marker: { color: cssVar(hues[i % hues.length]) },
        hovertemplate: `chain ${i}<br>rank bin %{x:.2f}<br>%{y} draws<extra></extra>`,
      }));
      const expected = total / (nBins * p.chains.length);
      layout.barmode = 'group';
      layout.shapes = [{ type: 'line', xref: 'paper', x0: 0, x1: 1, y0: expected, y1: expected,
        line: { color: cssVar('--lg-text-2'), width: 1.4, dash: 'dash' } }];
      layout.annotations = [{ xref: 'paper', x: 1, yref: 'paper', y: 1, xanchor: 'right',
        yanchor: 'bottom', showarrow: false, text: '- - -  even mixing',
        font: { size: 10, color: cssVar('--lg-text-2') } }];
      layout.margin.t = 22;
      layout.xaxis = { ...layout.xaxis, title: { text: 'rank within all chains', standoff: 8 } };
      layout.yaxis = { ...layout.yaxis, title: { text: 'draws in bin', standoff: 6 } };
    }
    Plotly.react(tr, traces, layout, { displayModeBar: false, responsive: true });
  }

  renderV2AcrossFits(name);
  // Deliberately not scoped: this figure's whole subject is the difference
  // between the two arms, so filtering to one arm would empty it.
  renderV2ArmPairs(name);
  renderV2ParamStats(name, p);
}

function renderV2ParamStats(name, p) {
  const tbl = v2El('param-stats');
  if (!tbl) return;
  const converged = p.rhat <= 1.01;
  tbl.innerHTML = '<thead><tr><th>statistic</th><th>value</th><th>reading</th></tr></thead><tbody>'
    + [
      ['posterior mean', p.mean.toFixed(3), 'the number quoted elsewhere on this page'],
      ['posterior SD', p.sd.toFixed(3), 'how uncertain that number is'],
      ['89% HDI', `[${p.lo.toFixed(3)}, ${p.hi.toFixed(3)}]`,
        (p.lo <= 0 && p.hi >= 0) ? '<b>includes zero</b> — no credible effect'
          : 'excludes zero — a credible effect'],
      ['R&#770;', p.rhat.toFixed(3), converged
        ? 'at or below 1.01 — chains agree'
        : '<b>above 1.01 — chains disagree, do not report this as final</b>'],
      ['ESS (bulk)', String(p.ess_bulk), p.ess_bulk < 400
        ? '<b>below 400 — too few effective draws</b>' : 'adequate'],
      ['ESS (tail)', String(p.ess_tail), p.ess_tail < 400
        ? 'below 400 — interval edges are noisy' : 'adequate'],
    ].map((r) => `<tr><td class="sym">${r[0]}</td><td class="unit">${r[1]}</td><td>${r[2]}</td></tr>`).join('')
    + '</tbody>';
  const verdict = v2El('param-verdict');
  if (verdict) {
    verdict.textContent = converged
      ? `converged — R-hat ${p.rhat.toFixed(2)}, ESS ${p.ess_bulk}`
      : `provisional — R-hat ${p.rhat.toFixed(2)}, ESS ${p.ess_bulk}: chains do not agree yet`;
    verdict.className = `param-verdict ${converged ? '' : 'warn'}`;
  }
}

// ---- the same parameter across every model version ----
//
// Most parameters exist in several fits. Overlaying their posteriors answers a
// question no single fit can: is this number a property of the data, or of the
// height form that happened to be bolted on beside it?

// Geometry for the across-fits overlay. The plot area is fixed; the legend
// grows downward from it.
const V2_ACROSS_PLOT_H = 300;   // drawing area for the densities, px
const V2_ACROSS_AXIS_H = 46;    // x tick labels + axis title, px

// Give the legend exactly the room it turned out to need.
//
// Plotly lays a horizontal legend out in uniform-width columns sized by its
// widest entry, so how many rows N model versions take is not predictable from
// the label lengths -- and guessing low puts the legend on top of the curves.
// Draw once, measure the rendered legend, then correct the margin. The row
// count depends on the chart's width, which this pass does not change, so one
// correction converges.
function v2SizeAcrossLegend(el) {
  const legend = el.querySelector('.legend');
  if (!legend || !el._fullLayout) return;
  const legendH = Math.ceil(legend.getBoundingClientRect().height) + 10;
  const want = 10 + V2_ACROSS_PLOT_H + V2_ACROSS_AXIS_H + legendH;
  if (Math.abs(el._fullLayout.height - want) < 4) return;
  Plotly.relayout(el, { height: want, 'margin.b': V2_ACROSS_AXIS_H + legendH });
}

function renderV2AcrossFits(name) {
  const el = v2El('across-fits');
  const note = v2El('across-note');
  if (!el || typeof Plotly === 'undefined') return;
  const have = v2ScopedFitNames().filter((f) => v2Fit(f).params[name]);
  if (have.length < 2) {
    Plotly.purge(el);
    el.innerHTML = '<p class="form-noparams">Fewer than two of the models '
      + 'currently shown contain this parameter, so there is nothing to compare '
      + 'it against. Widen the selection above, or pick another parameter.</p>';
    if (note) note.textContent = '';
    return;
  }
  // Only clear when Plotly does not already own this node. Wiping innerHTML
  // on an initialised plot destroys its SVG but leaves _fullLayout behind, and
  // the next react() then treats it as an in-place update and draws nothing.
  if (!el._fullLayout) el.innerHTML = '';
  const allDraws = have.flatMap((f) => v2Fit(f).params[name].chains.flat());
  const [lo, hi] = v2SharedRange(allDraws, null);
  const grid = v2Grid(lo, hi, 160);
  const traces = have.map((f) => {
    const p = v2Fit(f).params[name];
    const bad = p.rhat > 1.01;
    return {
      type: 'scatter', mode: 'lines',
      name: `${v2FitLabel(f)}${bad ? ' ⚠' : ''}`,
      x: grid, y: v2Kde(p.chains.flat(), grid),
      line: { color: v2FitHue(f), width: 2,
              dash: bad ? 'dot' : 'solid' },
      hovertemplate: `${v2FitLabel(f)}<br>%{x:.3f}<extra></extra>`,
    };
  });
  const layout = chartLayout('value');
  layout.xaxis = { ...layout.xaxis, title: { text: 'value', standoff: 8 }, range: [lo, hi] };
  layout.yaxis = { ...layout.yaxis, title: { text: 'density', standoff: 6 }, showticklabels: false };

  // The legend carries one entry per fitted model version, and there are
  // currently 22. A fixed bottom margin cannot serve that -- too small and the
  // legend climbs over the curves, too large and it wastes the pane -- so the
  // margin is sized from the legend instead. The plot area itself stays a
  // constant V2_ACROSS_PLOT_H: adding or removing a model version must change
  // the legend's height, never the height of the curves.
  layout.height = 10 + V2_ACROSS_PLOT_H + V2_ACROSS_AXIS_H + 120;   // provisional
  layout.margin = { l: 48, r: 16, t: 10, b: V2_ACROSS_AXIS_H + 120 };
  layout.legend = {
    ...layout.legend,
    orientation: 'h',
    x: 0,
    xanchor: 'left',
    // y is in plot-area units, so this puts the legend's top edge exactly
    // AXIS_H below the axis: clear of the tick labels, clear of the curves.
    y: -(V2_ACROSS_AXIS_H / V2_ACROSS_PLOT_H),
    yanchor: 'top',
  };
  Plotly.react(el, traces, layout, { displayModeBar: false, responsive: true });
  v2SizeAcrossLegend(el);

  if (note) {
    const means = have.map((f) => v2Fit(f).params[name].mean);
    const spread = Math.max(...means) - Math.min(...means);
    const widest = Math.max(...have.map((f) => v2Fit(f).params[name].sd));
    // Compare how far the models disagree against how uncertain any one of
    // them is. Below ~half an SD the disagreement is invisible next to the
    // noise; past ~1.5 SD the choice of height form is genuinely driving the
    // answer. In between, say so rather than forcing a verdict.
    const ratio = widest > 0 ? spread / widest : 0;
    let verdict;
    if (ratio < 0.5) {
      verdict = 'That is well inside the uncertainty of a single fit, so this '
        + 'parameter is <b>not sensitive to the height form</b> — it is a property '
        + 'of the data.';
    } else if (ratio < 1.5) {
      verdict = 'Those are comparable, so the fits are <b>consistent with each other</b> '
        + 'to within their own uncertainty — but not so tightly that the spread is '
        + 'negligible. Read the number as robust, the third decimal as not.';
    } else {
      verdict = '<b>The models disagree by more than any one of them claims to be '
        + 'uncertain</b>, so this number depends on which height form sits beside it. '
        + 'Treat it as conditional on that choice, not as a property of the data.';
    }
    note.innerHTML = `Across ${have.length} fits the posterior mean moves by `
      + `<b>${spread.toFixed(3)}</b>, against a within-fit SD of ${widest.toFixed(3)} `
      + `(ratio ${ratio.toFixed(1)}&times;). ${verdict}`
      + ' Dotted lines mark fits that have not converged.';
  }
}

