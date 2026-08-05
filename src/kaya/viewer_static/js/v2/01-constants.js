// --- which pane these renderers are drawing into -------------------------
//
// The same renderers now serve two panes: the archived `Archive - v2 Notes`
// tab, which owns the `v2-*` element ids, and the current `Grading Model`
// tab, which owns `gm-*`. Element ids must be unique per document, so nothing
// below may hardcode a prefix -- ids resolve through `v2El` / `v2Id` instead.
//
// V2_NS is switched by the tab renderers and read synchronously. Several
// renderers are async, so `withV2Ns` serialises whole renders: a tab render
// waits for the previous one to finish rather than flipping the namespace out
// from under an awaiting renderer.
let V2_NS = 'v2-';
let V2_PANE = 'tab-grading-v2';
let v2RenderChain = Promise.resolve();

const v2Id = (name) => V2_NS + name;
const v2El = (name) => document.getElementById(V2_NS + name);
const v2Pane = () => document.getElementById(V2_PANE);
const v2Sel = (sel) => `#${V2_PANE} ${sel}`;

// Wraps an event handler so the interaction re-enters the namespace the
// handler was bound in. Without this a slider on the current Grading Model tab
// would, on input, look up `v2-*` ids and silently redraw the archived pane.
function v2Bound(fn) {
  const ns = V2_NS;
  const pane = V2_PANE;
  return (...args) => withV2Ns(ns, pane, () => fn(...args));
}

function withV2Ns(ns, paneId, fn) {
  const run = async () => {
    V2_NS = ns;
    V2_PANE = paneId;
    try {
      return await fn();
    } finally {
      V2_NS = 'v2-';
      V2_PANE = 'tab-grading-v2';
    }
  };
  v2RenderChain = v2RenderChain.then(run, run);
  return v2RenderChain;
}

// Coalesce the post-slide resize.
//
// Opening or closing the symbols panel changes the pane's usable width by the
// gutter, so anything that measured its own width has to be told. That pass
// includes the corner plot, which is N x N subplots -- so toggling the panel a
// few times in a second would queue one full pass per toggle and wedge the
// renderer. One pass per burst, aimed at the pane the toggle happened in.
const v2SizeTimers = new Map();

function v2SizeSoon(delay = 240) {
  const ns = V2_NS;
  const pane = V2_PANE;
  clearTimeout(v2SizeTimers.get(ns));
  v2SizeTimers.set(ns, setTimeout(() => withV2Ns(ns, pane, sizeV2FormGrid), delay));
}

// A tab renders once, on first activation.
//
// Every figure on both explainer pages is a redraw of a static payload, so
// re-rendering on each activation buys nothing -- and it costs ~40 Plotly
// rebuilds per pane. Switching between the two tabs faster than a render
// completes used to queue them faster than they could drain, which wedged the
// renderer outright. Interactions still redraw their own figures; only the
// bulk build is skipped.
const v2TabRendered = new Set();

function renderV2TabOnce(ns, paneId, fn) {
  if (v2TabRendered.has(ns)) return v2RenderChain;
  v2TabRendered.add(ns);
  return withV2Ns(ns, paneId, fn).catch((e) => {
    // A failed build must be retryable -- otherwise a transient payload error
    // leaves the tab permanently blank.
    v2TabRendered.delete(ns);
    throw e;
  });
}

// KaTeX + its auto-render extension load with `defer`, so they aren't
// guaranteed to exist yet when this (non-deferred) script runs -- waiting
// for DOMContentLoaded (which fires only after all deferred scripts have
// executed, per spec) sidesteps any load-order race instead of guessing.

// ==========================================================================
// Grading Model v2 tab
// Fitted values are baked in as constants (same pattern as EXPLORER_GYMS):
// these come from a specific PyMC run, not from the live viewer payloads.
// Source run: net50/confident, 29 gyms, 20,014 obs, 10,357 climbers.
// ==========================================================================
// Populated from /static/v2_results.json (regenerate with
// scripts/build_v2_results.py). Empty until that loads -- deliberately not
// seeded with stale numbers from an older fit, which would silently render
// figures that no longer match any fit on disk.
let V2_GYMS = [];
let V2_RESULTS = null;

function v2Stats() {
  const R = V2_RESULTS;
  if (!R) return [];
  const brands = new Set(R.gyms.map((g) => g.b)).size;
  const sg = R.sigma_gym;
  return [
    {v: String(R.n_gyms), l: 'gyms', s: `${brands} companies, one connected network`},
    {v: R.spread.toFixed(2), l: 'grades', s: 'stiffest minus softest'},
    {v: `${R.n_sig}/${R.n_gyms}`, l: 'credibly differ', s: '89% interval excludes zero'},
    {v: (R.spread / 0.254).toFixed(1) + '&times;', l: 'wider than v1',
     s: 'vs the 6 Touchstone gyms alone'},
    {v: sg.mean.toFixed(3), l: '&sigma;<sub>gym</sub>', s: 'was 0.128 in v1'},
    {v: R.dataset.n_obs.toLocaleString(), l: 'observations',
     s: `${R.dataset.n_users.toLocaleString()} climbers`},
  ];
}

// distance below own max -> share of sends (from 2,085,765 bouldering sends)
const V2_DISCARD = [[0,7.0],[1,15.5],[2,22.0],[3,19.9],[4,14.3],[5,21.3]];

// mean height by name-based P(female) bin -- independent validation
const V2_GENDER_VAL = [
  {b:'0-.02',x:0.01,h:69.17,n:11421},{b:'.02-.1',x:0.06,h:69.09,n:3650},
  {b:'.1-.3',x:0.20,h:68.11,n:1757},{b:'.3-.5',x:0.40,h:66.94,n:902},
  {b:'.5-.7',x:0.60,h:65.90,n:711},{b:'.7-.9',x:0.80,h:64.82,n:1046},
  {b:'.9-.98',x:0.94,h:64.27,n:1667},{b:'.98-1',x:0.99,h:64.22,n:4373},
];

const V2_GG = [
  ['gender_guesser label','n','true mean P(female)','v1 coded it as','mean height'],
  ['male','15,731','0.025','0.0','69.12'],
  ['mostly_male','2,150','0.142','0.0','68.71'],
  ['unknown','5,225','<b>0.383</b>','<b>0.5</b>','66.85'],
  ['andy','1,182','0.485','0.5','66.47'],
  ['mostly_female','887','<b>0.660</b>','<b>1.0</b>','65.79'],
  ['female','6,651','0.947','1.0','64.39'],
];

const V2_DIAG = [
  ['parameter block','R-hat','ESS','trustworthy?'],
  ['gym corrections','1.00 &ndash; 1.05','96 &ndash; 578','<span class="ok">yes</span>'],
  ['&sigma;<sub>gym</sub>','1.06','64','<span class="mid">marginal</span>'],
  ['&beta;<sub>gender</sub> = &minus;1.60','1.05','73','<span class="mid">marginal</span>'],
  ['&lambda;<sub>0</sub>, &kappa;','1.01','326 &ndash; 405','<span class="ok">yes</span>'],
  ['height terms &gamma;','1.08 &ndash; 1.13','26 &ndash; 45','<span class="bad">no</span>'],
  ['&beta;<sub>0</sub>, &delta;<sub>1</sub>','1.18 &ndash; 1.19','16 &ndash; 18','<span class="bad">no</span>'],
];

const V2_DECISIONS = [
  {t:'Treat impossible bodies as missing',
   w:'Heights of 12in and 96in, ape indices of &plusmn;30in. Under 1% of rows, but the 50 most extreme users held 10.3% of all squared-height leverage.',
   r:'Kept as <i>missing</i>, not dropped &mdash; those climbers’ sends are still valid data.'},
  {t:'Standardise height and ape index',
   w:'In raw centred inches, h&sup2; reached 3,136, so a N(0, 0.3) prior on the quadratic implied contributions of &plusmn;106 grades against a 12-grade scale.',
   r:'In z-units the prior means what it says: &plusmn;1.9 grades.'},
  {t:'Indicators, not imputation, for missing bodies',
   w:'43.9% of ape indices and 15.8% of heights are absent. v1 filled them with the median, asserting a measurement never taken and piling 44% of users onto one value.',
   r:'A missingness dummy lets the slope be identified only by climbers who reported a value, while the missing group gets its own offset. Unbiased under MAR, at one parameter instead of ~19,000 latents.'},
  {t:'Centre every design column',
   w:'h&sup2; and a&sup2; had means of 0.85 and 0.64, entangling the intercept with every slope &mdash; &beta;<sub>0</sub> came back at R-hat 1.19, ESS 16.',
   r:'Centring makes &beta;<sub>0</sub> orthogonal to the slopes.'},
  {t:'Sum-to-zero gym corrections',
   w:'The model is identified only up to an additive ability/correction shift. A zero-<i>mean</i> prior anchors that only softly &mdash; the realised mean still drifts by &sigma;<sub>gym</sub>/&radic;G.',
   r:'ZeroSumNormal enforces it exactly, which is what &ldquo;relative to the average gym&rdquo; was always meant to mean.'},
  {t:'Log-link the gap rate, and centre visit count',
   w:'&kappa; multiplied raw visit counts (median ~7), making it little more than a rescaling of &lambda;<sub>0</sub> &mdash; the identical pathology the v1 code documents fixing for &rho;, but never applied to &kappa;.',
   r:'exp() keeps the rate positive without sign constraints and decouples the two.'},
  {t:'Marginalise the gap analytically',
   w:'Normal minus Exponential is an ExGaussian, available in closed form.',
   r:'Removes 32,501 latent variables, restores a real observed node so LOO works, and matches numerical integration to 1e-8.'},
  {t:'Probabilistic gender, sharpened by height but never by ability',
   w:'Letting climbing performance inform inferred gender would make &ldquo;tall people are stronger&rdquo; and &ldquo;tall people are more likely male&rdquo; mutually reinforcing.',
   r:'Cutting that feedback keeps the height effect identified by variation in <i>names</i>, which is independent of height.'},
];

// Four categorical colours, all drawn from the existing token palette and all
// with real opacity -- an earlier version used --lg-gold-soft (10% alpha) for
// Bouldering Project, which rendered those gyms effectively invisible.
const V2_BRAND_COLOURS = {
  'Touchstone': '--lg-cat-1',           // blue, the house accent (17 of 29 gyms)
  'Movement': '--lg-cat-2',             // copper
  'Bouldering Project': '--lg-cat-3',   // green
  'Stronghold': '--lg-cat-5',           // violet -- ochre read as a second copper
};

function v2Colour(row) {
  return cssVar(V2_BRAND_COLOURS[row.b] || '--lg-text-2') || '#888';
}

// Numbers quoted in the prose, filled from the same JSON the charts use.
// They were hardcoded and had drifted: the text claimed a 1.26-grade spread
// across "twenty" gyms while the data under it said 1.29 across 19. Prose that
// restates a figure has to read it, or it silently becomes wrong the next time
// the model is refitted.
function renderV2InlineFigures() {
  const R = V2_RESULTS;
  if (!R) return;
  // v1 measured six Touchstone gyms spanning 0.25 grades. That figure belongs
  // to the v1 model and is not recomputable from this JSON, so it stays a
  // constant -- but the multiple derived from it must not.
  const V1_TOUCHSTONE_SPAN = 0.25;
  const brand = (b) => R.gyms.filter((g) => g.b === b).map((g) => g.m);
  const mean = (a) => a.reduce((x, y) => x + y, 0) / (a.length || 1);
  const signed = (x) => `${x < 0 ? '\u2212' : '+'}${Math.abs(x).toFixed(2)}`;
  const fmt = {
    spread: () => `${(+R.spread).toFixed(2)} grades`,
    n_gyms: () => String(R.n_gyms),
    n_sig: () => String(R.n_sig),
    vs_v1: () => `${(R.spread / V1_TOUCHSTONE_SPAN).toFixed(1)}\u00d7`,
    movement_gap: () => `${(mean(brand('Movement'))
      - mean(brand('Touchstone'))).toFixed(2)} grades stiffer`,
    touchstone_range: () => {
      const t = brand('Touchstone');
      return `${signed(Math.min(...t))} to ${signed(Math.max(...t))}`;
    },
    pct_single: () => (R.pct_single_obs != null
      ? `${(100 * R.pct_single_obs).toFixed(0)}%` : '59%'),
    // The width of the ability prior, which is what a climber's posterior
    // falls back to when their only observation is held out. The whole
    // importance-sampling failure is this number against the ~0.5 the
    // posterior had while the row was in.
    sigma_user: () => (R.sigma_user
      ? `${(+R.sigma_user.mean).toFixed(2)} grades` : '—'),
  };
  document.querySelectorAll('[data-v2]').forEach((el) => {
    const f = fmt[el.dataset.v2];
    if (f) el.textContent = f();
  });
}

function renderV2Stats() {
  renderV2InlineFigures();
  const host = v2El('stats');
  if (!host) return;
  host.innerHTML = v2Stats().map((s) => `
    <div class="stat-tile">
      <div class="stat-value">${s.v}</div>
      <div class="stat-label">${s.l}</div>
      <div class="stat-sub">${s.s}</div>
    </div>`).join('');
}

function renderV2Decisions() {
  const host = v2El('decisions-grid');
  if (!host) return;
  host.innerHTML = V2_DECISIONS.map((d) => `
    <div class="decision-card">
      <div class="decision-title">${d.t}</div>
      <div class="decision-why"><span class="lbl">Problem</span>${d.w}</div>
      <div class="decision-res"><span class="lbl">Resolution</span>${d.r}</div>
    </div>`).join('');
}

function renderV2Table(id, rows) {
  const el = document.getElementById(id);
  if (!el) return;
  const [head, ...body] = rows;
  el.innerHTML = `<thead><tr>${head.map((h) => `<th>${h}</th>`).join('')}</tr></thead>`
    + `<tbody>${body.map((r) => `<tr>${r.map((c) => `<td>${c}</td>`).join('')}</tr>`).join('')}</tbody>`;
}

// Takes an element id so the current Grading Model tab can render the same
// figure into its own container. Default keeps the archived v2 page's
// behaviour byte-for-byte; ids must be unique per document, the figure need not be.
function renderV2GymChart(elId = v2Id('gym-chart')) {
  const el = document.getElementById(elId);
  if (!el || typeof Plotly === 'undefined') return;
  const rows = V2_GYMS;
  // One trace per company, so Plotly's own legend does the show/hide. The row
  // order is pinned to the (correction-sorted) full list -- otherwise splitting
  // into traces would regroup the axis by company.
  const order = rows.map((r) => r.g);
  const brands = [...new Set(rows.map((r) => r.b))]
    .sort((a, b) => rows.filter((r) => r.b === b).length - rows.filter((r) => r.b === a).length);
  const traces = brands.map((b) => {
    const rs = rows.filter((r) => r.b === b);
    const c = v2Colour(rs[0]);
    return {
      type: 'scatter', mode: 'markers', name: b, legendgroup: b,
      x: rs.map((r) => r.m),
      y: rs.map((r) => r.g),
      // Error bars take the company's colour too -- a grey bar reads as a
      // separate annotation rather than as this point's uncertainty.
      error_x: {
        type: 'data', symmetric: false,
        array: rs.map((r) => r.hi - r.m),
        arrayminus: rs.map((r) => r.m - r.lo),
        color: hexToRgba(c, 0.45), thickness: 1.6, width: 3,
      },
      // Hollow marker = interval still contains zero. Fills are knocked back
      // so 29 dots read as a field rather than 29 competing signals.
      marker: {
        size: 11,
        color: rs.map((r) => (r.s ? hexToRgba(c, 0.72) : cssVar('--lg-card'))),
        line: { width: 2, color: hexToRgba(c, 0.85) },
      },
      hovertemplate: `<b>%{y}</b><br>${b}<br>correction %{x:+.3f} grades<extra></extra>`,
    };
  });
  const layout = chartLayout('grading correction (grades) — negative = softer, positive = stiffer');
  layout.height = Math.max(360, rows.length * 26 + 90);
  layout.margin = { l: 260, r: 30, t: 14, b: 52 };
  layout.shapes = [{
    type: 'line', xref: 'x', yref: 'paper', x0: 0, x1: 0, y0: 0, y1: 1,
    line: { color: cssVar('--lg-text-2'), width: 1, dash: 'dot' },
  }];
  layout.yaxis = {
    ...layout.yaxis, automargin: true, tickfont: { size: 11 },
    categoryorder: 'array', categoryarray: order,
    range: [-0.5, rows.length - 0.5],
  };
  layout.showlegend = true;
  // Parked bottom-right, the one corner the sorted data leaves empty.
  // Vertical, not the horizontal default from chartLayout -- a horizontal
  // legend spreads across the bottom and lands on top of the softest gyms.
  layout.legend = {
    ...layout.legend, orientation: 'v',
    x: 0.99, y: 0.01, xanchor: 'right', yanchor: 'bottom',
    bgcolor: cssVar('--lg-card'), bordercolor: cssVar('--lg-border'),
    borderwidth: 1, font: { size: 11 }, itemsizing: 'constant',
  };
  Plotly.react(el, traces, layout, { displayModeBar: false, responsive: true });
}

// Takes an element id so the current Grading Model tab can render the same
// figure into its own container. Default keeps the archived v2 page's
// behaviour byte-for-byte; ids must be unique per document, the figure need not be.
function renderV2BrandChart(elId = v2Id('brand-chart')) {
  const el = document.getElementById(elId);
  if (!el || typeof Plotly === 'undefined') return;
  const brands = ['Touchstone', 'Bouldering Project', 'Stronghold', 'Movement'];
  const traces = brands.map((b) => {
    const rows = V2_GYMS.filter((r) => r.b === b);
    return {
      type: 'box', name: b, y: rows.map(() => b), x: rows.map((r) => r.m),
      orientation: 'h', boxpoints: 'all', jitter: 0.5, pointpos: 0,
      marker: { size: 9, color: cssVar(V2_BRAND_COLOURS[b] || '--lg-text-2') },
      line: { color: cssVar('--lg-border') },
      fillcolor: 'rgba(0,0,0,0)',
      hovertemplate: '%{text}<br>%{x:+.3f}<extra></extra>',
      text: rows.map((r) => r.g),
    };
  });
  const layout = chartLayout('grading correction (grades)');
  layout.height = 300;
  layout.showlegend = false;
  layout.margin = { l: 150, r: 24, t: 12, b: 48 };
  layout.shapes = [{ type: 'line', x0: 0, x1: 0, y0: -0.5, y1: 3.5,
    line: { color: cssVar('--lg-text-2'), width: 1, dash: 'dot' } }];
  Plotly.react(el, traces, layout, { displayModeBar: false, responsive: true });
}

function renderV2DiscardChart() {
  const el = v2El('discard-chart');
  if (!el || typeof Plotly === 'undefined') return;
  const trace = {
    type: 'bar',
    x: V2_DISCARD.map((d) => (d[0] === 5 ? '5+' : String(d[0]))),
    y: V2_DISCARD.map((d) => d[1]),
    marker: {
      color: V2_DISCARD.map((d) => (d[0] <= 1 ? cssVar('--lg-gold') : cssVar('--lg-text-2'))),
    },
    hovertemplate: '%{y:.1f}% of sends<extra></extra>',
  };
  const layout = chartLayout('grades below that climber’s own max at the same gym');
  layout.height = 260;
  layout.yaxis = { ...layout.yaxis, title: { text: '% of all sends' } };
  layout.annotations = [{
    x: 1.5, y: 20, xref: 'x', yref: 'y', showarrow: false,
    text: 'highlighted: within 1 grade of max (22.5%)',
    font: { size: 11, color: cssVar('--lg-text-2') },
  }];
  Plotly.react(el, [trace], layout, { displayModeBar: false, responsive: true });
}

function renderV2GenderValidation() {
  const el = v2El('gender-validation');
  if (!el || typeof Plotly === 'undefined') return;
  const trace = {
    type: 'scatter', mode: 'lines+markers',
    x: V2_GENDER_VAL.map((d) => d.x), y: V2_GENDER_VAL.map((d) => d.h),
    marker: { size: V2_GENDER_VAL.map((d) => 8 + Math.sqrt(d.n) / 18),
      color: cssVar('--lg-gold') },
    line: { color: cssVar('--lg-gold'), width: 2 },
    text: V2_GENDER_VAL.map((d) => `n = ${d.n.toLocaleString()}`),
    hovertemplate: 'P(female) %{x}<br>mean height %{y:.2f} in<br>%{text}<extra></extra>',
  };
  const layout = chartLayout('name-based P(female)');
  layout.height = 300;
  layout.yaxis = { ...layout.yaxis, title: { text: 'mean reported height (in)' } };
  Plotly.react(el, [trace], layout, { displayModeBar: false, responsive: true });
}

function renderV2HeightHist() {
  const el = v2El('height-hist');
  if (!el || typeof Plotly === 'undefined') return;
  // Illustrative shape: real distribution is ~N(67,4) with junk at the edges.
  const xs = [], ys = [];
  for (let h = 10; h <= 100; h += 1) {
    xs.push(h);
    let y = 3400 * Math.exp(-((h - 67.5) ** 2) / (2 * 4.2 ** 2));
    if (h === 12 || h === 13 || h === 96) y += 60;
    ys.push(y);
  }
  const layout = chartLayout('reported height (inches)');
  layout.height = 280;
  layout.yaxis = { ...layout.yaxis, title: { text: 'climbers' } };
  layout.shapes = [{
    type: 'rect', x0: 48, x1: 84, y0: 0, y1: 3600, yref: 'y',
    fillcolor: cssVar('--lg-gold'), opacity: 0.10, line: { width: 0 },
  }];
  layout.annotations = [
    { x: 12, y: 220, text: '12 in', showarrow: true, arrowhead: 0, ax: 0, ay: -26,
      font: { size: 11, color: cssVar('--lg-highlight') } },
    { x: 96, y: 220, text: '96 in (slider cap)', showarrow: true, arrowhead: 0, ax: 0, ay: -26,
      font: { size: 11, color: cssVar('--lg-highlight') } },
  ];
  Plotly.react(el, [{
    type: 'bar', x: xs, y: ys, marker: {
      color: xs.map((h) => (h >= 48 && h <= 84 ? cssVar('--lg-gold') : cssVar('--lg-highlight'))),
    },
    hovertemplate: '%{x} in<extra></extra>',
  }], layout, { displayModeBar: false, responsive: true });
}


// --- symbol glossary: every term that appears in an equation on this page ---
const V2_SYMBOLS = [
  ['group', 'Indices'],
  ['u', '\\(u\\)', 'a climber', '&mdash;'],
  ['g', '\\(g\\)', 'a gym', '&mdash;'],
  ['group', 'Ability'],
  ['beta0', '\\(\\beta_0\\)', 'baseline ability &mdash; the grade an average climber sends at an average gym', 'grades'],
  ['sigma_user', '\\(\\sigma_{\\text{user}}\\)', 'spread of natural ability between climbers', 'grades'],
  ['eps', '\\(\\tilde\\epsilon_u\\)', 'climber \\(u\\)&rsquo;s personal ability offset, standardised', 'SDs'],
  ['x', '\\(\\mathbf{x}_u\\)', 'covariate row for climber \\(u\\): gender, height terms, ape terms, missingness flags', '&mdash;'],
  ['group', 'Body and gender'],
  ['h', '\\(\\tilde h\\)', 'height, centred at the median and divided by its SD', 'SDs (1 SD &asymp; {H_SD} in)'],
  ['a', '\\(\\tilde a\\)', 'ape index (wingspan &minus; height), centred and scaled the same way', 'SDs (1 SD &asymp; {A_SD} in)'],
  ['G', '\\(G\\)', 'probability the climber is female, from their first name sharpened by height. 0 = male, 1 = female', 'probability'],
  ['gM', '\\(\\gamma_1^{M},\\ \\gamma_2^{M}\\)', 'slope and curvature of the height curve for <b>male users</b>. Identical to \\(\\gamma_1,\\gamma_2\\) &mdash; the male curve <i>is</i> the baseline', 'grades / SD, grades / SD²'],
  ['gF', '\\(\\gamma_1^{F},\\ \\gamma_2^{F}\\)', 'slope and curvature of the height curve for <b>female users</b>. Not sampled directly; equals \\(\\gamma_k+\\gamma_k^{\\times}\\)', 'grades / SD, grades / SD²'],
  ['gbase', '\\(\\gamma_1,\\ \\gamma_2\\)', 'what the sampler actually fits: the baseline (male-user) slope and curvature', 'grades / SD, grades / SD²'],
  ['gx', '\\(\\gamma_1^{\\times},\\ \\gamma_2^{\\times}\\)', 'the <b>gender difference</b> &mdash; how far the female curve departs from the male one. <b>This is the parameter the gender question turns on</b>; if its interval covers zero, the two curves are the same shape', 'grades / SD, grades / SD²'],
  ['delta', '\\(\\delta_1,\\ \\delta_2\\)', 'the same pair for ape index: linear slope and curvature', 'grades / SD'],
  ['sat', '\\(A,\\ h_0,\\ s\\)', 'saturating form only: how much reach is worth in total, the height it stops paying off at, and how sharply it levels', 'grades, SDs, SDs'],
  ['kappa_h', '\\(\\kappa_h\\)', 'vertex form only: how sharply ability falls away from the best height. Larger = a tighter optimum. <b>Distinct from the gap-rate \\(\\kappa\\) below</b>', 'grades / SD²'],
  ['p', '\\(p\\)', 'vertex form only: <b>the best height</b>, estimated directly rather than derived from \\(-\\gamma_1/2\\gamma_2\\)', 'SDs from median'],
  ['group', 'Name &rarr; gender'],
  ['p_name', '\\(p_{\\text{name}}\\)', 'calibrated probability that this first name belongs to a woman, from nomquamgender &mdash; before height is taken into account', 'probability'],
  ['musd', '\\(\\mu_M,\\sigma_M,\\ \\mu_F,\\sigma_F\\)', 'mean and SD of height within each gender group, used to sharpen the name prior. <b>Estimated without any ability data</b>, so the gender guess cannot be contaminated by the effect under test', 'inches'],
  ['group', 'Gyms'],
  ['sigma_gym', '\\(\\sigma_{\\text{gym}}\\)', 'spread of grading style across gyms &mdash; the headline &ldquo;how much do gyms differ&rdquo; number', 'grades'],
  ['delta_g', '\\(\\tilde\\delta_g\\)', 'gym \\(g\\)&rsquo;s standardised offset, constrained so all gyms sum to zero', 'SDs'],
  ['gym', '\\(\\text{gym}_g\\)', 'gym \\(g\\)&rsquo;s grading correction. Positive = stiffer than average', 'grades'],
  ['C', '\\(C_{u,g}\\)', 'structural ceiling &mdash; the hardest grade climber \\(u\\) could send at gym \\(g\\)', 'grades'],
  ['group', 'The gap, and what we observe'],
  ['m', '\\(m_{u,g}\\)', '<b>the observed data</b>: the hardest grade \\(u\\) actually logged at \\(g\\)', 'grades'],
  ['gap', '\\(\\text{gap}_{u,g}\\)', 'how far below their ceiling that logged max sits. Never negative', 'grades'],
  ['lambda', '\\(\\lambda_{u,g}\\)', 'rate of that gap &mdash; higher rate means a smaller expected gap', '1 / grades'],
  ['lambda0', '\\(\\lambda_0\\)', 'baseline gap rate for a typical climber at a typical gym', '1 / grades'],
  ['n', '\\(\\tilde n_{u,g}\\)', 'how many days \\(u\\) logged at \\(g\\), centred on the median (~8)', 'ratio'],
  ['kappa', '\\(\\kappa\\)', 'how much more of their ceiling a climber finds per extra visit', '&mdash;'],
  ['r', '\\(\\tilde r_u\\)', 'sends per session, centred &mdash; a proxy for how completely someone logs', 'ratio'],
  ['rho', '\\(\\rho\\)', 'how much that logging-completeness shifts the gap. <b>Fitted at &asymp;0</b>', '&mdash;'],
  ['sigma_link', '\\(\\sigma_{\\text{link}}\\)', 'residual noise: grades are integers on a continuous scale, so a labelled V5 is really 4.5&ndash;5.5', 'grades'],
];

const V2_FORMS = [
  ['form', 'equation', 'the claim it makes', 'params'],
  ['Zero', '\\(f=0\\)', 'Height does not affect climbing ability at all.', '0'],
  ['Linear', '\\(\\gamma_1\\tilde h\\)', 'Every inch helps (or hurts) by the same amount, forever.', '1'],
  ['Quadratic', '\\(\\gamma_1\\tilde h+\\gamma_2\\tilde h^2\\)', 'There is an optimal height, or an accelerating trend &mdash; one bend, no more.', '2'],
  ['Quadratic &times; gender', '\\((1-G)f_M(\\tilde h)+G\\,f_F(\\tilde h)\\)', 'Height works <i>differently</i> for men and women &mdash; a separate quadratic for each, blended by the probability \\(G\\). <b>This is what v1 concluded.</b>', '4'],
  ['Saturating', '\\(A\\,\\text{logistic}((\\tilde h-h_0)/s)\\)', 'Reach helps until you have enough of it, then stops paying. Never tested in v1.', '3'],
  ['Vertex quadratic', '\\(-\\kappa_h(\\tilde h-p)^2\\)', 'Same curve family as the plain quadratic, but the <b>best height \\(p\\) is a parameter</b> with its own credible interval &mdash; instead of being derived as \\(-\\gamma_1/2\\gamma_2\\) with error propagated through a ratio.', '2'],
];

// Each form's parameters, defaults and curve(s). Drives both the overlay
// chart and the interactive cards, so the two can never drift apart.
// `fitted: true` means the defaults are real posterior means (v3_conf,
// net50/confident); otherwise they are illustrative and marked as such.
// Height centring, in inches. These are a property of the fitted dataset, not
// constants -- v2Scales() reads them off the fits once those have loaded, and
// these are only the value before that. (An earlier hard-coded 3.4 was simply
// wrong for net50/confident, whose SD is 3.92 in.)
const v2HMed = () => v2Scales().h_median;
const v2HSd = () => v2Scales().h_sd;

const V2_FORM_SPECS = [
  {
    key: 'zero', label: 'Zero', fitted: false,
    eq: 'f(\\tilde h) = 0',
    claim: 'Height does not affect ability at all. The null the others must beat.',
    params: [],
    curves: () => [{ name: 'Zero', colour: '--lg-info', dash: 'solid', f: () => 0 }],
  },
  {
    key: 'linear', label: 'Linear', fitted: false,
    eq: 'f(\\tilde h) = \\gamma_1\\tilde h',
    claim: 'Every inch helps (or hurts) by the same amount, forever. Cannot bend.',
    params: [{ id: 'g1', tex: '\\gamma_1', min: -0.6, max: 0.6, step: 0.01, def: 0.18 }],
    curves: (v) => [{ name: 'Linear', colour: '--lg-info', dash: 'solid', f: (z) => v.g1 * z }],
  },
  {
    key: 'quadratic', label: 'Quadratic', fitted: false,
    eq: 'f(\\tilde h) = \\gamma_1\\tilde h + \\gamma_2\\tilde h^{2}',
    claim: 'One bend, no more. A peak (or a trough) sits at \\(-\\gamma_1/2\\gamma_2\\), wherever the data put it.',
    params: [
      { id: 'g1', tex: '\\gamma_1', min: -0.6, max: 0.6, step: 0.01, def: 0.18 },
      { id: 'g2', tex: '\\gamma_2', min: -0.3, max: 0.3, step: 0.005, def: -0.09 },
    ],
    curves: (v) => [{ name: 'Quadratic', colour: '--lg-info', dash: 'solid',
                      f: (z) => v.g1 * z + v.g2 * z * z }],
    note: (v) => (Math.abs(v.g2) < 1e-6 ? 'no curvature - this is a straight line'
      : `vertex at ${(v2HMed() + (-v.g1 / (2 * v.g2)) * v2HSd()).toFixed(1)} in `
        + `(${v.g2 < 0 ? 'a peak' : 'a trough'})`),
  },
  {
    key: 'quadratic_x_gender', label: 'Quadratic × gender', fitted: true,
    eq: '\\begin{aligned} f &= (1-G)\\left(\\gamma_1\\tilde h + \\gamma_2\\tilde h^{2}\\right) \\\\'
      + ' &\\quad + G\\left((\\gamma_1{+}\\gamma_1^{\\times})\\tilde h + (\\gamma_2{+}\\gamma_2^{\\times})\\tilde h^{2}\\right) \\end{aligned}',
    claim: 'A separate quadratic per gender. <b>This is what v1 concluded.</b> Set both \\(\\gamma^{\\times}\\) to zero and the two curves collapse onto each other.',
    params: [
      { id: 'g1', tex: '\\gamma_1', min: -0.6, max: 0.6, step: 0.01, def: -0.176 },
      { id: 'g2', tex: '\\gamma_2', min: -0.3, max: 0.3, step: 0.005, def: -0.069 },
      { id: 'g1x', tex: '\\gamma_1^{\\times}', min: -0.6, max: 0.6, step: 0.01, def: 0.153 },
      { id: 'g2x', tex: '\\gamma_2^{\\times}', min: -0.3, max: 0.3, step: 0.005, def: 0.159 },
    ],
    // Blue for male, orange for female, both solid -- the only card that
    // draws two curves, so no cross-form colour clash to design around.
    curves: (v) => [
      { name: 'Male users', colour: '--lg-info', dash: 'solid',
        f: (z) => v.g1 * z + v.g2 * z * z },
      { name: 'Female users', colour: '--lg-highlight', dash: 'solid',
        f: (z) => (v.g1 + v.g1x) * z + (v.g2 + v.g2x) * z * z },
    ],
  },
  {
    key: 'saturating', label: 'Saturating', fitted: false,
    eq: 'f(\\tilde h) = A\\,\\operatorname{logistic}\\!\\left(\\frac{\\tilde h - h_0}{s}\\right)',
    claim: 'Reach helps until you have enough of it, then stops paying. <b>Monotone by construction</b> &mdash; it cannot turn back down. Never tested in v1.',
    params: [
      { id: 'A', tex: 'A', min: -1.2, max: 1.2, step: 0.02, def: 0.55 },
      { id: 'h0', tex: 'h_0', min: -2.5, max: 2.5, step: 0.05, def: -0.4 },
      { id: 's', tex: 's', min: 0.1, max: 2.0, step: 0.05, def: 0.55 },
    ],
    curves: (v) => [{ name: 'Saturating', colour: '--lg-info', dash: 'solid',
                      f: (z) => v.A / (1 + Math.exp(-(z - v.h0) / Math.max(v.s, 0.05))) - v.A / 2 }],
  },
  {
    key: 'vertex_quadratic', label: 'Vertex quadratic', fitted: false,
    eq: 'f(\\tilde h) = -\\kappa_h\\left(\\tilde h - p\\right)^{2}',
    claim: 'The same curve family as the plain quadratic, but the <b>best height \\(p\\) is a parameter</b> you can read an interval off directly. Drag \\(p\\) to move the peak off the median.',
    params: [
      { id: 'kh', tex: '\\kappa_h', min: 0, max: 0.4, step: 0.005, def: 0.035 },
      { id: 'pk', tex: 'p', min: -3, max: 3, step: 0.05, def: 0.9 },
    ],
    curves: (v) => [{ name: 'Vertex quadratic', colour: '--lg-info', dash: 'solid',
                      f: (z) => -v.kh * (z - v.pk) * (z - v.pk) + v.kh * 2 }],
    note: (v) => `peak at ${(v2HMed() + v.pk * v2HSd()).toFixed(1)} in`,
  },
];

const V2_FORM_BY_KEY = Object.fromEntries(V2_FORM_SPECS.map((f) => [f.key, f]));
// Live slider state, seeded from each spec's defaults.
const v2FormState = Object.fromEntries(V2_FORM_SPECS.map((f) => [
  f.key, Object.fromEntries(f.params.map((p) => [p.id, p.def])),
]));

// Where each group actually sits: median +/- 1 SD from the cleaned data.
const V2_HEIGHT_BANDS = [
  { label: 'Male users', mid: 69.2, sd: 3.34, tok: '--lg-info', anchor: 'right' },
  { label: 'Female users', mid: 64.2, sd: 2.83, tok: '--lg-highlight', anchor: 'left' },
];

// Element id is a parameter so the current Grading Model tab can render the
// same symbol table into its own docked panel. V2_SYMBOLS already carries
// units for every row, so there is nothing to restate.
function renderV2Symbols(elId = v2Id('symbols')) {
  const el = document.getElementById(elId);
  if (!el) return;
  const sc = v2Scales();
  const sub = (t) => String(t)
    .replace('{H_SD}', sc.h_sd.toFixed(1))
    .replace('{A_SD}', sc.a_sd.toFixed(1));
  let html = '<tbody>';
  V2_SYMBOLS.forEach((r) => {
    if (r[0] === 'group') {
      html += `<tr class="sym-group"><td colspan="2">${r[1]}</td></tr>`;
    } else {
      const unit = (r[3] && r[3] !== '&mdash;') ? `<span class="unit">${sub(r[3])}</span>` : '';
      html += `<tr class="sym-row" data-sym="${r[0]}">`
        + `<td class="sym">${r[1]}</td><td>${r[2]}${unit}</td></tr>`;
    }
  });
  el.innerHTML = html + '</tbody>';
}

