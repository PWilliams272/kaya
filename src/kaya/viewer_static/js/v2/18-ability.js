// ---- The climber ability model, as a live demonstration
//
// Drives the ability-model section of /prelim. Notation follows the Findings
// page's "model in five equations" exactly, so the two read as one argument:
//
//   log lambda[u,g] = log lambda0 + kappa * n~[u,g] + rho * r~[u]
//   gap[u,g] ~ Exponential(lambda[u,g])
//   m[u,g]   = C[u,g] - gap[u,g] + eps,   eps ~ Normal(0, sigma_link)
//   -m[u,g]  ~ ExGaussian(-C[u,g], sigma_link, 1/lambda[u,g])
//
// TWO KINDS OF PANEL, AND THE DIFFERENCE BETWEEN THEM IS THE POINT.
//
//   The MODEL panel is the fitted thing. The curve is the ExGaussian above --
//   the density of m ALONE, where one climber's hardest logged send at one gym
//   lands. It is not the distribution of their individual sends; the model
//   assigns those none. Nothing in this panel is invented.
//
//   The ceiling is fixed, not a slider: the reader watches the observed
//   maximum climb while the truth stays put. sigma_link shapes the curve and
//   bands the m line, and does not move the logged sends -- it is noise on the
//   grade LABEL, and the model applies it only to the maximum.
//
//   The SIMULATION panels moved to 19-ability-sim.js, which must load AFTER
//   this file: it uses the helpers and the AB state object defined here, and
//   abRender below calls into it.

// The climber's true ceiling. FIXED, not a control: the reader is asked to
// watch the observed maximum move while the truth stays put, and a ceiling
// slider invites exactly the opposite reading.
const AB_CEILING = 6.2;

const AB = {
  C: AB_CEILING,                         // fixed -- no control for it
  visits: 13,
  sigmaLink: 0.25,
  attempts: 12,
  policy: 'all',
  seed: 7,
};

// Fitted values, overridden from v2_prelim.json when the page has it. Kept as
// literals too so this section still draws if the payload is missing.
const AB_FIT = { logLambda0: 0.0808, kappa: 0.2774, rho: -0.0179, nvScale: 7.0 };

const AB_POLICY = {
  all: { label: 'Log every send', keep: 'all' },
  proud: { label: 'Log only the proud ones', keep: 'proud' },
  sporadic: { label: 'Log sporadically, 1 in 4', keep: 'sporadic' },
  first: { label: 'Log the first send at each grade', keep: 'first' },
};

const abEl = (n) => document.getElementById(`ab-${n}`);

function abFit() {
  try {
    const f = V2_PRELIM?.fits?.find((x) => x.name === V2_PRELIM.reference);
    if (f?.params?.log_lambda0) {
      return {
        logLambda0: f.params.log_lambda0.mean,
        kappa: f.params.kappa.mean,
        rho: f.params.rho.mean,
        nvScale: AB_FIT.nvScale,
      };
    }
  } catch (e) { /* payload not loaded; the literals below are the fallback */ }
  return AB_FIT;
}

// log lambda = log lambda0 + kappa * n~ + rho * r~, with n~ the visit count
// centred on its median. Returns the RATE; the mean gap is its reciprocal.
function abLambda(visits, fit, rTilde = 0) {
  const nTilde = visits / fit.nvScale - 1.0;
  return Math.exp(fit.logLambda0 + fit.kappa * nTilde + fit.rho * rTilde);
}

function abRng(seed) {
  let s = seed >>> 0;
  return () => { s = (s * 1664525 + 1013904223) >>> 0; return s / 4294967296; };
}
function abNormal(rand) {
  const u = Math.max(rand(), 1e-12);
  return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * rand());
}
// Normal CDF via erf, for the ExGaussian density below.
function abPhi(z) {
  const t = 1 / (1 + 0.2316419 * Math.abs(z));
  const d = 0.3989422804014327 * Math.exp(-z * z / 2);
  const p = d * t * (0.319381530 + t * (-0.356563782 + t * (1.781477937
    + t * (-1.821255978 + t * 1.330274429))));
  return z >= 0 ? 1 - p : p;
}

// Density of m = C - Exponential(lambda) + Normal(0, s): where the climber's
// HARDEST LOGGED SEND lands. One number per climber-gym -- this is not the
// distribution of their individual sends, which the model never assigns one.
// It is the fourth equation, -m ~ ExGaussian(-C, sigma_link, 1/lambda), drawn.
//
// Checked against scipy.stats.exponnorm(K=(1/lambda)/s, loc=-C, scale=s) on a
// 4M-point grid: max absolute difference 4.4e-16, integral 1.0, mean
// C - 1/lambda, variance s^2 + 1/lambda^2. The leading factor is lambda, not
// lambda/2 -- with the half it integrates to 0.5.
function abExGaussPdf(m, C, s, lambda) {
  const z = C - m;                       // the gap, before noise
  const a = lambda * Math.exp((lambda / 2) * (lambda * s * s - 2 * z));
  return a * abPhi((z - lambda * s * s) / s);
}

// P(m > C): how much of the curve sits ABOVE the ceiling. All of it comes from
// sigma_link -- set that to zero and the density stops dead at C, because the
// shortfall alone is one-sided. Closed form, from m = C - Exp + Normal:
//
//   P(m > C) = P(Normal(0,s) > Exp(lambda))
//            = 1/2 - exp(lambda^2 s^2 / 2) * Phi(-lambda * s)
//
// Checked against scipy's exponnorm CDF at four (s, lambda) settings spanning
// both sliders -- agreement to 1e-16 at 0.1122, 0.1504, 0.4555 and 0.0218.
function abTailAboveCeiling(s, lambda) {
  return 0.5 - Math.exp((lambda * lambda * s * s) / 2) * abPhi(-lambda * s);
}

// ---- panel 0: the lay of the land ----
//
// Static and uninteractive on purpose. It names the four objects the equations
// are about to use, and two of the four are things the data never contains:
//
//   logged sends (dots)   in the data, and all the model ever sees
//   m, their maximum      in the data -- one number per climber-gym
//   failed attempts (x)   NOT recorded by Kaya, and not in the likelihood
//   C, the true ceiling   never observed by anyone, the thing being estimated
//
// Attempts appear below the ceiling as well as above it, which is not a
// drawing error: you fail climbs you are capable of. That is what makes C a
// limit rather than a guarantee, and it is why the shortfall exists at all.

const AB_LAND_C = 6.6;          // the true ceiling

// Every mark's position, hard-coded as [grade, height]. Laid out once offline
// by a settling beeswarm -- each mark tried a few jittered x's about its grade
// and took the one that landed in the lowest free row -- then frozen here. The
// heap should LOOK casual without BEING casual: no RNG runs on this page, so
// the figure is byte-identical on every load and can be reasoned about.
//
// Sends and attempts share one band and are interleaved within each grade,
// because they happened interleaved. Note the attempt at V4 and the several at
// V5 and V6, below the ceiling: you fail climbs you are capable of, which is
// what makes C a limit rather than a guarantee, and why m lands short of it.
const AB_LAND_SENDS = [
  [2.026, 0.0406], [1.927, 0.0403], [3.0, 0.0493], [2.867, 0.0438], [3.106, 0.0424], [2.998, 0.1054],
  [3.217, 0.0482], [4.021, 0.0494], [3.922, 0.0494], [4.133, 0.0435], [3.995, 0.1045], [4.121, 0.1084],
  [3.823, 0.0398], [4.002, 0.165], [3.786, 0.1022], [4.151, 0.1743], [3.899, 0.1647], [4.998, 0.0482],
  [5.099, 0.0403], [4.866, 0.0489], [5.001, 0.1114], [5.13, 0.1023], [5.1, 0.1686], [5.221, 0.0425],
  [4.78, 0.1023], [5.009, 0.2271], [5.114, 0.2276], [5.002, 0.2926], [4.791, 0.2353], [6.007, 0.0477],
  [6.127, 0.1648], [5.905, 0.1741], [5.898, 0.2255],
];

const AB_LAND_ATTEMPTS = [
  [3.891, 0.1049], [4.903, 0.106], [5.0, 0.1734], [4.829, 0.1672], [4.906, 0.2344], [6.11, 0.0457],
  [5.888, 0.0394], [5.79, 0.0505], [6.003, 0.1109], [6.155, 0.111], [5.825, 0.1099], [6.008, 0.1741],
  [6.022, 0.2332], [6.152, 0.2261], [7.0, 0.042], [7.109, 0.0462], [6.894, 0.0495], [7.004, 0.1028],
  [6.896, 0.106], [7.123, 0.1049], [8.011, 0.0469], [8.129, 0.0429],
];

function abLandXY(pts) {
  return { x: pts.map((q) => q[0]), y: pts.map((q) => q[1]) };
}

// A FEATHERED stroke: the same line drawn three times, widest and faintest
// first, so its long edges fall off into the page instead of ending at a hard
// boundary. Plotly exposes no blur, no line cap and no gradient on a shape, so
// stacking passes is the only way to get a soft edge out of it.
//
// Composited, the centre lands near alpha 0.58 -- darker than the flat 0.42 it
// replaces -- while the outermost pass is faint enough to read as a fade.
//
// The halo is deliberately TIGHT: an 8px outer pass read as fuzzy rather than
// soft, so the widest pass is now barely wider than the core. Enough to take
// the hard edge off, not enough to blur it.
const AB_AXIS_PASSES = [[5.4, 0.09], [3.6, 0.19], [2.4, 0.40]];

function abSoftLine(x0, x1, y0, y1) {
  const rgb = cssVar('--pm-axis-rgb');
  return AB_AXIS_PASSES.map(([width, alpha]) => ({
    type: 'line', x0, x1, y0, y1, layer: 'below',
    line: { color: `rgba(${rgb},${alpha})`, width },
  }));
}

// Ticks straddle the spline rather than hanging below it, and are half again
// as tall as the plain Plotly ones they replace. In data units, because the
// y-axis is unitless here: HALF the total, each way from the axis.
const AB_TICK_HALF = 0.021;

function abPlotLandscape() {
  const el = abEl('landscape');
  if (!el || typeof Plotly === 'undefined') return;

  const sends = abLandXY(AB_LAND_SENDS);
  const attempts = abLandXY(AB_LAND_ATTEMPTS);
  // m is READ OFF THE DOTS -- whichever drawn send is hardest -- rather than
  // being placed at a nominal grade. Otherwise a jittered dot can land to the
  // right of its own maximum, which is the one thing this figure cannot say.
  const landM = Math.max(...sends.x);

  const traces = [
    // Alpha on the sends so a dense heap reads as depth rather than as a solid
    // blue mass, and so the crosses mixed into it stay legible.
    { type: 'scatter', mode: 'markers', name: 'logged sends', ...sends,
      marker: { symbol: 'circle', size: 11, color: cssVar('--lg-cat-1'), opacity: 0.62 },
      hoverinfo: 'skip' },
    { type: 'scatter', mode: 'markers', name: 'attempts, never recorded',
      ...attempts,
      marker: {
        symbol: 'x-thin', size: 10, opacity: 0.75,
        line: { color: cssVar('--lg-text-2'), width: 1.8 },
      },
      hoverinfo: 'skip' },
  ];

  const layout = chartLayout('');
  layout.height = 250;
  layout.margin = { l: 16, r: 16, t: 56, b: 44 };
  layout.paper_bgcolor = 'rgba(0,0,0,0)';
  layout.plot_bgcolor = 'rgba(0,0,0,0)';
  // Inert: this is a diagram, not an instrument. No hover, no drag, no zoom --
  // a tooltip on a mark that stands for "a send" has nothing true to say.
  // Review mode's label dragging still works; `edits` does not go through
  // dragmode.
  layout.hovermode = false;
  layout.dragmode = false;
  layout.xaxis = {
    ...layout.xaxis, automargin: false, range: [1.6, 8.6], showgrid: false,
    zeroline: false, tickmode: 'array', tickvals: [2, 3, 4, 5, 6, 7, 8],
    ticktext: ['V2', 'V3', 'V4', 'V5', 'V6', 'V7', 'V8'],
    // Plotly's own spine and ticks are switched off and redrawn as feathered
    // shapes below -- it can only hang a tick BELOW the axis, and it cannot
    // soften an edge at all. The ticks stay on as fully transparent spacers so
    // the tick LABELS keep their standoff from the line.
    ticks: 'outside', ticklen: 7, tickcolor: 'rgba(0,0,0,0)',
    title: undefined, showline: false, fixedrange: true,
  };
  layout.yaxis = {
    // Starts below zero so the upper AND lower halves of each tick have room;
    // shapes drawn outside the range would be clipped at the axis.
    ...layout.yaxis, range: [-0.035, 0.60], visible: false, showgrid: false,
    zeroline: false, showticklabels: false, title: undefined, fixedrange: true,
  };
  layout.legend = {
    orientation: 'h', yanchor: 'bottom', y: 1.04, xanchor: 'left', x: 0,
    font: { size: 11.5 },
  };
  layout.showlegend = true;

  layout.shapes = [
    ...abSoftLine(1.6, 8.6, 0, 0),
    ...[2, 3, 4, 5, 6, 7, 8].flatMap(
      (t) => abSoftLine(t, t, -AB_TICK_HALF, AB_TICK_HALF)),
    { type: 'line', x0: AB_LAND_C, x1: AB_LAND_C, y0: 0, y1: 0.46,
      line: { color: cssVar('--lg-highlight'), width: 2.4 } },
    { type: 'line', x0: landM, x1: landM, y0: 0, y1: 0.46,
      line: { color: cssVar('--lg-cat-1'), width: 2.2 } },
    { type: 'line', x0: AB_LAND_C, x1: landM, y0: 0.365, y1: 0.365,
      line: { color: cssVar('--lg-highlight'), width: 1.6, dash: 'dash' } },
  ];

  layout.annotations = [
    { x: AB_LAND_C, y: 0.52, xanchor: 'left', xshift: 7,
      text: "<i>C</i> – climber's true ability", showarrow: false,
      font: { size: 12, color: cssVar('--lg-highlight') } },
    { x: landM, y: 0.52, xanchor: 'right', xshift: -7,
      text: '<i>m</i> – hardest send logged', showarrow: false,
      font: { size: 12, color: cssVar('--lg-cat-1') } },
    { x: landM, y: 0.365, ax: 26, ay: 0, axref: 'pixel', ayref: 'pixel',
      text: '', showarrow: true, arrowhead: 2, arrowsize: 1.1, arrowwidth: 1.6,
      arrowcolor: cssVar('--lg-highlight') },
    { x: AB_LAND_C, y: 0.365, xanchor: 'left', xshift: 9,
      text: 'the shortfall', showarrow: false,
      font: { size: 11.5, color: cssVar('--lg-highlight') } },
  ];

  Plotly.react(el, traces, layout, { displayModeBar: false, responsive: true });
}

// ---- panel 1: the model ----

// ONE FIXED LOGBOOK, in visit order. Sixty entries, integer V grades, because
// that is what a Kaya logbook is. Sliding the visits control reveals the first
// n of these -- nothing ever moves, entries are only added, exactly as a real
// logbook grows. sigma_link does not touch them: the model assigns no
// distribution to an individual send, only to their maximum.
//
// The running maximum is the whole point of the picture:
//
//   n = 1   ->  V4        shortfall 2.2
//   n = 4   ->  V5        shortfall 1.2
//   n = 11  ->  V6        shortfall 0.2
//   n = 56  ->  V7        shortfall -0.8   (a soft V7 -- see the caption)
//
// so the hardest logged send creeps toward the ceiling as a step function of
// exposure and never gets there on schedule. That is the bias the model
// exists to undo.
const AB_LOGBOOK = [
  4, 3, 4, 5, 4, 5, 4, 5, 3, 5, 6, 4, 5, 5, 4,
  6, 5, 5, 4, 6, 5, 6, 5, 4, 6, 5, 6, 6, 5, 4,
  6, 5, 6, 6, 5, 6, 4, 6, 5, 6, 6, 5, 6, 6, 5,
  6, 6, 5, 6, 6, 6, 5, 6, 6, 5, 7, 6, 5, 6, 6,
];

function abMarkers(n) {
  return AB_LOGBOOK.slice(0, Math.max(1, Math.min(n, AB_LOGBOOK.length)));
}

// Arrange the logbook into a block above each grade tick, filling left to
// right then upwards. Every send is at an integer grade, so a plain vertical
// stack would be sixty deep at V6 by the end of the slider; laying each row
// out sideways keeps the pile the shape of a small histogram bar instead.
// Positions depend only on the index within a grade, so adding a send never
// moves one already drawn.
// Offsets alternate outwards from the tick -- 0, +1, -1, +2, -2 -- so a lone
// send sits exactly on its grade and a full row stays centred on it. An
// offset depends only on the index within the grade, so adding a send never
// moves one already drawn.
const AB_PILE_COLS = [0, 1, -1, 2, -2];
const AB_PILE_DX = 0.048;

function abPile(xs) {
  const seen = new Map();
  return xs.map((x) => {
    const k = seen.get(x) || 0;
    seen.set(x, k + 1);
    return {
      x: x + AB_PILE_COLS[k % AB_PILE_COLS.length] * AB_PILE_DX,
      row: Math.floor(k / AB_PILE_COLS.length),
    };
  });
}

function abPlotModel(p, fit) {
  const el = abEl('model');
  if (!el || typeof Plotly === 'undefined') return;
  const lambda = abLambda(p.visits, fit);
  const meanGap = 1 / lambda;

  // The observed maximum is read off the logbook itself, not computed from a
  // formula: it is whatever the hardest revealed entry happens to be.
  const sends = abMarkers(p.visits);
  const pile = abPile(sends);
  const maxLogged = Math.max(...sends);
  const gap = p.C - maxLogged;

  // The markers occupy the bottom band, the density the top. With no y-axis
  // the two only have to not collide -- the vertical dimension carries no
  // quantity here beyond "how many sends piled up". The step shrinks so the
  // tallest column still fits the band.
  const CURVE_FLOOR = 0.34;
  const MARK_BASE = 0.05;
  const tallest = Math.max(...pile.map((d) => d.row), 0);
  const MARK_STEP = Math.min(0.05, (CURVE_FLOOR - MARK_BASE - 0.05) / Math.max(tallest, 1));

  // V3 to V7 at the fixed ceiling, with a hair of overhang each side so the
  // outermost column of a marker pile is not clipped by the axis end. The
  // right end gives way when a large sigma_link pushes m's error bar past it,
  // rather than letting the bar run off the plot and misread as shorter.
  const lo = p.C - 3.35;
  const hi = Math.max(p.C + 0.9, maxLogged + p.sigmaLink + 0.45);
  const ticks = [];
  for (let t = Math.ceil(lo); t <= Math.floor(hi); t++) ticks.push(t);

  const x = [];
  const raw = [];
  for (let m = lo - 0.5; m <= hi + 0.5; m += 0.01) {
    x.push(m); raw.push(abExGaussPdf(m, p.C, p.sigmaLink, lambda));
  }
  const peak = Math.max(...raw, 1e-9);
  const y = raw.map((v) => CURVE_FLOOR + (v / peak) * (1 - CURVE_FLOOR));

  const traces = [
    // Invisible floor, so the curve's fill stops at CURVE_FLOOR instead of
    // running all the way down through the marker band behind the dots.
    { type: 'scatter', mode: 'lines', x, y: x.map(() => CURVE_FLOOR),
      line: { width: 0 }, hoverinfo: 'skip', showlegend: false },
    { type: 'scatter', mode: 'lines', x, y,
      fill: 'tonexty', fillcolor: 'rgba(21,101,192,0.10)',
      line: { color: cssVar('--lg-cat-1'), width: 2.6 },
      hoverinfo: 'skip', showlegend: false },
    { type: 'scatter', mode: 'markers',
      x: pile.map((d) => d.x),
      y: pile.map((d) => MARK_BASE + d.row * MARK_STEP),
      customdata: sends,
      marker: { symbol: 'circle', size: 8, color: cssVar('--lg-cat-1') },
      showlegend: false,
      hovertemplate: 'logged send: V%{customdata}<extra></extra>' },
  ];

  const layout = chartLayout('');
  layout.height = 340;
  layout.margin = { l: 16, r: 16, t: 54, b: 46 };
  // Transparent, so the page's own background shows through instead of the
  // chart sitting in a white tile.
  layout.paper_bgcolor = 'rgba(0,0,0,0)';
  layout.plot_bgcolor = 'rgba(0,0,0,0)';
  // No y-axis and no grid: nothing on the vertical carries a quantity the
  // reader needs, and gridlines behind a marker pile only add noise. The grade
  // axis itself is drawn, since everything in the picture is read against it.
  layout.xaxis = {
    ...layout.xaxis, automargin: false, range: [lo, hi], showgrid: false,
    zeroline: false, tickmode: 'array', tickvals: ticks,
    ticktext: ticks.map((t) => `V${t}`),
    ticks: 'outside', ticklen: 5, title: undefined,
    showline: true, linecolor: cssVar('--lg-text-3'), linewidth: 1.2,
  };
  layout.yaxis = {
    ...layout.yaxis, range: [0, 1.38], visible: false, showgrid: false,
    zeroline: false, showticklabels: false, title: undefined, fixedrange: true,
  };
  layout.showlegend = false;

  // The two lines sit only a fraction of a grade apart, so their labels are
  // stacked on separate rows rather than side by side -- at the default
  // ceiling they would otherwise print straight through each other. Each
  // label hangs off whichever side of its own line has room.
  const C_TOP = 1.30;
  const M_TOP = 1.10;
  const SIG_Y = M_TOP - 0.10;
  const side = (xv) => (xv - lo > 1.5
    ? { xanchor: 'right', xshift: -7 }
    : { xanchor: 'left', xshift: 7 });

  layout.shapes = [
    // What sigma_link actually is, drawn as an error bar on m: the grade
    // written on that one climb could be off by this much. It is the only
    // place the parameter touches an observation -- it never moves the other
    // logged sends, because the model gives an individual send no distribution
    // at all, only their maximum.
    { type: 'line', x0: maxLogged - p.sigmaLink, x1: maxLogged + p.sigmaLink,
      y0: SIG_Y, y1: SIG_Y,
      line: { color: cssVar('--lg-cat-1'), width: 1.4 } },
    { type: 'line', x0: maxLogged - p.sigmaLink, x1: maxLogged - p.sigmaLink,
      y0: SIG_Y - 0.035, y1: SIG_Y + 0.035,
      line: { color: cssVar('--lg-cat-1'), width: 1.4 } },
    { type: 'line', x0: maxLogged + p.sigmaLink, x1: maxLogged + p.sigmaLink,
      y0: SIG_Y - 0.035, y1: SIG_Y + 0.035,
      line: { color: cssVar('--lg-cat-1'), width: 1.4 } },
    { type: 'line', x0: p.C, x1: p.C, y0: 0, y1: C_TOP,
      line: { color: cssVar('--lg-danger'), width: 2.4 } },
    { type: 'line', x0: maxLogged, x1: maxLogged, y0: 0, y1: M_TOP,
      line: { color: cssVar('--lg-cat-1'), width: 2.2, dash: 'dash' } },
    // The gap, as a dashed shaft. Plotly annotations cannot dash their own
    // arrow, so the shaft is a shape and the head is a stub annotation.
    { type: 'line', x0: p.C, x1: maxLogged, y0: 0.255, y1: 0.255,
      line: { color: cssVar('--lg-highlight'), width: 1.6, dash: 'dash' } },
  ];

  layout.annotations = [
    { x: p.C, y: C_TOP, text: "<i>C</i> \u2013 climber's true ceiling",
      showarrow: false, ...side(p.C),
      font: { size: 12, color: cssVar('--lg-danger') } },
    { x: maxLogged, y: M_TOP, text: '<i>m</i> \u2013 hardest send logged',
      showarrow: false, ...side(maxLogged),
      font: { size: 12, color: cssVar('--lg-cat-1') } },
    { x: maxLogged - p.sigmaLink, y: SIG_Y, xanchor: 'right', xshift: -6,
      text: '\u00b1\u03c3<sub>link</sub>', showarrow: false,
      font: { size: 10.5, color: cssVar('--lg-cat-1') } },
    // Arrowhead only: a short tail pointing back up the dashed shaft above.
    // It flips when sigma_link is large enough to push the hardest logged send
    // past the ceiling, so the arrow always runs C -> m rather than lying.
    { x: maxLogged, y: 0.255, ax: gap > 0 ? 26 : -26, ay: 0,
      axref: 'pixel', ayref: 'pixel',
      text: '', showarrow: true, arrowhead: 2, arrowsize: 1.1, arrowwidth: 1.6,
      arrowcolor: cssVar('--lg-danger') },
    // Sat on the page background so the dashed shaft does not run through the
    // digits when the gap is small enough that the label overhangs the line.
    { x: (p.C + maxLogged) / 2, y: 0.255, yshift: 14,
      text: `shortfall ${gap.toFixed(1)}`, showarrow: false,
      bgcolor: cssVar('--lg-bg'),
      font: { size: 11.5, color: cssVar('--lg-danger') } },
  ];

  Plotly.react(el, traces, layout, { displayModeBar: false, responsive: true });

  const note = abEl('model-note');
  if (note) {
    const g1 = 1 / abLambda(1, fit);
    const g50 = 1 / abLambda(50, fit);
    const above = 100 * abTailAboveCeiling(p.sigmaLink, lambda);
    note.innerHTML = `The dots are one fixed logbook, <b>${p.visits}</b> sends `
      + `in the order they were climbed. Sliding <i>n</i> only reveals more of `
      + `it \u2014 nothing already drawn ever moves, exactly as a real logbook `
      + `grows. <b><i>m</i> is read off the dots</b>: it is whichever revealed `
      + `send is hardest, so it climbs in steps and then sits still for a long `
      + `time. Right now the shortfall is <b>${gap.toFixed(1)} grades</b>.<br><br>`
      + `The curve is the model \u2014 the distribution of <b><i>m</i> alone</b>, `
      + `where this climber's hardest logged send would land across repeats of `
      + `the same ${p.visits} visits. It is not the spread of their sends. Its `
      + `left tail is the shortfall (exponential, one-sided: you can log below `
      + `your ceiling, never above it by trying); its width is `
      + `\u03c3<sub>link</sub>, noise on the grade <i>label</i>, which is `
      + `the only reason any of the curve sits past <i>C</i> \u2014 `
      + `<b>${above.toFixed(1)}%</b> of it does at the current setting, and `
      + `that is what a soft V7 sent by a V6.2 climber looks like. The error `
      + `bar on <i>m</i> is that same \u00b1\u03c3<sub>link</sub>: it is the `
      + `only thing \u03c3<sub>link</sub> touches in the data, which is why `
      + `moving that slider never moves a dot.<br><br>`
      + `At <b>${p.visits}</b> visits the rate is \u03bb = `
      + `${lambda.toFixed(3)}, so the <i>expected</i> shortfall is `
      + `<b>${meanGap.toFixed(2)} grades</b>; across the exposure range in the `
      + `data it runs from <b>${g1.toFixed(2)}</b> at a single visit to `
      + `<b>${g50.toFixed(2)}</b> at fifty. <b>The observed maximum is a biased `
      + `view of the ceiling, and the size of the bias depends on how often `
      + `someone climbed.</b>`;
  }
}

// ---- wiring ----

function abRender() {
  AB.visits = parseInt(abEl('visits')?.value ?? AB.visits, 10);
  AB.sigmaLink = parseFloat(abEl('sigma')?.value ?? AB.sigmaLink);
  AB.attempts = parseInt(abEl('attempts')?.value ?? AB.attempts, 10);
  AB.policy = abEl('policy')?.value ?? AB.policy;
  const show = (id, v) => { const e = abEl(`${id}-val`); if (e) e.textContent = v; };
  show('visits', String(AB.visits));
  show('sigma', AB.sigmaLink.toFixed(2));
  show('attempts', String(AB.attempts));

  const fit = abFit();
  const session = abSession(AB, abRng(AB.seed));
  abPlotModel(AB, fit);
  abPlotSession(AB, session);
  abPlotSend(AB);
  abPlotSimGap(AB, fit);
  abReadout(AB, session, fit);
}

function renderAbilityModel() {
  if (!abEl('session')) return;
  // Static, so it is drawn once and never touched by abRender.
  abPlotLandscape();
  ['visits', 'sigma', 'attempts', 'policy'].forEach((id) => {
    abEl(id)?.addEventListener('input', abRender);
    abEl(id)?.addEventListener('change', abRender);
  });
  abEl('resample')?.addEventListener('click', () => {
    AB.seed = (AB.seed * 31 + 17) % 100000;
    abRender();
  });
  abRender();
}
