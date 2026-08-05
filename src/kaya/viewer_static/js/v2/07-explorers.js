// ---- gap-likelihood explorer ----
//
// The model never sees a failed attempt, so the observed max is the ceiling
// minus an Exponential gap, blurred by grade rounding. That composition is an
// Exponentially Modified Gaussian; these two charts are it, drawn.

const V2_GAP_STATE = { C: 6.0, visits: 8, sigma: 0.5, kappa: 0.277, rho: -0.018, rel: 0 };
// Fitted on v3_conf: log lambda = log_lambda0 + kappa*n_tilde + rho*r_tilde.
const V2_LOG_LAMBDA0 = 0.082, V2_MEDIAN_VISITS = 8;

const v2NormPdf = (x, mu, sd) => Math.exp(-0.5 * ((x - mu) / sd) ** 2) / (sd * Math.sqrt(2 * Math.PI));

// Rate for a climber with `visits` days logged and centred reliability `rel`.
function v2GapRate(visits, kappa, rho, rel) {
  const nTilde = visits / V2_MEDIAN_VISITS - 1;
  return Math.exp(V2_LOG_LAMBDA0 + kappa * nTilde + rho * rel);
}

// Density of the observed max: (C - Exponential(rate)) + Normal(0, sigma).
// Convolution done numerically -- clearer to read than the ExGaussian closed
// form, and this is a picture, not the likelihood.
function v2ObservedDensity(xs, C, rate, sigma) {
  const gaps = [], step = 0.02;
  for (let g = 0; g < 12; g += step) gaps.push(g);
  return xs.map((x) => {
    let acc = 0;
    for (const g of gaps) acc += rate * Math.exp(-rate * g) * v2NormPdf(x, C - g, sigma) * step;
    return acc;
  });
}

function renderV2GapChart() {
  const el = document.getElementById('v2-gap-chart');
  if (!el || typeof Plotly === 'undefined') return;
  const { C, visits, sigma, kappa, rho, rel } = V2_GAP_STATE;
  const rate = v2GapRate(visits, kappa, rho, rel);
  const xs = [];
  for (let x = C - 7; x <= C + 2.5; x += 0.02) xs.push(x);
  const ys = v2ObservedDensity(xs, C, rate, sigma);

  const traces = [{
    type: 'scatter', mode: 'lines', name: 'observed max', x: xs, y: ys,
    fill: 'tozeroy', line: { color: cssVar('--lg-info'), width: 2 },
    fillcolor: `color-mix(in srgb, ${cssVar('--lg-info')} 18%, transparent)`,
    hovertemplate: 'grade %{x:.2f}<br>density %{y:.3f}<extra></extra>',
  }];
  const layout = chartLayout('logged max grade (V)');
  layout.height = 260;
  layout.yaxis = { ...layout.yaxis, title: { text: 'density', standoff: 6 }, showticklabels: false };
  layout.xaxis = { ...layout.xaxis, title: { text: 'logged max grade (V)', standoff: 8 } };
  layout.margin = { l: 46, r: 16, t: 26, b: 46 };
  layout.showlegend = false;
  layout.shapes = [{
    type: 'line', x0: C, x1: C, y0: 0, y1: 1, yref: 'paper',
    line: { color: cssVar('--lg-danger'), width: 2, dash: 'dash' },
  }];
  const mean = C - 1 / rate;
  layout.annotations = [
    { x: C, y: 1, yref: 'paper', yanchor: 'bottom', xanchor: 'left', showarrow: false,
      text: ` true ceiling C = ${C.toFixed(1)}`, font: { size: 10, color: cssVar('--lg-danger') } },
    { x: mean, y: 0.5, yref: 'paper', yanchor: 'bottom', xanchor: 'right', showarrow: false,
      text: `mean logged ${mean.toFixed(2)} `, font: { size: 10, color: cssVar('--lg-text-2') } },
  ];
  Plotly.react(el, traces, layout, { displayModeBar: false, responsive: true });

  const note = document.getElementById('v2-gap-note');
  if (note) {
    note.textContent = `expected gap ${(1 / rate).toFixed(2)} grades `
      + `— a climber at V${C.toFixed(1)} logs V${mean.toFixed(2)} on average`;
  }
}

function renderV2VisitsChart() {
  const el = document.getElementById('v2-visits-chart');
  if (!el || typeof Plotly === 'undefined') return;
  const { kappa, rho, rel } = V2_GAP_STATE;
  const xs = [];
  for (let n = 1; n <= 40; n += 1) xs.push(n);
  const ys = xs.map((n) => 1 / v2GapRate(n, kappa, rho, rel));

  const traces = [{
    type: 'scatter', mode: 'lines', name: 'expected gap', x: xs, y: ys,
    line: { color: cssVar('--lg-info'), width: 2.5 },
    hovertemplate: '%{x} visits → %{y:.2f} grades below ceiling<extra></extra>',
  }];
  const layout = chartLayout('days logged at the gym');
  layout.height = 260;
  layout.yaxis = { ...layout.yaxis, title: { text: 'expected gap (grades)', standoff: 6 }, rangemode: 'tozero' };
  layout.xaxis = { ...layout.xaxis, title: { text: 'days logged at the gym', standoff: 8 } };
  layout.margin = { l: 58, r: 16, t: 26, b: 46 };
  layout.showlegend = false;
  layout.shapes = [{
    type: 'line', x0: V2_MEDIAN_VISITS, x1: V2_MEDIAN_VISITS, y0: 0, y1: 1, yref: 'paper',
    line: { color: cssVar('--lg-text-2'), width: 1, dash: 'dot' },
  }];
  layout.annotations = [{
    x: V2_MEDIAN_VISITS, y: 1, yref: 'paper', yanchor: 'bottom', xanchor: 'left',
    showarrow: false, text: ' median climber', font: { size: 10, color: cssVar('--lg-text-2') },
  }];
  Plotly.react(el, traces, layout, { displayModeBar: false, responsive: true });

  const note = document.getElementById('v2-visits-note');
  if (note) {
    const g1 = 1 / v2GapRate(1, kappa, rho, rel), g30 = 1 / v2GapRate(30, kappa, rho, rel);
    note.textContent = `1 visit: ${g1.toFixed(2)} grades below — 30 visits: ${g30.toFixed(2)}`;
  }
}

const V2_GAP_SLIDERS = [
  { host: 'v2-gap-controls', id: 'C', tex: 'C', label: 'true ceiling', min: 3, max: 10, step: 0.1 },
  { host: 'v2-gap-controls', id: 'visits', tex: 'n', label: 'visits', min: 1, max: 40, step: 1 },
  { host: 'v2-gap-controls', id: 'sigma', tex: '\\sigma_{\\text{link}}', label: 'rounding noise', min: 0.05, max: 1.5, step: 0.05 },
  { host: 'v2-visits-controls', id: 'kappa', tex: '\\kappa', label: 'visit effect', min: 0, max: 0.8, step: 0.01 },
  { host: 'v2-visits-controls', id: 'rho', tex: '\\rho', label: 'reliability effect', min: -0.5, max: 0.5, step: 0.01 },
  { host: 'v2-visits-controls', id: 'rel', tex: '\\tilde r', label: 'how completely they log', min: -1.5, max: 1.5, step: 0.05 },
];

function bindV2GapExplorer() {
  const hosts = { 'v2-gap-controls': [], 'v2-visits-controls': [] };
  V2_GAP_SLIDERS.forEach((sl) => hosts[sl.host]?.push(sl));
  Object.entries(hosts).forEach(([hostId, sliders]) => {
    const host = document.getElementById(hostId);
    if (!host) return;
    const noteId = hostId === 'v2-gap-controls' ? 'v2-gap-note' : 'v2-visits-note';
    host.innerHTML = sliders.map((sl) => `
      <label class="form-slider wide">
        <span class="form-slider-tex">\\(${sl.tex}\\)</span>
        <input type="range" id="v2-gs-${sl.id}" data-k="${sl.id}"
               min="${sl.min}" max="${sl.max}" step="${sl.step}" value="${V2_GAP_STATE[sl.id]}" />
        <output id="v2-gs-val-${sl.id}" class="form-slider-val"></output>
        <span class="form-slider-label">${sl.label}</span>
      </label>`).join('')
      + `<div class="form-card-foot"><span class="form-card-note" id="${noteId}"></span></div>`;
  });
  document.querySelectorAll('[id^="v2-gs-"]').forEach((inp) => {
    if (inp.tagName !== 'INPUT') return;
    inp.addEventListener('input', () => {
      V2_GAP_STATE[inp.dataset.k] = parseFloat(inp.value);
      renderV2GapExplorer();
    });
  });
}

function renderV2GapExplorer() {
  V2_GAP_SLIDERS.forEach((sl) => {
    const out = document.getElementById(`v2-gs-val-${sl.id}`);
    if (out) out.textContent = V2_GAP_STATE[sl.id].toFixed(sl.step < 0.1 ? 2 : 1);
  });
  renderV2GapChart();
  renderV2VisitsChart();
}

// ---- one interactive card per functional form ----

function renderV2FormCard(spec) {
  const chart = document.getElementById(`v2-fc-chart-${spec.key}`);
  if (!chart || typeof Plotly === 'undefined') return;
  const vals = v2FormState[spec.key];
  const { inches, z } = v2HeightGrid();

  const traces = spec.curves(vals).map((c) => ({
    type: 'scatter', mode: 'lines', name: c.name,
    x: inches, y: z.map(c.f),
    line: { color: cssVar(c.colour), width: 2.5, dash: c.dash },
    hovertemplate: `${c.name}<br>%{x:.0f} in → %{y:+.2f} grades<extra></extra>`,
  }));

  const layout = chartLayout('Height (in)');
  layout.height = 250;
  // Hold a common y-range so the cards stay comparable and a small slider
  // nudge doesn't silently rescale the axis under you -- but grow it rather
  // than clip a curve when someone drags a parameter to an extreme.
  const ys = traces.flatMap((t) => t.y);
  const span = Math.max(1.35, Math.abs(Math.min(...ys)) * 1.08, Math.max(...ys) * 1.08);
  layout.yaxis = { ...layout.yaxis, zeroline: true, range: [-span, span],
                   title: { text: 'Ability impact (grades)', standoff: 6 } };
  layout.xaxis = { ...layout.xaxis, title: { text: 'Height (in)', standoff: 8 } };
  layout.margin = { l: 58, r: 14, t: 26, b: 48 };
  layout.showlegend = spec.curves(vals).length > 1;
  // Keep the legend clear of the x tick labels -- at -0.22 its background sat
  // 2px under them and hid every tick but the last.
  // Must clear the x-axis title, not just the tick labels.
  layout.legend = { ...layout.legend, orientation: 'h', y: -0.52, x: 0, font: { size: 10 } };
  if (layout.showlegend) layout.margin.b = 100;
  const bands = v2BandShapes(true);
  layout.shapes = bands.shapes;
  layout.annotations = bands.annotations;
  Plotly.react(chart, traces, layout, { displayModeBar: false, responsive: true });

  // Readouts: current value beside each slider, plus the derived note.
  spec.params.forEach((prm) => {
    const out = document.getElementById(`v2-fc-val-${spec.key}-${prm.id}`);
    if (out) out.textContent = vals[prm.id].toFixed(prm.step < 0.01 ? 3 : 2);
  });
  const noteEl = document.getElementById(`v2-fc-note-${spec.key}`);
  if (noteEl) noteEl.textContent = spec.note ? spec.note(vals) : '';
}

function renderV2FormCards() {
  const host = document.getElementById('v2-form-cards');
  if (!host) return;
  host.innerHTML = V2_FORM_SPECS.map((spec) => {
    const badge = spec.fitted
      ? '<span class="form-badge fitted">fitted &middot; v3_conf</span>'
      : '<span class="form-badge">illustrative</span>';
    const sliders = spec.params.length
      ? spec.params.map((p) => `
          <label class="form-slider">
            <span class="form-slider-tex">\\(${p.tex}\\)</span>
            <input type="range" id="v2-fc-in-${spec.key}-${p.id}"
                   data-form="${spec.key}" data-param="${p.id}"
                   min="${p.min}" max="${p.max}" step="${p.step}" value="${p.def}" />
            <output id="v2-fc-val-${spec.key}-${p.id}" class="form-slider-val"></output>
          </label>`).join('')
      : '<p class="form-noparams">No parameters &mdash; there is nothing to adjust. That is the point.</p>';
    return `
      <section class="form-card">
        <header class="form-card-head">
          <h4>${spec.label}</h4>${badge}
        </header>
        <div class="form-card-eq">\\[${spec.eq}\\]</div>
        <p class="form-card-claim">${spec.claim}</p>
        <div id="v2-fc-chart-${spec.key}" class="form-card-chart"></div>
        <div class="form-card-controls">
          ${sliders}
          <div class="form-card-foot">
            <span class="form-card-note" id="v2-fc-note-${spec.key}"></span>
            ${spec.params.length ? `<button type="button" class="ghost-button form-reset" data-form="${spec.key}">Reset</button>` : ''}
          </div>
        </div>
      </section>`;
  }).join('');

  host.querySelectorAll('input[type="range"]').forEach((inp) => {
    inp.addEventListener('input', () => {
      const spec = V2_FORM_BY_KEY[inp.dataset.form];
      v2FormState[spec.key][inp.dataset.param] = parseFloat(inp.value);
      renderV2FormCard(spec);
    });
  });
  host.querySelectorAll('.form-reset').forEach((btn) => {
    btn.addEventListener('click', () => {
      const spec = V2_FORM_BY_KEY[btn.dataset.form];
      spec.params.forEach((p) => {
        v2FormState[spec.key][p.id] = p.def;
        const inp = document.getElementById(`v2-fc-in-${spec.key}-${p.id}`);
        if (inp) inp.value = p.def;
      });
      renderV2FormCard(spec);
    });
  });

  setV2FormGridWidth();
  V2_FORM_SPECS.forEach(renderV2FormCard);
  sizeV2FormGrid();
  if (!renderV2FormCards.resizeBound) {
    renderV2FormCards.resizeBound = true;
    let t;
    window.addEventListener('resize', () => {
      clearTimeout(t);
      t = setTimeout(sizeV2FormGrid, 150);
    });
  }
}

// The grid bleeds wider than the prose column so three cards fit comfortably.
// How much room there is depends on whether the symbols panel is reserving its
// gutter, which CSS can't see -- so measure the pane and hand CSS the number.
function setV2FormGridWidth() {
  const host = document.getElementById('v2-form-cards');
  const pane = document.getElementById('tab-grading-v2');
  if (!host || !pane) return;
  const cs = getComputedStyle(pane);
  const usable = pane.clientWidth
    - parseFloat(cs.paddingLeft || 0) - parseFloat(cs.paddingRight || 0);
  const shell = host.parentElement.clientWidth;
  // Never narrower than the column it sits in, never wider than the pane.
  const w = Math.max(shell, Math.min(1240, usable));
  // Set on the pane, not the grid: the gap explorer, posterior grid and
  // parameter detail all bleed to the same width and inherit it from here.
  pane.style.setProperty('--fg-w', `${Math.round(w)}px`);
  void host.offsetWidth;   // force layout, so anything drawn next sees the new width
}

function sizeV2FormGrid() {
  setV2FormGridWidth();
  const corner = document.getElementById('v2-corner');
  // The everything plot pins its own width, so resizing it is not enough --
  // it has to be redrawn against the new pane width.
  const cornerWide = corner?.classList.contains('chart-bleed-wide');
  if (cornerWide) {
    const before = corner._fullLayout?.width || 0;
    setV2CornerWidth();
    // Redrawing 120 subplots is expensive; only do it if the width moved.
    if (Math.abs(corner.clientWidth - before) > 4) renderV2Corner();
  }
  if (typeof Plotly === 'undefined') return;
  const ids = V2_FORM_SPECS.map((spec) => `v2-fc-chart-${spec.key}`)
    .concat(['v2-gap-chart', 'v2-visits-chart', 'v2-param-dens', 'v2-param-trace',
             'v2-across-fits', 'v2-fitted-height', 'v2-fitted-ape'],
            cornerWide ? [] : ['v2-corner'])
    .concat(Object.keys(v2Fit(v2SelectedFit())?.params || {}).map((n) => `v2-pt-${n}`));
  ids.forEach((id) => {
    const el = document.getElementById(id);
    // _fullLayout is only set once Plotly has actually drawn into the node.
    if (el && el._fullLayout) Plotly.Plots.resize(el);
  });
}

// Info dots open on hover for free (CSS), but a click pins them open so touch
// works and so a long panel can be scrolled without it vanishing underfoot.
let infoDotsBound = false;
function bindInfoDots() {
  if (infoDotsBound) return;
  infoDotsBound = true;
  document.addEventListener('click', (ev) => {
    const dot = ev.target.closest('.infodot');
    const wrap = dot?.closest('.infowrap');
    document.querySelectorAll('.infowrap.is-pinned').forEach((w) => {
      if (w !== wrap) w.classList.remove('is-pinned');
    });
    if (wrap) {
      wrap.classList.toggle('is-pinned');
      // Flip the anchor if the popover would run off the left of the pane.
      const pop = wrap.querySelector('.infopop');
      if (pop) {
        pop.classList.remove('pop-left');
        const r = pop.getBoundingClientRect();
        if (r.left < 8) pop.classList.add('pop-left');
      }
      ev.preventDefault();
    } else if (!ev.target.closest('.infopop')) {
      document.querySelectorAll('.infowrap.is-pinned')
        .forEach((w) => w.classList.remove('is-pinned'));
    }
  });
  document.addEventListener('keydown', (ev) => {
    if (ev.key !== 'Escape') return;
    document.querySelectorAll('.infowrap.is-pinned')
      .forEach((w) => w.classList.remove('is-pinned'));
  });
}


async function renderV2Tab() {
  // Gyms, headline numbers and the LOO table all come from the fits on disk,
  // so nothing gym-shaped can render until that file has loaded.
  const ok = await loadV2Results();
  if (!ok) {
    const host = document.getElementById('v2-stats');
    if (host) {
      host.innerHTML = '<p class="form-noparams">Fitted results could not be '
        + 'loaded (/static/v2_results.json). Regenerate with '
        + 'scripts/build_v2_results.py.</p>';
    }
  }
  bindInfoDots();
  renderV2FormsLoo();
  renderV2Replication();
  renderV2Stats();
  renderV2Decisions();
  renderV2Symbols();
  renderV2FormsTable();
  renderV2FormCards();
  bindV2GapExplorer();
  renderV2GapExplorer();
  renderV2Inference();
  renderV2Table('v2-gg-table', V2_GG);
  renderV2Table('v2-diag-table', V2_DIAG);
  renderV2GymChart();
  renderV2BrandChart();
  renderV2DiscardChart();
  renderV2GenderValidation();
  renderV2HeightHist();
  renderV2Time();
  renderV2Reliability();
  // The glossary and forms table are injected as innerHTML *after* KaTeX's
  // auto-render already ran on DOMContentLoaded, so their \( ... \) spans
  // would otherwise stay as literal source. Typeset them explicitly.
  if (typeof window.renderMathInElement === 'function') {
    ['v2-symbols', 'v2-forms-table', 'v2-form-cards', 'v2-gap-controls', 'v2-visits-controls'].forEach((id) => {
      const el = document.getElementById(id);
      if (el) {
        window.renderMathInElement(el, {
          delimiters: [
            { left: '\\[', right: '\\]', display: true },
            { left: '\\(', right: '\\)', display: false },
          ],
          throwOnError: false,
        });
      }
    });
  }
  bindV2Glossary();
}


document.addEventListener('DOMContentLoaded', () => {
  if (typeof window.renderMathInElement === 'function') {
    window.renderMathInElement(document.body, {
      delimiters: [
        { left: '$$', right: '$$', display: true },
        { left: '\\(', right: '\\)', display: false },
      ],
    });
  }
});