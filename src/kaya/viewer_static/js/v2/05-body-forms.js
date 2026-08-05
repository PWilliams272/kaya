// ---- what each model concluded about the body ----
//
// The parameter posteriors above are knobs; these are the curves they add up
// to. Every fit's height form and shared ape form, drawn on one pair of axes
// with credible bands, so the shapes can be compared directly.

// The centring/scaling the model applied. Comes from the fits on disk -- the
// numbers are a property of the dataset, not constants.
const V2_SCALES_FALLBACK = {
  h_median: 68.0, h_sd: 3.917, a_median: 0.0, a_sd: 1.558,
  h_lo: 59, h_hi: 76, a_lo: -3, a_hi: 5,
};
const v2Scales = () => ({ ...V2_SCALES_FALLBACK, ...(V2_POST?.scales || {}) });

// Pinned axis limits for the fitted-curve panels, computed once across both
// genders and every fit. Reset it if the fits ever reload.
let v2FittedRange = null;

// f_height for one draw, in z-units, per the model's own definition. G is the
// gender indicator the interaction terms multiply.
function v2HeightAt(form, d, z, G) {
  switch (form) {
    case 'zero': return 0;
    case 'linear': return d.gamma1 * z;
    case 'linear_x_gender': return (d.gamma1 + G * d.gamma1_x) * z;
    case 'quadratic': return d.gamma1 * z + d.gamma2 * z * z;
    case 'quadratic_x_gender':
      return d.gamma1 * z + d.gamma2 * z * z
        + G * (d.gamma1_x * z + d.gamma2_x * z * z);
    case 'vertex_quadratic': return -d.vq_curv * (z - d.vq_peak) ** 2;
    case 'saturating':
      return d.sat_amp / (1 + Math.exp(-(z - d.sat_h0) / (d.sat_scale + 1e-6)));
    default: return 0;
  }
}

function v2ApeAt(d, z, G) {
  let v = (d.delta1 || 0) * z + (d.delta2 || 0) * z * z;
  if (d.delta1_x !== undefined) v += G * ((d.delta1_x || 0) * z + (d.delta2_x || 0) * z * z);
  return v;
}

// Percentile of an already-sorted array.
function v2Pct(sorted, p) {
  const i = (sorted.length - 1) * p;
  const lo = Math.floor(i), hi = Math.ceil(i);
  return lo === hi ? sorted[lo] : sorted[lo] + (sorted[hi] - sorted[lo]) * (i - lo);
}

// Posterior mean curve plus an 89% band, evaluated draw by draw and centred
// per draw at z = 0. Centring per draw (not on the mean curve) is what keeps
// the band honest: the constant is not identified, only the shape is.
function v2CurveBand(fitName, kind, zs, G) {
  const fit = v2Fit(fitName);
  if (!fit) return null;
  const form = fit.height_form;
  const HEIGHT_PARAMS = {
    zero: [], linear: ['gamma1'], quadratic: ['gamma1', 'gamma2'],
    linear_x_gender: ['gamma1', 'gamma1_x'],
    quadratic_x_gender: ['gamma1', 'gamma2', 'gamma1_x', 'gamma2_x'],
    vertex_quadratic: ['vq_curv', 'vq_peak'],
    saturating: ['sat_amp', 'sat_h0', 'sat_scale'],
  };
  if (kind === 'height' && form === 'zero') return { flat: true };
  // An unrecognised height form must not fall through as "needs no
  // parameters" -- that produced an empty column set and threw on the first
  // draw, taking the whole figure with it. A form this function does not know
  // how to draw is simply not drawn.
  // The ape curve needs only delta1; delta2 is picked up when the fit has it,
  // exactly like the gender interactions below. Demanding delta2 hid the
  // linear-ape arm, which has a perfectly drawable straight ape curve --
  // v2ApeAt already treats a missing term as zero.
  const need = kind === 'height' ? HEIGHT_PARAMS[form] : ['delta1'];
  if (!need) return null;
  if (need.some((p) => !fit.params[p])) return null;
  if (!need.length) return null;

  const cols = {};
  need.forEach((p) => { cols[p] = fit.params[p].chains.flat(); });
  if (kind === 'ape') {
    ['delta2', 'delta1_x', 'delta2_x'].forEach((p) => {
      if (fit.params[p]) cols[p] = fit.params[p].chains.flat();
    });
  }
  const nD = cols[Object.keys(cols)[0]].length;
  const mean = [], lo = [], hi = [];
  const at = kind === 'height'
    ? (d, z) => v2HeightAt(form, d, z, G)
    : (d, z) => v2ApeAt(d, z, G);
  // One draw object reused across the grid -- this runs ~250k times.
  const d = {};
  for (const z of zs) {
    const vals = new Array(nD);
    for (let i = 0; i < nD; i++) {
      for (const p in cols) d[p] = cols[p][i];
      vals[i] = at(d, z) - at(d, 0);
    }
    let s = 0;
    for (const v of vals) s += v;
    mean.push(s / nD);
    vals.sort((a, b) => a - b);
    lo.push(v2Pct(vals, 0.055));
    hi.push(v2Pct(vals, 0.945));
  }
  return { mean, lo, hi };
}

// Where each group actually sits on this axis, median +/- 1 SD, in the same
// two colours the height-form cards higher up the page use. Both are always
// drawn -- the curves are misleading without them, since most of the x-range
// holds almost nobody -- but the selected group is the emphasised one.
function v2GroupBands(kind, selected) {
  const sc = v2Scales();
  const defs = [
    { g: 'male', label: 'Male users', tok: '--lg-info', anchor: 'right',
      st: sc[`${kind}_male`] },
    { g: 'female', label: 'Female users', tok: '--lg-highlight', anchor: 'left',
      st: sc[`${kind}_female`] },
  ].filter((d) => d.st);
  // Height puts the two groups side by side; ape puts them concentric, both
  // centred on zero. Labels anchored the same way in both cases collide in
  // the concentric one, so which way they grow depends on the geometry.
  const nested = defs.length === 2
    && Math.abs(defs[0].st.median - defs[1].st.median) < 0.5 * Math.min(defs[0].st.sd, defs[1].st.sd);
  const shapes = [], annotations = [];
  defs.forEach((d) => {
    const on = d.g === selected;
    const lo = d.st.median - d.st.sd, hi = d.st.median + d.st.sd;
    shapes.push({
      type: 'rect', xref: 'x', yref: 'paper',
      x0: lo, x1: hi, y0: 0, y1: 1,
      fillcolor: cssVar(d.tok), opacity: on ? 0.2 : 0.05,
      // A dashed edge on the selected band as well as a stronger fill: on a
      // pale ground, fill alone has to get muddy before it reads.
      line: on
        ? { width: 1, color: cssVar(d.tok), dash: 'dot' }
        : { width: 0 },
    });
    // Side by side: anchor each label to its own outer edge, growing inward
    // over its own band. Concentric: anchor to opposite edges, growing
    // outward into the empty space on either side.
    const left = d.anchor === 'left';
    annotations.push({
      x: nested ? (left ? lo : hi) : (left ? lo : hi),
      xanchor: nested ? (left ? 'right' : 'left') : (left ? 'left' : 'right'),
      y: 1, yref: 'paper', yanchor: 'bottom', showarrow: false,
      text: on ? `<b>${d.label}</b>` : d.label,
      font: { size: on ? 11 : 10, color: cssVar(d.tok) },
      opacity: on ? 1 : 0.4,
    });
  });
  return { shapes, annotations };
}

function renderV2FittedForms() {
  if (typeof Plotly === 'undefined' || !V2_POST) return;
  const hEl = document.getElementById('v2-fitted-height');
  const aEl = document.getElementById('v2-fitted-ape');
  if (!hEl || !aEl) return;
  // The card bleeds past the prose column; set that width before drawing or
  // Plotly measures the narrow column and the second panel overhangs.
  setV2FormGridWidth();
  const G = document.getElementById('v2-fitted-gender')?.value === 'female' ? 1 : 0;
  const showBand = document.getElementById('v2-fitted-band')?.checked !== false;
  const sc = v2Scales();
  // This panel compares model *forms*, so every curve on it has to come from
  // the same data. v3_all is fitted on all first names rather than the
  // confident-name subset, so it belongs to a different comparison -- and it
  // is the arm that never converged, whose female curve alone doubled the
  // y-range everything else had to share.
  const names = v2FitNames().filter((f) => v2Fit(f)?.name_filter === sc.name_filter);
  const dropped = v2FitNames().filter((f) => !names.includes(f));

  const grid = (lo, hi, n = 55) => {
    const step = (hi - lo) / (n - 1), out = [];
    for (let i = 0; i < n; i++) out.push(lo + i * step);
    return out;
  };
  const hIn = grid(sc.h_lo, sc.h_hi);
  // Ape index is centred on zero by construction, so its axis is too, and it
  // runs out to the 99.5th percentile of |ape| rather than the asymmetric
  // 1st/99th percentiles the height axis uses.
  const aMax = Math.ceil(sc.a_abs || Math.max(Math.abs(sc.a_lo), sc.a_hi));
  const aIn = grid(-aMax, aMax);
  const hZ = hIn.map((v) => (v - sc.h_median) / sc.h_sd);
  const aZ = aIn.map((v) => (v - sc.a_median) / sc.a_sd);

  // Axis limits are computed once over BOTH genders and EVERY fit, then
  // pinned. Otherwise switching gender or clicking a model out of the legend
  // rescales the axes and the shapes appear to change when they have not.
  if (!v2FittedRange) {
    const span = (kind, zs) => {
      let lo = Infinity, hi = -Infinity;
      [0, 1].forEach((g) => names.forEach((fn) => {
        const b = v2CurveBand(fn, kind, zs, g);
        if (!b || b.flat) { lo = Math.min(lo, 0); hi = Math.max(hi, 0); return; }
        b.lo.forEach((v) => { if (v < lo) lo = v; });
        b.hi.forEach((v) => { if (v > hi) hi = v; });
      }));
      if (!Number.isFinite(lo)) return [-1, 1];
      const pad = (hi - lo) * 0.08 || 0.1;
      return [lo - pad, hi + pad];
    };
    v2FittedRange = { h: span('height', hZ), a: span('ape', aZ) };
  }

  const build = (kind, xs, zs) => {
    const traces = [];
    const allNames = v2FitNames();
    names.forEach((fn) => {
      const c = cssVar(V2_FIT_HUES[allNames.indexOf(fn) % V2_FIT_HUES.length]);
      const label = v2FitLabel(fn);
      const band = v2CurveBand(fn, kind, zs, G);
      if (!band) return;
      if (band.flat) {
        // The zero form is a claim too: a flat line at zero, no band.
        traces.push({
          type: 'scatter', mode: 'lines', name: label, legendgroup: fn,
          x: xs, y: xs.map(() => 0),
          line: { color: c, width: 2, dash: 'dot' },
          hovertemplate: `${label}<br>no height effect<extra></extra>`,
        });
        return;
      }
      if (showBand) {
        traces.push({
          type: 'scatter', mode: 'lines', x: xs, y: band.lo, legendgroup: fn,
          line: { width: 0 }, showlegend: false, hoverinfo: 'skip',
        });
        traces.push({
          type: 'scatter', mode: 'lines', x: xs, y: band.hi, legendgroup: fn,
          line: { width: 0 }, fill: 'tonexty', fillcolor: hexToRgba(c, 0.13),
          showlegend: false, hoverinfo: 'skip',
        });
      }
      traces.push({
        type: 'scatter', mode: 'lines', name: label, legendgroup: fn,
        x: xs, y: band.mean, line: { color: c, width: 2.2 },
        hovertemplate: `${label}<br>%{x:.0f} → %{y:+.2f} grades<extra></extra>`,
      });
    });
    return traces;
  };

  const layoutFor = (title, ytitle) => {
    const l = chartLayout(title);
    l.height = 340;
    // Seven models wrap to two legend rows; the bottom margin and legend y are
    // measured against that, not guessed, or the legend lands on the x title.
    l.margin = { l: 56, r: 20, t: 10, b: 124 };
    l.xaxis = { ...l.xaxis, automargin: false,
      title: { text: title, standoff: 10 } };
    l.yaxis = { ...l.yaxis, automargin: false, title: { text: ytitle, standoff: 6 },
      zeroline: true, zerolinecolor: cssVar('--lg-text-2') };
    l.legend = { ...l.legend, orientation: 'h', y: -0.42, yanchor: 'top', x: 0,
      font: { size: 10 } };
    return l;
  };

  const sel = G ? 'female' : 'male';
  const hLayout = layoutFor('height (inches)', 'grade impact');
  const hb = v2GroupBands('h', sel);
  hLayout.shapes = hb.shapes;
  hLayout.annotations = hb.annotations;
  hLayout.margin.t = 24;               // room for the band labels
  hLayout.xaxis = { ...hLayout.xaxis, range: [sc.h_lo, sc.h_hi], autorange: false };
  hLayout.yaxis = { ...hLayout.yaxis, range: v2FittedRange.h, autorange: false };
  Plotly.react(hEl, build('height', hIn, hZ), hLayout,
    { displayModeBar: false, responsive: true });

  const aLayout = layoutFor('ape index (inches)', 'grade impact');
  const ab = v2GroupBands('a', sel);
  aLayout.shapes = ab.shapes;
  aLayout.annotations = ab.annotations;
  aLayout.margin.t = 24;
  aLayout.xaxis = { ...aLayout.xaxis, range: [-aMax, aMax], autorange: false,
    zeroline: true, zerolinecolor: cssVar('--lg-text-2') };
  aLayout.yaxis = { ...aLayout.yaxis, range: v2FittedRange.a, autorange: false };
  Plotly.react(aEl, build('ape', aIn, aZ), aLayout,
    { displayModeBar: false, responsive: true });

  const note = document.getElementById('v2-fitted-note');
  if (note) {
    // The honest summary number: how far the best curve travels across the
    // middle 98% of climbers, compared with how wide its band is there.
    const spans = names.map((fn) => {
      const b = v2CurveBand(fn, 'height', hZ, G);
      if (!b || b.flat) return null;
      const span = Math.max(...b.mean) - Math.min(...b.mean);
      const width = Math.max(...b.hi.map((v, i) => v - b.lo[i]));
      return { fn, span, width };
    }).filter(Boolean);
    const worst = spans.sort((a, b2) => b2.span - a.span)[0];
    // Only the ape x gender arm's ape curve responds to the toggle; every
    // other model's ape term is gender-blind. Say so, or the toggle looks
    // broken on the right-hand panel.
    const apeByGender = names.filter((f) => v2Fit(f)?.params?.delta1_x)
      .map((f) => v2FitLabel(f));
    const hm = sc.h_male, hf = sc.h_female;
    const bands = (hm && hf)
      ? 'Shaded strips are each group&rsquo;s median &plusmn;1 SD &mdash; '
        + `male ${(hm.median - hm.sd).toFixed(0)}&ndash;${(hm.median + hm.sd).toFixed(0)} in, `
        + `female ${(hf.median - hf.sd).toFixed(0)}&ndash;${(hf.median + hf.sd).toFixed(0)} in `
        + '&mdash; with the selected group emphasised. Outside them the curves are '
        + 'extrapolation. '
      : '';
    const dropNote = dropped.length
      ? `<b>${dropped.map((f) => v2FitLabel(f)).join(', ')}</b> `
        + `${dropped.length === 1 ? 'is' : 'are'} not drawn here: fitted on a `
        + 'different user set, so the curves would not be comparable. '
      : '';
    const apeNote = apeByGender.length
      ? `On the ape panel the selector only changes <b>${apeByGender.join('</b>, <b>')}</b>: `
        + 'every other model&rsquo;s ape term is the same for everyone. The bands '
        + 'still differ, because the two groups&rsquo; ape distributions do. '
      : 'On the ape panel the selector changes only the bands &mdash; every '
        + 'model&rsquo;s ape term is the same for everyone. ';
    note.innerHTML = worst
      ? bands + apeNote + dropNote
        + `The largest height effect any model claims is <b>${v2FitLabel(worst.fn)}</b>, `
        + `travelling <b>${worst.span.toFixed(2)} grades</b> across the whole range `
        + `&mdash; against a credible band up to <b>${worst.width.toFixed(2)} grades</b> wide. `
        + (worst.span < worst.width
          ? 'The band is wider than the effect, which is the whole story: the shapes '
            + 'differ but none of them is separated from a flat line.'
          : 'That is a real effect, but read it against a gym-to-gym spread of '
            + 'well over a grade.')
      : '';
  }
}

// ---- corner plot ----

// Canonical ordering for the everything-at-once corner plot: the same order
// the symbol glossary walks, so the two read alike.
const V2_PARAM_ORDER = Object.keys(V2_PARAM_TEX);

function v2CornerNames(groupKey, fits) {
  const present = (n) => fits.some((f) => f.params[n]);
  if (groupKey === 'all') return V2_PARAM_ORDER.filter(present);
  return (V2_CORNER_GROUPS[groupKey]?.of || []).filter(present);
}

// Pearson correlation of two equal-length draw vectors.
function v2Corr(a, b) {
  const n = Math.min(a.length, b.length);
  if (n < 3) return 0;
  let ma = 0, mb = 0;
  for (let i = 0; i < n; i++) { ma += a[i]; mb += b[i]; }
  ma /= n; mb /= n;
  let sab = 0, sa = 0, sb = 0;
  for (let i = 0; i < n; i++) {
    const da = a[i] - ma, db = b[i] - mb;
    sab += da * db; sa += da * da; sb += db * db;
  }
  return sa && sb ? sab / Math.sqrt(sa * sb) : 0;
}

function renderV2Corner() {
  const el = document.getElementById('v2-corner');
  if (!el || typeof Plotly === 'undefined' || !V2_POST) return;
  const groupKey = document.getElementById('v2-corner-group')?.value || 'height';
  let overlay = document.getElementById('v2-corner-overlay')?.value || 'one';
  let style = document.getElementById('v2-corner-style')?.value || 'both';
  const primaryName = v2SelectedFit();

  // The everything plot bleeds out past the prose column; the grouped ones
  // stay inside it. Do this before drawing so Plotly measures the final width.
  const wide = groupKey === 'all';
  el.classList.toggle('chart-bleed-wide', wide);
  if (wide) setV2CornerWidth();

  // Say it in the control, not only in the caption underneath the plot.
  const ovSel = document.getElementById('v2-corner-overlay');
  const ovAll = ovSel?.querySelector('option[value="all"]');
  if (ovAll) {
    ovAll.disabled = wide;
    ovAll.textContent = wide ? 'All models overlaid — too slow here' : 'All models overlaid';
    if (wide && ovSel.value === 'all') { ovSel.value = 'one'; overlay = 'one'; }
  }

  const names0 = ['one', undefined].includes(overlay)
    ? v2CornerNames(groupKey, [v2Fit(primaryName)].filter(Boolean))
    : v2CornerNames(groupKey, v2FitNames().map(v2Fit).filter(Boolean));
  const names = names0;
  const N = names.length;
  if (N < 2) {
    Plotly.purge(el);
    el.innerHTML = '<p class="form-noparams">This fit does not contain enough of '
      + 'these parameters to draw a corner plot.</p>';
    const n0 = document.getElementById('v2-corner-note');
    if (n0) n0.innerHTML = '';
    return;
  }
  if (!el._fullLayout) el.innerHTML = '';

  // Which fits are drawn, and in what colour. A fit earns a place only if it
  // shares at least two of these parameters -- one gets you a diagonal and
  // nothing else.
  const allNames = v2FitNames();
  const chosen = (overlay === 'all' ? allNames : [primaryName])
    .filter((n) => {
      const f = v2Fit(n);
      return f && names.filter((p) => f.params[p]).length >= 2;
    });
  if (!chosen.length) chosen.push(primaryName);
  const colourOf = {};
  chosen.forEach((n) => {
    colourOf[n] = cssVar(V2_FIT_HUES[allNames.indexOf(n) % V2_FIT_HUES.length]);
  });

  // Cost control. Every cell is a separate SVG subplot, and contours cost
  // several times a scatter, so a 15-parameter plot across 7 fits has to give
  // something up or the page locks for seconds.
  const nCells = (N * (N + 1)) / 2;
  let downgraded = '';
  // Overlaying in the everything view is not a fallback, it is not possible:
  // every panel-model pair is its own SVG subplot trace, and 171 panels x 7
  // models is ~1,200 of them, which takes Plotly well over half a minute and
  // freezes the tab while it works. Measured, not assumed -- contours-only
  // does not help, because the cost is the trace count, not the trace type.
  if (chosen.length > 1 && nCells > 90) {
    const keep = chosen.includes(primaryName) ? primaryName : chosen[0];
    chosen.length = 0;
    chosen.push(keep);
    downgraded = `Overlaying models is off in this view: ${nCells} panels &times; every `
      + 'model is ~1,200 separate plots, which takes over half a minute to draw. '
      + `Showing <b>${v2FitLabel(keep)}</b> alone. Pick a parameter group `
      + 'above to compare models.';
  }
  const load = nCells * chosen.length;
  // Points are what makes a smaller overlay unreadable as well as slow: seven
  // scatter clouds on one panel is mud, and contours survive the density.
  const heavy = load > 260 || nCells > 60;
  if (heavy && chosen.length > 1 && style !== 'contour') {
    style = 'contour';
    downgraded += (downgraded ? ' ' : '')
      + `Contours only here: ${nCells} panels &times; ${chosen.length} models `
      + 'is more scatter than one panel can show. Narrow the parameter group '
      + 'to get the points back.';
  } else if (heavy && style === 'both') {
    style = 'points';
    downgraded += (downgraded ? ' ' : '')
      + `Points only here: ${nCells} panels is more than contours can be fitted `
      + 'to at a usable speed.';
  }
  const MAX_PTS = load > 300 ? 120 : (chosen.length > 3 ? 220 : 500);
  // Coarser contours when there are hundreds of panels: at 56px a cell, four
  // levels and 18 bins is detail nobody can see and everybody waits for.
  const cLevels = heavy ? 2 : 4;
  const cBins = heavy ? 12 : 18;

  // Draws, per fit, thinned to the same stride across parameters so each cell
  // is a genuine joint sample rather than a scatter of unrelated numbers.
  const data = {};       // fit -> param -> full draws
  const pts = {};        // fit -> param -> thinned draws
  chosen.forEach((fn) => {
    const f = v2Fit(fn);
    data[fn] = {}; pts[fn] = {};
    let stride = 1;
    names.forEach((p) => {
      if (!f.params[p]) return;
      const d = f.params[p].chains.flat();
      data[fn][p] = d;
      stride = Math.max(stride, Math.ceil(d.length / MAX_PTS));
    });
    names.forEach((p) => {
      if (data[fn][p]) pts[fn][p] = data[fn][p].filter((_, i) => i % stride === 0);
    });
  });

  // One shared range per parameter across every drawn fit, so a column and its
  // row line up and the overlay is actually comparable.
  const ranges = names.map((p) => {
    let lo = Infinity, hi = -Infinity;
    chosen.forEach((fn) => {
      const d = data[fn][p];
      if (!d) return;
      for (const v of d) { if (v < lo) lo = v; if (v > hi) hi = v; }
    });
    if (!Number.isFinite(lo)) return [-1, 1];
    const pad = (hi - lo) * 0.06 || 0.05;
    return [lo - pad, hi + pad];
  });

  const traces = [], layout = chartLayout('');
  // Grouped plots get generous square-ish cells; the everything plot is width
  // bound, so it goes square instead of stretching cells into tall slivers.
  // Its width has to be stated outright -- react() reuses the width it last
  // measured, which for the first draw is the prose column, not the bled-out
  // element.
  if (wide) {
    layout.width = Math.max(520, el.clientWidth);
    layout.height = layout.width;
  } else {
    layout.height = Math.max(380, (N > 8 ? 108 : 150) * N);
  }
  layout.margin = { l: N > 8 ? 62 : 78, r: 16, t: 14, b: N > 8 ? 58 : 66 };
  const dense = N > 11;   // tick labels stop being readable past here
  delete layout.xaxis; delete layout.yaxis;

  const gap = 0.055 / Math.max(1, N - 1);
  const cell = (k) => (k === 0 ? '' : String(k + 1));
  const seenLegend = {};
  let k = 0;
  for (let i = 0; i < N; i++) {
    for (let j = 0; j <= i; j++) {
      const xa = `x${cell(k)}`, ya = `y${cell(k)}`;
      const isDiag = i === j;
      chosen.forEach((fn) => {
        const c = colourOf[fn];
        const label = v2FitLabel(fn);
        // Legend entry once per fit, on whichever cell that fit first appears
        // in; legendgroup makes the click toggle the whole model at once.
        const legend = () => {
          const first = !seenLegend[fn];
          seenLegend[fn] = true;
          return { legendgroup: fn, name: label, showlegend: first };
        };
        if (isDiag) {
          const d = data[fn][names[i]];
          if (!d) return;
          const grid = v2Grid(ranges[i][0], ranges[i][1], 90);
          traces.push({
            type: 'scatter', mode: 'lines', x: grid, y: v2Kde(d, grid),
            xaxis: xa, yaxis: ya,
            fill: chosen.length === 1 ? 'tozeroy' : undefined,
            line: { color: c, width: 1.5 },
            fillcolor: `color-mix(in srgb, ${c} 22%, transparent)`,
            hoverinfo: 'skip', ...legend(),
          });
          return;
        }
        const dx = data[fn][names[j]], dy = data[fn][names[i]];
        if (!dx || !dy) return;
        if (style !== 'points') {
          traces.push({
            type: 'histogram2dcontour',
            x: dx, y: dy, xaxis: xa, yaxis: ya,
            colorscale: [[0, c], [1, c]], showscale: false,
            ncontours: cLevels, contours: { coloring: 'lines' },
            line: { width: 1.1, smoothing: 1.3 },
            nbinsx: cBins, nbinsy: cBins,
            hoverinfo: 'skip', ...legend(),
          });
        }
        if (style !== 'contour') {
          traces.push({
            type: 'scatter', mode: 'markers',
            x: pts[fn][names[j]], y: pts[fn][names[i]], xaxis: xa, yaxis: ya,
            marker: {
              color: c, size: N > 8 ? 2 : 2.5,
              // Points under contours are texture, not the message; the more
              // models are stacked the more they have to step back.
              opacity: style === 'both' ? 0.5 / (chosen.length + 2) : 0.34,
            },
            hovertemplate: `${label}<br>${names[j]} %{x:.3f}<br>${names[i]} %{y:.3f}<extra></extra>`,
            ...legend(),
          });
        }
      });
      const bottom = i === N - 1;
      const left = j === 0 && !isDiag;
      const tf = { size: N > 8 ? 8 : 9 };
      const titleFont = { size: N > 8 ? 9 : 10 };
      layout[`xaxis${cell(k)}`] = {
        domain: [j / N + gap, (j + 1) / N - gap],
        anchor: ya, range: ranges[j],
        showticklabels: bottom && !dense, nticks: N > 8 ? 3 : 4, automargin: false,
        gridcolor: cssVar('--lg-border'), zerolinecolor: cssVar('--lg-border'),
        title: bottom ? { text: names[j], standoff: 6, font: titleFont } : undefined,
        tickfont: tf,
      };
      layout[`yaxis${cell(k)}`] = {
        domain: [1 - (i + 1) / N + gap, 1 - i / N - gap],
        anchor: xa,
        // The diagonal's vertical axis is a density, not the parameter, so it
        // gets neither the shared range nor a label.
        range: isDiag ? undefined : ranges[i],
        showticklabels: left && !dense, nticks: N > 8 ? 3 : 4, automargin: false,
        gridcolor: cssVar('--lg-border'), zerolinecolor: cssVar('--lg-border'),
        title: left ? { text: names[i], standoff: 6, font: titleFont } : undefined,
        tickfont: tf,
      };
      k++;
    }
  }

  // The upper-right triangle of a corner plot is empty by construction, which
  // is exactly where the legend wants to live.
  layout.showlegend = chosen.length > 1;
  if (layout.showlegend) {
    // Top-right is empty by construction in a lower-triangle corner plot.
    layout.legend = {
      ...layout.legend, orientation: 'v',
      x: 1, y: 1, xanchor: 'right', yanchor: 'top',
      bgcolor: cssVar('--lg-card'), bordercolor: cssVar('--lg-border'),
      borderwidth: 1, font: { size: 11 }, itemsizing: 'constant',
    };
  }
  // responsive is off in the wide case on purpose: the width is pinned above,
  // and Plotly's resize observer would otherwise redraw 120 subplots on every
  // frame of the glossary panel's slide.
  Plotly.react(el, traces, layout, { displayModeBar: false, responsive: !wide });

  const note = document.getElementById('v2-corner-note');
  if (!note) return;
  const primary = v2Fit(chosen.includes(primaryName) ? primaryName : chosen[0]);
  const pName = v2FitLabel(chosen.includes(primaryName) ? primaryName : chosen[0]);
  let worst = null;
  for (let i = 0; i < N; i++) {
    for (let j = i + 1; j < N; j++) {
      const a = primary.params[names[i]], b = primary.params[names[j]];
      if (!a || !b) continue;
      const r = v2Corr(a.chains.flat(), b.chains.flat());
      if (!worst || Math.abs(r) > Math.abs(worst.r)) worst = { r, a: names[i], b: names[j] };
    }
  }
  let html = 'Diagonal cells are each parameter on its own; off-diagonal cells are pairs. ';
  if (chosen.length > 1) {
    html += `Each colour is one model &mdash; click the legend to isolate one. `
      + 'A parameter a model does not contain is simply absent from its cells. '
      + `Correlations quoted below are for <b>${pName}</b>. `;
  }
  if (worst) {
    html += `Strongest pairing: <b>${worst.a}</b> and <b>${worst.b}</b>, `
      + `correlation <b>${worst.r.toFixed(2)}</b>. `
      + (Math.abs(worst.r) > 0.7
        ? 'A tight diagonal ridge like that means the two are <b>trading off against '
          + 'each other</b> — the data pin down their combination far better than '
          + 'either alone, and the sampler has to crawl along that ridge. This is '
          + 'the geometry behind the convergence trouble on this page.'
        : 'Nothing here is strongly entangled, which is what you want: each '
          + 'parameter is being identified more or less on its own.');
  }
  if (downgraded) html += ` <b>${downgraded}</b>`;
  note.innerHTML = html;
}

// The everything-at-once corner plot wants the whole pane, not the 920px prose
// column. Same trick as setV2FormGridWidth, but without its 1240px cap.
function setV2CornerWidth() {
  const el = document.getElementById('v2-corner');
  const pane = document.getElementById('tab-grading-v2');
  if (!el || !pane) return;
  const cs = getComputedStyle(pane);
  const usable = pane.clientWidth
    - parseFloat(cs.paddingLeft || 0) - parseFloat(cs.paddingRight || 0);
  const shell = el.parentElement.clientWidth;
  pane.style.setProperty('--fg-wide', `${Math.round(Math.max(shell, usable))}px`);
  void el.offsetWidth;   // force layout so Plotly measures the new width
}

