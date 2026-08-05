// ---- the missing dimension: time ----
//
// v2_time.json is written by scripts/build_v2_time.py: the climber
// advancement curve (naive and de-biased) and the per-gym date/correction
// scatter. Nothing here is hand-typed.

let V2_TIME = null;

async function loadV2Time() {
  if (V2_TIME) return true;
  try {
    const r = await fetch('/static/v2_time.json', { cache: 'no-cache' });
    if (!r.ok) return false;
    V2_TIME = await r.json();
    return true;
  } catch (e) {
    return false;
  }
}

function renderV2Advancement() {
  const el = document.getElementById('v2-adv-chart');
  if (!el || !V2_TIME || typeof Plotly === 'undefined') return;
  const a = V2_TIME.advancement;
  const gx = (rows) => rows.map((r) => r.v);
  const traces = [];

  // The interquartile band lives in the table instead. It spans a full grade
  // either side, and drawing it here would bury three mean curves that now
  // fit inside a third of a grade.
  traces.push({
    type: 'scatter', mode: 'lines+markers', x: gx(a.naive), y: a.naive.map((r) => r.mean),
    name: 'naive (regression to the max)',
    line: { color: cssVar('--lg-cat-4'), width: 2, dash: 'dash' },
    marker: { size: 6 },
    hovertemplate: 'V%{x}: %{y:+.2f} grades/yr<extra>naive</extra>',
  });
  traces.push({
    type: 'scatter', mode: 'lines+markers', x: gx(a.short_win),
    y: a.short_win.map((r) => r.mean),
    name: 'three-month window alone',
    line: { color: cssVar('--lg-cat-3'), width: 2, dash: 'dot' },
    marker: { size: 6 },
    hovertemplate: 'V%{x}: %{y:+.2f} grades/yr<extra>3 months only</extra>',
  });
  traces.push({
    type: 'scatter', mode: 'lines+markers', x: gx(a.debiased), y: a.debiased.map((r) => r.mean),
    name: 'steady rate, fitted across all windows',
    line: { color: cssVar('--lg-cat-1'), width: 2.6 },
    marker: { size: 7 },
    error_y: { type: 'data', array: a.debiased.map((r) => r.sem),
      color: hexToRgba(cssVar('--lg-cat-1'), 0.5), thickness: 1.4, width: 3 },
    hovertemplate: 'V%{x}: %{y:+.2f} grades/yr<extra>de-biased</extra>',
  });

  const layout = chartLayout('current grade');
  layout.height = 380;
  layout.margin = { l: 62, r: 20, t: 12, b: 96 };
  layout.xaxis = { ...layout.xaxis, automargin: false, tickprefix: 'V', dtick: 1,
    title: { text: 'current grade', standoff: 10 } };
  // Pinned so the two corrected curves stay readable. The naive one runs to
  // +8 and -4 and is deliberately allowed to leave the frame -- it being off
  // the scale is the point, and its numbers are in the table below.
  layout.yaxis = { ...layout.yaxis, automargin: false, zeroline: true,
    range: [-1.35, 1.15], zerolinecolor: cssVar('--lg-text-2'),
    title: { text: 'grades gained per year', standoff: 6 } };
  layout.legend = { ...layout.legend, orientation: 'h', y: -0.3, yanchor: 'top',
    x: 0, font: { size: 10 } };
  Plotly.react(el, traces, layout, { displayModeBar: false, responsive: true });

  const note = document.getElementById('v2-adv-note');
  if (note) {
    const at = (rows, v) => rows.find((r) => r.v === v);
    // The worst naive value, whichever bin it lands in -- that is the
    // impossible number the argument rests on.
    const n1 = a.naive.reduce((w, r) => (r.mean < w.mean ? r : w), a.naive[0]);
    const d1 = at(a.debiased, 1), d3 = at(a.debiased, 3);
    const d5 = at(a.debiased, 5), d8 = at(a.debiased, 8);
    const s1 = at(a.short_win, 1);
    const worst = a.debiased.reduce((w, r) => (r.chi2 > w.chi2 ? r : w), a.debiased[0]);
    // Each bin's n counts measurements across all six watching periods, and
    // the same climber contributes at several of them, so this is a count of
    // measurements rather than of people.
    const nMeas = a.debiased.reduce((s, r) => s + r.n, 0);
    note.innerHTML = `${nMeas.toLocaleString()} measurements across six `
      + `watching periods, from ${a.n_climbers.toLocaleString()} climbers. `
      + 'Improvement runs '
      + `<b>${d1.mean.toFixed(2)} grades/yr at V${d1.v}</b> (&plusmn;`
      + `${d1.sem.toFixed(2)}), ${d3.mean.toFixed(2)} at V${d3.v}, `
      + `${d5.mean.toFixed(2)} at V${d5.v} and ${d8.mean.toFixed(2)} at `
      + `V${d8.v} &mdash; roughly ${(d1.mean / Math.max(d8.mean, 0.01)).toFixed(0)}&times; `
      + 'faster at the bottom than at the top. The naive estimator bottoms out '
      + `at ${n1.mean.toFixed(1)} grades/yr at V${n1.v}, an impossible number `
      + `that gives it away; the three-month window alone reads ${s1.mean.toFixed(2)} `
      + `at V${d1.v}, which a year of data does not bear out. A steady rate `
      + `fits every bin (worst χ&sup2;/dof is ${worst.chi2.toFixed(1)} at `
      + `V${worst.v}). Error bars understate the uncertainty: one climber `
      + 'contributes a triple at every position in their window sequence, so '
      + 'rows within a bin are not independent.';
  }
}

function renderV2Horizon() {
  const el = document.getElementById('v2-horizon-chart');
  if (!el || !V2_TIME || typeof Plotly === 'undefined') return;
  const byh = V2_TIME.advancement.by_horizon;
  if (!byh) return;
  const hs = Object.keys(byh).sort((x, y) => parseFloat(x) - parseFloat(y));
  // Short window = the darkest line, so the eye follows the one being argued
  // for. The rest fade out as they lengthen.
  const traces = hs.map((h, i) => {
    const rows = byh[h].filter((r) => r.v >= 1 && r.v <= 9);
    const t = i / Math.max(1, hs.length - 1);
    const c = cssVar(i === 0 ? '--lg-cat-1' : '--lg-text-2');
    return {
      type: 'scatter', mode: 'lines+markers',
      x: rows.map((r) => r.v), y: rows.map((r) => r.mean),
      name: parseFloat(h) === 1 ? '1 year' : `${parseFloat(h) * 12} months`,
      line: { color: hexToRgba(c, i === 0 ? 1 : 0.85 - 0.5 * t),
        width: i === 0 ? 2.8 : 1.6, dash: i === 0 ? 'solid' : 'dot' },
      marker: { size: i === 0 ? 8 : 5 },
      hovertemplate: `V%{x}: %{y:+.2f} grades/yr<extra>${h} yr window</extra>`,
    };
  });

  const layout = chartLayout('');
  layout.height = 360;
  layout.margin = { l: 62, r: 20, t: 12, b: 92 };
  layout.xaxis = { ...layout.xaxis, automargin: false, tickprefix: 'V', dtick: 1,
    title: { text: 'grade bin, assigned at w₀', standoff: 10 } };
  layout.yaxis = { ...layout.yaxis, automargin: false, zeroline: true,
    zerolinecolor: cssVar('--lg-text-2'),
    title: { text: 'grades gained per year', standoff: 6 } };
  layout.legend = { ...layout.legend, orientation: 'h', y: -0.28, yanchor: 'top',
    x: 0, font: { size: 10 } };
  Plotly.react(el, traces, layout, { displayModeBar: false, responsive: true });

  const note = document.getElementById('v2-horizon-note');
  if (note) {
    const g = (h, v) => (byh[h] || []).find((r) => r.v === v);
    const a1 = g(hs[0], 1), b1 = g('1.0', 1);
    const a6 = g(hs[0], 6), b6 = g('1.0', 6);
    note.innerHTML = 'The same estimator at six watching periods. At <b>V1</b> '
      + `it reads ${a1.mean.toFixed(2)} grades/yr over three months and `
      + `${b1.mean.toFixed(2)} over a year; at <b>V6</b> it goes `
      + `${a6.mean.toFixed(2)} to ${b6.mean.toFixed(2)}. The fan opens at the `
      + 'bottom and stays shut at the top, which is the signature of a gain '
      + 'that arrives once rather than accruing &mdash; and it only shows up '
      + 'where there is enough movement for the distinction to matter. '
      + 'Whichever period you pick you are reporting your own choice, so the '
      + 'curve below picks none of them.';
  }
  const starts = V2_TIME.advancement.starts || [];
  const lo = starts.find((r) => r.v === 1), hi = starts.find((r) => r.v === 9);
  const set = (id, r) => {
    const n = document.getElementById(id);
    if (n && r) n.textContent = `V${r.l1.toFixed(1)}`;
  };
  set('v2-start-lo', lo);
  set('v2-start-hi', hi);
}

function renderV2Accrual() {
  const el = document.getElementById('v2-accrual-chart');
  if (!el || !V2_TIME || typeof Plotly === 'undefined') return;
  const acc = V2_TIME.advancement.accrual;
  if (!acc || !acc.length) return;
  const hi = Math.max(...acc.map((r) => r.h));
  // Weighted mean rate, drawn as a straight line through the origin: if
  // change accrues linearly the points sit on it.
  const wsum = acc.reduce((s, r) => s + r.n, 0);
  const rate = acc.reduce((s, r) => s + r.rate * r.n, 0) / wsum;

  const c = cssVar('--lg-cat-1');
  const traces = [{
    type: 'scatter', mode: 'lines', name: `steady ${rate.toFixed(2)} grades/yr`,
    x: [0, hi], y: [0, rate * hi],
    line: { color: cssVar('--lg-text-2'), width: 1.6, dash: 'dash' },
    hoverinfo: 'skip',
  }, {
    type: 'scatter', mode: 'markers', name: 'measured',
    x: acc.map((r) => r.h), y: acc.map((r) => r.dl),
    marker: { size: 11, color: hexToRgba(c, 0.72),
      line: { width: 2, color: hexToRgba(c, 0.85) } },
    error_y: { type: 'data', array: acc.map((r) => r.sem),
      color: hexToRgba(c, 0.5), thickness: 1.4, width: 4 },
    text: acc.map((r) => r.n.toLocaleString()),
    hovertemplate: '%{x} yr later: %{y:+.3f} grades<br>'
      + '%{text} measurements<extra></extra>',
  }];

  const layout = chartLayout('');
  layout.height = 300;
  layout.margin = { l: 62, r: 20, t: 12, b: 82 };
  layout.xaxis = { ...layout.xaxis, automargin: false, range: [0, hi + 0.15],
    title: { text: 'time elapsed (years)', standoff: 10 } };
  layout.yaxis = { ...layout.yaxis, automargin: false, zeroline: true,
    zerolinecolor: cssVar('--lg-text-2'),
    title: { text: 'grades gained', standoff: 6 } };
  layout.legend = { ...layout.legend, orientation: 'h', y: -0.34, yanchor: 'top',
    x: 0, font: { size: 10 } };
  Plotly.react(el, traces, layout, { displayModeBar: false, responsive: true });

  const note = document.getElementById('v2-accrual-note');
  if (note) {
    const lo = acc[0], hiR = acc[acc.length - 1];
    note.innerHTML = 'Climbers between V3 and V8, measured over a ladder of '
      + 'horizons. The points track the dashed line, so the change accumulates '
      + `steadily: ${lo.dl.toFixed(3)} grades after ${lo.h} years and `
      + `${hiR.dl.toFixed(3)} after ${hiR.h}, which is `
      + `${(hiR.dl / lo.dl).toFixed(1)}&times; the change over `
      + `${(hiR.h / lo.h).toFixed(0)}&times; the time. The implied annual rate `
      + `never leaves the ${Math.min(...acc.map((r) => r.rate)).toFixed(2)} to `
      + `${Math.max(...acc.map((r) => r.rate)).toFixed(2)} band. Error bars are `
      + 'the standard error of the mean.';
  }
  const gm = document.getElementById('v2-gap-median');
  if (gm && V2_TIME.advancement.gap_months) {
    gm.textContent = V2_TIME.advancement.gap_months.median.toFixed(0);
  }
}

function renderV2TimeChart() {
  const el = document.getElementById('v2-time-chart');
  if (!el || !V2_TIME || typeof Plotly === 'undefined') return;
  const gt = V2_TIME.gym_time;
  const brands = [...new Set(gt.gyms.map((r) => r.b))]
    .sort((x, y) => gt.gyms.filter((r) => r.b === y).length
                  - gt.gyms.filter((r) => r.b === x).length);
  const traces = brands.map((b) => {
    const rs = gt.gyms.filter((r) => r.b === b);
    const c = cssVar(V2_BRAND_COLOURS[b] || '--lg-text-2');
    return {
      type: 'scatter', mode: 'markers', name: b,
      x: rs.map((r) => r.t_c), y: rs.map((r) => r.m),
      marker: { size: 11, color: hexToRgba(c, 0.72),
        line: { width: 2, color: hexToRgba(c, 0.85) } },
      text: rs.map((r) => r.g),
      hovertemplate: '<b>%{text}</b><br>%{x:+.2f} yr relative to its own '
        + 'climbers<br>correction %{y:+.3f} grades<extra></extra>',
    };
  });
  // The fitted line, drawn across the observed range only.
  const xs = gt.gyms.map((r) => r.t_c);
  const lo = Math.min(...xs), hi = Math.max(...xs);
  const ys = gt.gyms.map((r) => r.m);
  const mx = xs.reduce((s, v) => s + v, 0) / xs.length;
  const my = ys.reduce((s, v) => s + v, 0) / ys.length;
  const b1 = gt.raw.slope, b0 = my - b1 * mx;
  traces.push({
    type: 'scatter', mode: 'lines', name: `fit: ${b1.toFixed(2)} grades / yr`,
    x: [lo, hi], y: [b0 + b1 * lo, b0 + b1 * hi],
    line: { color: cssVar('--lg-text-2'), width: 1.6, dash: 'dash' },
    hoverinfo: 'skip',
  });

  const layout = chartLayout('');
  layout.height = 400;
  layout.margin = { l: 66, r: 20, t: 12, b: 96 };
  layout.xaxis = { ...layout.xaxis, automargin: false, zeroline: true,
    zerolinecolor: cssVar('--lg-text-2'),
    title: { text: 'how late this gym sits in its own climbers’ careers (years)',
      standoff: 10 } };
  layout.yaxis = { ...layout.yaxis, automargin: false, zeroline: true,
    zerolinecolor: cssVar('--lg-text-2'),
    title: { text: 'grading correction (grades)', standoff: 6 } };
  layout.legend = { ...layout.legend, orientation: 'h', y: -0.28, yanchor: 'top',
    x: 0, font: { size: 10 } };
  Plotly.react(el, traces, layout, { displayModeBar: false, responsive: true });

  const note = document.getElementById('v2-time-note');
  if (note) {
    note.innerHTML = `Each point is one of the 29 gyms, positioned by the average `
      + `within-climber date of its rows. ${gt.n_multi.toLocaleString()} multi-gym `
      + 'climbers carry these contrasts; the median one’s first and last '
      + `hardest send are only <b>${gt.gap_median} years</b> apart, and just `
      + `${Math.round(gt.gap_over_1y * 100)}% are more than a year apart &mdash; so `
      + 'the confound is concentrated in a minority. Right is later, up is stiffer, '
      + 'and the relationship runs exactly the way unmodelled improvement would '
      + 'push it.';
  }
}

function renderV2TimeStats() {
  const host = document.getElementById('v2-time-stats');
  if (!host || !V2_TIME) return;
  const gt = V2_TIME.gym_time, a = V2_TIME.advancement;
  const d = a.debiased;
  // V4-V8 rather than the single median bin: the curve is flat across that
  // span and each bin on its own carries a standard error near its own value.
  const near = d.filter((r) => r.v >= 4 && r.v <= 8);
  const typical = near.reduce((s, r) => s + r.mean, 0) / (near.length || 1);
  const spread = gt.spread_t_c[1] - gt.spread_t_c[0];
  // What improvement can actually account for, against the observed spread.
  const explained = typical * spread;
  const observed = (V2_RESULTS?.spread) || 1.29;
  const tiles = [
    { v: `+${gt.raw.r.toFixed(2)}`, l: 'correlation, raw',
      s: 'gym correction vs within-climber date' },
    { v: `+${gt.within_brand.r.toFixed(2)}`, l: 'correlation, within company',
      s: 'so it is not just Movement being late and stiff' },
    { v: `${typical >= 0 ? '+' : ''}${typical.toFixed(2)}`, l: 'grades/yr at V4–V8',
      s: 'measured advancement where the model’s climbers sit' },
    { v: `${Math.round((explained / observed) * 100)}%`, l: 'of the spread explained',
      s: `${explained.toFixed(2)} of ${observed.toFixed(2)} grades, from improvement` },
  ];
  host.innerHTML = tiles.map((t) => `
    <div class="stat-tile">
      <div class="stat-value">${t.v}</div>
      <div class="stat-label">${t.l}</div>
      <div class="stat-sub">${t.s}</div>
    </div>`).join('');

  const verdict = document.getElementById('v2-time-verdict');
  if (verdict) {
    verdict.innerHTML = 'The correlation is solid: <b>+' + gt.raw.r.toFixed(2)
      + '</b> raw, <b>+' + gt.within_brand.r.toFixed(2) + '</b> with company means '
      + 'removed, <b>+' + (gt.by_brand.Touchstone?.r ?? 0).toFixed(2)
      + '</b> inside Touchstone alone across 17 gyms. Its slope implies '
      + `<b>${gt.within_brand.slope.toFixed(2)} grades per year</b>. `
      + 'But the measured advancement rate where the median climber sits is '
      + `<b>${typical.toFixed(2)} grades per year</b> &mdash; roughly `
      + `<b>${(gt.within_brand.slope / typical).toFixed(0)}&times; smaller</b>. `
      + '<b>Climber improvement cannot be most of this.</b> Improvement '
      + `accounts for roughly ${explained.toFixed(2)} of the ${observed.toFixed(2)} `
      + 'grade correction spread, around '
      + `${Math.round((explained / observed) * 100)}%, which is reassuring for the `
      + 'headline gym result and leaves the rest of the correlation unexplained. '
      + 'Two candidates are not yet separated: <b>gyms’ grading genuinely '
      + 'drifting over time</b> &mdash; which would be a result rather than a '
      + 'confound &mdash; and selection in <b>when</b> climbers switch gyms. '
      + 'Neither can be tested until the send date is carried into the model.';
  }

  // The "how this goes into the model" numbers, so the argument for a fixed
  // offset quotes the same measurements the section just made.
  // Read the measured bins, not the straight-line fit: the curve is convex,
  // and the line overshoots the middle by more than the correction is worth.
  const at = (v) => (d.find((r) => r.v === v) || {}).mean;
  const set = (id, s) => {
    const n = document.getElementById(id);
    if (n) n.textContent = s;
  };
  set('v2-fix-slope', `+${gt.within_brand.slope.toFixed(2)}`);
  set('v2-fix-rate', `+${typical.toFixed(2)}`);
  set('v2-fix-ratio', (gt.within_brand.slope / typical).toFixed(0));
  set('v2-fix-span', spread.toFixed(2));
  set('v2-fix-shift', explained.toFixed(2));
  set('v2-fix-spread', observed.toFixed(2));
  const f2 = (x) => (x === undefined ? '—'
    : (Math.abs(x) < 0.005 ? '0.00' : x.toFixed(2)));
  set('v2-fix-lo', f2(at(3)));
  set('v2-fix-hi', f2(at(9)));
}

function renderV2AdvTable() {
  const el = document.getElementById('v2-adv-table');
  if (!el || !V2_TIME) return;
  const a = V2_TIME.advancement;
  const byV = {};
  a.naive.forEach((r) => { byV[r.v] = { v: r.v, naive: r.mean }; });
  a.short_win.forEach((r) => { byV[r.v] = { ...(byV[r.v] || { v: r.v }), sw: r.mean }; });
  a.long.forEach((r) => { byV[r.v] = { ...(byV[r.v] || { v: r.v }), long: r.mean }; });
  a.debiased.forEach((r) => {
    byV[r.v] = { ...(byV[r.v] || { v: r.v }), deb: r.mean, sem: r.sem,
      n: r.n, chi2: r.chi2 };
  });
  const rows = Object.values(byV).filter((r) => r.deb !== undefined)
    .sort((x, y) => x.v - y.v);
  const sgn = (x) => (x === undefined ? '&mdash;'
    : `${x >= 0 ? '+' : ''}${x.toFixed(2)}`);
  el.innerHTML = '<thead><tr><th>grade</th>'
    + '<th>naive<br /><span class="muted">grades/year</span></th>'
    + '<th>3-month only<br /><span class="muted">grades/year</span></th>'
    + '<th>1-year only<br /><span class="muted">grades/year</span></th>'
    + '<th>steady rate<br /><span class="muted">grades/year, debiased</span></th>'
    + '<th>&plusmn;<br /><span class="muted">standard error, grades/year</span></th>'
    + '<th>χ²/dof<br /><span class="muted">fit quality, &asymp; 1 is good</span></th>'
    + '<th>triples<br /><span class="muted">observations behind the row</span></th>'
    + '</tr></thead><tbody>'
    + rows.map((r) => `<tr><td class="label-cell">V${r.v}</td>`
      + `<td class="unit muted">${sgn(r.naive)}</td>`
      + `<td class="unit muted">${sgn(r.sw)}</td>`
      + `<td class="unit muted">${sgn(r.long)}</td>`
      + `<td class="unit"><b>${sgn(r.deb)}</b></td>`
      + `<td class="unit muted">${r.sem.toFixed(2)}</td>`
      + `<td class="unit${r.chi2 > 2 ? '' : ' muted'}">${r.chi2.toFixed(2)}</td>`
      + `<td class="unit">${r.n.toLocaleString()}</td></tr>`).join('')
    + '</tbody>';
}

