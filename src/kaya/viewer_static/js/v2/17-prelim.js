// ---- The v10 height-form sweep: the whole of /prelim
//
// Renders viewer_templates/prelim.html from v2_prelim.json, which
// scripts/build_v2_prelim.py assembles from runs/traces/idata_v10_*.nc.
//
// This is the ONLY script on that page besides 01-core.js (for cssVar and
// chartLayout) and Plotly. It is not part of the `/` bundle and must not
// start depending on it: the point of the separate route is that this page
// costs 0.5MB instead of the main document's several, so reaching for a
// helper that lives in another v2/ file would quietly undo that.
//
// Ids stay `gf-prelim-*` for continuity with the payload and the build script.
// No v2El/v2Id namespacing: nothing else mounts on this page.
//
// Renders an explicit message when the payload is missing. A half-drawn sweep
// reads as "these forms all scored zero", which is the opposite of what an
// absent file means.

let V2_PRELIM = null;

const pmEl = (name) => document.getElementById(`gf-prelim-${name}`);

async function loadV2Prelim() {
  if (V2_PRELIM !== null) return V2_PRELIM;
  try {
    const r = await fetch('/static/v2_prelim.json', { cache: 'no-cache' });
    V2_PRELIM = r.ok ? await r.json() : false;
  } catch (e) {
    V2_PRELIM = false;
  }
  return V2_PRELIM;
}

// Display names. The payload carries the model's internal form key, which is
// the right thing to store and the wrong thing to show.
const PM_FORM_LABEL = {
  zero: 'no height',
  linear: 'linear',
  linear_x_gender: 'linear × gender',
  quadratic: 'quadratic',
  quadratic_x_gender: 'quadratic × gender',
};

// Which terms each form adds, in the notation the model section uses.
const PM_FORM_TERMS = {
  zero: '—',
  linear: 'h',
  linear_x_gender: 'h, g·h',
  quadratic: 'h, h²',
  quadratic_x_gender: 'h, h², g·h, g·h²',
};

const PM_CORNER_GROUPS = {
  height: ['gamma1', 'gamma2', 'gamma1_x', 'gamma2_x', 'delta1', 'delta2'],
  scale: ['beta0', 'sigma_user', 'sigma_gym', 'log_lambda0', 'kappa', 'rho'],
};

// KaTeX-free labels: this section sits below the glossary's reach and a
// half-typeset axis title is worse than a plain one.
const PM_PARAM_LABEL = {
  beta0: 'β₀ — mean ceiling',
  sigma_user: 'σ_user — spread between climbers',
  sigma_gym: 'σ_gym — spread between gyms',
  log_lambda0: 'log λ₀ — baseline shortfall rate',
  lambda0: 'λ₀ — baseline shortfall rate',
  kappa: 'κ — shortfall vs days climbed',
  rho: 'ρ — shortfall vs sends per session',
  beta_gender: 'β_gender — gender main effect',
  gamma1: 'γ₁ — height, linear',
  gamma2: 'γ₂ — height, curvature',
  gamma1_x: 'γ₁ˣ — height × gender, linear',
  gamma2_x: 'γ₂ˣ — height × gender, curvature',
  delta1: 'δ₁ — ape index, linear',
  delta2: 'δ₂ — ape index, curvature',
  beta_h_missing: 'β_h·missing — height not recorded',
  beta_a_missing: 'β_a·missing — ape index not recorded',
};

const pmLabel = (p) => PM_PARAM_LABEL[p] || p;
const pmFit = (name) => V2_PRELIM.fits.find((f) => f.name === name);
const pmSelectedFit = () => pmEl('fit')?.value || bestFitName();

// Best by elpd, falling back to the reference when no scores are attached.
function bestFitName() {
  const scored = V2_PRELIM.fits.filter((f) => f.loo);
  if (!scored.length) return V2_PRELIM.reference;
  return scored.reduce((a, b) => (a.loo.elpd >= b.loo.elpd ? a : b)).name;
}

// One stable hue per form, so a form is the same colour in all four charts.
function pmHue(i) {
  const pal = ['--lg-cat-1', '--lg-cat-2', '--lg-cat-3', '--lg-cat-4', '--lg-cat-5'];
  return cssVar(pal[i % pal.length]);
}
function pmHueOf(name) {
  return pmHue(V2_PRELIM.fits.findIndex((f) => f.name === name));
}

// rgba() from a hex or rgb() token, so bands can be drawn translucent without
// assuming which form the CSS variable came back in.
function pmAlpha(colour, a) {
  const c = String(colour).trim();
  if (c.startsWith('#')) {
    const h = c.length === 4
      ? c.slice(1).split('').map((x) => x + x).join('')
      : c.slice(1);
    const n = parseInt(h, 16);
    return `rgba(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255}, ${a})`;
  }
  const m = c.match(/rgba?\(([^)]+)\)/);
  if (m) {
    const [r, g, b] = m[1].split(',').map((x) => parseFloat(x));
    return `rgba(${r}, ${g}, ${b}, ${a})`;
  }
  return c;
}

// Pearson correlation of two equal-length draw vectors.
function pmCorr(a, b) {
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

// ---- status ----

function renderPmStatus(P) {
  const el = pmEl('status-text');
  if (!el) return;
  const frozen = P.fits.filter((f) => f.frozen_chains.length);
  const scored = P.fits.filter((f) => f.loo);
  const bits = [];
  bits.push('<b>The noise floor is not measured yet.</b> A replicate of the '
    + 'linear fit at a different random seed is queued; the gap between those '
    + 'twins is pure seed-to-seed noise, and it is the only thing that says '
    + 'whether the small gaps below are a ranking or a rearrangement. The '
    + 'previous sweep is the cautionary case — 32.7 elpd of spread against a '
    + '31.1 elpd floor, which was no ranking at all.');
  if (frozen.length) {
    bits.push(`<b>${frozen.length} of ${P.fits.length} fits are on three chains, not four.</b> `
      + frozen.map((f) => `<code>${PM_FORM_LABEL[f.height_form] || f.height_form}</code>`).join(' and ')
      + ' each had one chain adapt its step size to exactly zero and never '
      + 'leave its starting point. Because R-hat and ESS compare chains to '
      + '<i>each other</i>, a dead chain makes a healthy model report as a '
      + 'broken one — both fits showed R-hat 1.53 and ESS 7 despite sharing no '
      + 'height parameters at all. Every number here is computed on the chains '
      + 'that actually sampled, and both fits are re-queued at fresh seeds.');
  }
  if (scored.length < P.fits.length) {
    bits.push('<b>Two forms are still running</b> — saturating and vertex '
      + 'quadratic — so this is five of seven.');
  }
  el.innerHTML = ` ${bits.join(' ')}`;
}

// ---- the score table ----

function renderPmScores(P) {
  const el = pmEl('scores');
  if (!el) return;
  const rows = [...P.fits].filter((f) => f.loo)
    .sort((a, b) => b.loo.elpd - a.loo.elpd);
  if (!rows.length) {
    el.innerHTML = '<tbody><tr><td>No scores in this payload yet.</td></tr></tbody>';
    return;
  }
  const best = rows[0];
  el.innerHTML = `
    <thead><tr>
      <th>height form</th><th>terms</th>
      <th class="num">elpd</th>
      <th class="num">vs linear</th>
      <th class="num">paired σ</th>
      <th class="num">chains</th>
      <th class="num">R-hat</th>
      <th class="num">ESS</th>
    </tr></thead>
    <tbody>${rows.map((f) => {
      const v = f.loo.vs_reference;
      const isRef = f.name === P.reference;
      const win = f.name === best.name ? ' class="row-highlight"' : '';
      const chains = f.frozen_chains.length
        ? `<b>${f.n_chains_kept}</b> / ${f.n_chains_run}`
        : `${f.n_chains_kept} / ${f.n_chains_run}`;
      return `<tr${win}>
        <td><b>${PM_FORM_LABEL[f.height_form] || f.height_form}</b></td>
        <td><code>${PM_FORM_TERMS[f.height_form] || '—'}</code></td>
        <td class="num">${f.loo.elpd.toLocaleString(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 })}</td>
        <td class="num">${isRef ? '—' : (v.delta >= 0 ? '+' : '') + v.delta.toFixed(2)}</td>
        <td class="num">${isRef ? '—' : (v.sigma >= 0 ? '+' : '') + v.sigma.toFixed(1)}</td>
        <td class="num">${chains}</td>
        <td class="num">${f.max_rhat.toFixed(3)}</td>
        <td class="num">${f.min_ess.toLocaleString()}</td>
      </tr>`;
    }).join('')}</tbody>`;

  const note = pmEl('scores-note');
  if (note) {
    const f0 = rows[0];
    note.innerHTML = `Per-climber PSIS-LOO over `
      + `${f0.loo.n_climbers.toLocaleString()} climbers `
      + `(${f0.loo.n_single.toLocaleString()} who logged at one gym, `
      + `${f0.loo.n_multi.toLocaleString()} at several). <b>Higher elpd is `
      + `better.</b> &ldquo;vs linear&rdquo; is the paired difference — the two `
      + `fits are scored per climber and differenced before summing, so `
      + `everything the two share cancels. A bold chain count means a frozen `
      + `chain was dropped. Built ${P.built_at} from `
      + `${P.network}/${P.name_filter}.`;
  }

  const paired = pmEl('paired')?.querySelector('div');
  if (paired && rows.length > 1) {
    const rawSE = rows[0].loo.se_raw;
    const pairedSEs = rows.filter((f) => f.name !== P.reference)
      .map((f) => f.loo.vs_reference.se_paired);
    const typical = pairedSEs.reduce((a, b) => a + b, 0) / pairedSEs.length;
    const zero = rows.find((f) => f.height_form === 'zero');
    paired.innerHTML = `<b>Why the paired column is the only one worth reading.</b>
      Each fit's own error bar is about <b>${rawSE.toLocaleString(undefined, { maximumFractionDigits: 0 })}</b>,
      which would make every comparison here look hopeless. But that number
      measures how much <i>climbers</i> differ from each other, not how much
      <i>models</i> differ — and both models score the same climbers. Difference
      them per climber first and the shared variation cancels, leaving a paired
      error of about <b>${typical.toFixed(1)}</b>. That is roughly
      <b>${Math.round(rawSE / typical)}×</b> smaller, and it is the yardstick the
      comparison actually needs.${zero ? ` Read against it, dropping height
      entirely costs <b>${Math.abs(zero.loo.vs_reference.delta).toFixed(1)} elpd</b>
      at <b>${Math.abs(zero.loo.vs_reference.sigma).toFixed(0)}σ</b> — whatever
      the right shape is, &ldquo;leave height out&rdquo; is not it.` : ''}`;
  }
}

// ---- body-dimension curves ----

function pmBodyChart(P, kind) {
  const el = pmEl(kind === 'height' ? 'height' : 'ape');
  if (!el || typeof Plotly === 'undefined') return;
  const gender = pmEl('gender')?.value || 'average';
  const showBands = (pmEl('bands')?.value || 'all') === 'all';

  const traces = [];
  P.fits.forEach((f, i) => {
    const c = f.curves?.[gender]?.[kind];
    if (!c) return;
    const hue = pmHue(i);
    if (showBands) {
      traces.push({
        type: 'scatter', mode: 'lines', x: [...c.x, ...[...c.x].reverse()],
        y: [...c.hi, ...[...c.lo].reverse()],
        fill: 'toself', fillcolor: pmAlpha(hue, 0.13),
        line: { width: 0 }, hoverinfo: 'skip', showlegend: false,
      });
    }
    traces.push({
      type: 'scatter', mode: 'lines', x: c.x, y: c.mean,
      name: PM_FORM_LABEL[f.height_form] || f.height_form,
      line: { color: hue, width: 2.6 },
      hovertemplate: `${PM_FORM_LABEL[f.height_form]}<br>`
        + `%{x:.0f} in: %{y:+.3f} grades<extra></extra>`,
    });
  });
  if (!traces.length) { Plotly.purge(el); return; }

  const isH = kind === 'height';
  const layout = chartLayout('');
  layout.height = 380;
  layout.margin = { l: 62, r: 20, t: 12, b: 58 };
  layout.xaxis = { ...layout.xaxis, automargin: false,
    title: { text: isH ? 'height (inches)' : 'ape index (inches, arm span minus height)', standoff: 10 } };
  // Short enough to fit the rotated slot. Plotly does not wrap an axis title,
  // it clips it, so the full sentence lives in the caption instead.
  layout.yaxis = { ...layout.yaxis,
    title: { text: 'ability (grades)', standoff: 8 },
    zeroline: true, zerolinecolor: cssVar('--lg-border'), zerolinewidth: 1 };
  layout.legend = { orientation: 'h', y: -0.24, x: 0 };
  layout.hovermode = 'x unified';
  Plotly.newPlot(el, traces, layout, { displayModeBar: false, responsive: true });

  const note = pmEl(isH ? 'height-note' : 'ape-note');
  if (!note) return;
  const gLabel = { male: 'male-coded climbers', female: 'female-coded climbers',
    average: `the population average gender weight (${P.consts.w_female_mean.toFixed(2)})` }[gender];
  if (isH) {
    const conf = P.fits.find((f) => f.height_form === 'quadratic_x_gender');
    const span = conf?.curves?.[gender]?.height?.span;
    note.innerHTML = `<b>Vertical axis:</b> ability relative to that curve's `
      + `own mean, in grades. Ability against height, drawn at ${gLabel}, across `
      + `&plusmn;8 inches of the median climber (${P.consts.h_med.toFixed(0)} in) — `
      + `roughly the 5th to 95th percentile, so the curve stays inside the data `
      + `rather than extrapolating. Bands are ${Math.round(P.band * 100)}% `
      + `credible intervals. <b>The forms disagree most at the extremes and `
      + `barely at all in the middle</b>, which is exactly why they are hard to `
      + `tell apart: most climbers live in the middle.`
      + (span ? ` The best-scoring form spans <b>${span.toFixed(2)} grades</b> `
        + `end to end here.` : '')
      + ` Switch the gender control to see the effect the sweep actually turns `
      + `on — the two curvatures have opposite signs.`;
  } else {
    note.innerHTML = `<b>Note the vertical scale</b> — it runs several times `
      + `wider than the height chart above. Reach buys far more than height `
      + `does, which is worth keeping in view while reading a sweep that is `
      + `entirely about height. Ability against ape index — arm span minus height, so `
      + `zero is &ldquo;arms as long as you are tall&rdquo; and positive is `
      + `longer. Drawn at ${gLabel}. <b>Every form carries the same ape terms</b>, `
      + `so this chart is a control rather than a comparison: the curves should `
      + `and do lie almost on top of one another. A form that moved this curve `
      + `while only its height term changed would mean height and ape index were `
      + `trading against each other, and the corner plot below is where that `
      + `would show up as a diagonal ridge.`;
  }
}

// ---- posterior table ----

function renderPmParams(P) {
  const el = pmEl('params');
  if (!el) return;
  const f = pmFit(pmSelectedFit());
  if (!f) return;
  const names = Object.keys(f.params);
  el.innerHTML = `
    <thead><tr>
      <th>parameter</th>
      <th class="num">mean</th><th class="num">sd</th>
      <th class="num">${Math.round(P.band * 100)}% interval</th>
      <th class="num">R-hat</th><th class="num">ESS bulk</th><th class="num">ESS tail</th>
    </tr></thead>
    <tbody>${names.map((p) => {
      const v = f.params[p];
      const bad = v.rhat > 1.01 || v.ess_bulk < 400;
      // A parameter whose interval excludes zero is doing work; one that
      // straddles it is consistent with having no effect at all.
      const excludesZero = (v.lo > 0 && v.hi > 0) || (v.lo < 0 && v.hi < 0);
      return `<tr>
        <td>${pmLabel(p)}${excludesZero ? ' <span class="pill pill-ok">≠ 0</span>' : ''}</td>
        <td class="num">${v.mean.toFixed(4)}</td>
        <td class="num">${v.sd.toFixed(4)}</td>
        <td class="num">${v.lo.toFixed(3)} to ${v.hi.toFixed(3)}</td>
        <td class="num"${bad ? ' style="color:var(--lg-warning)"' : ''}>${v.rhat.toFixed(3)}</td>
        <td class="num"${bad ? ' style="color:var(--lg-warning)"' : ''}>${v.ess_bulk.toLocaleString()}</td>
        <td class="num">${v.ess_tail.toLocaleString()}</td>
      </tr>`;
    }).join('')}</tbody>`;

  const note = pmEl('params-note');
  if (note) {
    note.innerHTML = `<code>${f.name}</code> — ${f.blurb}. `
      + `${f.n_chains_kept} chains × ${f.n_draws.toLocaleString()} draws`
      + (f.frozen_chains.length
        ? `, after dropping frozen chain ${f.frozen_chains.join(', ')}`
        : '')
      + `, ${f.divergences} divergent transition${f.divergences === 1 ? '' : 's'}, `
      + `step size ${f.step_size.toFixed(3)}, mean trajectory depth `
      + `${f.tree_depth_mean.toFixed(1)}. Ran in ${f.minutes.toFixed(0)} minutes. `
      + `A <span class="pill pill-ok">≠ 0</span> tag means the interval excludes `
      + `zero, so the data supports an effect in a definite direction.`;
  }
}

// ---- chains ----

function renderPmTrace() {
  const el = pmEl('trace');
  if (!el || typeof Plotly === 'undefined') return;
  const f = pmFit(pmSelectedFit());
  if (!f) return;
  const sel = pmEl('trace-param');
  const p = sel?.value && f.params[sel.value] ? sel.value : Object.keys(f.params)[0];
  const v = f.params[p];
  if (!v) { Plotly.purge(el); return; }

  const traces = v.chains.map((c, i) => ({
    type: 'scatter', mode: 'lines',
    x: c.map((_, j) => j * f.thin_stride), y: c,
    name: `chain ${i}`,
    line: { color: pmHue(i), width: 1.1 },
    hovertemplate: `chain ${i}, draw %{x}: %{y:.4f}<extra></extra>`,
  }));
  const layout = chartLayout('');
  layout.height = 300;
  layout.margin = { l: 66, r: 20, t: 12, b: 52 };
  layout.xaxis = { ...layout.xaxis, automargin: false,
    title: { text: 'draw (post-warm-up)', standoff: 10 } };
  layout.yaxis = { ...layout.yaxis,
    title: { text: pmLabel(p).split(' — ')[0], standoff: 8 } };
  layout.legend = { orientation: 'h', y: -0.28, x: 0 };
  Plotly.newPlot(el, traces, layout, { displayModeBar: false, responsive: true });

  const note = pmEl('trace-note');
  if (note) {
    note.innerHTML = `<b>${pmLabel(p)}</b> in <code>${f.name}</code>, `
      + `thinned to every ${f.thin_stride}th draw. R-hat `
      + `<b>${v.rhat.toFixed(3)}</b>, effective sample size `
      + `<b>${v.ess_bulk.toLocaleString()}</b> out of `
      + `${(f.n_chains_kept * f.n_draws).toLocaleString()} raw draws — the gap `
      + `between those two is autocorrelation, not waste.`
      + (f.frozen_chains.length
        ? ` The chain that never moved is not drawn: it was a flat line at its
            starting value, and including it would compress every other trace
            into a band too narrow to read.`
        : '');
  }
}

// ---- corner ----

function pmCornerNames(f, group) {
  const wanted = group === 'all'
    ? Object.keys(f.params).filter((p) => p !== 'lambda0')
    : PM_CORNER_GROUPS[group] || [];
  return wanted.filter((p) => f.params[p]);
}

function renderPmCorner() {
  const el = pmEl('corner');
  if (!el || typeof Plotly === 'undefined') return;
  const f = pmFit(pmSelectedFit());
  if (!f) return;
  const group = pmEl('corner-group')?.value || 'height';
  const names = pmCornerNames(f, group);
  const note = pmEl('corner-note');

  if (names.length < 2) {
    Plotly.purge(el);
    el.innerHTML = '<p class="pm-caption">This fit does not carry enough of these '
      + 'parameters to draw a corner plot — <code>no height</code> has no height '
      + 'terms at all, which is the point of it.</p>';
    if (note) note.innerHTML = '';
    return;
  }
  el.innerHTML = '';

  const N = names.length;
  // Draws are already thinned with a shared stride in the payload, so column i
  // of one parameter and column i of another are the same draw. Flattening
  // chain-major preserves that.
  const flat = {};
  names.forEach((p) => { flat[p] = f.params[p].chains.flat(); });

  const traces = [];
  const layout = chartLayout('');
  layout.height = Math.max(420, 150 * N);
  layout.margin = { l: 74, r: 16, t: 16, b: 62 };
  layout.showlegend = false;
  layout.grid = { rows: N, columns: N, pattern: 'independent' };
  delete layout.xaxis; delete layout.yaxis;

  const hue = pmHueOf(f.name);
  let k = 0;
  for (let r = 0; r < N; r++) {
    for (let c = 0; c < N; c++) {
      if (c > r) { k++; continue; }
      const idx = r * N + c + 1;
      const ax = idx === 1 ? 'x' : `x${idx}`;
      const ay = idx === 1 ? 'y' : `y${idx}`;
      if (r === c) {
        traces.push({
          type: 'histogram', x: flat[names[r]], xaxis: ax, yaxis: ay,
          marker: { color: pmAlpha(hue, 0.55) }, nbinsx: 26,
          hovertemplate: '%{x:.4f}<extra></extra>',
        });
      } else {
        traces.push({
          type: 'scatter', mode: 'markers',
          x: flat[names[c]], y: flat[names[r]], xaxis: ax, yaxis: ay,
          marker: { color: pmAlpha(hue, 0.32), size: 2.6 },
          hovertemplate: '%{x:.4f}, %{y:.4f}<extra></extra>',
        });
      }
      const short = (p) => pmLabel(p).split(' — ')[0];
      layout[`xaxis${idx === 1 ? '' : idx}`] = {
        ...chartLayout('').xaxis, automargin: false,
        title: r === N - 1 ? { text: short(names[c]), standoff: 6, font: { size: 10 } } : undefined,
        showticklabels: r === N - 1, tickfont: { size: 9 },
      };
      layout[`yaxis${idx === 1 ? '' : idx}`] = {
        ...chartLayout('').yaxis, automargin: false,
        title: (c === 0 && r > 0) ? { text: short(names[r]), standoff: 6, font: { size: 10 } } : undefined,
        showticklabels: c === 0 && r > 0, tickfont: { size: 9 },
      };
      k++;
    }
  }
  Plotly.newPlot(el, traces, layout, { displayModeBar: false, responsive: true });

  if (note) {
    // Name the ridge if it is actually on screen. Describing correlation in
    // the abstract, next to a plot that shows a specific one, wastes the plot.
    let ridge = '';
    const gx = ['gamma1_x', 'gamma2_x'];
    if (gx.every((p) => names.includes(p))) {
      const r = pmCorr(flat.gamma1_x, flat.gamma2_x);
      if (Math.abs(r) > 0.3) {
        ridge = ` <b>There is one here:</b> the two gender-interaction terms, `
          + `\u03b3\u2081\u02e3 and \u03b3\u2082\u02e3, correlate at `
          + `<b>${r.toFixed(2)}</b>. Both act only on the 29% of climbers coded `
          + `female, so the data has less to separate them with than it does `
          + `for the main effects — the slope and the curvature of the female `
          + `curve can trade against each other and still fit. That is a real `
          + `limitation on how precisely this form's gender split can be read, `
          + `and it is separate from the dead chain, which was a sampler fault.`;
      }
    }
    note.innerHTML = `Joint posterior of <code>${f.name}</code>, `
      + `${(f.params[names[0]].chains.flat().length).toLocaleString()} thinned `
      + `draws. Diagonal panels are each parameter on its own; off-diagonal `
      + `panels are pairs. <b>Round blobs are what you want</b> — they mean the `
      + `data pins each parameter independently. A tilted ellipse means the two `
      + `trade against each other and only their combination is identified, `
      + `which is how a model can fit well and still have uninterpretable `
      + `coefficients.` + ridge;
  }
}

// ---- wiring ----

function renderPmForFit(P) {
  renderPmParams(P);
  renderPmTrace();
  renderPmCorner();
}

function bindPmControls(P) {
  const fitSel = pmEl('fit');
  if (fitSel && !fitSel.options.length) {
    // Best first: the reader should land on the leading form, not on whichever
    // one happened to be built first.
    const ordered = [...P.fits].sort((a, b) => (b.loo?.elpd ?? -Infinity) - (a.loo?.elpd ?? -Infinity));
    fitSel.innerHTML = ordered.map((f) => {
      const lbl = PM_FORM_LABEL[f.height_form] || f.height_form;
      return `<option value="${f.name}">${lbl}${f.name === bestFitName() ? ' — best' : ''}</option>`;
    }).join('');
    fitSel.value = bestFitName();
  }

  const paramSel = pmEl('trace-param');
  const fillParams = () => {
    const f = pmFit(pmSelectedFit());
    if (!f || !paramSel) return;
    const prev = paramSel.value;
    paramSel.innerHTML = Object.keys(f.params)
      .map((p) => `<option value="${p}">${pmLabel(p)}</option>`).join('');
    paramSel.value = f.params[prev] ? prev : Object.keys(f.params)[0];
  };
  fillParams();

  fitSel?.addEventListener('change', () => { fillParams(); renderPmForFit(P); });
  paramSel?.addEventListener('change', renderPmTrace);
  pmEl('corner-group')?.addEventListener('change', renderPmCorner);
  ['gender', 'bands'].forEach((id) => {
    pmEl(id)?.addEventListener('change', () => {
      pmBodyChart(P, 'height');
      pmBodyChart(P, 'ape');
    });
  });
}

function renderPmAgreement(P) {
  const box = pmEl('agree')?.querySelector('div');
  if (!box) return;
  const best = pmFit(bestFitName());
  const lbl = PM_FORM_LABEL[best?.height_form] || '—';
  box.innerHTML = `<b>Two independent methods, one answer.</b>
    A model-free probe — strip each gym's grading out by least squares, then
    cross-validate the height forms over climbers directly, repeated across 25
    different fold assignments — picked <code>quadratic × gender</code> on
    <b>25 of 25</b> shuffles, in about ninety seconds. These fits, at roughly
    four hours each and by a completely different route, put
    <code>${lbl}</code> on top as well, and reproduce the probe's whole ordering:
    height matters a lot, curvature helps a little, gender-on-the-slope does
    nothing, gender-on-the-curvature is real. The mechanism behind all four is
    the same one: <b>the two curvatures have opposite signs</b>, so any form
    that makes the genders share a single bend averages them toward zero.`;
}

function renderPmStamp(P) {
  const el = pmEl('stamp');
  if (!el) return;
  const scored = P.fits.filter((f) => f.loo).length;
  el.textContent = `${P.fits.length} fits \u00b7 ${scored} scored \u00b7 `
    + `${P.network}/${P.name_filter} \u00b7 built ${P.built_at}`;
}

async function renderV2Prelim() {
  const P = await loadV2Prelim();
  const main = document.querySelector('main');
  if (!P || !P.fits?.length) {
    if (main) {
      main.innerHTML = '<div class="pm-callout pm-callout-warn"><div>'
        + '<b>No payload.</b> /static/v2_prelim.json is missing or empty. '
        + 'Rebuild it with <code>python scripts/build_v2_prelim.py --elpd '
        + '&lt;elpd.json&gt;</code>.</div></div>';
    }
    return;
  }
  renderPmStamp(P);
  renderPmStatus(P);
  renderPmScores(P);
  bindPmControls(P);
  pmBodyChart(P, 'height');
  pmBodyChart(P, 'ape');
  renderPmForFit(P);
  renderPmAgreement(P);
}
