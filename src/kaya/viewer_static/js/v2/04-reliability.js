// ---- can the model comparison hear itself? ----
//
// v2_reliability.json is written by scripts/build_v2_reliability.py: the same
// seven height forms scored on progressively better-observed climbers, plus
// refits of one model that give the noise floor. Nothing here is hand-typed.

let V2_REL = null;

async function loadV2Reliability() {
  if (V2_REL) return true;
  try {
    const r = await fetch('/static/v2_reliability.json', { cache: 'no-cache' });
    if (!r.ok) return false;
    V2_REL = await r.json();
    return true;
  } catch (e) {
    return false;
  }
}

// Which model version the reliability figures are showing. Both are kept and
// both are reachable: the old one is the evidence that the fix was needed.
const V2_ARM_LABEL = {
  unmarginalized: 'every climber keeps their own ability offset (10,397 parameters)',
  marginalized: 'single-observation offsets integrated out (4,241 parameters)',
};
let V2_ARM = null;

function v2Arm() {
  if (!V2_REL) return null;
  const key = V2_ARM && V2_REL.arms[V2_ARM] ? V2_ARM : V2_REL.primary;
  return V2_REL.arms[key] ? { key, ...V2_REL.arms[key] } : null;
}

function renderV2ArmPicker() {
  const host = document.getElementById('v2-arm-picker');
  if (!host || !V2_REL) return;
  const keys = Object.keys(V2_REL.arms);
  const cur = v2Arm();
  if (keys.length < 2) {
    host.innerHTML = `<span class="muted">Showing: ${V2_ARM_LABEL[cur.key]}. `
      + 'The other version is still fitting.</span>';
    return;
  }
  host.innerHTML = keys.map((k) => `
    <button type="button" class="seg-btn${k === cur.key ? ' on' : ''}"
            data-arm="${k}">${k === 'marginalized' ? 'Offsets integrated out'
                                                   : 'Original model'}</button>`).join('')
    + `<span class="muted seg-note">${V2_ARM_LABEL[cur.key]}</span>`;
  host.querySelectorAll('[data-arm]').forEach((b) => {
    b.onclick = () => { V2_ARM = b.dataset.arm; renderV2Noise();
      renderV2SubsetTable(); renderV2ArmPicker(); };
  });
}

function renderV2Noise() {
  const el = document.getElementById('v2-noise-chart');
  const arm = v2Arm();
  if (!el || !arm || typeof Plotly === 'undefined') return;
  const subs = V2_REL.subsets, ks = subs.map((s) => String(s.k));
  const xs = subs.map((s, i) => i);
  const label = (s) => (s.k === 1 ? 'all rows'
    : `climbers with ${s.k}+ rows`);

  const traces = [];
  // The noise band first, so the model lines draw over it. This is the whole
  // point of the figure: a gap inside the band is not a result.
  const nz = ks.map((k) => (arm.noise[k] || {}).range || 0);
  traces.push({
    type: 'scatter', mode: 'lines', x: xs, y: nz.map((v) => -v),
    line: { width: 0 }, showlegend: false, hoverinfo: 'skip',
  });
  traces.push({
    type: 'scatter', mode: 'lines', x: xs, y: nz.map(() => 0),
    line: { width: 0 }, fill: 'tonexty',
    fillcolor: hexToRgba(cssVar('--lg-text-2'), 0.16),
    name: 'noise: spread across refits of one model',
    hovertemplate: 'noise floor<extra></extra>',
  });

  const real = arm.models.filter((m) => !m.replicate_of);
  real.forEach((m, i) => {
    const c = cssVar(V2_FIT_HUES[i % V2_FIT_HUES.length]);
    traces.push({
      type: 'scatter', mode: 'lines+markers', name: m.label,
      x: xs, y: ks.map((k) => m.by_subset[k].gap),
      line: { color: c, width: 2.2 }, marker: { size: 8 },
      hovertemplate: `%{y:+.1f} vs the best model<extra>${m.label}</extra>`,
    });
  });

  const layout = chartLayout('');
  layout.height = 420;
  layout.margin = { l: 66, r: 20, t: 12, b: 118 };
  layout.xaxis = { ...layout.xaxis, automargin: false,
    tickmode: 'array', tickvals: xs, ticktext: subs.map(label),
    title: { text: 'which observations are scored', standoff: 12 } };
  layout.yaxis = { ...layout.yaxis, automargin: false, zeroline: true,
    zerolinecolor: cssVar('--lg-text-2'),
    title: { text: 'score gap from the best model (higher is better)',
      standoff: 6 } };
  layout.legend = { ...layout.legend, orientation: 'h', y: -0.26,
    yanchor: 'top', x: 0, font: { size: 10 } };
  Plotly.react(el, traces, layout, { displayModeBar: false, responsive: true });

  const note = document.getElementById('v2-noise-note');
  if (note) {
    const n1 = arm.noise['1'] || {}, n3 = arm.noise['3'] || {};
    const s3 = subs.find((s) => s.k === 3) || {};
    note.innerHTML = `<b>${V2_ARM_LABEL[arm.key]}.</b> Each line is one height `
      + 'form, scored against the best model in the same column. The grey band '
      + `is how far apart ${n1.n_runs || '?'} fits of the <b>identical</b> `
      + 'model land &mdash; pure noise, since nothing about them differs but '
      + 'the random seed. Scored on all rows that band is '
      + `<b>${(n1.range || 0).toFixed(1)}</b> points wide; scored on climbers `
      + `with three or more observations &mdash; still `
      + `${Math.round((s3.share || 0) * 100)}% of the data &mdash; it is `
      + `<b>${(n3.range || 0).toFixed(1)}</b>. Nothing was refitted between `
      + 'these columns; the same per-observation scores are simply added up '
      + 'over different rows.';
    // A refit that failed to converge is not sampling noise, it is a broken
    // chain, so it is out of the band -- but hiding it would overstate how
    // dependably this model fits, which is the very thing being measured.
    (arm.noise_excluded || []).forEach((ex) => {
      note.innerHTML += ` <b>One refit is excluded from that band:</b> `
        + `&ldquo;${ex.label}&rdquo; did not converge (R-hat `
        + `${ex.max_rhat.toFixed(2)}, against a limit of `
        + `${(arm.rhat_gate || 1.2).toFixed(1)}), so its score measures a `
        + 'failed fit rather than run-to-run variation. That it happened at '
        + 'all is worth knowing: this model does not converge every time, and '
        + 'a single fit should be checked before it is trusted.';
    });
  }
}

function renderV2SubsetTable() {
  const el = document.getElementById('v2-subset-table');
  const arm = v2Arm();
  if (!el || !arm) return;
  const subs = V2_REL.subsets;
  const head = subs.map((s) => (s.k === 1 ? 'all rows' : `${s.k}+ rows`));
  const sub = subs.map((s) => `${s.rows.toLocaleString()} rows`);
  el.innerHTML = '<thead><tr><th>height model</th>'
    + head.map((h, i) => `<th>${h}<br /><span class="muted">${sub[i]}</span></th>`).join('')
    + '<th>unreliable rows<br /><span class="muted">at 3+ rows</span></th></tr></thead><tbody>'
    + arm.models.map((m) => {
      const rep = m.replicate_of ? ' muted' : '';
      return `<tr><td class="label-cell${rep}">${m.label}</td>`
        + subs.map((s) => {
          const g = m.by_subset[String(s.k)].gap;
          return `<td class="unit${rep}">${g >= 0 ? '+' : ''}${g.toFixed(1)}</td>`;
        }).join('')
        + `<td class="unit muted">${Math.round(m.by_subset['3'].bad_k * 100)}%</td></tr>`;
    }).join('')
    + '</tbody>';
}

function renderV2ArmCompare() {
  const el = document.getElementById('v2-arm-compare');
  if (!el || !V2_REL) return;
  const a = V2_REL.arms.unmarginalized, b = V2_REL.arms.marginalized;
  if (!a || !b) {
    el.innerHTML = '<p class="caption">The side-by-side comparison appears '
      + 'once both versions have finished fitting.</p>';
    return;
  }
  const row = (name, get, fmt = (x) => x) => {
    const va = get(a), vb = get(b);
    return `<tr><td class="label-cell">${name}</td>`
      + `<td class="unit">${va === undefined ? '&mdash;' : fmt(va)}</td>`
      + `<td class="unit"><b>${vb === undefined ? '&mdash;' : fmt(vb)}</b></td></tr>`;
  };
  const noise = (arm, k) => (arm.noise[k] || {}).range;
  const badk = (arm, k) => {
    const m = arm.models.find((x) => !x.replicate_of);
    return m ? m.by_subset[k].bad_k : undefined;
  };
  const f1 = (x) => x.toFixed(1);
  const pc = (x) => `${Math.round(x * 100)}%`;
  el.innerHTML = '<thead><tr><th>&nbsp;</th><th>original model</th>'
    + '<th>offsets integrated out</th></tr></thead><tbody>'
    + row('parameters', (x) => (x === a ? 10397 : 4241), (x) => x.toLocaleString())
    + row('noise between refits, all rows', (x) => noise(x, '1'), f1)
    + row('noise between refits, 3+ rows', (x) => noise(x, '3'), f1)
    + row('unreliable rows, all rows', (x) => badk(x, '1'), pc)
    + row('unreliable rows, 3+ rows', (x) => badk(x, '3'), pc)
    + '</tbody>';
}

// ---- do the two model versions give the same ANSWERS? ----
//
// v2_arm_params.json is written by scripts/build_v2_arm_params.py. The two
// versions' predictive scores are not comparable, but their parameters are the
// same parameters, so the question that matters can be asked directly.

let V2_ARMP = null;
let V2_ARMP_FORM = null;

// The model's parameters have terse mathematical names. Nobody should have to
// remember which Greek letter meant what, so every row says it in words and
// gives its units.
const V2_PARAM_LABEL = {
  beta0: ['baseline ceiling', 'hardest grade an average climber sends, in V-grades'],
  beta_gender: ['gender offset', 'grades, women relative to men'],
  gamma1: ['height, straight-line term', 'grades gained per inch above average'],
  gamma2: ['height, curvature term', 'bend in that line; 0 = perfectly straight'],
  gamma1_x: ['height term, women', 'the straight-line height effect, fitted separately for women'],
  gamma2_x: ['height curvature, women', 'the curvature, fitted separately for women'],
  delta1: ['ape index, straight-line term', 'grades per inch of arm span beyond height'],
  delta2: ['ape index, curvature term', 'bend in the ape-index line'],
  beta_h_missing: ['no height on file', 'grades, climbers who never entered a height'],
  beta_a_missing: ['no arm span on file', 'grades, climbers who never entered a span'],
  sigma_user: ['spread between climbers', 'grades; how far apart natural ability runs'],
  sigma_gym: ['spread between gyms', 'grades; how far apart gym stiffness runs'],
  log_lambda0: ['effort baseline', 'log scale; how far past their ceiling a climber reaches'],
  kappa: ['effect of gym visits', 'more visits → sends further past the ceiling'],
  rho: ['effect of time at the gym', 'per year; ~0 means no drift'],
};

async function renderV2ArmParams() {
  const host = document.getElementById('v2-armparam-chart');
  if (!host) return;
  if (!V2_ARMP) {
    try {
      const r = await fetch('/static/v2_arm_params.json', { cache: 'no-cache' });
      if (r.ok) V2_ARMP = await r.json();
    } catch (e) { /* not built yet */ }
  }
  const forms = (V2_ARMP && V2_ARMP.forms) || {};
  const ready = Object.keys(forms).filter((k) => forms[k].gyms && forms[k].gyms.length);
  const key = ready.includes(V2_ARMP_FORM) ? V2_ARMP_FORM : ready[0];
  const note = document.getElementById('v2-armparam-note');
  const tbl = document.getElementById('v2-armparam-table');
  const pick = document.getElementById('v2-armparam-picker');
  if (!key) {
    host.style.display = 'none';
    if (pick) pick.style.display = 'none';
    if (tbl) tbl.closest('.data-table-wrap').style.display = 'none';
    if (note) {
      note.innerHTML = 'This comparison appears once both versions of the same '
        + 'height form have finished fitting.';
    }
    return;
  }
  // One picker for both the chart and the table below it: the question is asked
  // per height form, and the answer is only convincing if it holds for all.
  if (pick) {
    pick.style.display = ready.length > 1 ? '' : 'none';
    if (ready.length > 1) {
      pick.innerHTML = '<span class="muted seg-note" style="margin:0 10px 0 0">'
        + 'height form:</span>'
        + ready.map((k) => `<button type="button" class="seg-btn`
          + `${k === key ? ' on' : ''}" data-form="${k}">${k}</button>`).join('');
      pick.querySelectorAll('[data-form]').forEach((b) => {
        b.onclick = () => { V2_ARMP_FORM = b.dataset.form; renderV2ArmParams(); };
      });
    }
  }
  host.style.display = '';
  if (tbl) tbl.closest('.data-table-wrap').style.display = '';
  const f = forms[key], gyms = f.gyms, st = f.stats;

  const brands = [...new Set(gyms.map((g) => g.b))];
  const traces = brands.map((b) => {
    const rs = gyms.filter((g) => g.b === b);
    const c = cssVar(V2_BRAND_COLOURS[b] || '--lg-text-2');
    return {
      type: 'scatter', mode: 'markers', name: b || 'other',
      x: rs.map((g) => g.old.mean), y: rs.map((g) => g.new.mean),
      error_x: { type: 'data', symmetric: false,
        array: rs.map((g) => g.old.hi - g.old.mean),
        arrayminus: rs.map((g) => g.old.mean - g.old.lo),
        color: hexToRgba(c, 0.35), thickness: 1.2, width: 0 },
      error_y: { type: 'data', symmetric: false,
        array: rs.map((g) => g.new.hi - g.new.mean),
        arrayminus: rs.map((g) => g.new.mean - g.new.lo),
        color: hexToRgba(c, 0.35), thickness: 1.2, width: 0 },
      marker: { size: 10, color: hexToRgba(c, 0.72),
        line: { width: 2, color: hexToRgba(c, 0.85) } },
      text: rs.map((g) => g.g || g.id),
      hovertemplate: '<b>%{text}</b><br>original %{x:+.3f}'
        + '<br>marginalized %{y:+.3f}<extra></extra>',
    };
  });
  // The identity line: a gym that landed in the same place sits exactly on it,
  // so the eye reads "did the answer change" without doing arithmetic.
  const all = gyms.flatMap((g) => [g.old.mean, g.new.mean]);
  const lo = Math.min(...all) - 0.05, hi = Math.max(...all) + 0.05;
  traces.unshift({
    type: 'scatter', mode: 'lines', x: [lo, hi], y: [lo, hi],
    line: { color: cssVar('--lg-text-2'), width: 1.4, dash: 'dash' },
    name: 'unchanged', hoverinfo: 'skip',
  });

  const layout = chartLayout('');
  layout.height = 440;
  layout.margin = { l: 70, r: 20, t: 12, b: 96 };
  layout.xaxis = { ...layout.xaxis, automargin: false, range: [lo, hi],
    zeroline: true, zerolinecolor: cssVar('--lg-text-2'),
    title: { text: 'gym correction, original model (grades)', standoff: 10 } };
  layout.yaxis = { ...layout.yaxis, automargin: false, range: [lo, hi],
    zeroline: true, zerolinecolor: cssVar('--lg-text-2'),
    scaleanchor: 'x', scaleratio: 1,
    title: { text: 'gym correction, offsets integrated out', standoff: 6 } };
  layout.legend = { ...layout.legend, orientation: 'v', x: 0.02, y: 0.98,
    xanchor: 'left', yanchor: 'top', font: { size: 10 } };
  Plotly.react(host, traces, layout, { displayModeBar: false, responsive: true });

  if (note) {
    note.innerHTML = `Each point is one of the ${st.n_gyms} gyms, on the `
      + `<b>${key}</b> height form. Bars are 89% intervals. Points on the `
      + 'dashed line did not move. Across all gyms the two versions correlate '
      + `<b>${st.corr.toFixed(3)}</b>, the largest single shift is `
      + `<b>${st.max_shift.toFixed(3)} grades</b> (${st.max_shift_in_sd.toFixed(1)}&times; `
      + `that gym's own standard deviation), and the spread from softest to `
      + `stiffest goes ${st.spread_old.toFixed(2)} &rarr; `
      + `${st.spread_new.toFixed(2)} grades. `
      + `${st.rank_changes} of ${st.n_gyms} gyms change position in the ranking.`;
  }

  if (tbl) {
    const rows = Object.entries(f.scalars).filter(([, v]) => v.old && v.new);
    tbl.innerHTML = '<thead><tr><th>what it measures</th>'
      + '<th>original model<br /><span class="muted">mean &plusmn; sd</span></th>'
      + '<th>offsets integrated out<br /><span class="muted">mean &plusmn; sd</span></th>'
      + '<th>how far it moved<br /><span class="muted">&times; the original sd; '
      + 'under 1 = within noise</span></th></tr></thead><tbody>'
      + rows.map(([k, v]) => {
        const z = Math.abs(v.old.mean - v.new.mean) / Math.max(v.old.sd, 1e-9);
        const [name, unit] = V2_PARAM_LABEL[k] || [k, ''];
        return `<tr><td class="label-cell">${name}`
          + (unit ? `<br /><span class="muted" style="font-size:11px">${unit}</span>` : '')
          + `</td>`
          + `<td class="unit">${v.old.mean.toFixed(3)} <span class="muted">&plusmn;${v.old.sd.toFixed(3)}</span></td>`
          + `<td class="unit">${v.new.mean.toFixed(3)} <span class="muted">&plusmn;${v.new.sd.toFixed(3)}</span></td>`
          + `<td class="unit${z > 2 ? '' : ' muted'}">${z.toFixed(1)}</td></tr>`;
      }).join('') + '</tbody>';
  }
}

// ---- scored against a fixed reference, with an error on the difference ----
//
// v2_vs_null.json is written by scripts/build_v2_vs_null.py. Every height form
// is scored against the same "no height term" model rather than against
// whichever form won its column, and every gap carries the error on the
// difference itself -- which is much smaller than the error on either total,
// because the two models find the same rows hard and that part cancels.

let V2_VSNULL = null;
let V2_VSNULL_ARM = 'marginalized';

async function renderV2VsNull() {
  const tbl = document.getElementById('v2-vsnull-table');
  if (!tbl) return;
  if (!V2_VSNULL) {
    try {
      const r = await fetch('/static/v2_vs_null.json', { cache: 'no-cache' });
      if (r.ok) V2_VSNULL = await r.json();
    } catch (e) { /* not built yet */ }
  }
  const note = document.getElementById('v2-vsnull-note');
  const pick = document.getElementById('v2-vsnull-picker');
  const arms = (V2_VSNULL && V2_VSNULL.arms) || {};
  const keys = Object.keys(arms);
  if (!keys.length) {
    tbl.closest('.data-table-wrap').style.display = 'none';
    if (pick) pick.style.display = 'none';
    if (note) note.textContent = 'This table appears once the fits have been scored.';
    return;
  }
  tbl.closest('.data-table-wrap').style.display = '';
  const arm = keys.includes(V2_VSNULL_ARM) ? V2_VSNULL_ARM : keys[0];

  if (pick) {
    pick.style.display = '';
    pick.innerHTML = keys.map((k) => `<button type="button" class="seg-btn`
      + `${k === arm ? ' on' : ''}" data-vsnull="${k}">`
      + `${k === 'marginalized' ? 'Offsets integrated out' : 'Original model'}`
      + '</button>').join('');
    pick.querySelectorAll('[data-vsnull]').forEach((b) => {
      b.onclick = () => { V2_VSNULL_ARM = b.dataset.vsnull; renderV2VsNull(); };
    });
  }

  const subs = V2_VSNULL.subsets;
  // Best first, judged on the >=3 column: it is the one where the identical
  // refits agree, so it is the only column worth sorting on.
  const models = [...arms[arm].models].sort((a, b) =>
    b.by_subset['3'].diff - a.by_subset['3'].diff);

  tbl.innerHTML = '<thead><tr><th>height form</th>'
    + subs.map((k) => `<th>climbers with<br />&ge;${k} send${k > 1 ? 's' : ''}`
      + '</th>').join('')
    + '</tr></thead><tbody>'
    + models.map((m) => {
      const cells = subs.map((k) => {
        const b = m.by_subset[String(k)];
        // Two standard errors is the threshold for calling a gap real; below
        // it the number is greyed rather than hidden, because "we looked and
        // could not tell" is itself a result.
        const real = b.z !== null && Math.abs(b.z) >= 2;
        return `<td class="unit${real ? '' : ' muted'}">`
          + `${b.diff >= 0 ? '+' : ''}${b.diff.toFixed(1)}`
          + ` <span class="muted">&plusmn;${b.se.toFixed(1)}</span></td>`;
      }).join('');
      return `<tr><td class="label-cell">${m.label}</td>${cells}</tr>`;
    }).join('') + '</tbody>';

  if (note) {
    note.innerHTML = 'How much better each height form predicts than a model '
      + `with <b>no height term at all</b>, in elpd &mdash; <b>higher is `
      + 'better, and 0 would mean height adds nothing</b>. The &plusmn; is the '
      + 'error on that difference, not on either model\'s total. Numbers in '
      + 'full contrast clear two standard errors; <span class="muted">greyed '
      + 'ones do not, and mean the comparison could not resolve them</span>. '
      + 'Columns count fewer rows as they narrow, so compare only <i>down</i> '
      + 'a column. The <b>&ge;3 sends</b> column is the trustworthy one: it is '
      + 'where two fits of the identical model agree (+25.4 and +25.0), while '
      + 'in the first column those same two fits differ by 31 points. '
      + 'Built by <code>scripts/build_v2_vs_null.py</code>.';
  }
}

// ---- do the samplers agree? ----
//
// v2_samplers.json is written by scripts/compare_samplers.py: the same shared
// parameters, as each sampler saw them.

async function renderV2Samplers() {
  // v2-crosssampler-*, NOT v2-sampler-* — those belong to renderV2Sampler()
  // (no trailing 's'), the per-fit diagnostics table further up the page. Both
  // sections used to carry the same two ids, so getElementById handed both
  // renderers the diagnostics elements. This function is async and resolved
  // last, so its "not run yet" branch hid the diagnostics table and replaced
  // its caption with the cross-sampler placeholder text.
  const el = document.getElementById('v2-crosssampler-table');
  const note = document.getElementById('v2-crosssampler-note');
  if (!el) return;
  let d = null;
  try {
    const r = await fetch('/static/v2_samplers.json', { cache: 'no-cache' });
    if (r.ok) d = await r.json();
  } catch (e) { /* not run yet */ }
  if (!d || !d.params || !d.params.length) {
    el.closest('.data-table-wrap').style.display = 'none';
    if (note) {
      note.innerHTML = 'The cross-sampler comparison appears once the '
        + 'independent runs have finished. Only the parameters every sampler '
        + 'reports are compared &mdash; the gym corrections are a vector in '
        + 'one and 28 scalars in another, so they are handled separately.';
    }
    return;
  }
  el.closest('.data-table-wrap').style.display = '';
  const S = d.samplers;
  el.innerHTML = '<thead><tr><th>parameter</th>'
    + S.map((s) => `<th>${s}<br /><span class="muted">mean (sd)</span></th>`).join('')
    + '<th>largest gap<br /><span class="muted">as a fraction of sd</span></th>'
    + '</tr></thead><tbody>'
    + d.params.map((p) => {
      const ref = p[S[0]];
      const worst = Math.max(...S.slice(1).map(
        (s) => Math.abs(p[s].mean - ref.mean) / Math.max(ref.sd, 1e-12)));
      return `<tr><td class="label-cell">${p.param}</td>`
        + S.map((s) => `<td class="unit">${p[s].mean.toFixed(3)} `
          + `<span class="muted">(${p[s].sd.toFixed(3)})</span></td>`).join('')
        + `<td class="unit${worst > 0.1 ? '' : ' muted'}">${worst.toFixed(3)}</td></tr>`;
    }).join('')
    + '</tbody>';
  if (note) {
    note.innerHTML = `Posterior mean and standard deviation for every parameter `
      + `all ${S.length} samplers report. The last column is the largest `
      + 'disagreement between them, measured against the width of the '
      + 'posterior itself &mdash; the only scale on which the question has an '
      + `answer. The worst case anywhere is <b>${d.worst_frac_sd.toFixed(3)} `
      + 'standard deviations</b>. With enough draws two samplers always differ '
      + '<i>statistically</i>; what matters is whether they differ by enough '
      + 'to change a conclusion.';
  }
}

// ---- grouped k-fold: predicting a climber the model never saw ----
//
// v2_kfold.json is written by scripts/build_v2_kfold.py, which adds up the
// per-fold row scores produced by `scripts/run_batch.py --batch kfold`. Until
// that batch runs, build_v2_kfold.py writes {models: [], ...} and this section
// hides itself rather than showing an empty table — same contract as
// renderV2Samplers above.

async function renderV2Kfold() {
  const el = document.getElementById('v2-kfold-table');
  const note = document.getElementById('v2-kfold-note');
  if (!el) return;
  let d = null;
  try {
    const r = await fetch('/static/v2_kfold.json', { cache: 'no-cache' });
    if (r.ok) d = await r.json();
  } catch (e) { /* not built yet */ }
  if (!d || !d.models || !d.models.length) {
    el.closest('.data-table-wrap').style.display = 'none';
    if (note) {
      note.innerHTML = 'The grouped k-fold comparison appears once the fold '
        + 'refits have finished &mdash; it costs one full refit per fold per '
        + 'height form, so it is run as a batch '
        + '(<code>scripts/run_batch.py --batch kfold</code>) rather than with '
        + 'the rest of the page.';
    }
    return;
  }
  el.closest('.data-table-wrap').style.display = '';
  const refLabel = (d.models.find((m) => m.name === d.reference) || {}).label
    || 'no height term';
  el.innerHTML = '<thead><tr>'
    + '<th>height form</th>'
    + '<th>held-out score<br /><span class="muted">total elpd, higher is better</span></th>'
    + '<th>per observation<br /><span class="muted">elpd / row, higher is better</span></th>'
    + `<th>gap vs. &ldquo;${refLabel}&rdquo;<br /><span class="muted">elpd, positive = height helps</span></th>`
    + '<th>gap in standard errors<br /><span class="muted">|z| &gt; 2 is a real gap</span></th>'
    + '<th>worst R&#770; across folds<br /><span class="muted">&le; 1.01 wanted</span></th>'
    + '</tr></thead><tbody>'
    + d.models.map((m) => {
      const gap = m.vs_null === undefined
        ? '<span class="muted">reference</span>'
        : `${m.vs_null > 0 ? '+' : ''}${m.vs_null.toFixed(1)} `
          + `<span class="muted">&plusmn;${m.se.toFixed(1)}</span>`;
      const z = (m.z === undefined || m.z === null)
        ? '<span class="muted">&mdash;</span>'
        : `<span class="${Math.abs(m.z) > 2 ? '' : 'muted'}">${m.z.toFixed(1)}</span>`;
      const rhatBad = m.worst_rhat > 1.01;
      return `<tr><td class="label-cell">${m.label}</td>`
        + `<td class="unit">${m.elpd.toFixed(1)}</td>`
        + `<td class="unit">${m.per_row.toFixed(3)}</td>`
        + `<td class="unit">${gap}</td>`
        + `<td class="unit">${z}</td>`
        + `<td class="unit${rhatBad ? '' : ' muted'}">${m.worst_rhat.toFixed(3)}</td></tr>`;
    }).join('')
    + '</tbody>';
  if (note) {
    const nRows = d.n_rows ? d.n_rows.toLocaleString() : 'the';
    note.innerHTML = `Grouped k-fold cross-validation over ${nRows} observations `
      + 'scored by every model, each climber held out exactly once. '
      + 'elpd is the expected log pointwise predictive density &mdash; the '
      + 'model&rsquo;s total log-probability for data it did not see, so higher '
      + 'is better and only differences between rows are meaningful. The gap '
      + 'column differences the two models row by row before summing, which '
      + 'cancels the shared difficulty of the same hard climbers and is why '
      + 'its error bar is far tighter than either total&rsquo;s. Built by '
      + '<code>scripts/build_v2_kfold.py</code>.';
  }
}

async function renderV2Reliability() {
  renderV2Samplers();
  renderV2Kfold();
  renderV2ArmParams();
  renderV2VsNull();
  if (!(await loadV2Reliability())) return;
  renderV2ArmPicker();
  renderV2Noise();
  renderV2SubsetTable();
  renderV2ArmCompare();
}

async function renderV2Time() {
  if (!(await loadV2Time())) return;
  renderV2Advancement();
  renderV2Horizon();
  renderV2Accrual();
  renderV2AdvTable();
  renderV2TimeChart();
  renderV2TimeStats();
}

