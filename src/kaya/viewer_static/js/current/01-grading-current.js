// ==========================================================================
// Grading Model tab (current)
//
// The presented page. It renders the same figures as the archived v2 notes and
// reads exactly the same payloads -- there is no second source of numbers, and
// nothing here is hand-typed. What differs is what it shows and in what order.
//
// Element ids are namespaced `gm-` because the archived v2 page still owns the
// `v2-` ones and ids must be unique per document. The v2 renderers resolve
// their ids through `v2El`/`v2Id`, so this file mounts them by switching the
// namespace once, in `renderCurrentTab`, rather than duplicating any of them.
// Only the tables whose *copy* differs from the archive are re-implemented
// below; every figure is the shared renderer.
//
// `data-v2` slots are NOT namespaced on purpose: renderV2InlineFigures uses a
// document-wide selector, so the shared inline figures fill on both pages with
// no extra wiring.
// ==========================================================================

// ---- inline figures that only this page needs ----
//
// Same contract as renderV2InlineFigures: a `data-gm` attribute names a
// formatter, and the formatter reads a payload. Prose that restates a figure
// has to read it, or it silently goes wrong the next time the model is refitted.

function gmAdvRow(v) {
  const rows = (V2_TIME && V2_TIME.advancement && V2_TIME.advancement.debiased) || [];
  return rows.find((r) => Number(r.v) === v) || null;
}

function gmNaiveRow(v) {
  const rows = (V2_TIME && V2_TIME.advancement && V2_TIME.advancement.naive) || [];
  return rows.find((r) => Number(r.v) === v) || null;
}

function renderGmInlineFigures() {
  const signed = (x, dp = 2) => `${x < 0 ? '−' : '+'}${Math.abs(x).toFixed(dp)}`;
  const adv = (v) => {
    const r = gmAdvRow(v);
    return r ? signed(r.mean) : '—';
  };
  const gt = (V2_TIME && V2_TIME.gym_time) || {};
  const fmt = {
    adv_v1: () => adv(1),
    adv_v5: () => adv(5),
    adv_v9: () => adv(9),
    adv_v11: () => adv(11),
    adv_v11_sem: () => {
      const r = gmAdvRow(11);
      return r ? r.sem.toFixed(2) : '—';
    },
    // Quoted as a magnitude in the prose ("losing N grades a year"), so the
    // sign is carried by the sentence rather than the number.
    naive_v11: () => {
      const r = gmNaiveRow(11);
      return r ? Math.abs(r.mean).toFixed(1) : '—';
    },
    gt_r: () => (gt.raw ? gt.raw.r.toFixed(2) : '—'),
    gt_slope: () => (gt.raw ? gt.raw.slope.toFixed(2) : '—'),
    gt_r_wb: () => (gt.within_brand ? gt.within_brand.r.toFixed(2) : '—'),
  };
  document.querySelectorAll('[data-gm]').forEach((el) => {
    const f = fmt[el.dataset.gm];
    if (f) el.textContent = f();
  });
}

// ---- headline stat tiles ----

function renderGmStats() {
  const host = document.getElementById('gm-stats');
  if (!host || typeof v2Stats !== 'function') return;
  host.innerHTML = v2Stats().map((s) => `
    <div class="stat-tile">
      <div class="stat-value">${s.v}</div>
      <div class="stat-label">${s.l}</div>
      <div class="stat-sub">${s.s}</div>
    </div>`).join('');
}

// ---- height: the model comparison, with its convergence gate visible ----

function renderGmLooTable() {
  const el = document.getElementById('gm-loo-table');
  const note = document.getElementById('gm-loo-note');
  if (!el || !V2_RESULTS) return;
  const forms = V2_RESULTS.forms || [];
  if (!forms.length) { el.innerHTML = ''; return; }

  const hasDse = forms.some((f) => f.dse !== undefined);
  el.innerHTML = '<thead><tr><th>height form</th>'
    + '<th>free parameters<br /><span class="muted">fewer is simpler</span></th>'
    + '<th>held-out score<br /><span class="muted">LOO elpd, higher is better</span></th>'
    + '<th>&Delta; vs best<br /><span class="muted">elpd below the top row, 0 is best</span></th>'
    + (hasDse ? '<th>SE of &Delta;<br /><span class="muted">standard error of that gap</span></th>' : '')
    + '<th>max R&#770;<br /><span class="muted">chain agreement, &le; 1.01 wanted</span></th>'
    + '<th>min ESS<br /><span class="muted">effective sample size, &ge; 400 wanted</span></th>'
    + '</tr></thead><tbody>'
    + forms.map((f, i) => {
      const converged = f.rhat <= 1.01;
      // A gap inside one SE of the difference is not a ranking, it is noise.
      const real = f.dse !== undefined && Math.abs(f.d_elpd) > 2 * f.dse;
      // `usable` is written by build_v2_results.py from the shared convergence
      // gate. Older payloads predate it, so absence is not treated as failure.
      const gated = f.usable === false;
      return `<tr${i === 0 ? ' class="row-best"' : ''}>`
        + `<td><b>${f.label}</b>${i === 0 ? ' <span class="pill-best">best</span>' : ''}`
        + (gated ? ' <span class="verdict-pill verdict-null">not converged</span>' : '')
        + '</td>'
        + `<td class="unit">${f.k ?? '&mdash;'}</td>`
        + `<td class="unit">${f.elpd.toFixed(1)}</td>`
        + `<td class="unit${i && !real ? ' muted' : ''}">${f.d_elpd === 0 ? '&mdash;' : f.d_elpd.toFixed(1)}</td>`
        + (hasDse ? `<td class="unit">${i === 0 ? '&mdash;' : `&plusmn;${f.dse?.toFixed(1) ?? '?'}`}</td>` : '')
        + `<td class="unit ${converged ? '' : 'bad'}">${f.rhat.toFixed(2)}</td>`
        + `<td class="unit ${f.ess >= 400 ? '' : 'bad'}">${f.ess}</td></tr>`;
    }).join('') + '</tbody>';

  if (note) {
    const sep = forms.filter((f, i) => i && f.dse !== undefined && Math.abs(f.d_elpd) > 2 * f.dse);
    note.innerHTML = `<b>${forms.length} of 6 height forms fitted.</b> `
      + 'The <b>SE of &Delta;</b> column is the honest one: it is the standard error '
      + 'of the <i>difference</i>, from paired pointwise LOO, which accounts for every '
      + 'model being scored on the same observations. '
      + (sep.length
        ? `Only ${sep.map((f) => f.label.toLowerCase()).join(', ')} `
          + `${sep.length === 1 ? 'is' : 'are'} separated from the leader by more than `
          + 'two of those standard errors; every other gap here is inside the noise, '
          + 'so the ranking is an ordering, not a result. '
        : '<b>No gap in this table exceeds two of those standard errors.</b> The '
          + 'ranking is an ordering, not a result: on this data leave-one-out '
          + 'cross-validation cannot tell these height forms apart. ')
      + 'Built by <code>scripts/build_v2_results.py</code>.';
  }
}

// ---- height: scored against a fixed no-height reference ----

async function renderGmVsNull() {
  const el = document.getElementById('gm-vsnull-table');
  const note = document.getElementById('gm-vsnull-note');
  const picker = document.getElementById('gm-vsnull-picker');
  if (!el) return;
  if (!V2_VSNULL) {
    try {
      const r = await fetch('/static/v2_vs_null.json', { cache: 'no-cache' });
      if (r.ok) V2_VSNULL = await r.json();
    } catch (e) { /* not built yet */ }
  }
  const arms = (V2_VSNULL && V2_VSNULL.arms) || {};
  const keys = Object.keys(arms);
  if (!keys.length) {
    el.closest('.data-table-wrap').style.display = 'none';
    if (picker) picker.style.display = 'none';
    if (note) note.textContent = 'This table appears once the fits have been scored.';
    return;
  }
  el.closest('.data-table-wrap').style.display = '';
  const armKey = keys.includes(V2_VSNULL_ARM) ? V2_VSNULL_ARM : keys[0];
  const arm = arms[armKey];
  const subsets = V2_VSNULL.subsets || [];

  if (picker) {
    picker.style.display = '';
    picker.innerHTML = keys.map((k) => '<button type="button" class="seg-btn'
      + `${k === armKey ? ' on' : ''}" data-gmvsnull="${k}">`
      + `${k === 'marginalized' ? 'Offsets integrated out' : 'Original model'}</button>`).join('');
    if (!picker.dataset.bound) {
      picker.dataset.bound = '1';
      picker.addEventListener('click', (e) => {
        const btn = e.target.closest('[data-gmvsnull]');
        if (!btn) return;
        V2_VSNULL_ARM = btn.dataset.gmvsnull;
        renderGmVsNull();
      });
    }
  }

  el.innerHTML = '<thead><tr><th>height form</th>'
    + subsets.map((k) => `<th>climbers with &ge;${k} send${k > 1 ? 's' : ''}<br />`
      + `<span class="muted">elpd vs &ldquo;${V2_VSNULL.reference}&rdquo;, higher is better</span></th>`).join('')
    + '</tr></thead><tbody>'
    + (arm.models || []).map((m) => {
      const cells = subsets.map((k) => {
        const b = m.by_subset && m.by_subset[String(k)];
        if (!b) return '<td class="unit muted">&mdash;</td>';
        // A gap inside two standard errors of itself is not a result.
        const real = b.se ? Math.abs(b.diff) > 2 * b.se : false;
        return `<td class="unit${real ? '' : ' muted'}">`
          + `${b.diff > 0 ? '+' : ''}${b.diff.toFixed(1)} `
          + `<span class="muted">&plusmn;${(b.se ?? 0).toFixed(1)}</span></td>`;
      }).join('');
      return `<tr><td class="label-cell">${m.label}</td>${cells}</tr>`;
    }).join('') + '</tbody>';

  if (note) {
    note.innerHTML = 'How much better each height form predicts than a model with '
      + `<b>${V2_VSNULL.reference}</b>, in elpd &mdash; <b>higher is better, and 0 `
      + 'would mean height adds nothing</b>. The &plusmn; is the error on that '
      + 'difference, not on either model&rsquo;s total. Numbers in full contrast clear '
      + 'two standard errors; <span class="muted">greyed ones do not, and mean the '
      + 'comparison could not resolve them</span>. Columns count fewer rows as they '
      + 'narrow, so compare only <i>down</i> a column. Built by '
      + '<code>scripts/build_v2_vs_null.py</code>.';
  }
}

// ---- diagnostics ----

function renderGmSampler() {
  const guide = document.getElementById('gm-sampler-guide');
  const tbl = document.getElementById('gm-sampler-table');
  const note = document.getElementById('gm-sampler-note');
  if (guide && typeof V2_SAMPLER_GUIDE !== 'undefined') {
    guide.innerHTML = '<thead><tr><th>statistic</th><th>what it actually means</th>'
      + '<th>you want</th></tr></thead><tbody>'
      + V2_SAMPLER_GUIDE.map((r) =>
        `<tr><td class="sym">${r[0]}</td><td>${r[1]}</td><td class="unit">${r[2]}</td></tr>`).join('')
      + '</tbody>';
  }
  if (!tbl) return;
  const fit = (typeof v2Fit === 'function' && typeof v2SelectedFit === 'function')
    ? v2Fit(v2SelectedFit()) : null;
  if (!fit) {
    tbl.closest('.data-table-wrap').style.display = 'none';
    if (note) {
      note.innerHTML = 'Sampler statistics appear once <code>v2_posterior.json</code> '
        + 'has loaded. Regenerate with <code>scripts/build_v2_posteriors.py</code>.';
    }
    return;
  }
  tbl.closest('.data-table-wrap').style.display = '';
  const st = fit.sample_stats || {};
  const rows = [];
  if (st.divergences) {
    rows.push(['divergences', String(st.divergences.total ?? 0),
      (st.divergences.total ?? 0) === 0
        ? 'None. The sampler never fell off &mdash; the geometry here is hard, not broken.'
        : 'Non-zero &mdash; some regions were not explored reliably.']);
  }
  if (st.tree_depth) {
    rows.push(['mean tree depth', st.tree_depth.overall_mean.toFixed(2),
      'At the cap means every draw cost the maximum number of steps.']);
  }
  if (st.step_size) {
    rows.push(['step size', st.step_size.overall_mean.toFixed(4),
      'Small means the surface is sharply curved somewhere and it has to inch along.']);
  }
  if (st.accept) {
    rows.push(['acceptance rate', st.accept.overall_mean.toFixed(3),
      'Hitting the tuning target only means tuning worked, not that the answer is right.']);
  }
  rows.push(['max R&#770; (R-hat)', (fit.max_rhat ?? 0).toFixed(3),
    (fit.max_rhat ?? 9) <= 1.01
      ? 'Chains agree. 1.00 is perfect agreement.'
      : '<b>Above 1.01 &mdash; the chains did not explore the same distribution.</b>']);

  tbl.innerHTML = '<thead><tr><th>statistic</th><th>this fit</th><th>reading</th></tr></thead>'
    + '<tbody>' + rows.map((r) =>
      `<tr><td class="label-cell">${r[0]}</td><td class="unit">${r[1]}</td><td>${r[2]}</td></tr>`).join('')
    + '</tbody>';

  if (note) {
    const conv = fit.convergence;
    note.innerHTML = `Fit <code>${v2SelectedFit()}</code>, from `
      + '<code>v2_posterior.json</code> (built by '
      + '<code>scripts/build_v2_posteriors.py</code>).'
      + (conv && conv.converged === false
        ? ` <b>This fit did not clear the convergence gate:</b> ${(conv.reasons || []).join('; ')}.`
        : '');
  }
}

// ---- orchestrator ----

// Everything below runs with the shared renderers pointed at this pane's ids.
// `withV2Ns` serialises whole renders, so an await in here can never let the
// archived tab's render flip the namespace mid-flight.
function renderCurrentTab() {
  return renderV2TabOnce('gm-', 'tab-grading-current', renderCurrentTabInner);
}

async function renderCurrentTabInner() {
  const ok = await loadV2Results();
  if (!ok) {
    const host = v2El('stats');
    if (host) {
      host.innerHTML = '<p class="form-noparams">Fitted results could not be loaded '
        + '(/static/v2_results.json). Regenerate with '
        + '<code>scripts/build_v2_results.py</code>.</p>';
    }
  }
  bindInfoDots();

  renderGmStats();
  renderV2InlineFigures();
  renderV2Symbols();
  renderV2GymChart();
  renderV2BrandChart();
  renderGmLooTable();

  // The model section: the six height forms, one interactive card each, and
  // the gap explorer that makes the ExGaussian likelihood legible.
  renderV2FormsTable();
  renderV2FormCards();
  bindV2GapExplorer();
  renderV2GapExplorer();

  await loadV2Time();
  renderGmInlineFigures();
  renderV2AdvTable();
  renderV2Advancement();
  renderV2Horizon();
  renderV2Accrual();
  renderV2TimeChart();
  renderV2TimeStats();

  await renderGmVsNull();

  // How the held-out score is computed and where it fails, then the version of
  // the model where that failure stops existing. Both read v2_psis.json.
  await renderV2Psis();
  await renderV2PsisArms();

  // Priors and posteriors, chain mixing, the across-fit overlays, the fitted
  // height/ape curves and the corner plots. All of it reads
  // v2_posterior.json, so it renders once that file is in.
  await renderV2Inference();
  renderGmSampler();

  bindV2Glossary();

  // The symbol table and the innerHTML-injected cards arrive after KaTeX's
  // auto-render already ran on DOMContentLoaded, so typeset them explicitly.
  if (typeof window.renderMathInElement === 'function') {
    ['symbols', 'forms-table', 'form-cards', 'gap-controls', 'visits-controls',
     'loo-note', 'vsnull-note'].forEach((id) => {
      const el = v2El(id);
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
}
