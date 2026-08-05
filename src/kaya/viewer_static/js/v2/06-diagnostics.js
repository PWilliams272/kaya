// ---- sampler diagnostics ----

const V2_SAMPLER_GUIDE = [
  ['divergences', 'The sampler simulates a ball rolling across the probability surface. A divergence is the ball flying off the track — the simulation broke down. Any divergence means some region was explored badly and the answer can be biased.', 'zero'],
  ['tree depth', 'To pick its next sample the sampler simulates a trajectory, doubling its length until the path doubles back. Tree depth counts the doublings — depth 10 means 1,023 steps. Hitting the cap means it ran out of budget before the path turned around.', 'below the cap of 10'],
  ['leapfrog steps', 'The total simulation steps per sample. This is essentially the price of each draw, and it is why one fit here takes over an hour.', 'as low as possible'],
  ['step size', 'How far the simulation moves per step. The sampler tunes this automatically. A very small step size means the surface is sharply curved somewhere and it has to inch along.', 'large, but it is set for you'],
  ['acceptance rate', 'The fraction of proposed moves kept. Tuned towards a target you choose — 0.90 here. Hitting the target only means tuning worked. <b>It says nothing about whether the answer is right.</b>', 'close to the target'],
  ['R&#770; (R-hat)', 'Run four chains from different starting points. If they all explored the same distribution, the variation between chains matches the variation within one. R-hat is that ratio; 1.00 is perfect agreement.', '≤ 1.01'],
  ['ESS', 'Consecutive draws are correlated, so 2,000 draws are not worth 2,000 independent ones. Effective sample size is what they are actually worth.', '≥ 400'],
];

function renderV2Sampler() {
  const tbl = document.getElementById('v2-sampler-table');
  const note = document.getElementById('v2-sampler-note');
  const guide = document.getElementById('v2-sampler-guide');
  const fit = v2Fit(v2SelectedFit());

  if (guide) {
    guide.innerHTML = '<thead><tr><th>statistic</th><th>what it actually means</th>'
      + '<th>you want</th></tr></thead><tbody>'
      + V2_SAMPLER_GUIDE.map((r) =>
        `<tr><td class="sym">${r[0]}</td><td>${r[1]}</td><td class="unit">${r[2]}</td></tr>`).join('')
      + '</tbody>';
  }
  if (!tbl || !fit) return;
  const st = fit.sample_stats || {};
  const rows = [];
  if (st.divergences) {
    rows.push(['divergences', String(st.divergences.total ?? 0),
      (st.divergences.total ?? 0) === 0
        ? 'None. The sampler never fell off — the geometry here is hard, not broken.'
        : 'Non-zero — some regions were not explored reliably.']);
  }
  if (st.tree_depth) {
    rows.push(['mean tree depth', st.tree_depth.overall_mean.toFixed(2),
      st.tree_depth.max >= 10
        ? '<b>Pinned at the cap of 10</b> — 1,023 steps for every draw, the full price, every time.'
        : 'Comfortably below the cap.']);
  }
  if (st.n_steps) {
    rows.push(['mean leapfrog steps / draw', st.n_steps.overall_mean.toFixed(0),
      'Directly proportional to run time.']);
  }
  if (st.step_size) {
    rows.push(['mean step size', st.step_size.overall_mean.toFixed(4),
      'Small steps plus deep trees is the signature of a narrow, curved posterior.']);
  }
  if (st.accept) {
    rows.push(['mean acceptance rate', st.accept.overall_mean.toFixed(3),
      'Target was 0.90. Hitting it means tuning worked, nothing more.']);
  }
  tbl.innerHTML = '<thead><tr><th>statistic</th><th>this fit</th><th>reading</th></tr></thead><tbody>'
    + rows.map((r) => `<tr><td class="sym">${r[0]}</td><td class="unit">${r[1]}</td><td>${r[2]}</td></tr>`).join('')
    + '</tbody>';

  if (note) {
    const ps = Object.values(fit.params);
    const bad = ps.filter((p) => p.rhat > 1.01).length;
    note.innerHTML = `Zero divergences with tree depth at the cap is a specific `
      + `diagnosis: there is no pathological funnel for the sampler to fall into, `
      + `but the posterior is stretched and correlated enough that even 1,023 steps `
      + `per draw leaves <b>${bad} of ${ps.length} parameters above the R&#770; 1.01 `
      + `threshold</b> in this fit. More draws would help; a better parameterisation `
      + `would help more.`;
  }
}

// ---- wiring ----

function bindV2Inference() {
  if (bindV2Inference.done || !V2_POST) return;
  bindV2Inference.done = true;

  const fitSel = document.getElementById('v2-fit-pick');
  if (fitSel && !fitSel.options.length) {
    // Two arms of the same model, so the list is grouped rather than flat:
    // "v3_lin" and "v3_lin_marg" are the same height form fitted two ways, and
    // a flat list of fifteen names gives no way to see that.
    const byArm = { unmarginalized: [], marginalized: [] };
    v2FitNames().forEach((f) => byArm[v2Fit(f).arm || 'unmarginalized']?.push(f));
    const groupLabel = {
      unmarginalized: 'Original model — one ability offset per climber',
      marginalized: 'Offsets integrated out',
    };
    Object.entries(byArm).forEach(([arm, names]) => {
      if (!names.length) return;
      const g = document.createElement('optgroup');
      g.label = groupLabel[arm];
      names.forEach((f) => {
        const fit = v2Fit(f);
        const o = document.createElement('option');
        o.value = f;
        o.textContent = `${f} — ${V2_FIT_LABEL[fit.base || f] || fit.height_form}`
          + (fit.max_rhat > 1.01 ? ` (R-hat ${fit.max_rhat.toFixed(2)})` : '');
        g.appendChild(o);
      });
      fitSel.appendChild(g);
    });
    if (v2FitNames().includes('v3_conf')) fitSel.value = 'v3_conf';
  }
  const paramSel = document.getElementById('v2-param-pick');
  const fillParams = () => {
    if (!paramSel) return;
    const keep = paramSel.value;
    paramSel.innerHTML = '';
    Object.keys(v2Fit(v2SelectedFit()).params).forEach((n) => {
      const o = document.createElement('option');
      o.value = n; o.textContent = `${n} — ${V2_PARAM_BLURB[n] || ''}`;
      paramSel.appendChild(o);
    });
    if ([...paramSel.options].some((o) => o.value === keep)) paramSel.value = keep;
  };
  fillParams();

  const groupSel = document.getElementById('v2-corner-group');
  if (groupSel && !groupSel.options.length) {
    Object.entries(V2_CORNER_GROUPS).forEach(([k, g]) => {
      const o = document.createElement('option');
      o.value = k; o.textContent = g.label;
      groupSel.appendChild(o);
    });
    const o = document.createElement('option');
    o.value = 'all'; o.textContent = 'Everything (full width)';
    groupSel.appendChild(o);
  }

  fitSel?.addEventListener('change', () => {
    fillParams();
    renderV2PostGrid();
    renderV2ParamDetail(paramSel.value);
    renderV2Corner();
    renderV2Sampler();
  });
  paramSel?.addEventListener('change', () => renderV2ParamDetail(paramSel.value));
  ['v2-param-wide', 'v2-trace-mode'].forEach((id) => {
    document.getElementById(id)?.addEventListener('change',
      () => renderV2ParamDetail(paramSel.value));
  });
  ['v2-corner-group', 'v2-corner-overlay', 'v2-corner-style'].forEach((id) => {
    document.getElementById(id)?.addEventListener('change', renderV2Corner);
  });
  ['v2-fitted-gender', 'v2-fitted-band'].forEach((id) => {
    document.getElementById(id)?.addEventListener('change', renderV2FittedForms);
  });
}

async function loadV2Posterior() {
  if (V2_POST) return V2_POST;
  try {
    const r = await fetch('/static/v2_posterior.json');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    V2_POST = await r.json();
  } catch (e) {
    const host = document.getElementById('v2-post-grid');
    if (host) {
      host.innerHTML = '<p class="form-noparams">Posterior draws could not be loaded, '
        + 'so this section is empty. Regenerate with scripts/build_v2_posteriors.py. '
        + 'The numbers quoted elsewhere on the page are unaffected.</p>';
    }
    return null;
  }
  return V2_POST;
}

async function renderV2Inference() {
  if (!(await loadV2Posterior())) return;
  bindV2Inference();
  // The glossary quotes the height/ape SDs, which only arrive with the fits.
  renderV2Symbols();
  if (typeof window.renderMathInElement === 'function') {
    const el = document.getElementById('v2-symbols');
    if (el) window.renderMathInElement(el, { delimiters: [{ left: '\\(', right: '\\)', display: false }] });
  }
  renderV2PostGrid();
  renderV2Sampler();
  renderV2FittedForms();
  renderV2Corner();
  const sel = document.getElementById('v2-param-pick');
  renderV2ParamDetail(sel?.value || Object.keys(v2Fit(v2SelectedFit()).params)[0]);
}


// ---- glossary panel: open/close + equation-to-symbol highlighting ----

const V2_GLOSS_KEY = 'kaya.v2.glossary.open';

function setV2GlossaryOpen(open, persist = true) {
  const panel = document.getElementById('v2-glossary');
  const btn = document.getElementById('v2-gloss-toggle');
  if (!panel) return;
  panel.dataset.open = open ? 'true' : 'false';
  // Reserve the gutter so the centred article re-centres beside the panel
  // instead of running underneath it.
  document.getElementById('tab-grading-v2')?.classList.toggle('gloss-open', open);
  // The grid's usable width just changed by the width of the panel gutter.
  setTimeout(sizeV2FormGrid, 240);
  if (btn) btn.setAttribute('aria-expanded', open ? 'true' : 'false');
  if (persist) {
    try { localStorage.setItem(V2_GLOSS_KEY, open ? '1' : '0'); } catch (e) { /* private mode */ }
  }
}

// Dim every row except the ones this equation actually uses, and bring the
// first match into view if the panel has scrolled past it.
function highlightV2Symbols(keys) {
  const panel = document.getElementById('v2-glossary');
  if (!panel) return;
  const rows = panel.querySelectorAll('[data-sym]');
  if (!keys) {
    panel.classList.remove('is-filtered');
    rows.forEach((r) => r.classList.remove('sym-hit'));
    return;
  }
  const want = new Set(keys);
  panel.classList.add('is-filtered');
  let first = null;
  rows.forEach((r) => {
    const hit = want.has(r.dataset.sym);
    r.classList.toggle('sym-hit', hit);
    if (hit && !first && r.classList.contains('sym-row')) first = r;
  });
  const scroller = panel.querySelector('.glossary-scroll');
  if (first && scroller) {
    const fr = first.getBoundingClientRect();
    const sr = scroller.getBoundingClientRect();
    if (fr.top < sr.top || fr.bottom > sr.bottom) {
      const reduce = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
      scroller.scrollTo({
        top: scroller.scrollTop + (fr.top - sr.top) - 12,
        behavior: reduce ? 'auto' : 'smooth',
      });
    }
  }
}

let v2GlossaryBound = false;

function bindV2Glossary() {
  if (v2GlossaryBound) return;
  const panel = document.getElementById('v2-glossary');
  if (!panel) return;
  v2GlossaryBound = true;

  const toggle = document.getElementById('v2-gloss-toggle');
  if (toggle) {
    toggle.addEventListener('click', () => {
      setV2GlossaryOpen(panel.dataset.open !== 'true');
    });
  }
  const opener = document.getElementById('v2-gloss-open');
  if (opener) {
    opener.addEventListener('click', () => {
      setV2GlossaryOpen(true);
      panel.querySelector('.glossary-scroll')?.scrollTo({ top: 0 });
    });
  }

  // Hovering an equation filters the panel. Opening it on hover would be
  // jarring, so a shut panel just pulses the handle instead.
  document.querySelectorAll('#tab-grading-v2 .eqn').forEach((eq) => {
    const keys = (eq.dataset.syms || '').split(/\s+/).filter(Boolean);
    const on = () => { if (panel.dataset.open === 'true') highlightV2Symbols(keys); };
    const off = () => highlightV2Symbols(null);
    eq.addEventListener('mouseenter', on);
    eq.addEventListener('mouseleave', off);
    eq.addEventListener('focus', on);
    eq.addEventListener('blur', off);

    // Clicking the equation is the way in when the panel is shut: it opens the
    // panel and lands on this equation's symbols. highlightV2Symbols scrolls
    // the first hit into view, but only once the panel has actually widened.
    // Click is a toggle: open the panel onto this equation's symbols, or shut
    // it again if it is already open.
    const openTo = () => {
      if (panel.dataset.open === 'true') {
        highlightV2Symbols(null);
        setV2GlossaryOpen(false);
        return;
      }
      setV2GlossaryOpen(true);
      setTimeout(() => highlightV2Symbols(keys), 220);
    };
    eq.setAttribute('role', 'button');
    eq.setAttribute('title', 'Show these symbols in the reference panel (click again to close it)');
    eq.addEventListener('click', openTo);
    eq.addEventListener('keydown', (ev) => {
      if (ev.key === 'Enter' || ev.key === ' ') { ev.preventDefault(); openTo(); }
    });
  });

  // Reverse direction: hovering a definition marks the equations that use it.
  panel.addEventListener('mouseover', (ev) => {
    const row = ev.target.closest('[data-sym]');
    if (!row) return;
    document.querySelectorAll('#tab-grading-v2 .eqn').forEach((eq) => {
      const keys = (eq.dataset.syms || '').split(/\s+/);
      eq.classList.toggle('eqn-active', keys.includes(row.dataset.sym));
    });
  });
  panel.addEventListener('mouseleave', () => {
    document.querySelectorAll('#tab-grading-v2 .eqn.eqn-active')
      .forEach((eq) => eq.classList.remove('eqn-active'));
  });

  document.addEventListener('keydown', (ev) => {
    if (ev.key === 'Escape' && panel.dataset.open === 'true'
        && document.getElementById('tab-grading-v2')?.classList.contains('active')) {
      setV2GlossaryOpen(false);
    }
  });

  // Default open where there is a gutter to open into; remember the choice.
  let stored = null;
  try { stored = localStorage.getItem(V2_GLOSS_KEY); } catch (e) { /* private mode */ }
  const wide = window.matchMedia('(min-width: 1180px)').matches;
  setV2GlossaryOpen(stored === null ? wide : stored === '1', false);

  // Only now, once the resting position is set, allow the slide to animate --
  // see the .is-animated note in the CSS. rAF is the clean signal but never
  // fires while the tab is in the background, so a timer backs it up;
  // whichever lands first wins and the other is a no-op.
  const pane = document.getElementById('tab-grading-v2');
  const enableAnim = () => {
    if (panel.classList.contains('is-animated')) return;
    panel.getAnimations().forEach((a) => a.cancel());
    pane?.getAnimations().forEach((a) => a.cancel());
    panel.classList.add('is-animated');
    pane?.classList.add('gloss-anim');
  };
  requestAnimationFrame(() => requestAnimationFrame(enableAnim));
  setTimeout(enableAnim, 120);
}

function renderV2FormsTable() {
  const el = document.getElementById('v2-forms-table');
  if (!el) return;
  const [h, ...body] = V2_FORMS;
  el.innerHTML = `<thead><tr>${h.map((x) => `<th>${x}</th>`).join('')}</tr></thead><tbody>`
    + body.map((r) => `<tr><td><b>${r[0]}</b></td><td class="sym">${r[1]}</td><td>${r[2]}</td><td class="unit">${r[3]}</td></tr>`).join('')
    + '</tbody>';
}

// Shared x-grid: inches across the plausible climbing range, plus the same
// values in SD units, which is what every curve function takes.
function v2HeightGrid() {
  const inches = [];
  for (let h = 58; h <= 78; h += 0.25) inches.push(h);
  return { inches, z: inches.map((h) => (h - v2HMed()) / v2HSd()) };
}

// Bands showing where each group actually sits. Always drawn -- the shapes are
// misleading without them, since most of the x-range holds almost nobody.
function v2BandShapes(showLabels) {
  const shapes = [], annotations = [];
  V2_HEIGHT_BANDS.forEach((b) => {
    shapes.push({ type: 'rect', xref: 'x', yref: 'paper',
      x0: b.mid - b.sd, x1: b.mid + b.sd, y0: 0, y1: 1,
      fillcolor: cssVar(b.tok), opacity: 0.07, line: { width: 0 } });
    if (showLabels) {
      // The two bands overlap (65.9-67.0 in), so centring both labels would
      // collide on a ~330px card. Anchor each to its band's *outer* edge and
      // they grow away from each other instead.
      annotations.push({
        x: b.anchor === 'left' ? b.mid - b.sd : b.mid + b.sd,
        xanchor: b.anchor === 'left' ? 'left' : 'right',
        y: 1, yref: 'paper', yanchor: 'bottom', showarrow: false,
        text: b.label, font: { size: 10, color: cssVar(b.tok) },
      });
    }
  });
  return { shapes, annotations };
}

