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
  const tbl = v2El('sampler-table');
  const note = v2El('sampler-note');
  const guide = v2El('sampler-guide');
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
  // Per-namespace: each pane owns its own controls and binds them once.
  bindV2Inference.done = bindV2Inference.done || new Set();
  if (bindV2Inference.done.has(V2_NS) || !V2_POST) return;
  bindV2Inference.done.add(V2_NS);

  const fitSel = v2El('fit-pick');
  if (fitSel && !fitSel.options.length) {
    // Two arms of the same model, so the list is grouped rather than flat:
    // "v3_lin" and "v3_lin_marg" are the same height form fitted two ways, and
    // a flat list of fifteen names gives no way to see that.
    const byArm = { unmarginalized: [], marginalized: [] };
    v2FitNames().forEach((f) => byArm[v2Fit(f).arm || 'unmarginalized']?.push(f));
    const groupLabel = {
      unmarginalized: 'Original model — one ability offset per climber',
      marginalized: 'Climber offsets integrated out',
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
  const paramSel = v2El('param-pick');
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

  const groupSel = v2El('corner-group');
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

  fitSel?.addEventListener('change', v2Bound(() => {
    fillParams();
    renderV2PostGrid();
    renderV2ParamDetail(paramSel.value);
    renderV2Corner();
    renderV2Sampler();
  }));
  paramSel?.addEventListener('change', v2Bound(() => renderV2ParamDetail(paramSel.value)));
  ['param-wide', 'trace-mode'].forEach((id) => {
    v2El(id)?.addEventListener('change', v2Bound(() => renderV2ParamDetail(paramSel.value)));
  });
  ['corner-group', 'corner-overlay', 'corner-style'].forEach((id) => {
    v2El(id)?.addEventListener('change', v2Bound(renderV2Corner));
  });

  // One control, every multi-model figure. Splitting it per figure was the
  // obvious alternative and is worse: the reader would narrow the overlay,
  // scroll down, and find the curves still showing all twenty-two.
  const scopeSel = v2El('fit-scope');
  const extrasBox = v2El('fit-extras');
  if (scopeSel) scopeSel.value = v2Scope().arm;
  if (extrasBox) extrasBox.checked = v2Scope().extras;
  const applyScope = v2Bound(() => {
    const sc = v2Scope();
    if (scopeSel) sc.arm = scopeSel.value;
    if (extrasBox) sc.extras = extrasBox.checked;
    // The pinned axis range was computed over the previous selection, so it has
    // to go or the curves keep the old plot's limits.
    v2FittedRange = null;
    renderV2AcrossFits(v2El('param-pick')?.value);
    renderV2FittedForms();
    renderV2Corner();
  });
  scopeSel?.addEventListener('change', applyScope);
  extrasBox?.addEventListener('change', applyScope);

  v2El('rhat-n')?.addEventListener('change', v2Bound(renderV2RhatScale));
  ['fitted-gender', 'fitted-band'].forEach((id) => {
    v2El(id)?.addEventListener('change', v2Bound(renderV2FittedForms));
  });
}

async function loadV2Posterior() {
  if (V2_POST) return V2_POST;
  try {
    const r = await fetch('/static/v2_posterior.json');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    V2_POST = await r.json();
  } catch (e) {
    const host = v2El('post-grid');
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
    const el = v2El('symbols');
    if (el) window.renderMathInElement(el, { delimiters: [{ left: '\\(', right: '\\)', display: false }] });
  }
  renderV2PostGrid();
  renderV2Sampler();
  renderV2FittedForms();
  renderV2Corner();
  await renderV2RhatScale();
  await renderV2RhatParams();
  await renderV2RhatArms();
  const sel = v2El('param-pick');
  renderV2ParamDetail(sel?.value || Object.keys(v2Fit(v2SelectedFit()).params)[0]);
}


// ---- glossary panel: open/close + equation-to-symbol highlighting ----

const V2_GLOSS_KEY = 'kaya.v2.glossary.open';

function setV2GlossaryOpen(open, persist = true) {
  const panel = v2El('glossary');
  const btn = v2El('gloss-toggle');
  if (!panel) return;
  panel.dataset.open = open ? 'true' : 'false';
  // Reserve the gutter so the centred article re-centres beside the panel
  // instead of running underneath it.
  v2Pane()?.classList.toggle('gloss-open', open);
  // The grid's usable width just changed by the width of the panel gutter.
  v2SizeSoon();
  if (btn) btn.setAttribute('aria-expanded', open ? 'true' : 'false');
  if (persist) {
    try { localStorage.setItem(V2_GLOSS_KEY, open ? '1' : '0'); } catch (e) { /* private mode */ }
  }
}

// Dim every row except the ones this equation actually uses, and bring the
// first match into view if the panel has scrolled past it.
function highlightV2Symbols(keys) {
  const panel = v2El('glossary');
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

const v2GlossaryBound = new Set();

function bindV2Glossary() {
  if (v2GlossaryBound.has(V2_NS)) return;
  const panel = v2El('glossary');
  if (!panel) return;
  v2GlossaryBound.add(V2_NS);

  const toggle = v2El('gloss-toggle');
  if (toggle) {
    toggle.addEventListener('click', v2Bound(() => {
      setV2GlossaryOpen(panel.dataset.open !== 'true');
    }));
  }
  const opener = v2El('gloss-open');
  if (opener) {
    opener.addEventListener('click', v2Bound(() => {
      setV2GlossaryOpen(true);
      panel.querySelector('.glossary-scroll')?.scrollTo({ top: 0 });
    }));
  }

  // Hovering an equation filters the panel. Opening it on hover would be
  // jarring, so a shut panel just pulses the handle instead.
  document.querySelectorAll(v2Sel('.eqn')).forEach((eq) => {
    const keys = (eq.dataset.syms || '').split(/\s+/).filter(Boolean);
    const on = v2Bound(() => { if (panel.dataset.open === 'true') highlightV2Symbols(keys); });
    const off = v2Bound(() => highlightV2Symbols(null));
    eq.addEventListener('mouseenter', on);
    eq.addEventListener('mouseleave', off);
    eq.addEventListener('focus', on);
    eq.addEventListener('blur', off);

    // Clicking the equation is the way in when the panel is shut: it opens the
    // panel and lands on this equation's symbols. highlightV2Symbols scrolls
    // the first hit into view, but only once the panel has actually widened.
    // Click is a toggle: open the panel onto this equation's symbols, or shut
    // it again if it is already open.
    const openTo = v2Bound(() => {
      if (panel.dataset.open === 'true') {
        highlightV2Symbols(null);
        setV2GlossaryOpen(false);
        return;
      }
      setV2GlossaryOpen(true);
      const show = v2Bound(() => highlightV2Symbols(keys));
      setTimeout(show, 220);
    });
    eq.setAttribute('role', 'button');
    eq.setAttribute('title', 'Show these symbols in the reference panel (click again to close it)');
    eq.addEventListener('click', openTo);
    eq.addEventListener('keydown', (ev) => {
      if (ev.key === 'Enter' || ev.key === ' ') { ev.preventDefault(); openTo(); }
    });
  });

  // Reverse direction: hovering a definition marks the equations that use it.
  panel.addEventListener('mouseover', v2Bound((ev) => {
    const row = ev.target.closest('[data-sym]');
    if (!row) return;
    document.querySelectorAll(v2Sel('.eqn')).forEach((eq) => {
      const keys = (eq.dataset.syms || '').split(/\s+/);
      eq.classList.toggle('eqn-active', keys.includes(row.dataset.sym));
    });
  }));
  panel.addEventListener('mouseleave', v2Bound(() => {
    document.querySelectorAll(v2Sel('.eqn.eqn-active'))
      .forEach((eq) => eq.classList.remove('eqn-active'));
  }));

  document.addEventListener('keydown', v2Bound((ev) => {
    if (ev.key === 'Escape' && panel.dataset.open === 'true'
        && v2Pane()?.classList.contains('active')) {
      setV2GlossaryOpen(false);
    }
  }));

  // Default open where there is a gutter to open into; remember the choice.
  let stored = null;
  try { stored = localStorage.getItem(V2_GLOSS_KEY); } catch (e) { /* private mode */ }
  const wide = window.matchMedia('(min-width: 1180px)').matches;
  setV2GlossaryOpen(stored === null ? wide : stored === '1', false);

  // Only now, once the resting position is set, allow the slide to animate --
  // see the .is-animated note in the CSS. rAF is the clean signal but never
  // fires while the tab is in the background, so a timer backs it up;
  // whichever lands first wins and the other is a no-op.
  const pane = v2Pane();
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
  const el = v2El('forms-table');
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



// ---- what R-hat actually costs you ----
//
// v2_rhat.json is written by scripts/build_v2_rhat.py straight from the
// traces. Nothing here is hand-typed: the point of the section is that the
// chains can settle these questions themselves.

let V2_RHAT = null;

async function loadV2Rhat() {
  if (V2_RHAT) return true;
  try {
    const r = await fetch('/static/v2_rhat.json', { cache: 'no-cache' });
    if (!r.ok) return false;
    V2_RHAT = await r.json();
    return true;
  } catch (e) {
    return false;
  }
}

// B/W = n(R^2 - 1) + 1, the identity the section is built on. Reading it in
// this direction is the whole trick: pick an R-hat, get the autocorrelation.
const v2BwFromRhat = (r, n) => n * (r * r - 1) + 1;

async function renderV2RhatScale() {
  const el = v2El('rhat-scale');
  if (!el || !(await loadV2Rhat())) return;
  const sel = v2El('rhat-n');
  const n = Number(sel?.value) || V2_RHAT.n_draws;
  const rows = [1.0, 1.005, 1.01, 1.02, 1.05, 1.1];
  el.innerHTML = '<thead><tr><th>R&#770;</th>'
    + '<th>B/W<br /><span class="muted">autocorrelation factor</span></th>'
    + '<th>effective draws per chain<br /><span class="muted">out of '
    + `${n.toLocaleString()}, higher is better</span></th>`
    + '<th>what that means</th></tr></thead><tbody>'
    + rows.map((r) => {
      const bw = Math.max(1, v2BwFromRhat(r, n));
      const eff = n / bw;
      // The gate is where the page draws its own line, so it is the row that
      // gets emphasised rather than an arbitrary "bad" threshold.
      const here = Math.abs(r - V2_RHAT.gate) < 1e-9;
      const verdict = bw < 2 ? 'draws are essentially independent'
        : bw < 12 ? 'usable, but a fraction of the draws are real'
          : bw < 30 ? 'most of the run is redundant'
            : 'the chains have barely explored';
      return `<tr${here ? ' class="row-best"' : ''}>`
        + `<td class="unit"><b>${r.toFixed(3)}</b>${here ? ' <span class="pill-best">the line used here</span>' : ''}</td>`
        + `<td class="unit">${bw < 10 ? bw.toFixed(1) : Math.round(bw).toLocaleString()}&times;</td>`
        + `<td class="unit">${eff > 999 ? Math.round(eff).toLocaleString() : Math.round(eff)}</td>`
        + `<td class="muted">${verdict}</td></tr>`;
    }).join('') + '</tbody>';

  const note = v2El('rhat-scale-note');
  if (note) {
    const bwGate = v2BwFromRhat(V2_RHAT.gate, n);
    note.innerHTML = `Read off the identity above, at <b>${n.toLocaleString()} draws `
      + `per chain</b>. At this length R&#770; = ${V2_RHAT.gate} means the sampler is `
      + `<b>${Math.round(bwGate)}&times;</b> autocorrelated &mdash; about `
      + `<b>${Math.round(n / bwGate)}</b> genuinely independent draws per chain out of `
      + `${n.toLocaleString()}. Change the length and watch the same R&#770; buy a `
      + 'completely different amount of information.';
  }
}

async function renderV2RhatParams() {
  const el = v2El('rhat-params');
  if (!el || !(await loadV2Rhat())) return;
  const R = V2_RHAT;
  el.innerHTML = '<thead><tr><th>parameter</th>'
    + '<th>classic R&#770;<br /><span class="muted">the equation above</span></th>'
    + '<th>B/W<br /><span class="muted">implied autocorrelation</span></th>'
    + '<th>split R&#770;<br /><span class="muted">what this page reports</span></th>'
    + '<th>effective sample size<br /><span class="muted">of '
    + `${(R.n_chains * R.n_draws).toLocaleString()} draws</span></th></tr></thead><tbody>`
    + R.params.map((p) => {
      // The interesting rows are the ones where the two statistics disagree:
      // that gap is the whole argument for the stricter one.
      const gap = p.split - p.classic > 0.01;
      return `<tr><td class="label-cell">${p.name}</td>`
        + `<td class="unit${p.classic < 1 ? ' ok' : ''}">${p.classic.toFixed(4)}</td>`
        + `<td class="unit muted">${p.bw.toFixed(1)}&times;</td>`
        + `<td class="unit${gap ? ' bad' : ''}">${p.split.toFixed(4)}</td>`
        + `<td class="unit${p.ess < 400 ? ' bad' : ''}">${p.ess.toLocaleString()}</td></tr>`;
    }).join('') + '</tbody>';

  const note = v2El('rhat-params-note');
  if (note) {
    const low = R.lowest[0];
    const worst = R.params.reduce((a, b) => (b.split - b.classic > a.split - a.classic ? b : a));
    const len = R.lengths.map((l) => `${l.n} draws &rarr; ${l.rhat.toFixed(4)}`).join(', ');
    note.innerHTML = `<b>Below 1.0 happens.</b> If the chains land closer together `
      + 'than their own wandering predicts, <span>\\(B < W\\)</span> and R&#770; comes '
      + `out under 1. Across every fit on disk, <b>${R.below_one} of ${R.n_scalars} `
      + `scalar parameters</b> have classic R&#770; below 1.0, the lowest being `
      + `<b>${low.rhat.toFixed(5)}</b> (${low.param} in ${low.fit}). A value a hair `
      + 'under 1 is ordinary noise, not a sign of anything. '
      + '<br /><br />'
      + '<b>The two columns are different statistics.</b> This page reports arviz&rsquo;s '
      + '<b>rank-normalized split R&#770;</b>, which is stricter in two ways: it cuts '
      + `each chain in half first (so it is ${R.n_chains * 2} half-chains of `
      + `${R.n_draws / 2}, catching a chain that drifts <i>within itself</i>), and it `
      + 'replaces the draws by normal scores of their ranks, so it behaves on '
      + `heavy-tailed posteriors. The gap shows: <b>${worst.name}</b> reads `
      + `${worst.classic.toFixed(4)} classic and <b>${worst.split.toFixed(4)}</b> split. `
      + 'Where classic sits below 1 and split above the line, the four chain <i>means</i> '
      + 'agree while each chain is still drifting &mdash; the classic statistic cannot '
      + 'see that, which is why it is not the one quoted. '
      + `<br /><br />And the same chains scored at different lengths: ${len}. `
      + 'Same draws, same mixing, different R&#770;.';
  }
}

async function renderV2RhatArms() {
  const el = v2El('rhat-arms');
  if (!el || !(await loadV2Rhat())) return;
  const R = V2_RHAT;
  const cell = (v, best) => `<td class="unit${best ? ' ok' : ''}">${v}</td>`;
  el.innerHTML = '<thead><tr><th rowspan="2">height form</th>'
    + '<th colspan="2">max R&#770; <span class="muted">lower is better</span></th>'
    + '<th colspan="2">min effective sample size <span class="muted">higher is better</span></th>'
    + '</tr><tr><th class="muted">original</th><th>offsets integrated out</th>'
    + '<th class="muted">original</th><th>offsets integrated out</th></tr></thead><tbody>'
    + R.paired.map((p) => {
      const o = p.original, m = p.marginalized;
      return `<tr><td class="label-cell">${v2FitLabel(p.base)}</td>`
        + cell(o.rhat.toFixed(3), m.rhat > o.rhat)
        + cell(`<b>${m.rhat.toFixed(3)}</b>`, m.rhat < o.rhat)
        + cell(o.ess, m.ess < o.ess)
        + cell(`<b>${m.ess}</b>`, m.ess > o.ess) + '</tr>';
    }).join('') + '</tbody>';

  const note = v2El('rhat-arms-note');
  if (note) {
    const gains = R.paired.filter((p) => p.marginalized.ess > p.original.ess);
    const best = R.paired.reduce((a, b) =>
      (b.marginalized.ess / b.original.ess > a.marginalized.ess / a.original.ess ? b : a));
    const lost = R.paired.filter((p) => p.marginalized.ess <= p.original.ess);
    note.innerHTML = `Every fit here ran identically &mdash; ${R.n_chains} chains, `
      + `${R.n_draws} draws, same tuning, same data &mdash; so the only difference is `
      + `whether the climber offsets were sampled or integrated out. <b>Effective `
      + `sample size improved in ${gains.length} of ${R.paired.length} height forms</b>, `
      + `the largest gain being <b>${v2FitLabel(best.base)}</b> at `
      + `${best.original.ess} &rarr; ${best.marginalized.ess} `
      + `(${(best.marginalized.ess / best.original.ess).toFixed(1)}&times;). `
      + (lost.length
        ? `<b>${lost.map((p) => v2FitLabel(p.base)).join(', ')} went the other way</b> `
          + `(${lost.map((p) => `${p.original.ess} &rarr; ${p.marginalized.ess}`).join(', ')}), `
          + 'which is worth stating rather than rounding off: the improvement is '
          + 'strong and consistent, not universal. '
        : '')
      + 'This is the honest sense in which one version is better than the other. '
      + 'They do not give different answers &mdash; they cost different amounts to get.';
  }
}
