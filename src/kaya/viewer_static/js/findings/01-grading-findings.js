// ==========================================================================
// Findings tab — the presentation cut
//
// The third pane onto the same payloads. There is no second source of numbers
// anywhere in this file: every figure is a shared v2 renderer, mounted by
// switching the namespace once, exactly as 01-grading-current.js does for the
// detailed page.
//
// What makes this page shorter is WHICH SECTIONS EXIST, not which fits back
// them. It takes the same scope default as `gm-` (see V2_SCOPE_DEFAULT in
// 02-results.js) so the two pages cannot drift into quoting different
// primaries — a presentation page that disagrees with the detail page behind
// it is worse than either version on its own.
//
// Element ids are namespaced `gf-`. Ids must be unique per document and the
// other two explainer panes already own `gm-` and `v2-`.
// ==========================================================================

// The four candidate shapes worth showing. Nothing, a straight line, the
// fitted form, and one that levels off — that is the whole shape of the
// argument. `quadratic` restates `quadratic_x_gender` without the gender
// split, and `vertex_quadratic` restates `quadratic` with a different
// parameterisation of the same family; both belong on the detailed page.
const GF_FORM_KEYS = ['zero', 'linear', 'quadratic_x_gender', 'saturating'];

function renderFindingsTab() {
  return renderV2TabOnce('gf-', 'tab-grading-findings', renderFindingsTabInner);
}

async function renderFindingsTabInner() {
  const ok = await loadV2Results();
  if (!ok) {
    const host = v2El('stats');
    if (host) {
      host.innerHTML = '<p class="form-noparams">Fitted results could not be '
        + 'loaded (/static/v2_results.json). Regenerate with '
        + '<code>scripts/build_v2_results.py</code>.</p>';
    }
    return;
  }
  bindInfoDots();

  // 1-2. The two headline results.
  renderGfStats();
  renderV2InlineFigures();
  renderV2Symbols();
  renderV2GymChart();
  renderV2BrandChart();

  // 3. The model: equations are static, the gap explorer and the form cards
  // are live.
  bindV2GapExplorer();
  renderV2GapExplorer();
  renderV2FormCards(GF_FORM_KEYS);

  // 5. Improvement over time. renderGmInlineFigures fills the `data-gm` slots,
  // which are document-wide rather than namespaced, so this page's prose gets
  // the same numbers with no extra wiring.
  await loadV2Time();
  renderGmInlineFigures();
  renderV2AdvTable();
  renderV2Advancement();
  renderV2TimeChart();

  // 4 and 6. Both need v2_posterior.json, so they are loaded together even
  // though they sit either side of the time section on the page.
  await renderV2VsNull();
  await renderFindingsInference();

  bindV2Glossary();

  // Injected after KaTeX's DOMContentLoaded pass, so typeset explicitly.
  ['symbols', 'form-cards', 'gap-controls', 'loo-note', 'vsnull-note']
    .forEach((id) => v2Typeset(v2El(id)));
}

// ---- headline tiles ----
//
// Same payload as the detailed page's tiles. Re-implemented rather than shared
// only because the host element differs; the CONTENT comes from v2Stats().

function renderGfStats() {
  const host = v2El('stats');
  if (!host || typeof v2Stats !== 'function') return;
  host.innerHTML = v2Stats().map((s) => `
    <div class="stat-tile">
      <div class="stat-value">${s.v}</div>
      <div class="stat-label">${s.l}</div>
      <div class="stat-sub">${s.s}</div>
    </div>`).join('');
}

// ---- the evidence section ----
//
// A trimmed renderV2Inference: this page shows the LOO table, the posterior
// grid, one parameter's chains, and the sampler summary. It deliberately omits
// the corner plot, the across-fit overlays, the arm-pair small multiples and
// the whole R-hat teaching sequence — all of which are on the detailed page.

async function renderFindingsInference() {
  if (!(await loadV2Posterior())) return;
  bindV2Inference();
  renderV2FormsLoo();
  renderV2PostGrid();
  renderV2FittedForms();
  renderV2Sampler();
  const sel = v2El('param-pick');
  const first = sel?.value || Object.keys(v2Fit(v2SelectedFit()).params)[0];
  if (first) renderV2ParamDetail(first);
}
