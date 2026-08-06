// ---- the design basis: what orthogonalising the covariate block does ----
//
// Split out of 06-diagnostics.js, which was already at 592 lines against the
// 700-line cap this project keeps.
//
// Everything here reads the `design` block of v2_rhat.json, which is computed
// from the DESIGN MATRIX rather than from any trace. That is deliberate: it
// means the page can say what the change does and what it costs before the
// fit that tests whether it helps has finished, without pre-judging the
// answer. The fit's own verdict arrives separately.

// Raw column names as they appear in the model, rendered as the maths the
// column actually is. The design's columns are the interesting object here,
// not the coefficients on them, so this is a different map from
// V2_PARAM_LABEL.
const V2_COL_MATH = {
  beta_gender: 'G',
  gamma1: '\\tilde h',
  gamma2: '\\tilde h^{2}',
  gamma1_x: 'G\\tilde h',
  gamma2_x: 'G\\tilde h^{2}',
  delta1: '\\tilde a',
  delta2: '\\tilde a^{2}',
  beta_h_missing: '\\mathbf{1}_{\\text{no height}}',
  beta_a_missing: '\\mathbf{1}_{\\text{no span}}',
};

const v2ColMath = (n) => V2_COL_MATH[n] || n.replace(/_/g, '\\_');

// The orthogonalised version of a column, as maths. Parenthesised ALWAYS, not
// only when it looks necessary: half these columns already carry a superscript
// (`\tilde h^{2}`), and appending `^{\perp}` to one produces a double
// superscript, which is a hard KaTeX error -- the whole equation block then
// renders as red source text on the page.
const v2ColPerp = (n) => `{\\left(${v2ColMath(n)}\\right)}^{\\perp}`;
const v2ColName = (n) => (V2_PARAM_LABEL[n] ? V2_PARAM_LABEL[n][0] : n);

async function renderV2DesignForms() {
  const el = v2El('design-forms');
  if (!el || !(await loadV2Rhat())) return;
  const d = V2_RHAT.design;
  if (!d) { el.innerHTML = ''; return; }
  const rows = d.forms.map((f) => {
    // A form whose height terms are nonlinear in the parameters never puts
    // them in the design matrix, so there is nothing there to rotate. Saying
    // "1.0000" beside it without that caveat would read as a win.
    const note = f.rotatable
      ? (f.cond_raw >= 20 ? '<b>worth rotating</b>'
        : (f.cond_raw >= 8 ? 'some gain' : 'little to gain'))
      // These two still show a posterior ridge (saturating's beta0 against
        // sat_scale is the worst on the page) -- it just is not one this fix
        // reaches, because the parameters it involves are not design columns.
      : '<span class="muted">height terms are nonlinear in the parameters, so '
        + 'they are not columns at all &mdash; this fix cannot reach them</span>';
    return `<tr>
      <td class="label-cell">${f.label}</td>
      <td class="unit">${f.n_cols}</td>
      <td class="sym">\\(${v2ColMath(f.a)}\\) vs \\(${v2ColMath(f.b)}\\)</td>
      <td class="unit">${f.r >= 0 ? '+' : '−'}${Math.abs(f.r).toFixed(3)}</td>
      <td class="unit">${f.cond_raw.toFixed(1)}</td>
      <td class="unit">${f.cond_orth.toFixed(4)}</td>
      <td>${note}</td></tr>`;
  }).join('');
  el.innerHTML = '<thead><tr><th>height form</th><th>columns</th>'
    + '<th>most correlated pair</th><th>r</th><th>condition, raw</th>'
    + '<th>condition, orthogonal</th><th>reading</th></tr></thead>'
    + `<tbody>${rows}</tbody>`;
  v2Typeset(el);
}

async function renderV2DesignBasis() {
  const el = v2El('design-basis');
  if (!el || !(await loadV2Rhat())) return;
  const d = V2_RHAT.design;
  if (!d || !d.basis) { el.innerHTML = ''; return; }
  // Only the height block is worth showing. The ape and missingness columns
  // get rotated too, but their lines are long and say the same thing.
  const show = d.basis.filter((b) => b.name.startsWith('gamma'));
  const lines = show.map((b) => {
    const terms = b.terms.map((t) => {
      const sign = t.c >= 0 ? '+' : '-';
      return ` ${sign} ${Math.abs(t.c).toFixed(3)}\\,${v2ColMath(t.on)}`;
    }).join('');
    return `${v2ColPerp(b.name)} &= ${v2ColMath(b.name)}${terms}`;
    // Row separator is a bare `\\`, not `\\[2pt]`: KaTeX's auto-render scans
    // for `\\[` as a DISPLAY-MATH delimiter, so a row-spacing argument inside
    // an aligned block terminates the equation early and the rest of it is
    // dumped on the page as raw source. Spacing belongs in CSS anyway.
  }).join(' \\\\ ');
  el.innerHTML = `$$\\begin{aligned}${lines}\\end{aligned}$$`;
  v2Typeset(el);
}

async function renderV2DesignPrior() {
  const el = v2El('design-prior');
  const note = v2El('design-prior-note');
  if (!el || !(await loadV2Rhat())) return;
  const d = V2_RHAT.design;
  if (!d) { el.innerHTML = ''; return; }
  const p = d.prior;
  const rows = p.coefs.map((c) => `<tr>
      <td class="label-cell">${v2ColName(c.name)}</td>
      <td class="sym">\\(${v2ColMath(c.name)}\\)</td>
      <td class="unit">${c.sd.toFixed(3)}</td>
      <td class="unit">${c.implied.toFixed(3)}</td>
      <td class="unit">${c.ratio >= 2 ? `<b>${c.ratio.toFixed(2)}&times;</b>`
    : `${c.ratio.toFixed(2)}&times;`}</td></tr>`).join('');
  el.innerHTML = '<thead><tr><th>coefficient</th><th>on the column</th>'
    + '<th>prior SD, as written</th><th>implied prior SD, raw basis</th>'
    + '<th>ratio</th></tr></thead>'
    + `<tbody>${rows}</tbody>`;
  v2Typeset(el);

  if (note) {
    const ratio = p.curve_orth / p.curve_raw;
    const loosest = p.coefs.reduce((a, b) => (b.ratio > a.ratio ? b : a));
    note.innerHTML = 'The column rescaling is doing exactly what it was added '
      + 'for. Averaged over climbers, the prior standard deviation of the '
      + `<b>fitted curve</b> goes ${p.curve_raw.toFixed(3)} &rarr; `
      + `${p.curve_orth.toFixed(3)} grades &mdash; a ratio of `
      + `<b>${ratio.toFixed(3)}</b>, i.e. unchanged. What moves is the prior on `
      + `individual coefficients, up to <b>${loosest.ratio.toFixed(2)}&times;</b> `
      + `on ${v2ColName(loosest.name)}. That widening is the point rather than a `
      + 'side effect: independent priors on two columns correlated at '
      + '&minus;0.899 put most of their mass on combinations that cancel, which '
      + 'is a constraint on the curve nobody intended. Removing it is a '
      + '<b>change of model, not just of coordinates</b> &mdash; so the check on '
      + 'the refit is that the fitted curve moves less than its own credible '
      + 'band, not that it is identical.';
  }
}

async function renderV2Design() {
  await renderV2DesignForms();
  await renderV2DesignBasis();
  await renderV2DesignPrior();
}
