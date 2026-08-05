// ==========================================================================
// The same height form, fitted both ways
//
// The across-fits plot answers "does this parameter depend on which height
// form sits beside it?" This one answers a different question that the same
// figure cannot: "does it depend on whether the climber offsets were sampled
// or integrated out?"
//
// Overlaying all fourteen fits on one axis cannot show that, because the
// height form and the arm move together and nothing separates them. Pairing
// each form with its own marginalized twin holds the form fixed, so whatever
// is left is the arm.
// ==========================================================================

// Two arms, two fixed colours. The per-fit hues are wrong here: the variable
// on this figure is the arm, not the fit, and reusing the fit palette would
// give the same arm a different colour in every panel.
const V2_ARM_HUE = { unmarginalized: '--lg-cat-1', marginalized: '--lg-cat-4' };

// Below this, in units of the original fit's own posterior SD, the two arms
// are telling the same story and the difference is not worth a second look.
const V2_PAIR_SAME = 0.25;

// Every base height form that was fitted both ways, in the payload's order.
function v2ArmPairs() {
  const byBase = new Map();
  v2FitNames().forEach((n) => {
    const f = v2Fit(n);
    if (!f || (f.role || 'form') !== 'form') return;
    const rec = byBase.get(f.base) || {};
    rec[f.arm || 'unmarginalized'] = n;
    byBase.set(f.base, rec);
  });
  return [...byBase.entries()]
    .filter(([, r]) => r.unmarginalized && r.marginalized)
    .map(([base, r]) => ({ base, ...r }));
}

function renderV2ArmPairs(name) {
  const host = v2El('arm-pairs');
  const note = v2El('arm-pairs-note');
  if (!host || !name) return;
  const pairs = v2ArmPairs();
  // A parameter has to exist in BOTH arms of a form to be paired. gamma2 lives
  // in the quadratic forms and not the linear one, so which panels appear
  // changes with the parameter -- saying so beats a silently shorter grid.
  const usable = pairs.filter(({ unmarginalized, marginalized }) =>
    v2Fit(unmarginalized)?.params[name] && v2Fit(marginalized)?.params[name]);

  if (!usable.length) {
    host.innerHTML = '<p class="form-noparams">No height form has this parameter '
      + 'in both versions, so there is nothing to pair.</p>';
    if (note) note.textContent = '';
    return;
  }

  // One x-range across every panel. Per-panel ranges would let a form with a
  // tight posterior fill its tile exactly like one with a wide posterior, and
  // the grid would show nothing.
  const all = usable.flatMap(({ unmarginalized, marginalized }) => [
    ...v2Fit(unmarginalized).params[name].chains.flat(),
    ...v2Fit(marginalized).params[name].chains.flat()]);
  const [lo, hi] = v2SharedRange(all, null);
  const grid = v2Grid(lo, hi, 90);

  const rows = usable.map(({ base, unmarginalized, marginalized }) => {
    const a = v2Fit(unmarginalized).params[name];
    const b = v2Fit(marginalized).params[name];
    // In units of the ORIGINAL fit's own uncertainty: a shift is only
    // interesting relative to how well that fit pinned the parameter down.
    const shift = a.sd > 0 ? (b.mean - a.mean) / a.sd : 0;
    // Threshold the DISPLAYED value, not the raw one. A shift of 0.2496 prints
    // as 0.25 and would otherwise be described as not reaching 0.25.
    const shown = Math.round(shift * 100) / 100;
    return { base, a, b, shift, shown, bad: Math.max(a.rhat, b.rhat) > 1.01 };
  });

  host.innerHTML = rows.map(({ base, shown, bad }) => {
    const big = Math.abs(shown) > V2_PAIR_SAME;
    return `<div class="pair-tile">
      <span class="pair-tile-name">${v2FitLabel(base)}${bad ? ' ⚠' : ''}</span>
      <span class="pair-tile-chart" id="${v2Id(`pair-${base}`)}"></span>
      <span class="pair-tile-shift${big ? ' big' : ''}">moves ${shown < 0 ? '−' : '+'}${Math.abs(shown).toFixed(2)} sd</span>
    </div>`;
  }).join('');

  rows.forEach(({ base, a, b }) => {
    const el = document.getElementById(v2Id(`pair-${base}`));
    if (!el || typeof Plotly === 'undefined') return;
    const line = (p, arm, dash) => ({
      type: 'scatter', mode: 'lines', x: grid, y: v2Kde(p.chains.flat(), grid),
      line: { color: cssVar(V2_ARM_HUE[arm]), width: 1.6, dash },
      fill: 'tozeroy',
      fillcolor: `color-mix(in srgb, ${cssVar(V2_ARM_HUE[arm])} 14%, transparent)`,
      hoverinfo: 'skip',
    });
    Plotly.react(el, [line(a, 'unmarginalized', 'solid'),
                      line(b, 'marginalized', 'dot')], {
      height: 74, margin: { l: 2, r: 2, t: 2, b: 2 }, showlegend: false,
      paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)',
      xaxis: { visible: false, range: [lo, hi] }, yaxis: { visible: false },
    }, { displayModeBar: false, staticPlot: true, responsive: true });
  });

  if (note) {
    const worst = [...rows].sort((x, y) => Math.abs(y.shown) - Math.abs(x.shown))[0];
    const moved = rows.filter((r) => Math.abs(r.shown) > V2_PAIR_SAME);
    const skipped = pairs.length - usable.length;
    const swatch = (arm, dash) => `<span style="color:${cssVar(V2_ARM_HUE[arm])};`
      + `font-weight:700">${dash}</span>`;
    note.innerHTML = `${swatch('unmarginalized', '—')} <b>original</b> against `
      + `${swatch('marginalized', '···')} <b>offsets integrated out</b>, `
      + 'the same height form in each panel, on one shared horizontal scale. '
      + '<b>Shifts are in units of the original fit&rsquo;s own posterior SD</b> '
      + '&mdash; a parameter that moves by less than its own uncertainty has not '
      + 'really moved. '
      + (moved.length
        ? `<b>${moved.length} of ${rows.length} forms move by more than `
          + `${V2_PAIR_SAME} sd</b>, the largest being `
          + `<b>${v2FitLabel(worst.base)}</b> at ${worst.shown < 0 ? '−' : '+'}`
          + `${Math.abs(worst.shown).toFixed(2)} sd. `
        : `<b>No form moves by more than ${V2_PAIR_SAME} sd</b> &mdash; the largest `
          + `is ${v2FitLabel(worst.base)} at ${Math.abs(worst.shown).toFixed(2)}. `
          + 'On this parameter the two versions are the same answer. ')
      + (skipped
        ? `${skipped} form${skipped === 1 ? ' does' : 's do'} not contain this `
          + 'parameter in both versions and ' + (skipped === 1 ? 'is' : 'are')
          + ' not shown. '
        : '')
      + 'Panels marked ⚠ contain a fit whose chains did not agree.';
  }
}
