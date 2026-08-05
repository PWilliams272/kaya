// ==========================================================================
// How leave-one-out is really computed, and where it breaks
//
// Split out of 04-reliability.js, which was over the 700-line cap. Everything
// here reads v2_psis.json, written by scripts/build_v2_psis.py: the same
// diagnostic on both arms of the model, so the page can show the failure and
// its fix against each other rather than asserting the second.
// ==========================================================================

// ---- how leave-one-out is really computed, and where it breaks ----
//
// v2_psis.json is written by scripts/build_v2_psis.py. The chart answers one
// question directly from the draws: rank a row's 2,000 importance weights
// largest-first, and what share of the estimate do the top few carry? An even
// spread is healthy; a steep climb means the answer rests on a handful of
// draws, which is what Pareto k is measuring.

// A weight ratio reads as a magnitude, not a measurement: 133x and 1.1x say
// what 132.6x and 1x do not.
const v2Ratio = (v) => (v >= 10 ? Math.round(v).toLocaleString() : v.toFixed(1));

let V2_PSIS = null;
let V2_PSIS_GROUP = 1;
// Which arm the chart is showing. Defaults to 0 -- the unmarginalized model --
// because the section's job is to show the problem before it shows the fix.
let V2_PSIS_ARM = 0;

async function loadV2Psis() {
  if (!V2_PSIS) {
    try {
      const r = await fetch('/static/v2_psis.json', { cache: 'no-cache' });
      if (r.ok) V2_PSIS = await r.json();
    } catch (e) { /* not built yet */ }
  }
  return !!(V2_PSIS && V2_PSIS.arms);
}

async function renderV2Psis() {
  const host = v2El('psis-chart');
  if (!host) return;
  await loadV2Psis();
  const note = v2El('psis-note');
  const pick = v2El('psis-picker');
  const armPick = v2El('psis-arm-picker');
  const tbl = v2El('psis-table');
  if (!V2_PSIS || !V2_PSIS.arms) {
    host.style.display = 'none';
    if (pick) pick.style.display = 'none';
    if (armPick) armPick.style.display = 'none';
    if (note) note.textContent = 'Built by scripts/build_v2_psis.py.';
    return;
  }
  host.style.display = '';
  const arm = V2_PSIS.arms[V2_PSIS_ARM] || V2_PSIS.arms[0];
  const groups = arm.groups;
  const xs = arm.curve_x;
  const cur = groups.find((g) => g.k === V2_PSIS_GROUP) || groups[0];

  // The arm picker only appears where the page has asked for it. On the
  // archived v2 page there is no such element and the chart stays single-arm.
  if (armPick) {
    armPick.style.display = '';
    armPick.innerHTML = '<span class="muted seg-note" style="margin:0 10px 0 0">'
      + 'climber offsets:</span>'
      + V2_PSIS.arms.map((a, i) => `<button type="button" class="seg-btn`
        + `${i === V2_PSIS_ARM ? ' on' : ''}" data-psis-arm="${i}">`
        + `${a.label}</button>`).join('');
    // v2Bound: the handler fires long after the render that bound it, by which
    // point the namespace has been restored to the archived pane. Without it a
    // click here would silently redraw the archive instead of this page.
    armPick.querySelectorAll('[data-psis-arm]').forEach((b) => {
      b.onclick = v2Bound(() => {
        V2_PSIS_ARM = +b.dataset.psisArm;
        return renderV2Psis();
      });
    });
  }

  if (pick) {
    pick.style.display = '';
    pick.innerHTML = '<span class="muted seg-note" style="margin:0 10px 0 0">'
      + 'climber has:</span>'
      + groups.map((g) => `<button type="button" class="seg-btn`
        + `${g.k === cur.k ? ' on' : ''}" data-psis="${g.k}">${g.label} `
        + `${g.k === 1 ? 'row' : 'rows'}</button>`).join('');
    pick.querySelectorAll('[data-psis]').forEach((b) => {
      b.onclick = v2Bound(() => {
        V2_PSIS_GROUP = +b.dataset.psis;
        return renderV2Psis();
      });
    });
  }

  // Every group at once, with the selected one emphasised: the point is the
  // gradient between them, which a single curve cannot show.
  const traces = groups.map((g) => {
    const on = g.k === cur.k;
    return {
      type: 'scatter', mode: 'lines+markers',
      name: `${g.label} ${g.k === 1 ? 'row' : 'rows'}`,
      x: xs, y: g.curve.map((v) => v * 100),
      line: { width: on ? 3 : 1.4, color: cssVar(on ? '--lg-cat-1' : '--lg-text-2'),
        dash: on ? 'solid' : 'dot' },
      marker: { size: on ? 7 : 4 },
      opacity: on ? 1 : 0.5,
      hovertemplate: `%{x} draws carry %{y:.1f}%<extra>${g.label}</extra>`,
    };
  });
  // What an evenly-spread row would look like: 2,000 draws, no concentration.
  traces.push({
    type: 'scatter', mode: 'lines', name: 'perfectly even',
    x: xs, y: xs.map((v) => (100 * v) / arm.n_draws),
    line: { color: cssVar('--lg-text-3'), width: 1.4, dash: 'dash' },
    hovertemplate: 'even: %{x} draws carry %{y:.1f}%<extra></extra>',
  });

  const layout = chartLayout('');
  layout.height = 420;
  layout.margin = { l: 66, r: 20, t: 12, b: 96 };
  // Plotly's default log minor ticks read "1 2 5 10 2 5 100 2", which looks
  // like a mistake. Label only the decades and the points actually plotted.
  const ticks = [1, 10, 100, 1000, arm.n_draws];
  layout.xaxis = { ...layout.xaxis, automargin: false, type: 'log',
    tickmode: 'array', tickvals: ticks,
    ticktext: ticks.map((v) => v.toLocaleString()),
    title: { text: 'number of draws, ranked by weight (log scale)', standoff: 10 } };
  layout.yaxis = { ...layout.yaxis, automargin: false, range: [0, 100],
    ticksuffix: '%',
    title: { text: 'share of the estimate they carry', standoff: 6 } };
  layout.legend = { ...layout.legend, orientation: 'h', y: -0.22,
    yanchor: 'top', x: 0, font: { size: 10 } };
  Plotly.react(host, traces, layout, { displayModeBar: false, responsive: true });

  if (note) {
    const top10 = cur.curve[xs.indexOf(10)];
    note.innerHTML = `Each line is a typical row from climbers with that many `
      + `observations, using all ${arm.n_draws.toLocaleString()} posterior `
      + 'draws. <b>The dashed line is what perfect health looks like</b> '
      + '&mdash; every draw contributing equally. The further a curve sits '
      + `above it, the more the answer depends on a few draws. For climbers `
      + `with <b>${cur.label} ${cur.k === 1 ? 'row' : 'rows'}</b>, the top 10 `
      + `draws out of ${arm.n_draws.toLocaleString()} carry `
      + `<b>${(top10 * 100).toFixed(1)}%</b> of the estimate, and the single `
      + `largest weight is <b>${v2Ratio(cur.max_over_median)}&times;</b> `
      + 'a typical one.';
  }

  if (tbl) {
    tbl.innerHTML = '<thead><tr><th>climber has</th><th>rows</th>'
      + '<th>largest weight<br /><span class="muted">&times; a typical one</span></th>'
      + '<th>top 10 draws carry<br /><span class="muted">of the estimate</span></th>'
      + '<th>median Pareto k<br /><span class="muted">lower is better</span></th>'
      + '<th>rows over 0.7<br /><span class="muted">unreliable</span></th>'
      + '</tr></thead><tbody>'
      + groups.map((g) => {
        const t10 = g.curve[xs.indexOf(10)];
        const bad = g.bad_k > 0.1;
        return `<tr${g.k === cur.k ? ' style="font-weight:600"' : ''}>`
          + `<td class="label-cell">${g.label} ${g.k === 1 ? 'row' : 'rows'}</td>`
          + `<td class="unit">${g.n_rows.toLocaleString()}</td>`
          + `<td class="unit">${v2Ratio(g.max_over_median)}&times;</td>`
          + `<td class="unit">${(t10 * 100).toFixed(1)}%</td>`
          + `<td class="unit">${g.k_median.toFixed(2)}</td>`
          + `<td class="unit${bad ? '' : ' muted'}">${(g.bad_k * 100).toFixed(1)}%</td></tr>`;
      }).join('') + '</tbody>';
  }
}


// ---- did integrating the offsets out actually fix it? ----
//
// The same diagnostic on both arms, same rows, same draws. Single-observation
// climbers are the target; the multi-observation rows are the control, because
// marginalizing singles must leave them alone. If they moved, something other
// than the intended change happened.

async function renderV2PsisArms() {
  const el = v2El('psis-arms');
  if (!el) return;
  if (!(await loadV2Psis()) || V2_PSIS.arms.length < 2) {
    el.innerHTML = '<tbody><tr><td class="label-cell">Built by '
      + '<code>scripts/build_v2_psis.py</code>.</td></tr></tbody>';
    return;
  }
  const [before, after] = V2_PSIS.arms;
  const pct = (v) => `${(v * 100).toFixed(1)}%`;
  const rows = before.groups.map((g, i) => {
    const h = after.groups[i];
    // Fixed by construction, not measured: a single-observation climber has no
    // offset left to remove, so there is nothing for the weights to correct.
    const target = g.k === 1;
    return `<tr${target ? ' style="font-weight:600"' : ''}>`
      + `<td class="label-cell">${g.label} ${g.k === 1 ? 'row' : 'rows'}`
      + `${target ? '' : ' <span class="muted">(control)</span>'}</td>`
      + `<td class="unit">${g.n_rows.toLocaleString()}</td>`
      + `<td class="unit">${v2Ratio(g.max_over_median)}&times;</td>`
      + `<td class="unit">${v2Ratio(h.max_over_median)}&times;</td>`
      + `<td class="unit">${pct(g.bad_k)}</td>`
      + `<td class="unit">${pct(h.bad_k)}</td>`
      + `<td class="unit">${g.k_median.toFixed(2)} &rarr; ${h.k_median.toFixed(2)}</td>`
      + '</tr>';
  });
  el.innerHTML = '<thead><tr><th rowspan="2">climber has</th>'
    + '<th rowspan="2">rows</th>'
    + '<th colspan="2">largest weight <span class="muted">&times; typical</span></th>'
    + '<th colspan="2">rows over k = 0.7 <span class="muted">unreliable</span></th>'
    + '<th rowspan="2">median k<br /><span class="muted">lower is better</span></th>'
    + '</tr><tr>'
    + `<th class="muted">${before.label}</th><th>${after.label}</th>`
    + `<th class="muted">${before.label}</th><th>${after.label}</th>`
    + '</tr></thead><tbody>' + rows.join('')
    + '<tr><td class="label-cell"><b>every row</b></td>'
    + `<td class="unit">${(before.n_rows || 0).toLocaleString()}</td>`
    + '<td class="unit muted">&mdash;</td><td class="unit muted">&mdash;</td>'
    + `<td class="unit">${pct(before.overall_bad_k)}</td>`
    + `<td class="unit"><b>${pct(after.overall_bad_k)}</b></td>`
    + '<td class="unit muted">&mdash;</td></tr>'
    + '</tbody>';
}
