// ---- The ability model's SIMULATION panels: illustration, not the model
//
// Everything in this file is a generative STORY. There is no probability of
// sending an individual climb anywhere in the fitted likelihood -- the model
// goes straight to the maximum and never sees an attempt. These panels exist
// because the story makes the gap legible and shows where the two error terms
// come from, and they are labelled illustration wherever they appear.
//
// Split out of 18-ability.js, which holds the model itself. Both are classic
// scripts sharing one global scope: 18 must load first, since the helpers
// (abEl, abRng, abNormal, abLambda) and the AB state object live there, and
// 18's abRender calls into this file. See base.html on load order.

// ---- panel 2: one session ----

function abSession(p, rand) {
  const climbs = [];
  for (let i = 0; i < p.attempts; i++) {
    // True difficulty of the climb. Climbers pick near their limit.
    const d = Math.min(12, Math.max(0, p.C - 0.8 + 1.3 * abNormal(rand)));
    // Sendable only at or below the ceiling: the hard edge that makes the gap
    // one-sided. The 0.35 is how sharply effort falls off approaching it.
    const pSend = d > p.C ? 0 : 1 / (1 + Math.exp((d - p.C) / 0.35));
    const sent = rand() < pSend;
    // What the LABEL says, which is what gets logged. A soft climb reads
    // harder than it is, which is how a send above your own ceiling happens.
    const g = d + p.sigmaLink * abNormal(rand);
    climbs.push({ d, g, sent, logged: false });
  }
  const sent = climbs.filter((c) => c.sent);
  if (sent.length) {
    const best = Math.max(...sent.map((c) => c.g));
    if (p.policy === 'all') sent.forEach((c) => { c.logged = true; });
    else if (p.policy === 'proud') sent.forEach((c) => { c.logged = c.g >= best - 1.0; });
    else if (p.policy === 'sporadic') sent.forEach((c) => { c.logged = rand() < 0.25; });
    else if (p.policy === 'first') {
      const seen = new Set();
      sent.slice().sort((a, b) => b.g - a.g).forEach((c) => {
        const k = Math.round(c.g * 2);
        if (!seen.has(k)) { seen.add(k); c.logged = true; }
      });
    }
  }
  const logged = climbs.filter((c) => c.logged);
  const maxLogged = logged.length ? Math.max(...logged.map((c) => c.g)) : null;
  return { climbs, maxLogged, gap: maxLogged === null ? null : p.C - maxLogged };
}

function abPlotSession(p, session) {
  const el = abEl('session');
  if (!el || typeof Plotly === 'undefined') return;
  const rows = (f) => session.climbs.filter(f).map((c) => c.g);
  const failed = rows((c) => !c.sent);
  const unlogged = rows((c) => c.sent && !c.logged);
  const logged = rows((c) => c.sent && c.logged);
  const lane = (a, base) => a.map((_, i) => base + ((i % 5) - 2) * 0.05);

  const traces = [
    { type: 'scatter', mode: 'markers', name: 'tried, did not send',
      x: failed, y: lane(failed, 0.28),
      marker: { symbol: 'x', size: 9, color: cssVar('--lg-text-3'), line: { width: 1.4 } },
      hovertemplate: 'V%{x:.1f} — tried, did not send<extra></extra>' },
    { type: 'scatter', mode: 'markers', name: 'sent, never logged',
      x: unlogged, y: lane(unlogged, 0.60),
      marker: { symbol: 'circle-open', size: 10, color: cssVar('--lg-cat-2'), line: { width: 2 } },
      hovertemplate: 'V%{x:.1f} — sent, never logged<extra></extra>' },
    { type: 'scatter', mode: 'markers', name: 'sent and logged',
      x: logged, y: lane(logged, 0.92),
      marker: { symbol: 'circle', size: 11, color: cssVar('--lg-cat-1') },
      hovertemplate: 'V%{x:.1f} — logged; the model sees only these<extra></extra>' },
  ];

  const layout = chartLayout('');
  layout.height = 320;
  layout.margin = { l: 26, r: 20, t: 44, b: 54 };
  layout.xaxis = { ...layout.xaxis, automargin: false,
    range: [Math.max(0, p.C - 5), p.C + 2],
    title: { text: 'grade (V-scale)', standoff: 10 } };
  layout.yaxis = { ...layout.yaxis, range: [0, 1.3], showticklabels: false,
    showgrid: false, zeroline: false, title: undefined };
  layout.legend = { orientation: 'h', y: -0.26, x: 0 };
  layout.shapes = [{ type: 'line', x0: p.C, x1: p.C, y0: 0, y1: 1.14,
    line: { color: cssVar('--lg-danger'), width: 2.4 } }];
  layout.annotations = [{ x: p.C, y: 1.24, text: `C = V${p.C.toFixed(1)}`,
    showarrow: false, xanchor: 'center',
    font: { size: 11.5, color: cssVar('--lg-danger') } }];

  if (session.maxLogged !== null) {
    layout.shapes.push({ type: 'line', x0: session.maxLogged, x1: session.maxLogged,
      y0: 0, y1: 1.14, line: { color: cssVar('--lg-cat-1'), width: 2.2, dash: 'dash' } });
    layout.shapes.push({ type: 'rect',
      x0: Math.min(session.maxLogged, p.C), x1: Math.max(session.maxLogged, p.C),
      y0: 0.02, y1: 1.10, fillcolor: cssVar('--lg-danger'), opacity: 0.07,
      line: { width: 0 } });
    layout.annotations.push({ x: session.maxLogged, y: 1.24,
      text: `m = V${session.maxLogged.toFixed(1)}`, showarrow: false, xanchor: 'center',
      font: { size: 11.5, color: cssVar('--lg-cat-1') } });
    layout.annotations.push({ x: (p.C + session.maxLogged) / 2, y: 0.09,
      text: `gap ${session.gap.toFixed(2)}`, showarrow: false,
      font: { size: 11.5, color: cssVar('--lg-danger') } });
  }
  Plotly.react(el, traces, layout, { displayModeBar: false, responsive: true });
}

// ---- panel 3: P(send) and the simulated gap ----

function abPlotSend(p) {
  const el = abEl('send');
  if (!el || typeof Plotly === 'undefined') return;
  const x = [];
  const y = [];
  const lo = Math.max(0, p.C - 5);
  for (let d = lo; d <= p.C + 2; d += 0.02) {
    x.push(d); y.push(d > p.C ? 0 : 1 / (1 + Math.exp((d - p.C) / 0.35)));
  }
  const layout = chartLayout('');
  layout.height = 280;
  layout.margin = { l: 58, r: 20, t: 34, b: 52 };
  layout.xaxis = { ...layout.xaxis, automargin: false, range: [lo, p.C + 2],
    title: { text: 'true difficulty of the climb (V-scale)', standoff: 10 } };
  layout.yaxis = { ...layout.yaxis, range: [0, 1.05],
    title: { text: 'P(send)', standoff: 8 } };
  layout.showlegend = false;
  layout.shapes = [{ type: 'line', x0: p.C, x1: p.C, y0: 0, y1: 1.05,
    line: { color: cssVar('--lg-danger'), width: 2.2 } }];
  Plotly.react(el, [{ type: 'scatter', mode: 'lines', x, y,
    line: { color: cssVar('--lg-cat-3'), width: 2.8 },
    hovertemplate: 'true V%{x:.1f}: %{y:.0%}<extra></extra>' }],
  layout, { displayModeBar: false, responsive: true });
}

function abPlotSimGap(p, fit) {
  const el = abEl('simgap');
  if (!el || typeof Plotly === 'undefined') return;
  const rand = abRng(p.seed * 7919 + 13);
  const gaps = [];
  for (let i = 0; i < 3000; i++) {
    const s = abSession(p, rand);
    if (s.gap !== null) gaps.push(s.gap);
  }
  if (!gaps.length) { Plotly.purge(el); return; }
  const mean = gaps.reduce((a, b) => a + b, 0) / gaps.length;
  const neg = gaps.filter((g) => g < 0).length / gaps.length;

  const lambda = abLambda(p.visits, fit);
  const modelMean = 1 / lambda;
  const xmax = Math.max(1, mean + 3);
  const ex = [];
  const ey = [];
  for (let x = 0; x <= xmax; x += xmax / 200) {
    ex.push(x); ey.push(lambda * Math.exp(-lambda * x));
  }

  const layout = chartLayout('');
  layout.height = 290;
  layout.margin = { l: 58, r: 20, t: 34, b: 52 };
  layout.xaxis = { ...layout.xaxis, automargin: false, range: [-1.2, xmax],
    title: { text: 'gap: ceiling minus hardest logged send (grades)', standoff: 10 } };
  layout.yaxis = { ...layout.yaxis, title: { text: 'density', standoff: 8 } };
  layout.legend = { orientation: 'h', y: -0.3, x: 0 };
  layout.bargap = 0.02;
  layout.shapes = [{ type: 'line', x0: 0, x1: 0, y0: 0, y1: lambda * 1.1,
    line: { color: cssVar('--lg-text-3'), width: 1.4, dash: 'dot' } }];

  Plotly.react(el, [
    { type: 'histogram', x: gaps, histnorm: 'probability density',
      name: `simulated at ${p.attempts} attempts (mean ${mean.toFixed(2)})`,
      nbinsx: 50, marker: { color: cssVar('--lg-cat-1'), opacity: 0.42 },
      hovertemplate: '%{x:.2f}<extra></extra>' },
    { type: 'scatter', mode: 'lines', x: ex, y: ey,
      name: `the model at ${p.visits} visits: Exponential, mean ${modelMean.toFixed(2)}`,
      line: { color: cssVar('--lg-danger'), width: 2.6 },
      hovertemplate: '%{x:.2f}<extra></extra>' },
  ], layout, { displayModeBar: false, responsive: true });

  const note = abEl('simgap-note');
  if (note) {
    note.innerHTML = `Simulated mean gap <b>${mean.toFixed(2)}</b> grades at `
      + `${p.attempts} attempts, against the model's <b>${modelMean.toFixed(2)}</b> `
      + `at ${p.visits} visits — two different knobs, shown together so the `
      + `story and the fitted form can be lined up rather than assumed to `
      + `agree. <b>${(100 * neg).toFixed(0)}%</b> of simulated sessions land `
      + `<i>above</i> the ceiling: those are real, and they come from soft `
      + `grading (σ<sub>link</sub>) rather than from the climber exceeding `
      + `themselves. Push the attempts slider high and that share grows, `
      + `because the largest of many label errors grows — the honest limit of `
      + `this illustration, and the reason σ<sub>link</sub> is held small.`;
  }
}

// ---- readout ----

function abReadout(p, session, fit) {
  const el = abEl('readout');
  if (!el) return;
  const sent = session.climbs.filter((c) => c.sent).length;
  const logged = session.climbs.filter((c) => c.logged).length;
  const cells = [
    ['attempted', p.attempts, 'climbs tried this session'],
    ['sent', sent, `${Math.round((100 * sent) / p.attempts)}% of attempts`],
    ['logged', logged, sent ? `${Math.round((100 * logged) / sent)}% of sends` : '—'],
    ['hardest logged', session.maxLogged === null ? '—' : `V${session.maxLogged.toFixed(1)}`,
      'the only thing the model reads'],
    ['gap this session', session.gap === null ? '—' : session.gap.toFixed(2),
      `model expects ${(1 / abLambda(p.visits, fit)).toFixed(2)}`],
  ];
  el.innerHTML = cells.map(([k, v, s]) => `<div class="pm-stat">
    <div class="pm-stat-k">${k}</div><div class="pm-stat-v">${v}</div>
    <div class="pm-stat-s">${s}</div></div>`).join('');
}
