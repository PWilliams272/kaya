// ---- One number per gym is not enough: grade compression and gym drift
//
// v2_structure.json is written by scripts/build_v2_structure.py. Both effects
// come from the same identity on perpendicular axes -- a climber at two gyms
// cancels themselves out of the difference, so that difference is a statement
// about the two gyms alone. Regress it on the climber's ability and you get
// grade compression; regress it on the calendar date and you get gym drift.
//
// The section is built around the thing that makes the measurement honest
// rather than around the headline: ~100 noisy per-pair slopes scatter even
// when every gym is identical, so the split between real variation and
// estimation noise has to come before any number is quoted. That is why the
// heterogeneity table renders above the effect sizes.
//
// Namespaced like every other v2 renderer: ids resolve through v2El/v2Id.

let V2_STRUCTURE = null;

async function loadV2Structure() {
  if (V2_STRUCTURE !== null) return V2_STRUCTURE;
  try {
    const r = await fetch('/static/v2_structure.json', { cache: 'no-cache' });
    V2_STRUCTURE = r.ok ? await r.json() : false;
  } catch (e) {
    V2_STRUCTURE = false;
  }
  return V2_STRUCTURE;
}

const v2sCard = (k, v, sub) => `<div class="card kpi-card">
    <div class="eyebrow">${k}</div>
    <div class="kpi-value kpi-value-small">${v}</div>
    <div class="kpi-sub">${sub}</div>
  </div>`;

// A heterogeneity result as a table, in the order the reasoning runs: what the
// boring explanation predicts, what would still be unsurprising, what was seen.
function v2sHetTable(h, unit) {
  return `<table class="data-table">
      <thead><tr><th>quantity</th><th class="num">value</th><th>what it means</th></tr></thead>
      <tbody>
        <tr><td>gym pairs measured</td><td class="num">${h.k}</td>
          <td>each gives one estimate, with its own error bar</td></tr>
        <tr><td>median error bar</td><td class="num">${h.se_median.toFixed(4)}</td>
          <td>${unit} — measured from the data, not assumed</td></tr>
        <tr><td><b>expected Q if pure noise</b> (df)</td><td class="num">${h.df}</td>
          <td>where Q lands when no real differences exist — a prediction, not a bar</td></tr>
        <tr><td>Q would exceed this only 5% of the time</td><td class="num">${h.crit.toFixed(1)}</td>
          <td>the 95th percentile of that prediction</td></tr>
        <tr class="row-emph"><td><b>Q actually measured</b></td>
          <td class="num"><b>${h.Q.toFixed(1)}</b></td>
          <td>${(h.Q / h.df).toFixed(1)}× the boring prediction${
            h.p < 1e-6 ? ' — probability by chance below 1 in a million' : ''}</td></tr>
        <tr><td>I² = (Q − df) / Q</td><td class="num">${h.i2.toFixed(0)}%</td>
          <td>share of the scatter that is real; higher means more signal</td></tr>
        <tr class="row-emph"><td><b>τ</b> = √((Q − df) / C)</td>
          <td class="num"><b>${h.tau.toFixed(4)}</b></td>
          <td>${unit} — the real spread, noise removed</td></tr>
        <tr><td>per-gym spread (τ / √2)</td><td class="num">${h.per_gym.toFixed(4)}</td>
          <td>a pairwise difference has √2 the spread of one gym</td></tr>
      </tbody></table>`;
}

function v2sPowerTable(h, unit) {
  const rows = h.power.map((p) => `<tr>
      <td class="num">${p.tau.toFixed(3)}</td>
      <td class="num">${(100 * p.power).toFixed(0)}%</td>
    </tr>`).join('');
  return `<table class="data-table">
      <thead><tr><th class="num">if the true τ were…</th>
        <th class="num">chance of catching it</th></tr></thead>
      <tbody>${rows}</tbody></table>`;
}

// ---- compression ----

function renderV2StructureCompression() {
  const S = V2_STRUCTURE;
  if (!S) return;
  const c = S.compression;
  const lin = c.linear;

  const kpi = v2El('structure-comp-kpi');
  if (kpi) {
    kpi.innerHTML = [
      v2sCard('gym pairs', String(lin.k),
        `≥40 shared climbers each, ${lin.n_obs.toLocaleString()} paired sends`),
      v2sCard('real spread τ', lin.tau.toFixed(3),
        'grades of correction per grade of ability'),
      v2sCard('detection floor', lin.floor.toFixed(3),
        `smallest τ this design would catch — measured is ${(lin.tau / lin.floor).toFixed(1)}× it`),
      v2sCard('signal share I²', `${lin.i2.toFixed(0)}%`,
        'the rest is estimation noise; higher is better'),
    ].join('');
  }

  const het = v2El('structure-comp-het');
  if (het) het.innerHTML = v2sHetTable(lin, 'grades per grade');

  const pw = v2El('structure-comp-power');
  if (pw) {
    pw.innerHTML = `<div class="split-tables">
      <div><h4>How small an effect could this have caught?</h4>
        ${v2sPowerTable(lin, 'grades per grade')}
        <p class="caption">80% power arrives at τ ≈ ${lin.floor.toFixed(3)}. A null
          result here would have meant “smaller than about ${lin.floor.toFixed(2)}”,
          never “zero”.</p></div>
      <div><h4>Is it an artifact of how ability was measured?</h4>
        <table class="data-table">
          <thead><tr><th></th><th class="num">first pass</th>
            <th class="num">artifact-free</th></tr></thead>
          <tbody>
            <tr><td>gym pairs</td><td class="num">${lin.k}</td>
              <td class="num">${c.clean.k}</td></tr>
            <tr><td>I²</td><td class="num">${lin.i2.toFixed(0)}%</td>
              <td class="num">${c.clean.i2.toFixed(0)}%</td></tr>
            <tr class="row-emph"><td><b>τ</b></td>
              <td class="num"><b>${lin.tau.toFixed(4)}</b></td>
              <td class="num"><b>${c.clean.tau.toFixed(4)}</b></td></tr>
          </tbody></table>
        <p class="caption">The first pass measures ability from the climber’s own
          mean grade, which includes the two sends whose difference is the
          outcome — enough to manufacture a slope if the two gyms differ in
          spread. The replication uses only the ${c.n_climbers_3plus.toLocaleString()}
          climbers at three or more gyms and measures ability from the gyms
          <i>outside</i> the pair, so it shares no noise with the outcome. The two
          agree to a rounding error.</p></div>
    </div>`;
  }

  const cur = v2El('structure-comp-curve');
  if (cur) {
    const q = c.curvature;
    cur.innerHTML = `<table class="data-table">
      <thead><tr><th>term</th><th class="num">Q / df</th><th class="num">I²</th>
        <th class="num">τ</th><th class="num">80% power at τ</th>
        <th class="num">effect at the edge of the range</th></tr></thead>
      <tbody>
        <tr><td><b>linear</b><div class="cell-sub">correction tilts with ability</div></td>
          <td class="num">${lin.Q.toFixed(1)} / ${lin.df}</td>
          <td class="num">${lin.i2.toFixed(0)}%</td>
          <td class="num">${lin.tau.toFixed(4)}</td>
          <td class="num">${lin.floor.toFixed(3)}</td>
          <td class="num">${(lin.per_gym * 2.5).toFixed(2)} grades</td></tr>
        <tr><td><b>quadratic</b><div class="cell-sub">correction curves with ability</div></td>
          <td class="num">${q.Q.toFixed(1)} / ${q.df}</td>
          <td class="num">${q.i2.toFixed(0)}%</td>
          <td class="num">${q.tau.toFixed(4)}</td>
          <td class="num">${q.floor.toFixed(3)}</td>
          <td class="num">${(q.per_gym * 6.25).toFixed(2)} grades</td></tr>
      </tbody></table>
      <div class="callout-note"><b>Both are real.</b> Curvature clears its critical
        value of ${q.crit.toFixed(1)} at ${q.Q.toFixed(1)}, and the design has better
        power for it than for the slope. At ±2.5 grades from the centre of the
        ability range, curvature contributes about two-thirds what the tilt does —
        too much to drop. A per-gym <i>straight line</i> is not enough; the target is
        a per-gym curve.</div>`;
  }
}

// ---- drift ----

function renderV2StructureDrift() {
  const S = V2_STRUCTURE;
  if (!S) return;
  const d = S.drift;
  const h = d.het;

  const kpi = v2El('structure-drift-kpi');
  if (kpi) {
    kpi.innerHTML = [
      v2sCard('dated sends', `${(d.n_sends / 1e6).toFixed(2)}M`,
        `${d.date_min} to ${d.date_max}, ${d.n_climbers.toLocaleString()} climbers`),
      v2sCard('real spread τ', d.het.tau.toFixed(3),
        'grades per year, between gym pairs'),
      v2sCard('per-gym drift', d.per_gym_sd.toFixed(3),
        `grades per year — over ${d.span_years} yr that is ${d.accumulated_span} grades`),
      v2sCard('detection floor', h.floor.toFixed(3),
        `measured τ is ${(h.tau / h.floor).toFixed(1)}× it`),
    ].join('');
  }

  const het = v2El('structure-drift-het');
  if (het) het.innerHTML = v2sHetTable(h, 'grades per year');

  const routes = v2El('structure-drift-routes');
  if (routes) {
    routes.innerHTML = `<table class="data-table">
      <thead><tr><th>route</th><th class="num">per-gym spread</th><th>what it does</th></tr></thead>
      <tbody>
        <tr><td><b>1 — divide τ by √2</b></td><td class="num">${d.route1.toFixed(4)}</td>
          <td>never forms a per-gym number at all; one division</td></tr>
        <tr><td><b>2 — solve the network</b></td><td class="num">${d.per_gym_sd.toFixed(4)}</td>
          <td>${d.connectivity.used_components[0]}-unknown weighted least squares,
            zero-sum, shrunk at the measured τ</td></tr>
      </tbody></table>
      <div class="callout-note">The two share their input data and almost nothing
        else, so agreeing to ${Math.abs(100 * (d.per_gym_sd - d.route1) / d.route1).toFixed(0)}%
        is a <b>check, not a finding</b> — if the network solve were broken, or τ
        computed wrongly, there is no reason they would land in the same place.</div>`;
  }

  const conn = v2El('structure-drift-connectivity');
  if (conn) {
    const c = d.connectivity;
    conn.innerHTML = `<table class="data-table">
      <thead><tr><th>threshold</th><th class="num">gym groups the network splits into</th>
        <th>can per-gym rates be recovered?</th></tr></thead>
      <tbody>
        <tr><td>≥${c.strict_threshold} paired observations</td>
          <td class="num">${c.strict_components.join(' · ')}</td>
          <td><span class="pill pill-warn">no</span> — nothing links the groups</td></tr>
        <tr class="row-emph"><td>≥${c.used_threshold} paired observations</td>
          <td class="num">${c.used_components.join(' · ')}</td>
          <td><span class="pill pill-ok">yes</span> — one connected network</td></tr>
      </tbody></table>
      <p class="caption">Every measurement here is a <i>difference</i> — no
        measurement gives one gym’s rate alone. Differences chain into per-gym
        numbers only if the comparisons connect. At the strict threshold they did
        not, and the unknowable gaps between groups leaked into the estimates:
        standard errors near 19,000 and a per-gym spread of 0.294 instead of
        ${d.per_gym_sd.toFixed(3)}. Least squares does not report unknowability —
        it reports enormous uncertainty and a plausible-looking answer.</p>`;
  }

  const rob = v2El('structure-drift-robust');
  if (rob) {
    const rows = d.robust.map((r) => `<tr${r.window_days === d.window_days && !r.gap_controlled
      ? ' class="row-emph"' : ''}>
        <td>${r.window_days}-day window</td>
        <td>${r.gap_controlled ? 'yes' : 'no'}</td>
        <td class="num">${r.gap_days}</td>
        <td class="num">${r.k}</td>
        <td class="num">${r.i2.toFixed(0)}%</td>
        <td class="num"><b>${r.tau.toFixed(4)}</b></td>
      </tr>`).join('');
    rob.innerHTML = `<table class="data-table">
      <thead><tr><th>window</th><th>date-gap controlled</th>
        <th class="num">mean gap (days)</th><th class="num">pairs</th>
        <th class="num">I²</th><th class="num">τ</th></tr></thead>
      <tbody>${rows}</tbody></table>`;
  }

  const lead = v2El('structure-drift-leaders');
  if (lead) {
    const row = (g) => `<tr><td>${g.name}</td>
        <td class="num">${g.rate >= 0 ? '+' : ''}${g.rate.toFixed(3)}
          <div class="cell-sub">± ${g.se.toFixed(3)}</div></td></tr>`;
    lead.innerHTML = `<div class="split-tables">
      <div><h4>Stiffening fastest</h4>
        <table class="data-table"><thead><tr><th>gym</th>
          <th class="num">grades / year</th></tr></thead>
          <tbody>${d.stiffening.map(row).join('')}</tbody></table>
        <p class="caption">Grades getting <i>harder to earn</i>: the same climber
          logs lower numbers than before.</p></div>
      <div><h4>Softening fastest</h4>
        <table class="data-table"><thead><tr><th>gym</th>
          <th class="num">grades / year</th></tr></thead>
          <tbody>${d.softening.map(row).join('')}</tbody></table>
        <p class="caption">Grades getting <i>easier to earn</i>. Neither direction is
          better or worse — it is not a quality measure.</p></div>
    </div>`;
  }

  const scale = v2El('structure-drift-scale');
  if (scale) {
    scale.innerHTML = `<table class="data-table">
      <thead><tr><th>question</th><th class="num">answer</th><th>why they differ</th></tr></thead>
      <tbody>
        <tr><td>How much does one gym move <b>over ${d.span_years} years</b>?</td>
          <td class="num"><b>${d.accumulated_span} grades</b></td>
          <td>large — against a total correction spread of
            ${d.correction_range.toFixed(2)} grades</td></tr>
        <tr><td>How much does drift distort a <b>comparison between gyms</b>?</td>
          <td class="num"><b>${d.accum_sd.toFixed(3)} grades</b></td>
          <td>small — the gyms are nearly contemporaneous</td></tr>
      </tbody></table>
      <div class="callout-note">Both are correct, and confusing them is easy.
        Drift only distorts a <i>comparison</i> to the extent the two gyms’ data sits
        at different points on the calendar — and here it barely does: gym mean
        dates differ by only <b>${d.mean_date_sd} years</b> (sd), about
        ${Math.round(100 * d.mean_date_sd / d.span_years)}% of the
        ${d.span_years}-year window. Drift is a big effect on the axis it acts on
        and a small one on the axis the corrections live on.</div>`;
  }

  const conf = v2El('structure-drift-confound');
  if (conf) {
    const cf = d.confound;
    conf.innerHTML = `<table class="data-table">
      <thead><tr><th>gym correction, regressed on when its climbers logged</th>
        <th class="num">slope</th><th class="num">r</th></tr></thead>
      <tbody>
        <tr><td>as measured</td><td class="num">${cf.raw_slope >= 0 ? '+' : ''}${cf.raw_slope}</td>
          <td class="num">${cf.raw_r >= 0 ? '+' : ''}${cf.raw_r}</td></tr>
        <tr><td>after subtracting each gym’s accumulated drift</td>
          <td class="num">${cf.adj_slope >= 0 ? '+' : ''}${cf.adj_slope}</td>
          <td class="num">${cf.adj_r >= 0 ? '+' : ''}${cf.adj_r}</td></tr>
      </tbody></table>
      <div class="callout-note"><b>Removing drift does not shrink the confound — it
        slightly enlarges it.</b> So relative drift is eliminated as its cause, and
        climber improvement accounts for only about 8%. One honest limit: this design
        measures <i>relative</i> drift only. Drift shared by every gym cancels in the
        differences, exactly as a global compression slope does, so an industry-wide
        trend would be invisible here and is not ruled out. The leading remaining
        candidate is selection — who climbs where, and when.</div>`;
  }
}

async function renderV2Structure() {
  const s = await loadV2Structure();
  const section = document.getElementById(v2Id('structure'));
  if (!s) {
    if (section) section.style.display = 'none';
    return;
  }
  if (section) section.style.display = '';
  renderV2StructureCompression();
  renderV2StructureDrift();
}
