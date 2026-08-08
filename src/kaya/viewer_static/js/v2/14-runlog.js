// ---- The lab notebook: what is running, what is next, everything already run
//
// v2_runlog.json is written by scripts/build_v2_runlog.py. Three layers with
// different half-lives, and the page keeps them visually separate because they
// carry different authority: a live snapshot, a hand-maintained plan, and the
// finished record read off disk.
//
// The status block is a SNAPSHOT, not a live feed. The viewer serves
// precomputed payloads and never computes on request, so the page states its
// build time plainly rather than implying it is watching anything.
//
// NOT namespaced through v2El/v2Id, unlike the other v2 renderers. Those exist
// because two tabs render the same components and element ids must be unique
// per document; this tab renders components nothing else does, so it owns its
// ids outright and resolves them directly. Going through v2El would have
// resolved them against whichever namespace the last tab happened to set.

let V2_RUNLOG = null;

const rlEl = (name) => document.getElementById(`gm-runlog-${name}`);

async function loadV2Runlog() {
  if (V2_RUNLOG !== null) return V2_RUNLOG;
  try {
    const r = await fetch('/static/v2_runlog.json', { cache: 'no-cache' });
    V2_RUNLOG = r.ok ? await r.json() : false;
  } catch (e) {
    V2_RUNLOG = false;
  }
  return V2_RUNLOG;
}

// Every state gets a colour and, more importantly, a plain-language gloss.
// "retired" in particular means something a reader cannot guess.
// [class, short pill label, the gloss underneath]. The pill has to stay short
// or it wraps the column; the gloss is where the meaning goes.
const V2_RL_STATE = {
  running: ['pill-ok', 'running', 'holding cores right now'],
  'running?': ['pill-warn', 'unclear', 'the scheduler thinks so; no process holds it'],
  done: ['pill-ok', 'done', 'finished'],
  pending: ['pill-warn', 'queued', 'behind the running fits'],
  waiting: ['pill-warn', 'waiting', 'for cores to free'],
  retired: ['pill-warn', 'retired', 'will be skipped — superseded'],
  skipped: ['pill-warn', 'skipped', 'its gate failed'],
  failed: ['pill-warn', 'failed', 'exited non-zero'],
};

const V2_RL_VERDICT = {
  worked: ['pill-ok', 'worked'],
  'in progress': ['pill-warn', 'in progress'],
  partial: ['pill-warn', 'partial'],
  'ruled out': ['pill-warn', 'ruled out'],
  failed: ['pill-warn', 'failed'],
  open: ['pill-warn', 'open'],
};

const V2_RL_NEXT_STATE = {
  next: ['pill-ok', 'next up'],
  blocked: ['pill-warn', 'blocked'],
  queued: ['pill-warn', 'queued'],
  parallel: ['pill-ok', 'can run in parallel'],
  deferred: ['pill-warn', 'deferred'],
};

function v2rlPill(map, key) {
  const [cls, label] = map[key] || ['pill-warn', key];
  return `<span class="pill ${cls}">${label}</span>`;
}

// ---- layer 1: now ----

function renderV2RunlogStatus() {
  const R = V2_RUNLOG;
  if (!R) return;
  const s = R.status;

  const head = rlEl('now-head');
  if (head) {
    const live = s.n_live;
    head.innerHTML = `<div class="callout-note">
      <b>Snapshot taken ${R.built_at}</b>, not a live feed — this page serves
      precomputed data and never computes on request. Right now that snapshot
      says <b>${live} fit${live === 1 ? '' : 's'} running</b> and the scheduler
      is <b>${s.scheduler_up ? 'up' : 'down'}</b>. Re-run
      <code>scripts/build_v2_runlog.py</code> to refresh it.</div>`;
  }

  const el = rlEl('now');
  if (!el) return;
  el.innerHTML = `<table class="data-table">
    <thead><tr><th>job</th><th>state</th><th class="num">elapsed</th>
      <th>what it is</th></tr></thead>
    <tbody>${s.jobs.map((j) => {
      const gloss = (V2_RL_STATE[j.state] || [])[2] || '';
      const t = j.elapsed || (j.minutes ? `${j.minutes} min` : '—');
      return `<tr><td><code>${j.job}</code></td>
        <td>${v2rlPill(V2_RL_STATE, j.state)}
          <div class="cell-sub">${gloss}</div></td>
        <td class="num">${t}</td>
        <td>${j.note || ''}</td></tr>`;
    }).join('')}</tbody></table>`;
}

// ---- layer 2: next ----

function renderV2RunlogNext() {
  const R = V2_RUNLOG;
  if (!R) return;
  const el = rlEl('next');
  if (!el) return;
  el.innerHTML = R.next.map((n, i) => `<div class="card runlog-card">
      <div class="runlog-card-head">
        <span class="runlog-step">${i + 1}</span>
        <h4>${n.title}</h4>
        ${v2rlPill(V2_RL_NEXT_STATE, n.state)}
      </div>
      <p class="prose">${n.why}</p>
      ${n.gate ? `<div class="callout-note"><b>Gate.</b> ${n.gate}</div>` : ''}
      ${n.blocker ? `<p class="caption"><b>Blocked by:</b> ${n.blocker}</p>` : ''}
      ${n.how ? `<p class="caption"><b>How:</b> ${n.how}</p>` : ''}
    </div>`).join('');
}

// ---- layer 3: the log ----

function renderV2RunlogTotals() {
  const R = V2_RUNLOG;
  if (!R) return;
  const el = rlEl('totals');
  if (!el) return;
  const t = R.totals;
  const card = (k, v, sub) => `<div class="card kpi-card">
      <div class="eyebrow">${k}</div>
      <div class="kpi-value kpi-value-small">${v}</div>
      <div class="kpi-sub">${sub}</div></div>`;
  el.innerHTML = [
    card('fits run', String(t.runs), 'every one with a result file on disk'),
    card('compute', `${t.hours} h`, 'sampling time, summed across all of them'),
    card('converged', `${t.converged} / ${t.runs}`,
      `R-hat ≤ ${R.bars.rhat} and effective sample size ≥ ${R.bars.ess}`),
    card('traces kept', `${t.trace_gb} GB`,
      `${t.traces} NetCDF files under runs/traces/`),
  ].join('');
}

function renderV2RunlogCampaigns() {
  const R = V2_RUNLOG;
  if (!R) return;
  const el = rlEl('campaigns');
  if (!el) return;
  el.innerHTML = R.campaigns.map((c) => `<div class="card runlog-card">
      <div class="runlog-card-head">
        <span class="runlog-step">${c.prefix}</span>
        <h4>${c.title}</h4>
        ${v2rlPill(V2_RL_VERDICT, c.verdict)}
      </div>
      <p class="prose"><b>Asked:</b> ${c.asked}</p>
      <p class="prose"><b>Found:</b> ${c.found}</p>
      <p class="caption">${c.n_runs} fit${c.n_runs === 1 ? '' : 's'},
        ${c.n_converged} converged, ${(c.minutes / 60).toFixed(1)} h of compute</p>
    </div>`).join('');
}

function renderV2RunlogMeasurements() {
  const R = V2_RUNLOG;
  if (!R) return;
  const el = rlEl('measurements');
  if (!el) return;
  el.innerHTML = R.measurements.map((m) => `<div class="card runlog-card">
      <div class="runlog-card-head">
        <h4>${m.title}</h4>
        ${v2rlPill(V2_RL_VERDICT, m.verdict)}
        <span class="cell-sub">${m.when}</span>
      </div>
      <p class="prose"><b>How:</b> ${m.how}</p>
      <p class="prose"><b>Found:</b> ${m.found}</p>
      <p class="caption"><b>Where:</b> <code>${m.where}</code></p>
    </div>`).join('');
}

function renderV2RunlogTable() {
  const R = V2_RUNLOG;
  if (!R) return;
  const el = rlEl('table');
  if (!el) return;
  const num = (v, d = 0) => (v === null || v === undefined ? '—' : v.toFixed(d));
  const flag = (ok, v, d) => (v === null || v === undefined ? '—'
    : `${v.toFixed(d)} <span class="pill ${ok ? 'pill-ok' : 'pill-warn'}">${
      ok ? 'pass' : 'under'}</span>`);
  const rows = R.runs.slice().reverse().map((r) => {
    const tags = [];
    if (r.marginalize_all) tags.push('quadrature');
    if (r.centered) tags.push('centered');
    if (r.orthogonal) tags.push('orthogonal');
    return `<tr>
      <td><code>${r.name}</code>
        <div class="cell-sub">${r.form || '—'}${
          tags.length ? ` · ${tags.join(' · ')}` : ''}</div></td>
      <td class="num">${r.chains ?? '—'}×${r.draws ?? '—'}
        <div class="cell-sub">tune ${r.tune ?? '—'}</div></td>
      <td class="num">${flag(r.rhat_ok, r.rhat, 3)}</td>
      <td class="num">${flag(r.ess_ok, r.ess, 0)}</td>
      <td class="num">${r.divergences ?? '—'}</td>
      <td class="num">${r.depth_frac === null || r.depth_frac === undefined
        ? '—' : `${(100 * r.depth_frac).toFixed(0)}%`}</td>
      <td class="num">${r.elpd === null || r.elpd === undefined
        ? '—' : r.elpd.toFixed(0)}
        ${r.loo_unit ? `<div class="cell-sub">per ${r.loo_unit}</div>` : ''}</td>
      <td class="num">${num(r.minutes, 0)}</td>
      <td class="cell-sub">${r.trace_path
        ? `${r.trace_path.split('/').pop()}<br>${r.trace_mb} MB`
        : 'no trace'}</td>
    </tr>`;
  }).join('');
  el.innerHTML = `<thead><tr>
      <th>fit</th>
      <th class="num">chains×draws</th>
      <th class="num">R-hat<div class="cell-sub">bar ${R.bars.rhat}, lower better</div></th>
      <th class="num">eff. draws<div class="cell-sub">floor ${R.bars.ess}, higher better</div></th>
      <th class="num">div.<div class="cell-sub">lower better</div></th>
      <th class="num">at depth cap<div class="cell-sub">lower better</div></th>
      <th class="num">elpd<div class="cell-sub">higher better</div></th>
      <th class="num">min</th>
      <th>trace file</th>
    </tr></thead><tbody>${rows}</tbody>`;
}

async function renderV2Runlog() {
  const r = await loadV2Runlog();
  const tab = document.getElementById('tab-run-log');
  if (!r) {
    if (tab) tab.style.display = 'none';
    return;
  }
  const stamp = document.getElementById('gm-runlog-built');
  if (stamp) stamp.textContent = `Snapshot ${r.built_at} · ${r.totals.runs} fits · `
    + `${r.totals.hours} h of compute`;
  renderV2RunlogStatus();
  renderV2RunlogNext();
  renderV2RunlogTotals();
  renderV2RunlogCampaigns();
  renderV2RunlogMeasurements();
  renderV2RunlogTable();
}
