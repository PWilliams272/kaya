// ---- Every Tracked Gym: the full directory, on the Data Overview tab ----
//
// "Sends by Gym" above it is a bar chart, which answers "who is biggest" and
// nothing else. This answers the questions that actually come up when deciding
// what to pull next: is this gym in the model, when did its data start, and has
// it stopped reporting.
//
// Two sources, both already fetched:
//   * gyms.json  — every gym in the pull (currently 90), with counts and dates
//   * v2_results.json — the 29 that survive into the grading model, with the
//     company and the fitted correction
//
// The gap between those two numbers is the point of the table. A gym is only
// modellable if enough climbers visit it AND another gym in the network, so
// most pulled gyms are not in the model, and it is not obvious which from any
// other view on the site.

let GYM_DIR_SORT = { key: 'send_count', dir: -1 };

const GYM_DIR_COLUMNS = [
  { key: 'rank', label: '#', sortable: false },
  { key: 'gym_name', label: 'gym', align: 'label' },
  { key: 'gym_id', label: 'id' },
  { key: 'send_count', label: 'sends', num: true },
  { key: 'unique_users', label: 'climbers', num: true },
  { key: 'first_send', label: 'first send' },
  { key: 'last_send', label: 'last send' },
  { key: 'correction', label: 'grading correction', num: true },
];

// A gym's operating company. v2_results carries it for the 29 modelled gyms;
// for the rest it is read off the name, which is how the chains actually brand
// themselves ("Movement Santa Clara", "Touchstone Cliffs of Id").
const GYM_DIR_BRANDS = ['Movement', 'Touchstone', 'Bouldering Project', 'Stronghold',
  'Sender One', 'Earth Treks', 'Planet Granite', 'Vertical World', 'Momentum'];

function gymBrand(name) {
  const hit = GYM_DIR_BRANDS.find((b) => (name || '').toLowerCase().includes(b.toLowerCase()));
  return hit || '';
}

function gymDirectoryRows() {
  const gyms = appState.data.gyms || [];
  // V2_RESULTS is populated by loadV2Results(); if the explainer payload is
  // missing the table still renders, just without the model columns.
  const modelled = new Map(
    ((typeof V2_RESULTS !== 'undefined' && V2_RESULTS && V2_RESULTS.gyms) || [])
      .map((g) => [String(g.i), g]),
  );
  return gyms.map((g) => {
    const m = modelled.get(String(g.gym_id));
    return {
      gym_id: String(g.gym_id),
      gym_name: g.gym_name || String(g.gym_id),
      send_count: Number(g.send_count) || 0,
      unique_users: g.unique_users == null ? null : Number(g.unique_users),
      first_send: g.first_send || null,
      last_send: g.last_send || null,
      brand: (m && m.b) || gymBrand(g.gym_name),
      inModel: !!m,
      correction: m ? m.m : null,
      credible: m ? m.s : null,
    };
  });
}

function gymDirectoryFiltered(rows) {
  const q = (document.getElementById('gym-directory-search')?.value || '')
    .trim().toLowerCase();
  const scope = document.getElementById('gym-directory-scope')?.value || 'all';
  return rows.filter((r) => {
    if (scope === 'model' && !r.inModel) return false;
    if (scope === 'other' && r.inModel) return false;
    if (!q) return true;
    return `${r.gym_name} ${r.brand} ${r.gym_id}`.toLowerCase().includes(q);
  });
}

function gymDirectorySorted(rows) {
  const { key, dir } = GYM_DIR_SORT;
  return rows.slice().sort((a, b) => {
    const x = a[key];
    const y = b[key];
    // Nulls last regardless of direction: a gym with no fitted correction is
    // not "the smallest correction", it is a different kind of row.
    if (x == null && y == null) return 0;
    if (x == null) return 1;
    if (y == null) return -1;
    if (typeof x === 'number' && typeof y === 'number') return (x - y) * dir;
    return String(x).localeCompare(String(y)) * dir;
  });
}

function renderGymDirectory() {
  const table = document.getElementById('gym-directory-table');
  if (!table) return;
  const all = gymDirectoryRows();
  const rows = gymDirectorySorted(gymDirectoryFiltered(all));
  const totalSends = all.reduce((s, r) => s + r.send_count, 0);

  const head = GYM_DIR_COLUMNS.map((c) => {
    if (c.sortable === false) return `<th>${c.label}</th>`;
    const active = GYM_DIR_SORT.key === c.key;
    const arrow = active ? (GYM_DIR_SORT.dir === -1 ? ' &darr;' : ' &uarr;') : '';
    return `<th><button type="button" class="table-sort${active ? ' active' : ''}" `
      + `data-sort="${c.key}">${c.label}${arrow}</button></th>`;
  }).join('');

  const body = rows.map((r, i) => {
    const share = totalSends ? (100 * r.send_count / totalSends) : 0;
    const corr = r.correction == null
      ? '<span class="muted">not modelled</span>'
      : `${r.correction > 0 ? '+' : '−'}${Math.abs(r.correction).toFixed(2)}`
        + (r.credible ? '' : ' <span class="muted">(inside noise)</span>');
    return `<tr>
      <td class="unit">${i + 1}</td>
      <td class="label-cell">${r.gym_name}${r.brand
        ? ` <span class="muted">· ${r.brand}</span>` : ''}</td>
      <td class="unit">${r.gym_id}</td>
      <td class="unit">${formatNumber(r.send_count)} <span class="muted">${share.toFixed(1)}%</span></td>
      <td class="unit">${r.unique_users == null ? '&mdash;' : formatNumber(r.unique_users)}</td>
      <td class="unit">${r.first_send || '&mdash;'}</td>
      <td class="unit">${r.last_send || '&mdash;'}</td>
      <td class="unit">${corr}</td>
    </tr>`;
  }).join('');

  table.innerHTML = `<thead><tr>${head}</tr></thead><tbody>${body}</tbody>`;
  table.querySelectorAll('.table-sort').forEach((btn) => {
    btn.addEventListener('click', () => {
      const key = btn.dataset.sort;
      // Text sorts want A-Z first; numbers and dates want largest first.
      const textual = key === 'gym_name' || key === 'gym_id';
      GYM_DIR_SORT = GYM_DIR_SORT.key === key
        ? { key, dir: -GYM_DIR_SORT.dir }
        : { key, dir: textual ? 1 : -1 };
      renderGymDirectory();
    });
  });

  const note = document.getElementById('gym-directory-note');
  if (note) {
    const nModel = all.filter((r) => r.inModel).length;
    note.innerHTML = `<b>${all.length}</b> gyms are pulled. `
      + (nModel
        ? `<b>${nModel}</b> of them carry a grading correction &mdash; a gym is only `
          + 'modellable when it shares enough climbers with another gym in the '
          + 'network for the two to be told apart. The rest are pulled and stored, '
          + 'but no correction can be estimated for them yet.'
        : 'Model results are not loaded, so the correction column is empty.');
  }
  const foot = document.getElementById('gym-directory-foot');
  if (foot) {
    const shown = rows.length;
    const stale = all.filter((r) => r.last_send && r.last_send < gymDirectoryStaleCutoff());
    foot.innerHTML = `Showing <b>${shown}</b> of ${all.length}. `
      + (stale.length
        ? `<b>${stale.length}</b> ${stale.length === 1 ? 'gym has' : 'gyms have'} `
          + 'logged nothing in the last 30 days &mdash; sort by <i>last send</i> to '
          + 'find them. That is usually the updater, not the gym. '
        : '')
      + 'Counts and dates come from the pulled send logs; corrections come from the '
      + 'fitted model on the Findings tab.';
  }
}

// Relative to the newest send anywhere, not to today: the viewer is often
// looking at a cache built hours or days ago, and measuring staleness against
// the wall clock would flag every gym at once the moment the pull fell behind.
function gymDirectoryStaleCutoff() {
  const gyms = appState.data.gyms || [];
  const newest = gyms.reduce((m, g) => (g.last_send && g.last_send > m ? g.last_send : m), '');
  if (!newest) return '';
  const d = new Date(`${newest}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() - 30);
  return d.toISOString().slice(0, 10);
}

function bindGymDirectory() {
  if (bindGymDirectory.done) return;
  bindGymDirectory.done = true;
  ['gym-directory-search', 'gym-directory-scope'].forEach((id) => {
    const el = document.getElementById(id);
    if (el) el.addEventListener('input', renderGymDirectory);
  });
}
