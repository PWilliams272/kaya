// ---- Ability Explorer (Grading Model tab, interactive prototype) ----
// Coefficients are posterior MEAN point estimates from the validated 6-gym
// LA network fits -- this widget deliberately carries no uncertainty, it's a
// quick "what-if" calculator, not a substitute for the credible-interval
// results shown in the write-up around it.
const EXPLORER_GYMS = [
  { id: '260', name: 'Cliffs of Id', correction: 0.0727 },
  { id: '122', name: 'Hollywood Boulders', correction: 0.0712 },
  { id: '261', name: 'Verdigo Boulders', correction: -0.0707 },
  { id: '1100', name: 'Class 5', correction: -0.1188 },
  { id: '901', name: 'The Post', correction: -0.0387 },
  { id: '257', name: 'LA Boulders', correction: 0.0863 },
];

const EXPLORER_COEFFICIENTS = {
  betaGender: -0.9988,
  delta1: 0.0861, // ape index, linear term
  delta2: 0.0023, // ape index, quadratic term
  hMedian: 68,
  aMedian: 0,
  // Height term: final functional-form decision -- plain quadratic (not
  // vertex-reparameterized, not a bump) with a gender interaction. The
  // earlier bump-function fit was visually rejected: its gender-split
  // version had left/right widths differing 8x, which isn't an "optimal
  // height" bell, it's a cliff into a plateau. Result: no credible height
  // effect for male users (gamma1, gamma2 both include zero); a
  // real quadratic effect for female users, but its vertex falls
  // right at the bottom edge of the observed height range (~58in), so in
  // practice it reads as an accelerating "taller is better" trend, not a
  // peak in the middle.
  height: {
    male: { gamma1: -0.0028, gamma2: -0.0006 },
    female: { gamma1: 0.0654, gamma2: 0.0033 },
  },
};

function explorerHeightTerm(heightIn, genderCode) {
  const hc = heightIn - EXPLORER_COEFFICIENTS.hMedian;
  const coef = genderCode === 1 ? EXPLORER_COEFFICIENTS.height.female : EXPLORER_COEFFICIENTS.height.male;
  return coef.gamma1 * hc + coef.gamma2 * hc * hc;
}

function explorerAbility(heightIn, apeIn, genderCode, gymCorrection) {
  const ac = apeIn - EXPLORER_COEFFICIENTS.aMedian;
  const genderTerm = EXPLORER_COEFFICIENTS.betaGender * genderCode;
  const heightTerm = explorerHeightTerm(heightIn, genderCode);
  const apeTerm = EXPLORER_COEFFICIENTS.delta1 * ac + EXPLORER_COEFFICIENTS.delta2 * ac * ac;
  return {
    total: genderTerm + heightTerm + apeTerm + gymCorrection,
    genderTerm,
    heightTerm,
    apeTerm,
    gymTerm: gymCorrection,
  };
}

function populateExplorerGymSelect() {
  const select = document.getElementById('explorer-gym-select');
  if (!select || select.dataset.populated) {
    return;
  }
  EXPLORER_GYMS.forEach((gym) => {
    const opt = document.createElement('option');
    opt.value = gym.id;
    opt.textContent = gym.name;
    select.appendChild(opt);
  });
  select.dataset.populated = 'true';
}

function currentExplorerState() {
  const genderBtn = document.querySelector('#explorer-gender-toggle .segmented-toggle-btn.active');
  const genderCode = genderBtn ? Number(genderBtn.dataset.value) : 0;
  const height = Number(document.getElementById('explorer-height-slider').value);
  const ape = Number(document.getElementById('explorer-ape-slider').value);
  const gymId = document.getElementById('explorer-gym-select').value;
  const gym = EXPLORER_GYMS.find((g) => g.id === gymId) || EXPLORER_GYMS[0];
  return { genderCode, height, ape, gym };
}

function renderExplorerBreakdown(result) {
  const rows = [
    { label: 'Gender', value: result.genderTerm },
    { label: 'Height', value: result.heightTerm },
    { label: 'Ape index', value: result.apeTerm },
    { label: 'Gym', value: result.gymTerm },
  ];
  const maxAbs = Math.max(...rows.map((row) => Math.abs(row.value)), 0.05);
  const container = document.getElementById('explorer-breakdown');
  container.innerHTML = '';
  rows.forEach((row) => {
    const pct = Math.min(100, (Math.abs(row.value) / maxAbs) * 50);
    const wrap = document.createElement('div');
    wrap.className = 'explorer-breakdown-row';
    wrap.innerHTML = `
      <span class="explorer-breakdown-label">${row.label}</span>
      <span class="explorer-breakdown-track">
        <span class="explorer-breakdown-fill ${row.value >= 0 ? 'positive' : 'negative'}" style="width:${pct}%"></span>
      </span>
      <span class="explorer-breakdown-value">${row.value >= 0 ? '+' : ''}${row.value.toFixed(3)}</span>
    `;
    container.appendChild(wrap);
  });
}

function renderExplorerChart(state) {
  const heights = [];
  for (let h = 59; h <= 76; h += 0.5) {
    heights.push(h);
  }
  const yValues = heights.map((h) => explorerHeightTerm(h, state.genderCode));
  const curveTrace = {
    x: heights,
    y: yValues,
    type: 'scatter',
    mode: 'lines',
    line: { color: cssVar('--lg-gold'), width: 2.5 },
    name: state.genderCode === 1 ? 'Female users' : 'Male users',
  };
  const markerTrace = {
    x: [state.height],
    y: [explorerHeightTerm(state.height, state.genderCode)],
    type: 'scatter',
    mode: 'markers',
    marker: { color: cssVar('--lg-highlight'), size: 11, line: { color: cssVar('--lg-card'), width: 2 } },
    showlegend: false,
  };
  Plotly.react(
    'explorer-chart',
    [curveTrace, markerTrace],
    { ...chartLayout('Height (inches)'), showlegend: false, margin: { l: 48, r: 16, t: 10, b: 40 } },
    { responsive: true, displayModeBar: false },
  );
}

function refreshExplorer() {
  const state = currentExplorerState();
  const result = explorerAbility(state.height, state.ape, state.genderCode, state.gym.correction);
  document.getElementById('explorer-height-value').textContent = `${state.height} in`;
  document.getElementById('explorer-ape-value').textContent = `${state.ape} in`;
  const totalEl = document.getElementById('explorer-total-value');
  totalEl.textContent = `${result.total >= 0 ? '+' : ''}${result.total.toFixed(2)}`;
  renderExplorerBreakdown(result);
  renderExplorerChart(state);
}

function bindExplorerControls() {
  const wrap = document.getElementById('explorer-gender-toggle');
  if (!wrap || wrap.dataset.bound) {
    return;
  }
  populateExplorerGymSelect();
  wrap.querySelectorAll('.segmented-toggle-btn').forEach((btn) => {
    btn.addEventListener('click', () => {
      wrap.querySelectorAll('.segmented-toggle-btn').forEach((b) => b.classList.remove('active'));
      btn.classList.add('active');
      refreshExplorer();
    });
  });
  document.getElementById('explorer-height-slider').addEventListener('input', refreshExplorer);
  document.getElementById('explorer-ape-slider').addEventListener('input', refreshExplorer);
  document.getElementById('explorer-gym-select').addEventListener('change', refreshExplorer);
  wrap.dataset.bound = 'true';
}

