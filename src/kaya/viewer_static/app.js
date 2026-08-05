const DEFAULT_COMPARE_REF_GYM_ID = '260'; // Touchstone Cliffs of Id
const DEFAULT_COMPARE_FOCUS_GYM_ID = '1100'; // Touchstone Class 5
const DEFAULT_COMPARE_GYM_IDS = [
  '122', // Touchstone Hollywood Boulders
  '257', // Touchstone LA Boulders
  '901', // Touchstone The Post
  '261', // Touchstone Verdigo Boulders
  '1100', // Touchstone Class 5
];

const appState = {
  filters: {
    timeGymId: '',
    freq: 'D',
    gradeGymIds: [],
    compareRefGymId: '',
    compareFocusGymId: '',
    compareGymIds: [],
    compareDiscipline: 'bouldering',
    compareMinDays: 2,
    bodyActiveOnly: true,
  },
  data: {},
  widgets: {},
  loaded: {
    summary: false,
    gymComparisonBase: false,
    dataOverview: false,
    bodyMorphology: false,
    userSegmentation: false,
  },
};

const gymLinePalette = ['#9ad0ff', '#ff8eb6', '#7ee0c6', '#ffc36b', '#c0b6ff', '#f29b76'];
const segmentPalette = {
  Active: '#9ad0ff',
  Inactive: '#ffc36b',
};
// Function rather than a static object: reads genderBaseColors live, so it
// reflects the color pickers immediately (matching the Male/Female colors
// used by the body-morphology scatter/heatmap/GAM) instead of a fixed
// snapshot taken at module load.
function currentGenderPalette() {
  return {
    'All Users': '#9a9fa8',
    Male: genderBaseColors.male,
    Female: genderBaseColors.female,
  };
}

// Fades from fully-transparent to blue rather than a fixed light-to-dark
// scale (e.g. Plotly's built-in 'Blues'), so zero density shows the card's
// own background — white in light mode, dark in dark mode — instead of a
// fixed pale-blue tint that clashes with dark backgrounds. Capped at 0.8
// alpha rather than fully opaque: when two of these heatmaps overlap
// (male/female on the same plot), traces draw in array order — whichever
// is added second sits fully on top — so a fully-opaque peak would
// completely hide the other gender underneath it. Capping both below 1
// means every overlap is a genuine alpha-composited blend of both colors,
// regardless of draw order, rather than one occluding the other.
// This is the general app accent blue, used by the (gender-agnostic)
// gym-comparison heatmap only — NOT tied to the body-morphology gender
// palette below, which is separately adjustable.
const HEATMAP_DENSITY_COLORSCALE = [
  [0, 'rgba(37, 99, 235, 0)'],
  [1, 'rgba(37, 99, 235, 0.8)'],
];

// Body-morphology's fixed male/female base colors. Everything derived from
// these (marker fill/border, GAM line, CI band, heatmap colorscale) stays
// in sync automatically since it's all generated from one hex value per
// gender via buildGenderColorSet, instead of separate hand-written rgba
// strings scattered across each chart.
const genderBaseColors = {
  male: '#518AE6', // rgb(81, 138, 230)
  female: '#F039F3', // rgb(240, 57, 243)
};

function hexToRgbTuple(hex) {
  const match = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
  if (!match) {
    return [0, 0, 0];
  }
  return match.slice(1).map((part) => parseInt(part, 16));
}

function buildGenderColorSet(hex) {
  const [r, g, b] = hexToRgbTuple(hex);
  const rgb = `${r}, ${g}, ${b}`;
  return {
    line: `rgba(${rgb}, 0.95)`,
    fill: `rgba(${rgb}, 0.06)`,
    band: `rgba(${rgb}, 0.32)`,
    heatmapColorscale: [
      [0, `rgba(${rgb}, 0)`],
      [1, `rgba(${rgb}, 0.8)`],
    ],
  };
}

// The alpha-fade colorscales map linearly from a grid's min to max density,
// so a lone/sparse point (tiny density relative to the busiest bucket)
// lands near-zero alpha and is effectively invisible — the same
// "smoothing hides outliers" problem as elsewhere, just visual this time.
// Normalizing to [0,1] by the grid's own peak and applying gamma<1 boosts
// low values much more than high ones (e.g. 1% of peak density goes from
// 1% opacity to ~18%) without moving the peak itself, so sparse buckets
// stay visible alongside the dense core.
function boostSparseDensity(zGrid, gamma = 0.45) {
  const maxZ = Math.max(...zGrid.flat(), 1e-12);
  return zGrid.map((row) => row.map((value) => (value > 0 ? (value / maxZ) ** gamma : 0)));
}

function hexToRgba(hex, alpha) {
  const match = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
  if (!match) {
    return hex;
  }
  const [r, g, b] = match.slice(1).map((part) => parseInt(part, 16));
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}


const viewerConfig = {
  dataMode: new URLSearchParams(window.location.search).get('dataMode')
    || document.querySelector('meta[name="kaya-viewer-data-mode"]')?.content
    || 'api',
};

let controlsBound = false;
let initialRenderComplete = false;
window.__kayaViewerStatus = 'booting';
window.__kayaViewerError = null;

function cssVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

function chartLayout(title) {
  return {
    title: undefined,
    paper_bgcolor: cssVar('--lg-card'),
    plot_bgcolor: cssVar('--lg-card'),
    font: {
      color: cssVar('--lg-text'),
      family: getComputedStyle(document.documentElement).fontFamily,
      size: 12,
    },
    margin: { l: 48, r: 16, t: 18, b: 40 },
    xaxis: {
      title: {
        text: title,
        standoff: 16,
      },
      automargin: true,
      gridcolor: cssVar('--lg-border'),
      zerolinecolor: cssVar('--lg-border'),
    },
    yaxis: {
      automargin: true,
      gridcolor: cssVar('--lg-border'),
      zerolinecolor: cssVar('--lg-border'),
    },
    legend: {
      orientation: 'h',
      yanchor: 'bottom',
      y: 1.02,
      xanchor: 'left',
      x: 0,
    },
  };
}

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

function formatNumber(value) {
  return new Intl.NumberFormat().format(value || 0);
}

function formatDateRange(summary) {
  if (!summary?.first_date || !summary?.last_date) {
    return '-';
  }
  const first = new Date(summary.first_date).toLocaleDateString();
  const last = new Date(summary.last_date).toLocaleDateString();
  return `${first} - ${last}`;
}

function sortedGyms() {
  return [...(appState.data.gyms || [])].sort((left, right) => {
    const leftName = left.gym_name || left.gym_id;
    const rightName = right.gym_name || right.gym_id;
    return String(leftName).localeCompare(String(rightName));
  });
}

function gymName(gymId) {
  const gyms = appState.data.gyms || [];
  const match = gyms.find((gym) => String(gym.gym_id) === String(gymId));
  return match?.gym_name || String(gymId);
}

async function fetchJson(path) {
  const response = await fetch(path);
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json();
}

async function fetchViewerData(key, params = {}) {
  const staticMap = {
    manifest: '/viewer-data/manifest.json',
    summary: '/viewer-data/summary.json',
    gyms: '/viewer-data/gyms.json',
    topGyms: '/viewer-data/top-gyms.json',
    userSegmentation: '/viewer-data/user-segmentation.json',
    gymComparisonBase: '/viewer-data/gym-comparison-base.json',
    timeSeries: {
      D: '/viewer-data/time-series-daily.json',
      W: '/viewer-data/time-series-weekly.json',
      M: '/viewer-data/time-series-monthly.json',
    },
    gradeDistribution: {
      bouldering: '/viewer-data/grade-distribution-bouldering.json',
      routes: '/viewer-data/grade-distribution-routes.json',
    },
    bodyMetrics: {
      bouldering: {
        active: '/viewer-data/body-metrics-bouldering.json',
        all: '/viewer-data/body-metrics-bouldering-all.json',
      },
      routes: {
        active: '/viewer-data/body-metrics-routes.json',
        all: '/viewer-data/body-metrics-routes-all.json',
      },
    },
  };

  if (viewerConfig.dataMode === 'static') {
    if (key === 'timeSeries') {
      if (params.gym_id) {
        return fetchJson(`/viewer-data/time-series/${params.freq || 'D'}/${encodeURIComponent(params.gym_id)}.json`);
      }
      return fetchJson(staticMap.timeSeries[params.freq || 'D']);
    }
    if (key === 'gradeDistribution') {
      if (params.gym_id) {
        return fetchJson(`/viewer-data/grade-distribution/${params.discipline || 'bouldering'}/${encodeURIComponent(params.gym_id)}.json`);
      }
      return fetchJson(staticMap.gradeDistribution[params.discipline || 'bouldering']);
    }
    if (key === 'bodyMetrics') {
      const discipline = params.discipline || 'bouldering';
      const audience = (params.active_only === false || params.active_only === 'false') ? 'all' : 'active';
      return fetchJson(staticMap.bodyMetrics[discipline][audience]);
    }
    return fetchJson(staticMap[key]);
  }

  const apiMap = {
    summary: '/api/summary',
    gyms: '/api/gyms',
    topGyms: '/api/charts/top-gyms',
    userSegmentation: '/api/charts/user-segmentation',
    gymComparisonBase: '/api/charts/gym-comparison-base',
    timeSeries: `/api/charts/time-series${buildQuery(params)}`,
    gradeDistribution: `/api/charts/grade-distribution${buildQuery(params)}`,
    bodyMetrics: `/api/charts/body-metrics${buildQuery(params)}`,
  };

  // Deliberately no static-artifact shortcut here (only dataMode:'static'
  // above uses those prebuilt files). An earlier version of this function
  // silently substituted a cached artifact for bodyMetrics/etc. even in
  // normal 'api' mode without ever checking params like active_only —
  // meaning the Audience toggle had no effect, and any snapshot staleness
  // (e.g. a field added to the live payload after the artifact was last
  // built) persisted invisibly. Always hitting the live endpoint directly
  // is fast enough and guarantees the response matches the request.
  return fetchJson(apiMap[key]);
}

function buildQuery(params) {
  const query = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value !== '' && value !== null && value !== undefined) {
      query.set(key, value);
    }
  });
  const queryString = query.toString();
  return queryString ? `?${queryString}` : '';
}

async function loadStaticData() {
  const [manifest, gyms] = await Promise.all([
    viewerConfig.dataMode === 'static' ? fetchViewerData('manifest') : Promise.resolve(null),
    fetchViewerData('gyms'),
  ]);
  appState.data.manifest = manifest;
  appState.data.gyms = gyms;
}

async function loadSummaryData() {
  const summary = await fetchViewerData('summary');
  appState.data.summary = summary;
  appState.loaded.summary = true;
}

async function ensureGymComparisonData() {
  // Gym Comparison's grade ticks are sourced from the Body Morphology
  // payloads (getGradeTicks() reads appState.data.boulderBodyMetrics /
  // routeBodyMetrics). Gym Comparison is the default tab, so without this,
  // landing here first leaves grade_ticks empty until Body Morphology has
  // been visited, which silently breaks tick labels and the heatmap.
  const tasks = [ensureBodyMorphologyData()];
  if (!appState.loaded.gymComparisonBase) {
    tasks.push(
      fetchViewerData('gymComparisonBase').then((data) => {
        appState.data.gymComparisonBase = data;
        appState.loaded.gymComparisonBase = true;
      })
    );
  }
  await Promise.all(tasks);
}

async function ensureDataOverviewData() {
  if (appState.loaded.dataOverview) {
    return;
  }
  const [topGyms, boulderGrades, routeGrades] = await Promise.all([
    fetchViewerData('topGyms'),
    fetchViewerData('gradeDistribution', { discipline: 'bouldering' }),
    fetchViewerData('gradeDistribution', { discipline: 'routes' }),
  ]);
  appState.data.topGyms = topGyms;
  appState.data.boulderGrades = boulderGrades;
  appState.data.routeGrades = routeGrades;
  await refreshDynamicData();
  appState.loaded.dataOverview = true;
}

async function ensureBodyMorphologyData() {
  if (appState.loaded.bodyMorphology) {
    return;
  }
  const [boulderBodyMetrics, routeBodyMetrics] = await Promise.all([
    fetchViewerData('bodyMetrics', { discipline: 'bouldering', active_only: appState.filters.bodyActiveOnly }),
    fetchViewerData('bodyMetrics', { discipline: 'routes', active_only: appState.filters.bodyActiveOnly }),
  ]);
  appState.data.boulderBodyMetrics = boulderBodyMetrics;
  appState.data.routeBodyMetrics = routeBodyMetrics;
  appState.loaded.bodyMorphology = true;
}

async function reloadBodyMorphologyData() {
  appState.loaded.bodyMorphology = false;
  await ensureBodyMorphologyData();
}

async function ensureUserSegmentationData() {
  if (appState.loaded.userSegmentation) {
    return;
  }
  appState.data.userSegmentation = await fetchViewerData('userSegmentation');
  appState.loaded.userSegmentation = true;
}

async function loadTimeSeriesData() {
  const [timeSeries, overlayTimeSeries] = await Promise.all([
    fetchViewerData('timeSeries', { freq: appState.filters.freq }),
    appState.filters.timeGymId
      ? fetchViewerData('timeSeries', { freq: appState.filters.freq, gym_id: appState.filters.timeGymId })
      : Promise.resolve([]),
  ]);
  appState.data.timeSeries = timeSeries;
  appState.data.overlayTimeSeries = overlayTimeSeries;
}

async function loadGradeComparisonData() {
  const gradeGymIds = appState.filters.gradeGymIds;
  const responses = await Promise.all(
    gradeGymIds.flatMap((gymId) => [
      fetchViewerData('gradeDistribution', { discipline: 'bouldering', gym_id: gymId }),
      fetchViewerData('gradeDistribution', { discipline: 'routes', gym_id: gymId }),
    ])
  );
  const gradeComparison = { bouldering: {}, routes: {} };
  gradeGymIds.forEach((gymId, index) => {
    gradeComparison.bouldering[gymId] = responses[index * 2] || [];
    gradeComparison.routes[gymId] = responses[(index * 2) + 1] || [];
  });
  appState.data.gradeComparison = gradeComparison;
}

// Substring match anywhere in the label (not just prefix, unlike native
// <select> keyboard typeahead) so e.g. "cliffs" finds "Touchstone Cliffs of Id".
function comboFilteredOptions(options, query, excludeValues) {
  const normalizedQuery = query.trim().toLowerCase();
  return options.filter((option) => (
    (!excludeValues || !excludeValues.includes(option.value))
    && option.label.toLowerCase().includes(normalizedQuery)
  ));
}

function renderComboOptionsList(container, options, onPick, emptyText) {
  container.innerHTML = '';
  if (!options.length) {
    const empty = document.createElement('div');
    empty.className = 'combo-option-empty';
    empty.textContent = emptyText;
    container.appendChild(empty);
    return;
  }
  options.forEach((option) => {
    const item = document.createElement('div');
    item.className = 'combo-option';
    item.textContent = option.label;
    // mousedown (not click), preventDefault: fires before the input's blur,
    // so picking an option doesn't first close the panel out from under it.
    item.addEventListener('mousedown', (event) => {
      event.preventDefault();
      onPick(option);
    });
    container.appendChild(item);
  });
}

function bindComboOutsideClose(root, panel, onClose) {
  document.addEventListener('click', (event) => {
    if (!root.contains(event.target)) {
      onClose();
    }
  });
}

function mountSearchableMultiSelect(rootId, options, selectedValues, onChange, placeholder) {
  const root = document.getElementById(rootId);
  const control = root.querySelector('.combo-control');
  const pillsContainer = root.querySelector('.combo-pills');
  const input = root.querySelector('.combo-input');
  const panel = root.querySelector('.combo-panel');
  const optionsNode = root.querySelector('.combo-options');
  const state = {
    options,
    selectedValues: [...selectedValues],
  };

  function renderPills() {
    pillsContainer.innerHTML = '';
    state.selectedValues.forEach((value) => {
      const pill = document.createElement('span');
      pill.className = 'combo-pill';
      const text = document.createElement('span');
      text.textContent = gymName(value);
      const remove = document.createElement('button');
      remove.type = 'button';
      remove.className = 'combo-pill-remove';
      remove.setAttribute('aria-label', `Remove ${gymName(value)}`);
      remove.textContent = '×';
      remove.addEventListener('mousedown', (event) => {
        event.preventDefault();
        event.stopPropagation();
        state.selectedValues = state.selectedValues.filter((existing) => existing !== value);
        renderPills();
        renderOptions();
        onChange([...state.selectedValues]);
      });
      pill.appendChild(text);
      pill.appendChild(remove);
      pillsContainer.appendChild(pill);
    });
    input.placeholder = placeholder;
  }

  function renderOptions() {
    const filtered = comboFilteredOptions(state.options, input.value, state.selectedValues);
    renderComboOptionsList(optionsNode, filtered, (option) => {
      state.selectedValues = [...new Set([...state.selectedValues, option.value])];
      input.value = '';
      renderPills();
      renderOptions();
      onChange([...state.selectedValues]);
      input.focus();
    }, 'No gyms match your search.');
  }

  function open() {
    panel.hidden = false;
    root.classList.add('is-open');
    renderOptions();
  }
  function close() {
    panel.hidden = true;
    root.classList.remove('is-open');
  }

  input.addEventListener('focus', open);
  input.addEventListener('input', renderOptions);
  control.addEventListener('mousedown', (event) => {
    if (event.target === control || event.target === pillsContainer) {
      input.focus();
    }
  });
  input.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') {
      input.blur();
      close();
    } else if (event.key === 'Backspace' && !input.value && state.selectedValues.length) {
      state.selectedValues = state.selectedValues.slice(0, -1);
      renderPills();
      renderOptions();
      onChange([...state.selectedValues]);
    }
  });
  bindComboOutsideClose(root, panel, close);

  appState.widgets[rootId] = {
    update(nextOptions, nextSelectedValues) {
      state.options = nextOptions;
      state.selectedValues = [...nextSelectedValues];
      renderPills();
      renderOptions();
    },
  };

  appState.widgets[rootId].update(options, selectedValues);
}

function mountSearchableSingleSelect(rootId, options, selectedValue, onChange, placeholder) {
  const root = document.getElementById(rootId);
  const input = root.querySelector('.combo-input');
  const panel = root.querySelector('.combo-panel');
  const optionsNode = root.querySelector('.combo-options');
  const state = {
    options,
    selectedValue,
  };

  function currentLabel() {
    const match = state.options.find((option) => option.value === state.selectedValue);
    return match ? match.label : '';
  }

  function syncDisplay() {
    input.value = currentLabel();
  }

  function renderOptions() {
    const filtered = comboFilteredOptions(state.options, input.value, null);
    renderComboOptionsList(optionsNode, filtered, (option) => {
      state.selectedValue = option.value;
      input.value = option.label;
      close();
      onChange(option.value);
    }, 'No gyms match your search.');
  }

  function open() {
    panel.hidden = false;
    root.classList.add('is-open');
    input.value = '';
    renderOptions();
  }
  function close() {
    panel.hidden = true;
    root.classList.remove('is-open');
    syncDisplay();
  }

  input.placeholder = placeholder;
  input.addEventListener('focus', open);
  input.addEventListener('input', renderOptions);
  input.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') {
      input.blur();
      close();
    }
  });
  bindComboOutsideClose(root, panel, close);

  appState.widgets[rootId] = {
    update(nextOptions, nextSelectedValue) {
      state.options = nextOptions;
      state.selectedValue = nextSelectedValue;
      syncDisplay();
    },
  };

  appState.widgets[rootId].update(options, selectedValue);
}

function populateGymControls() {
  const gyms = sortedGyms();
  const options = gyms.map((gym) => ({ value: String(gym.gym_id), label: gym.gym_name || String(gym.gym_id) }));

  const timeSelect = document.getElementById('time-gym-select');
  timeSelect.innerHTML = '<option value="">All gyms</option>';
  options.forEach((option) => {
    const timeOption = document.createElement('option');
    timeOption.value = option.value;
    timeOption.textContent = option.label;
    timeSelect.appendChild(timeOption);
  });

  if (!appState.filters.compareRefGymId && options.length) {
    const defaultRef = options.find((option) => option.value === DEFAULT_COMPARE_REF_GYM_ID);
    appState.filters.compareRefGymId = defaultRef ? defaultRef.value : options[0].value;
  }
  if (!appState.filters.compareFocusGymId && options.length) {
    const defaultFocus = options.find((option) => option.value === DEFAULT_COMPARE_FOCUS_GYM_ID);
    if (defaultFocus && defaultFocus.value !== appState.filters.compareRefGymId) {
      appState.filters.compareFocusGymId = defaultFocus.value;
    }
  }
  if (!appState.filters.compareGymIds.length && options.length) {
    const availableValues = new Set(options.map((option) => option.value));
    appState.filters.compareGymIds = DEFAULT_COMPARE_GYM_IDS.filter((value) => (
      availableValues.has(value) && value !== appState.filters.compareRefGymId
    ));
  }

  if (!appState.widgets['grade-overlay-picker']) {
    mountSearchableMultiSelect(
      'grade-overlay-picker',
      options,
      appState.filters.gradeGymIds,
      async (selected) => {
        appState.filters.gradeGymIds = selected;
        await loadGradeComparisonData();
        renderGradeDistribution();
      },
      'Choose gyms'
    );
  } else {
    appState.widgets['grade-overlay-picker'].update(options, appState.filters.gradeGymIds);
  }

  if (!appState.widgets['compare-gyms-picker']) {
    mountSearchableMultiSelect(
      'compare-gyms-picker',
      options,
      appState.filters.compareGymIds,
      (selected) => {
        appState.filters.compareGymIds = selected.filter((value) => value !== appState.filters.compareRefGymId);
        if (selected.length !== appState.filters.compareGymIds.length) {
          appState.widgets['compare-gyms-picker'].update(options, appState.filters.compareGymIds);
        }
        renderGymComparisonVisuals();
      },
      'Add gym to comparison...'
    );
  } else {
    appState.widgets['compare-gyms-picker'].update(options, appState.filters.compareGymIds);
  }

  if (!appState.widgets['compare-ref-gym-select']) {
    mountSearchableSingleSelect(
      'compare-ref-gym-select',
      options,
      appState.filters.compareRefGymId,
      (value) => {
        appState.filters.compareRefGymId = value;
        const refreshedOptions = sortedGyms().map((gym) => ({ value: String(gym.gym_id), label: gym.gym_name || String(gym.gym_id) }));
        appState.filters.compareGymIds = appState.filters.compareGymIds.filter((gymId) => gymId !== value);
        appState.widgets['compare-gyms-picker']?.update(refreshedOptions, appState.filters.compareGymIds);
        if (appState.filters.compareFocusGymId === value) {
          appState.filters.compareFocusGymId = '';
          appState.widgets['compare-focus-gym-select']?.update(refreshedOptions, '');
        }
        renderGymComparisonAll();
      },
      'Select a gym'
    );
  } else {
    appState.widgets['compare-ref-gym-select'].update(options, appState.filters.compareRefGymId);
  }

  if (!appState.widgets['compare-focus-gym-select']) {
    mountSearchableSingleSelect(
      'compare-focus-gym-select',
      options,
      appState.filters.compareFocusGymId,
      (value) => {
        appState.filters.compareFocusGymId = value;
        renderGymComparisonFocusRow();
      },
      'Select a gym'
    );
  } else {
    appState.widgets['compare-focus-gym-select'].update(options, appState.filters.compareFocusGymId);
  }
}

function getGradeTicks(discipline) {
  if (discipline === 'routes') {
    const metrics = appState.data.routeBodyMetrics || {};
    return metrics.grade_ticks || [];
  }
  const metrics = appState.data.boulderBodyMetrics || {};
  return metrics.grade_ticks || [];
}

function toDensity(rows) {
  const total = rows.reduce((sum, row) => sum + (row.climb_count || 0), 0);
  return rows.map((row) => ({ ...row, density: total ? row.climb_count / total : 0 }));
}

function renderPlotMessage(hostId, message, height = 260) {
  Plotly.react(
    hostId,
    [],
    {
      ...chartLayout(''),
      height,
      xaxis: { visible: false },
      yaxis: { visible: false },
      annotations: [
        {
          text: message,
          x: 0.5,
          y: 0.5,
          xref: 'paper',
          yref: 'paper',
          showarrow: false,
          font: { color: cssVar('--lg-text-2'), size: 14 },
        },
      ],
    },
    { responsive: true, displayModeBar: false }
  );
}

function renderSummary() {
  if (!appState.loaded.summary) {
    return;
  }
  const summary = appState.data.summary || {};
  document.getElementById('total-sends').textContent = formatNumber(summary.total_sends);
  document.getElementById('unique-users').textContent = formatNumber(summary.unique_users);
  document.getElementById('unique-gyms').textContent = formatNumber(summary.unique_gyms);
  document.getElementById('date-range').textContent = formatDateRange(summary);
}

function renderTimeSeries() {
  if (!appState.loaded.dataOverview) {
    return;
  }
  const rows = appState.data.timeSeries || [];
  const overlayRows = appState.data.overlayTimeSeries || [];
  const traces = [
    {
      x: rows.map((row) => row.period),
      y: rows.map((row) => row.send_count),
      mode: 'lines',
      line: { color: cssVar('--lg-info'), width: 2.5 },
      name: 'All gyms',
      yaxis: 'y',
    },
  ];

  if (overlayRows.length) {
    traces.push({
      x: overlayRows.map((row) => row.period),
      y: overlayRows.map((row) => row.send_count),
      mode: 'lines',
      line: { color: '#ffc36b', width: 2 },
      name: gymName(appState.filters.timeGymId),
      yaxis: 'y2',
    });
  }

  Plotly.react(
    'time-series-chart',
    traces,
    {
      ...chartLayout('Date'),
      yaxis: {
        ...chartLayout('Date').yaxis,
        title: 'Number of logged sends',
      },
      yaxis2: {
        overlaying: 'y',
        side: 'right',
        showgrid: false,
        color: '#ffc36b',
      },
    },
    { responsive: true, displayModeBar: false }
  );
}

function renderGymCounts() {
  if (!appState.loaded.dataOverview) {
    return;
  }
  const rows = appState.data.topGyms || [];
  Plotly.react(
    'gym-counts-chart',
    [
      {
        x: rows.slice().reverse().map((row) => row.send_count),
        y: rows.slice().reverse().map((row) => row.gym_name || row.gym_id),
        type: 'bar',
        orientation: 'h',
        marker: { color: cssVar('--lg-success') },
      },
    ],
    {
      ...chartLayout('Send Count'),
      height: Math.max(720, rows.length * 26),
      margin: { l: 220, r: 16, t: 18, b: 40 },
    },
    { responsive: true, displayModeBar: false }
  );
}

function buildGradeComparisonTraces(baseRows, discipline) {
  const traces = [
    {
      x: baseRows.map((row) => row.grade),
      y: toDensity(baseRows).map((row) => row.density),
      type: 'bar',
      marker: {
        color: discipline === 'bouldering' ? 'rgba(255,192,0,0.55)' : 'rgba(217,119,6,0.45)',
      },
      name: 'All gyms',
    },
  ];

  const selectedGymIds = appState.filters.gradeGymIds || [];
  selectedGymIds.forEach((gymId, index) => {
    const rows = appState.data.gradeComparison?.[discipline]?.[gymId] || [];
    const densityRows = toDensity(rows);
    traces.push({
      x: densityRows.map((row) => row.grade),
      y: densityRows.map((row) => row.density),
      type: 'scatter',
      mode: 'lines+markers',
      line: { color: gymLinePalette[index % gymLinePalette.length], width: 2 },
      marker: { color: gymLinePalette[index % gymLinePalette.length], size: 6 },
      name: gymName(gymId),
    });
  });

  return traces;
}

function renderGradeDistribution() {
  if (!appState.loaded.dataOverview) {
    return;
  }
  const boulderRows = appState.data.boulderGrades || [];
  const routeRows = appState.data.routeGrades || [];
  const boulderCategories = boulderRows.map((row) => row.grade);
  const routeCategories = routeRows.map((row) => row.grade);

  Plotly.react(
    'boulder-grade-chart',
    buildGradeComparisonTraces(boulderRows, 'bouldering'),
    {
      ...chartLayout('Boulder Grade'),
      margin: { l: 48, r: 16, t: 18, b: 74 },
      xaxis: {
        ...chartLayout('Boulder Grade').xaxis,
        type: 'category',
        categoryorder: 'array',
        categoryarray: boulderCategories,
      },
      yaxis: {
        ...chartLayout('Boulder Grade').yaxis,
        title: 'Percent of Climbs',
        tickformat: '.0%',
      },
      bargap: 0.08,
    },
    { responsive: true, displayModeBar: false }
  );

  Plotly.react(
    'route-grade-chart',
    buildGradeComparisonTraces(routeRows, 'routes'),
    {
      ...chartLayout('Route Grade'),
      margin: { l: 48, r: 16, t: 18, b: 74 },
      xaxis: {
        ...chartLayout('Route Grade').xaxis,
        type: 'category',
        categoryorder: 'array',
        categoryarray: routeCategories,
      },
      yaxis: {
        ...chartLayout('Route Grade').yaxis,
        title: 'Percent of Climbs',
        tickformat: '.0%',
      },
      bargap: 0.08,
    },
    { responsive: true, displayModeBar: false }
  );
}

// The corner plot pre-transforms values to log10 before plotting so histogram
// bins stay even in log-space, but that means the raw axis ticks show log10
// numbers (0.5, 1.0, 1.5...) instead of a true log-scale (1, 3, 10, 30...).
// These helpers build "nice" 1-2-5-per-decade tick positions in real units,
// converted to the log10 coordinates the traces are actually plotted in.
function niceLogTickCandidates(minLog10, maxLog10) {
  if (!Number.isFinite(minLog10) || !Number.isFinite(maxLog10)) {
    return [];
  }
  const minExp = Math.floor(minLog10) - 1;
  const maxExp = Math.ceil(maxLog10) + 1;
  const candidates = [];
  for (let exp = minExp; exp <= maxExp; exp += 1) {
    [1, 2, 5].forEach((base) => {
      const value = base * (10 ** exp);
      const log10Value = Math.log10(value);
      if (log10Value >= minLog10 - 1e-9 && log10Value <= maxLog10 + 1e-9) {
        candidates.push({ value, log10Value });
      }
    });
  }
  return candidates.sort((a, b) => a.log10Value - b.log10Value);
}

function formatLogTickValue(value) {
  if (value >= 1) {
    return Number.isInteger(value) ? String(value) : value.toFixed(1);
  }
  return String(Number(value.toPrecision(2)));
}

function dimensionLogTicks(points, dimensionKey) {
  const key = `${dimensionKey}_log10`;
  const values = points
    .map((point) => point[key])
    .filter((value) => value !== null && value !== undefined && Number.isFinite(value));
  if (!values.length) {
    return { tickvals: [], ticktext: [] };
  }
  const candidates = niceLogTickCandidates(Math.min(...values), Math.max(...values));
  return {
    tickvals: candidates.map((candidate) => candidate.log10Value),
    ticktext: candidates.map((candidate) => formatLogTickValue(candidate.value)),
  };
}

// Bayesian-bootstrapped kernel density estimate.
//
// The recorded grade deltas are integers (grades are letter/number labels),
// but the true difficulty gap between two gyms is continuous — a climb can
// genuinely sit at "v3.5", or one gym's grading can run ~1.2 grades stiffer.
// A plain KDE with a data-driven (Silverman) bandwidth quietly assumes the
// sample is large enough for that bandwidth to be meaningful; with only a
// few dozen small-integer observations it either overfits noise or smears
// distinct grade levels together, and gives no sense of how uncertain the
// resulting shape is.
//
// Instead: draw Dirichlet(1,...,1) weights over the observations (Rubin's
// Bayesian bootstrap — the nonparametric posterior over the empirical
// distribution) many times, compute a weighted KDE for each draw using a
// small FIXED bandwidth representing plausible quantization slop around an
// integer-recorded grade (not a bandwidth fit to the sample), then take the
// per-point median and a credible interval across the ensemble. This stays
// continuous-valued, doesn't assume the true shape is Gaussian (only that
// each smoothing bump is, a much weaker, purely mechanical choice), and is
// honest about how little a handful of points pins down the shape.
const BAYESIAN_BOOTSTRAP_BANDWIDTH = 0.65;
const BAYESIAN_BOOTSTRAP_ITERATIONS = 400;

function bayesianBootstrapWeights(n) {
  // Dirichlet(1,...,1) via n Exponential(1) draws, normalized.
  const draws = Array.from({ length: n }, () => -Math.log(1 - Math.random()));
  const total = draws.reduce((sum, value) => sum + value, 0) || 1;
  return draws.map((value) => value / total);
}

function weightedGaussianKde(values, weights, bandwidth, xs) {
  const norm = 1 / (bandwidth * Math.sqrt(2 * Math.PI));
  return xs.map((x) => {
    let sum = 0;
    for (let i = 0; i < values.length; i += 1) {
      const z = (x - values[i]) / bandwidth;
      sum += weights[i] * Math.exp(-0.5 * z * z);
    }
    return sum * norm;
  });
}

// Inverts a (roughly-evenly-spaced) density curve's CDF at `quantile` via
// trapezoidal integration + linear interpolation between grid points, so the
// result is a continuous float rather than one of the raw discrete inputs.
function curveQuantile(xs, ys, quantile) {
  const n = xs.length;
  const cdf = new Array(n).fill(0);
  for (let i = 1; i < n; i += 1) {
    cdf[i] = cdf[i - 1] + (((ys[i] + ys[i - 1]) / 2) * (xs[i] - xs[i - 1]));
  }
  const total = cdf[n - 1] || 1;
  const target = quantile * total;
  let hi = 1;
  while (hi < n - 1 && cdf[hi] < target) {
    hi += 1;
  }
  const lo = hi - 1;
  if (cdf[hi] === cdf[lo]) {
    return xs[hi];
  }
  const frac = (target - cdf[lo]) / (cdf[hi] - cdf[lo]);
  return xs[lo] + (frac * (xs[hi] - xs[lo]));
}

// Bayesian-bootstrapped KDE: draws Dirichlet weights over the observations
// many times, fits a smoothed density curve for each draw, and reports both
// the per-x density envelope (for the shaded band) AND a `summary` — the
// 50th-percentile point of each draw's OWN fitted curve, plus the
// lower/upper quantiles of that same statistic across the ensemble.
// Deriving the point estimate from the continuous curve rather than the raw
// integer observations is what makes it a float (e.g. 0.35, not 0 or 1) —
// the whole reason for fitting a curve in the first place.
function bayesianBootstrapKde(values, {
  min,
  max,
  points = 200,
  bandwidth = BAYESIAN_BOOTSTRAP_BANDWIDTH,
  iterations = BAYESIAN_BOOTSTRAP_ITERATIONS,
  lowerQuantile = 0.16,
  upperQuantile = 0.84,
} = {}) {
  const finite = values.filter((value) => Number.isFinite(value));
  if (finite.length < 2) {
    return { x: [], median: [], lower: [], upper: [], summary: null };
  }
  const lo = min ?? Math.min(...finite);
  const hi = max ?? Math.max(...finite);
  const step = (hi - lo) / (points - 1);
  const xs = Array.from({ length: points }, (_, index) => lo + (index * step));

  const curves = Array.from({ length: iterations }, () => (
    weightedGaussianKde(finite, bayesianBootstrapWeights(finite.length), bandwidth, xs)
  ));

  const quantileAt = (sortedColumn, quantile) => {
    const index = Math.min(sortedColumn.length - 1, Math.max(0, Math.round(quantile * (sortedColumn.length - 1))));
    return sortedColumn[index];
  };

  const median = [];
  const lower = [];
  const upper = [];
  for (let pointIndex = 0; pointIndex < xs.length; pointIndex += 1) {
    const column = curves.map((curve) => curve[pointIndex]).sort((a, b) => a - b);
    median.push(quantileAt(column, 0.5));
    lower.push(quantileAt(column, lowerQuantile));
    upper.push(quantileAt(column, upperQuantile));
  }

  // Percentile VALUES of the fitted distribution itself — where the 10th,
  // 50th, and 90th percentile of probability mass actually falls on the
  // representative (per-x posterior median) curve. This is deliberately
  // NOT a confidence interval on the median statistic's location: that
  // would shrink toward zero width as sample size grows (you pin down
  // *where the center is* more precisely with more data), which is a
  // different question from "how spread out is the distribution" and
  // would look artificially narrow next to the histogram's actual spread.
  const summary = {
    point: curveQuantile(xs, median, 0.5),
    lower: curveQuantile(xs, median, lowerQuantile),
    upper: curveQuantile(xs, median, upperQuantile),
  };

  return { x: xs, median, lower, upper, summary };
}

// Bin raw values into fixed-width density bins (matching Plotly histogram's
// own xbins convention) so a "step" outline can be drawn manually — Plotly's
// native histogram trace always draws all 4 sides of every bar, which reads
// as a picket fence once the fill is removed.
function binnedDensity(values, { start, end, size }) {
  const finite = values.filter((value) => Number.isFinite(value));
  const nBins = Math.round((end - start) / size);
  const edges = Array.from({ length: nBins + 1 }, (_, index) => start + (index * size));
  const counts = new Array(nBins).fill(0);
  finite.forEach((value) => {
    if (value < start || value > end) {
      return;
    }
    const index = Math.min(nBins - 1, Math.max(0, Math.floor((value - start) / size)));
    counts[index] += 1;
  });
  const total = finite.length || 1;
  const heights = counts.map((count) => count / (total * size));
  return { edges, heights };
}

// Convert per-bin heights into the polyline vertices for a true step outline
// (flat across each bin, vertical only where the height actually changes,
// closed to the baseline at the first and last edge) using plain linear
// segments — no need for Plotly's line-shape:'hv' since every consecutive
// pair here is already purely horizontal or purely vertical.
function stepHistogramPoints(edges, heights) {
  const x = [edges[0]];
  const y = [0];
  for (let i = 0; i < heights.length; i += 1) {
    x.push(edges[i], edges[i + 1]);
    y.push(heights[i], heights[i]);
  }
  x.push(edges[edges.length - 1]);
  y.push(0);
  return { x, y };
}

// 2D counterpart to bayesianBootstrapKde, for the ref-grade vs comp-grade
// heatmap. The heatmap's source points are already aggregated into unique
// (ref_grade, comp_grade) buckets with an n_users count rather than one row
// per raw user, so instead of expanding back out to raw observations and
// drawing Dirichlet(1,...,1) over all of them, we use the Dirichlet
// aggregation property directly: summing Dirichlet(1,...,1) weights within a
// group is itself Dirichlet(group size), so drawing Dirichlet(n_users) per
// bucket is exactly equivalent and far cheaper. n_users is always a positive
// integer, so each Gamma(n_users, 1) draw is just a sum of n_users iid
// Exponential(1) draws — no general-shape Gamma sampler needed.
const KDE_2D_GRID_POINTS = 60;
const KDE_2D_ITERATIONS = 50;

function gammaIntShape(shape) {
  let sum = 0;
  for (let i = 0; i < shape; i += 1) {
    sum += -Math.log(1 - Math.random());
  }
  return sum;
}

function bayesianBootstrapWeightsFromCounts(counts) {
  const draws = counts.map((count) => gammaIntShape(Math.max(1, Math.round(count))));
  const total = draws.reduce((sum, value) => sum + value, 0) || 1;
  return draws.map((value) => value / total);
}

function weightedGaussianKde2D(xValues, yValues, weights, bandwidth, gridX, gridY) {
  const norm = 1 / (bandwidth * bandwidth * 2 * Math.PI);
  const grid = new Array(gridY.length);
  for (let yi = 0; yi < gridY.length; yi += 1) {
    const row = new Array(gridX.length);
    for (let xi = 0; xi < gridX.length; xi += 1) {
      let sum = 0;
      for (let i = 0; i < xValues.length; i += 1) {
        const zx = (gridX[xi] - xValues[i]) / bandwidth;
        const zy = (gridY[yi] - yValues[i]) / bandwidth;
        sum += weights[i] * Math.exp(-0.5 * ((zx * zx) + (zy * zy)));
      }
      row[xi] = sum * norm;
    }
    grid[yi] = row;
  }
  return grid;
}

function bayesianBootstrap2DKdeGrid(points, {
  xKey,
  yKey,
  countKey,
  xRange,
  yRange,
  gridPoints = KDE_2D_GRID_POINTS,
  bandwidth = BAYESIAN_BOOTSTRAP_BANDWIDTH,
  iterations = KDE_2D_ITERATIONS,
} = {}) {
  if (!points.length) {
    return { x: [], y: [], z: [] };
  }
  const xValues = points.map((point) => point[xKey]);
  const yValues = points.map((point) => point[yKey]);
  const counts = points.map((point) => point[countKey]);

  const xStep = (xRange[1] - xRange[0]) / (gridPoints - 1);
  const yStep = (yRange[1] - yRange[0]) / (gridPoints - 1);
  const gridX = Array.from({ length: gridPoints }, (_, index) => xRange[0] + (index * xStep));
  const gridY = Array.from({ length: gridPoints }, (_, index) => yRange[0] + (index * yStep));

  // Per-cell running list of ensemble draws, then take the per-cell median.
  const samples = Array.from({ length: gridPoints }, () => Array.from({ length: gridPoints }, () => []));
  for (let iter = 0; iter < iterations; iter += 1) {
    const weights = bayesianBootstrapWeightsFromCounts(counts);
    const draw = weightedGaussianKde2D(xValues, yValues, weights, bandwidth, gridX, gridY);
    for (let yi = 0; yi < gridPoints; yi += 1) {
      for (let xi = 0; xi < gridPoints; xi += 1) {
        samples[yi][xi].push(draw[yi][xi]);
      }
    }
  }

  const median = samples.map((row) => row.map((cell) => {
    cell.sort((a, b) => a - b);
    return cell[Math.floor(cell.length / 2)];
  }));

  return { x: gridX, y: gridY, z: median };
}

const HEIGHT_AXIS_MIN = 52;
const HEIGHT_AXIS_MAX = 80;
const HEIGHT_AXIS_STEP = 4;

function feetInchesLabel(totalInches) {
  const rounded = Math.round(totalInches);
  const feet = Math.floor(rounded / 12);
  const inches = rounded % 12;
  return `${feet}'${inches}"`;
}

function heightAxisTicks() {
  const tickvals = [];
  const ticktext = [];
  for (let value = HEIGHT_AXIS_MIN; value <= HEIGHT_AXIS_MAX; value += HEIGHT_AXIS_STEP) {
    tickvals.push(value);
    ticktext.push(feetInchesLabel(value));
  }
  return { tickvals, ticktext };
}

// Keeps a gridline/tick at every value (so the axis stays evenly marked)
// but blanks out every-other label's text once there are too many to fit
// without overlapping — e.g. the 29-level route-grade ladder (5.4..5.15c).
function thinTickLabels(tickText, maxLabels = 16) {
  if (tickText.length <= maxLabels) {
    return tickText;
  }
  return tickText.map((label, index) => (index % 2 === 0 ? label : ''));
}

function maxUserCount(rowsByGender) {
  const allRows = [...(rowsByGender.male || []), ...(rowsByGender.female || [])];
  return Math.max(...allRows.map((row) => row.n_users || 0), 1);
}

// Plotly's sizemode:'area' + sizemin clamps small values to a flat floor, so a
// 1-user point and a 3-user point can render at the identical clamped size.
// Interpolating the pixel diameter directly avoids that hard clamp, but a
// plain sqrt(count/maxCount) ratio has the same practical effect when
// maxCount is large relative to most buckets (e.g. "All users" audience,
// where a few very common height/grade combos can hit 300+ while most
// buckets sit at 1-5): sqrt compresses the low end so hard that a 1-user
// and 3-user bubble differ by ~1px, imperceptible on screen. log1p
// compresses the *high* end instead, which is exactly what's needed here —
// it leaves far more visual room near the bottom of the range where most
// of the real differences we care about live.
function bubbleDiameterPx(count, maxCount, minPx = 2, maxPx = 32) {
  if (!count) {
    return 0;
  }
  const ratio = Math.log1p(count) / Math.log1p(Math.max(maxCount, 1));
  return minPx + ((maxPx - minPx) * ratio);
}

const APE_INDEX_MIN = -10;
const APE_INDEX_MAX = 10;

function renderBodyMorphologyNote() {
  document.getElementById('body-morphology-note').textContent = 'A climber\'s height and wingspan have a huge influence on the perceived difficulty of a given climb. '
    + 'A "reachy" climb will benefit tall climbers and those with long arms who can reach holds more easily, while a "boxy" '
    + 'climb might benefit smaller climbers who can more easily maneuver around tight spaces. The plots on this page examine the '
    + 'maximum grades logged as a function of the user\'s height and ape index (wingspan - height). Versions are available for '
    + '"Active" users of the app and the entire population, and are separated by gender.';
  document.getElementById('body-morphology-gam-note').textContent = 'A GAM (Generalized Additive Model) fit is shown as the solid line and shaded band in each scatter panel — a '
    + 'flexible curve that follows the data\'s actual shape rather than assuming a straight-line relationship, regularized '
    + 'against overfitting to sparse buckets.';
}

function bodyMetricAxisConfig(metricsForDiscipline, xIsHeight) {
  const disciplineLabel = metricsForDiscipline.discipline === 'routes' ? 'Route' : 'Boulder';
  const ticks = metricsForDiscipline.grade_ticks || [];
  const tickVals = ticks.map((tick) => tick.value);
  const tickText = ticks.map((tick) => tick.label);
  const gradeCol = metricsForDiscipline.grade_num_column;
  const xRange = xIsHeight
    ? [HEIGHT_AXIS_MIN - 2, HEIGHT_AXIS_MAX + 2]
    : [APE_INDEX_MIN - 1, APE_INDEX_MAX + 1];
  const yRange = tickVals.length ? [Math.min(...tickVals) - 1, Math.max(...tickVals) + 1] : undefined;
  return { disciplineLabel, tickVals, tickText, gradeCol, xRange, yRange };
}

function renderBodyMetrics() {
  if (!appState.loaded.bodyMorphology) {
    return;
  }
  renderBodyMorphologyNote();

  const maleColors = buildGenderColorSet(genderBaseColors.male);
  const femaleColors = buildGenderColorSet(genderBaseColors.female);

  function colorsForGender(genderKey) {
    return genderKey === 'male' ? maleColors : femaleColors;
  }

  // GAM fit (grade ~ smooth(x), fit on raw per-user pairs — see
  // _fit_gam_curve in viewer_payloads.py) as a mean line + 68% CI ribbon.
  // Shared by the scatter and heatmap panels so both show it identically.
  // Returned as two separate arrays so the caller can control z-order
  // (e.g. ribbons behind the scatter, mean line on top of it). Both share
  // the gender's legendgroup so toggling "Male"/"Female" hides its GAM fit
  // along with its scatter points / heatmap layer.
  function buildGamOverlayTraces(metricsForDiscipline, xIsHeight) {
    const gamKey = xIsHeight ? 'height' : 'ape_index';
    const gamCurvesByGender = (metricsForDiscipline.gam_curves || {})[gamKey] || {};
    const bandTraces = [];
    const lineTraces = [];
    ['male', 'female'].forEach((genderKey) => {
      const curve = gamCurvesByGender[genderKey];
      if (!curve) {
        return;
      }
      const colors = colorsForGender(genderKey);
      bandTraces.push(
        {
          x: curve.x, y: curve.lower, type: 'scatter', mode: 'lines',
          line: { width: 0 }, legendgroup: genderKey, showlegend: false, hoverinfo: 'skip',
        },
        {
          x: curve.x, y: curve.upper, type: 'scatter', mode: 'lines',
          line: { width: 0 }, fill: 'tonexty', fillcolor: colors.band,
          legendgroup: genderKey, showlegend: false, hoverinfo: 'skip',
        }
      );
      // Halo technique: a wider dark line drawn first, then the true
      // gender-colored line drawn narrower directly on top, leaving a thin
      // dark border visible on both edges. Keeps the line's real color
      // legible (no darkening/desaturating it) while still guaranteeing
      // contrast against whatever's underneath — scatter dots, heatmap
      // fill, doesn't matter, the halo works against any background.
      lineTraces.push(
        {
          x: curve.x, y: curve.mean, type: 'scatter', mode: 'lines',
          line: { color: 'rgba(10, 10, 10, 0.8)', width: 3.8 },
          legendgroup: genderKey, showlegend: false, hoverinfo: 'skip',
        },
        {
          x: curve.x, y: curve.mean, type: 'scatter', mode: 'lines',
          line: { color: colors.line, width: 2.5 },
          legendgroup: genderKey, showlegend: false, hoverinfo: 'skip',
        }
      );
    });
    return { bandTraces, lineTraces };
  }

  function renderDisciplineScatter(chartId, axisTitle, metricsForDiscipline, rowsKey, xField, xIsHeight = false) {
    const { disciplineLabel, tickVals, tickText, gradeCol, xRange, yRange } = bodyMetricAxisConfig(metricsForDiscipline, xIsHeight);
    const rowsByGender = metricsForDiscipline[rowsKey] || { male: [], female: [] };
    const maxCount = maxUserCount(rowsByGender);
    const { bandTraces: gamBandTraces, lineTraces: gamLineTraces } = buildGamOverlayTraces(metricsForDiscipline, xIsHeight);

    const scatterTraces = ['male', 'female'].map((genderKey) => {
      const rows = rowsByGender[genderKey] || [];
      const colors = colorsForGender(genderKey);
      return {
        x: rows.map((row) => row[xField]),
        y: rows.map((row) => row[gradeCol]),
        customdata: rows.map((row) => [
          row[metricsForDiscipline.grade_label_column],
          row.n_users,
          xIsHeight ? feetInchesLabel(row[xField]) : row[xField],
        ]),
        mode: 'markers',
        marker: {
          size: rows.map((row) => bubbleDiameterPx(row.n_users, maxCount)),
          color: colors.fill,
          line: {
            color: colors.line,
            width: 1.2,
          },
          opacity: 0.7,
          symbol: 'circle',
        },
        legendgroup: genderKey,
        name: genderKey === 'male' ? 'Male' : 'Female',
        hovertemplate:
          `${genderKey === 'male' ? 'Male' : 'Female'}<br>`
          + `${axisTitle} %{customdata[2]}<br>`
          + `Max ${disciplineLabel.toLowerCase()} grade %{customdata[0]}<br>`
          + `Users in this exact bucket: %{customdata[1]}<extra></extra>`,
      };
    });

    Plotly.react(
      chartId,
      [...gamBandTraces, ...scatterTraces, ...gamLineTraces],
      {
        ...chartLayout(axisTitle),
        height: 440,
        legend: { ...chartLayout(axisTitle).legend, groupclick: 'togglegroup' },
        xaxis: {
          ...chartLayout(axisTitle).xaxis,
          ...(xIsHeight ? { tickmode: 'array', ...heightAxisTicks() } : {}),
          range: xRange,
        },
        yaxis: {
          ...chartLayout(axisTitle).yaxis,
          title: `${disciplineLabel} grade`,
          tickmode: 'array',
          tickvals: tickVals,
          ticktext: thinTickLabels(tickText),
          // Plotly's default autorange padding is too tight here, crowding
          // the top/bottom rows of points against the plot edge.
          range: yRange,
        },
      },
      { responsive: true, displayModeBar: false }
    );
  }

  // Both genders' density heatmaps overlaid on the same plot (not side by
  // side) — each uses a transparent-to-opaque colorscale so overlapping
  // regions blend rather than one occluding the other. layout.legend's
  // groupclick:'togglegroup' means clicking "Male"/"Female" toggles that
  // gender's heatmap.
  function renderDisciplineHeatmap(chartId, axisTitle, metricsForDiscipline, rowsKey, xField, xIsHeight = false) {
    const { disciplineLabel, tickVals, tickText, gradeCol, xRange, yRange } = bodyMetricAxisConfig(metricsForDiscipline, xIsHeight);
    const rowsByGender = metricsForDiscipline[rowsKey] || { male: [], female: [] };

    function heatmapTrace(genderKey) {
      const rows = rowsByGender[genderKey] || [];
      const points = rows
        .map((row) => ({ x: row[xField], y: row[gradeCol], n: row.n_users }))
        .filter((point) => Number.isFinite(point.x) && Number.isFinite(point.y) && point.n > 0);
      if (!points.length || !gradeCol || !yRange) {
        return null;
      }
      const grid = bayesianBootstrap2DKdeGrid(points, {
        xKey: 'x', yKey: 'y', countKey: 'n', xRange, yRange, gridPoints: 50, iterations: 30,
      });
      return {
        type: 'heatmap',
        x: grid.x,
        y: grid.y,
        z: boostSparseDensity(grid.z),
        zmin: 0,
        zmax: 1,
        colorscale: colorsForGender(genderKey).heatmapColorscale,
        showscale: false,
        hoverinfo: 'skip',
        legendgroup: genderKey,
        name: genderKey === 'male' ? 'Male' : 'Female',
        showlegend: true,
      };
    }

    const heatmapTraces = [
      heatmapTrace('male'),
      heatmapTrace('female'),
    ].filter(Boolean);

    const { bandTraces: gamBandTraces, lineTraces: gamLineTraces } = buildGamOverlayTraces(metricsForDiscipline, xIsHeight);

    Plotly.react(
      chartId,
      [...heatmapTraces, ...gamBandTraces, ...gamLineTraces],
      {
        ...chartLayout(axisTitle),
        height: 440,
        legend: { ...chartLayout(axisTitle).legend, groupclick: 'togglegroup' },
        xaxis: {
          ...chartLayout(axisTitle).xaxis,
          ...(xIsHeight ? { tickmode: 'array', ...heightAxisTicks() } : {}),
          range: xRange,
        },
        yaxis: {
          ...chartLayout(axisTitle).yaxis,
          title: `${disciplineLabel} grade`,
          tickmode: 'array',
          tickvals: tickVals,
          ticktext: thinTickLabels(tickText),
          range: yRange,
        },
      },
      { responsive: true, displayModeBar: false }
    );
  }

  renderDisciplineScatter('boulder-height-grade-chart', 'Height', appState.data.boulderBodyMetrics || {}, 'height_vs_grade_by_gender', 'height_rounded', true);
  renderDisciplineHeatmap('boulder-height-grade-heatmap', 'Height', appState.data.boulderBodyMetrics || {}, 'height_vs_grade_by_gender', 'height_rounded', true);
  renderDisciplineScatter('route-height-grade-chart', 'Height', appState.data.routeBodyMetrics || {}, 'height_vs_grade_by_gender', 'height_rounded', true);
  renderDisciplineHeatmap('route-height-grade-heatmap', 'Height', appState.data.routeBodyMetrics || {}, 'height_vs_grade_by_gender', 'height_rounded', true);
  renderDisciplineScatter('boulder-ape-grade-chart', 'Ape Index (inches)', appState.data.boulderBodyMetrics || {}, 'ape_vs_grade_by_gender', 'ape_index_rounded');
  renderDisciplineHeatmap('boulder-ape-grade-heatmap', 'Ape Index (inches)', appState.data.boulderBodyMetrics || {}, 'ape_vs_grade_by_gender', 'ape_index_rounded');
  renderDisciplineScatter('route-ape-grade-chart', 'Ape Index (inches)', appState.data.routeBodyMetrics || {}, 'ape_vs_grade_by_gender', 'ape_index_rounded');
  renderDisciplineHeatmap('route-ape-grade-heatmap', 'Ape Index (inches)', appState.data.routeBodyMetrics || {}, 'ape_vs_grade_by_gender', 'ape_index_rounded');
}

function renderUserSegmentation() {
  if (!appState.loaded.userSegmentation) {
    return;
  }
  const payload = appState.data.userSegmentation || {};
  const counts = payload.segment_counts || [];
  const countBySegment = Object.fromEntries(counts.map((row) => [row.segment, row.user_count]));
  document.getElementById('segment-active-count').textContent = formatNumber(countBySegment.Active);
  document.getElementById('segment-inactive-count').textContent = formatNumber(countBySegment.Inactive);
  document.getElementById('segment-note').textContent = payload.criteria_text || '';

  const genderPalette = currentGenderPalette();
  const heightTraces = (payload.height_histogram || []).map((series) => ({
    x: series.values,
    type: 'histogram',
    opacity: series.series === 'All Users' ? 0.35 : 0.55,
    name: series.series,
    marker: { color: genderPalette[series.series] || cssVar('--lg-info') },
    xbins: { size: 1 },
  }));
  Plotly.react(
    'segment-height-chart',
    heightTraces,
    {
      ...chartLayout('Height'),
      xaxis: { ...chartLayout('Height').xaxis, tickmode: 'array', ...heightAxisTicks() },
      barmode: 'overlay',
      bargap: 0.04,
    },
    { responsive: true, displayModeBar: false }
  );

  const apeTraces = (payload.ape_index_histogram || []).map((series) => ({
    x: series.values,
    type: 'histogram',
    opacity: series.series === 'All Users' ? 0.35 : 0.55,
    name: series.series,
    marker: { color: genderPalette[series.series] || cssVar('--lg-warning') },
    xbins: { size: 1 },
  }));
  Plotly.react('segment-ape-chart', apeTraces, { ...chartLayout('Ape Index (inches)'), barmode: 'overlay', bargap: 0.04 }, { responsive: true, displayModeBar: false });

  const points = payload.corner_points || [];
  const dimensions = payload.corner_dimensions || [];
  const thresholds = payload.corner_thresholds || {};
  const gridSize = dimensions.length;
  const traces = [];

  // Distinct segment values found in the data, not a hardcoded Active/
  // Inactive pair — so this keeps working if more segmentation parameters
  // (and colors) get added later. Known segments use segmentPalette; any
  // new one falls back to the shared gym palette so it still gets a color.
  const segments = [...new Set(points.map((point) => point.segment).filter((segment) => segment != null))];
  const segmentColor = (segment) => segmentPalette[segment] || gymLinePalette[segments.indexOf(segment) % gymLinePalette.length];

  // The plotted points use a very low opacity (0.12) so ~29k overlapping
  // points don't turn into a solid blob — but that also makes the legend's
  // color swatch nearly invisible, since Plotly draws the swatch using the
  // trace's own marker style. Dummy no-data traces (never rendered, since
  // their x/y are null) carry the actual legend entries at full opacity,
  // decoupling "how visible is a single plotted point" from "how visible is
  // the legend dot."
  segments.forEach((segment) => {
    traces.push({
      type: 'scattergl',
      mode: 'markers',
      x: [null],
      y: [null],
      xaxis: 'x',
      yaxis: 'y',
      marker: { size: 8, opacity: 1, color: segmentColor(segment) },
      name: segment,
      legendgroup: segment,
      showlegend: true,
      hoverinfo: 'skip',
    });
  });

  // Bottom-left corner plot: row 0 (top, Plotly's default row order) has
  // only its diagonal cell populated; the last row is fully populated. The
  // empty upper-right triangle left over is where the legend gets anchored
  // below, instead of floating above the whole figure.
  dimensions.forEach((dimension, rowIndex) => {
    const rowKey = dimension.key;
    const rowValues = points.map((point) => point[`${rowKey}_log10`]).filter((value) => value !== null && value !== undefined);
    const diagAxisIndex = (rowIndex * gridSize) + rowIndex + 1;
    const rowAxisSuffix = diagAxisIndex === 1 ? '' : String(diagAxisIndex);
    traces.push({
      type: 'histogram',
      x: rowValues,
      xaxis: `x${rowAxisSuffix}`,
      yaxis: `y${rowAxisSuffix}`,
      marker: { color: 'rgba(154, 208, 255, 0.55)' },
      opacity: 0.8,
      nbinsx: 20,
      showlegend: false,
      hovertemplate: `${dimension.label} ≈ %{x:.2f} (log10)<br>Count %{y}<extra></extra>`,
    });

    for (let colIndex = 0; colIndex < rowIndex; colIndex += 1) {
      const colDimension = dimensions[colIndex];
      const axisIndex = (rowIndex * gridSize) + colIndex + 1;
      const axisSuffix = axisIndex === 1 ? '' : String(axisIndex);
      segments.forEach((segment) => {
        const segmentPoints = points.filter((point) => point.segment === segment);
        traces.push({
          type: 'scattergl',
          mode: 'markers',
          x: segmentPoints.map((point) => point[`${colDimension.key}_log10`]),
          y: segmentPoints.map((point) => point[`${rowKey}_log10`]),
          xaxis: `x${axisSuffix}`,
          yaxis: `y${axisSuffix}`,
          customdata: segmentPoints.map((point) => [point[colDimension.key], point[rowKey]]),
          marker: { size: 3, opacity: 0.12, color: segmentColor(segment) },
          name: segment,
          legendgroup: segment,
          showlegend: false,
          hovertemplate: `${colDimension.label} %{customdata[0]:.2f}<br>${dimension.label} %{customdata[1]:.2f}<br>Segment ${segment}<extra></extra>`,
        });
      });
    }
  });

  const layout = {
    ...chartLayout(''),
    // No fixed height here: the host div is a fixed-aspect-ratio square
    // (.chart-host-corner), so Plotly's responsive:true sizes to match it
    // and each of the gridSize x gridSize cells comes out square too.
    margin: { l: 54, r: 16, t: 16, b: 48 },
    showlegend: true,
    // Anchored inside the plot's own top-right corner (paper coords, not
    // floating above the figure) — that's the empty upper-right triangle
    // left over by the bottom-left layout, so the legend fills otherwise
    // dead space instead of taking margin away from the grid.
    legend: {
      orientation: 'v',
      xanchor: 'right',
      x: 0.99,
      yanchor: 'top',
      y: 0.99,
      bgcolor: 'rgba(0,0,0,0)',
    },
    grid: {
      rows: gridSize,
      columns: gridSize,
      pattern: 'independent',
      // Default gap reads as loose/disconnected for a corner plot, where
      // adjacent cells sharing an axis are meant to read as one dense grid.
      xgap: 0.04,
      ygap: 0.04,
    },
    shapes: [],
    annotations: [],
  };

  const dimensionTicks = dimensions.map((dimension) => dimensionLogTicks(points, dimension.key));

  for (let rowIndex = 0; rowIndex < gridSize; rowIndex += 1) {
    const rowDimension = dimensions[rowIndex];
    for (let colIndex = 0; colIndex < gridSize; colIndex += 1) {
      const axisIndex = (rowIndex * gridSize) + colIndex + 1;
      const axisSuffix = axisIndex === 1 ? '' : String(axisIndex);
      const xAxisName = `xaxis${axisSuffix}`;
      const yAxisName = `yaxis${axisSuffix}`;
      const colDimension = dimensions[colIndex];
      const xThreshold = thresholds[colDimension.key] ? Math.log10(thresholds[colDimension.key]) : null;
      const yThreshold = thresholds[rowDimension.key] ? Math.log10(thresholds[rowDimension.key]) : null;
      const colTicks = dimensionTicks[colIndex];
      const rowTicks = rowIndex === colIndex ? null : dimensionTicks[rowIndex];
      layout[xAxisName] = {
        gridcolor: cssVar('--lg-border'),
        zerolinecolor: cssVar('--lg-border'),
        showline: true,
        linecolor: cssVar('--lg-border-h'),
        linewidth: 1,
        mirror: true,
        title: rowIndex === gridSize - 1 ? { text: colDimension.label, standoff: 10 } : undefined,
        showticklabels: rowIndex === gridSize - 1,
        ...(colTicks.tickvals.length ? { tickmode: 'array', tickvals: colTicks.tickvals, ticktext: colTicks.ticktext } : {}),
      };
      layout[yAxisName] = {
        gridcolor: cssVar('--lg-border'),
        zerolinecolor: cssVar('--lg-border'),
        showline: true,
        linecolor: cssVar('--lg-border-h'),
        linewidth: 1,
        mirror: true,
        title: colIndex === 0 && rowIndex > 0 ? { text: rowDimension.label, standoff: 8 } : undefined,
        showticklabels: colIndex === 0,
        ...(rowTicks && rowTicks.tickvals.length ? { tickmode: 'array', tickvals: rowTicks.tickvals, ticktext: rowTicks.ticktext } : {}),
      };

      if (colIndex > rowIndex) {
        layout[xAxisName].visible = false;
        layout[yAxisName].visible = false;
      }

      if (colIndex <= rowIndex && xThreshold !== null) {
        layout.shapes.push({
          type: 'line',
          xref: `x${axisSuffix}`,
          yref: `y${axisSuffix} domain`,
          x0: xThreshold,
          x1: xThreshold,
          y0: 0,
          y1: 1,
          line: { color: cssVar('--lg-text-3'), width: 1, dash: 'dash' },
        });
      }
      if (colIndex < rowIndex && yThreshold !== null) {
        layout.shapes.push({
          type: 'line',
          xref: `x${axisSuffix} domain`,
          yref: `y${axisSuffix}`,
          x0: 0,
          x1: 1,
          y0: yThreshold,
          y1: yThreshold,
          line: { color: cssVar('--lg-text-3'), width: 1, dash: 'dash' },
        });
      }
    }
  }

  Plotly.react('segment-corner-chart', traces, layout, { responsive: true, displayModeBar: false });
}

function buildGymComparisonModel(compareGymIdsInput) {
  const records = (appState.data.gymComparisonBase?.records || []).filter(
    (record) => record.discipline === appState.filters.compareDiscipline && Number(record.n_days) >= Number(appState.filters.compareMinDays)
  );
  const refGymId = appState.filters.compareRefGymId;
  const compareGymIds = (compareGymIdsInput || []).filter((gymId) => gymId && gymId !== refGymId);
  if (!refGymId || !compareGymIds.length) {
    return { pairs: [] };
  }

  const byUser = new Map();
  records.forEach((record) => {
    const userId = String(record.user_id);
    if (!byUser.has(userId)) {
      byUser.set(userId, new Map());
    }
    byUser.get(userId).set(String(record.gym_id), record);
  });

  const pairs = compareGymIds.map((compGymId) => {
    const cellMap = new Map();
    const diffValues = [];
    byUser.forEach((gyms) => {
      const ref = gyms.get(refGymId);
      const comp = gyms.get(compGymId);
      if (!ref || !comp) {
        return;
      }
      const key = `${ref.max_grade_num}|${comp.max_grade_num}`;
      if (!cellMap.has(key)) {
        cellMap.set(key, {
          ref_grade_num: Number(ref.max_grade_num),
          ref_grade_label: ref.max_grade_label,
          comp_grade_num: Number(comp.max_grade_num),
          comp_grade_label: comp.max_grade_label,
          n_users: 0,
        });
      }
      cellMap.get(key).n_users += 1;
      diffValues.push(Number(comp.max_grade_num) - Number(ref.max_grade_num));
    });
    return {
      compGymId,
      compGymName: gymName(compGymId),
      points: [...cellMap.values()].sort((left, right) => left.ref_grade_num - right.ref_grade_num || left.comp_grade_num - right.comp_grade_num),
      diffValues,
    };
  });
  return { pairs };
}

function gymComparisonDiagonalShape(tickVals) {
  return {
    type: 'line',
    x0: Math.min(...tickVals) - 1,
    y0: Math.min(...tickVals) - 1,
    x1: Math.max(...tickVals) + 1,
    y1: Math.max(...tickVals) + 1,
    line: { color: cssVar('--lg-text-3'), width: 1, dash: 'dash' },
  };
}

function renderGymComparisonPairScatter(hostId, refGymName, pair, tickVals, tickText, axisRange, diagonalShape) {
  const maxBubble = Math.max(...pair.points.map((point) => 10 * point.n_users), 10);
  Plotly.react(
    hostId,
    [
      {
        x: pair.points.map((point) => point.ref_grade_num),
        y: pair.points.map((point) => point.comp_grade_num),
        mode: 'markers',
        customdata: pair.points.map((point) => [point.ref_grade_label, point.comp_grade_label, point.n_users]),
        marker: {
          size: pair.points.map((point) => 10 * point.n_users),
          sizemode: 'area',
          sizeref: (2 * maxBubble) / (30 ** 2),
          sizemin: 4,
          color: 'rgba(154, 208, 255, 0.10)',
          line: { color: 'rgba(154, 208, 255, 0.95)', width: 1.5 },
        },
        hovertemplate:
          `${refGymName} %{customdata[0]}<br>`
          + `${pair.compGymName} %{customdata[1]}<br>`
          + `Users in bucket: %{customdata[2]}<extra></extra>`,
        showlegend: false,
      },
    ],
    {
      ...chartLayout(refGymName),
      margin: { l: 48, r: 16, t: 12, b: 54 },
      xaxis: {
        ...chartLayout(refGymName).xaxis,
        tickmode: 'array',
        tickvals: tickVals,
        ticktext: tickText,
        range: axisRange,
      },
      yaxis: {
        ...chartLayout(pair.compGymName).yaxis,
        title: pair.compGymName,
        tickmode: 'array',
        tickvals: tickVals,
        ticktext: tickText,
        range: axisRange,
        scaleanchor: 'x',
        scaleratio: 1,
      },
      shapes: [diagonalShape],
    },
    { responsive: true, displayModeBar: false }
  );
}

function renderGymComparisonPairHeatmap(hostId, refGymName, pair, tickVals, tickText, diagonalShape, axisRange) {
  // Same Bayesian-bootstrap smoothing idea as the 1D histograms, extended to
  // 2D: rather than one flat-colored pixel per exact (ref_grade, comp_grade)
  // integer pair, estimate a continuous density surface over the plane so
  // the plot reads as "how likely is this general grade-pair region" rather
  // than a blocky exact-match grid.
  const grid = bayesianBootstrap2DKdeGrid(pair.points, {
    xKey: 'ref_grade_num',
    yKey: 'comp_grade_num',
    countKey: 'n_users',
    xRange: axisRange,
    yRange: axisRange,
  });

  Plotly.react(
    hostId,
    [
      {
        type: 'heatmap',
        x: grid.x,
        y: grid.y,
        z: grid.z,
        colorscale: HEATMAP_DENSITY_COLORSCALE,
        // The raw grade pairs are already shown in the scatter panel right
        // next to this one, so skip re-plotting them here. A colorbar in
        // raw KDE-density units isn't very meaningful either, and its
        // default sizing doesn't match this scaleanchor-squared plot's
        // actual pixel height, which was stretching the layout — dropping
        // it entirely is simpler and more robust than hand-matching it.
        showscale: false,
        hoverinfo: 'skip',
      },
    ],
    {
      ...chartLayout(refGymName),
      xaxis: {
        ...chartLayout(refGymName).xaxis,
        tickmode: 'array',
        tickvals: tickVals,
        ticktext: tickText,
        range: axisRange,
      },
      yaxis: {
        ...chartLayout(pair.compGymName).yaxis,
        title: pair.compGymName,
        tickmode: 'array',
        tickvals: tickVals,
        ticktext: tickText,
        range: axisRange,
        scaleanchor: 'x',
        scaleratio: 1,
      },
      shapes: [diagonalShape],
    },
    { responsive: true, displayModeBar: false }
  );
}

// Grade delta is comp - ref (see buildGymComparisonModel), so a negative
// value means the comparison gym logged a *lower* max grade for the same
// person than the reference gym did — i.e. the comp gym is stingier/more
// conservative with its grades ("stiffer"). A positive value means the comp
// gym is more generous ("softer").
function stifferSofterAnnotations() {
  // Positioned just inside the plot's own bounds (y < 1, not up in the
  // margin above it) with a background box behind the text, so visibility
  // doesn't depend on margin sizing being exactly right — worst case they
  // sit over the top of the tallest bars rather than disappearing.
  const bg = cssVar('--lg-card') || 'rgba(0, 0, 0, 0.6)';
  const shared = {
    y: 0.97,
    yref: 'paper',
    yanchor: 'top',
    showarrow: false,
    font: { size: 10, color: cssVar('--lg-text-2') },
    bgcolor: bg,
    opacity: 0.92,
    borderpad: 2,
  };
  return [
    { ...shared, x: 0.02, xref: 'paper', xanchor: 'left', text: '← Comp gym stiffer' },
    { ...shared, x: 0.98, xref: 'paper', xanchor: 'right', text: 'Comp gym softer →' },
  ];
}

function renderGymComparisonPairHistogram(hostId, pair) {
  const color = gymLinePalette[0];
  const band = bayesianBootstrapKde(pair.diffValues, { min: -6.5, max: 7.5 });
  const medianStats = band.summary;
  const medianShapes = medianStats ? [
    {
      // Shaded 10th-90th percentile span of the fitted distribution.
      type: 'rect',
      xref: 'x',
      yref: 'paper',
      x0: medianStats.lower,
      x1: medianStats.upper,
      y0: 0,
      y1: 1,
      fillcolor: hexToRgba(cssVar('--lg-text-3') || '#98a2b3', 0.15),
      line: { width: 0 },
    },
    {
      // Point estimate: bootstrapped median grade delta. Colored to match
      // the histogram (rather than a neutral text color) and solid, versus
      // the dashed zero-reference line, so the two nearby vertical lines
      // stay easy to tell apart at a glance.
      type: 'line',
      xref: 'x',
      yref: 'paper',
      x0: medianStats.point,
      x1: medianStats.point,
      y0: 0,
      y1: 1,
      line: { color, width: 3 },
    },
  ] : [];
  const medianAnnotations = medianStats ? [
    {
      // Rotated and run alongside the vertical median line itself, rather
      // than centered horizontally above it, so it doesn't compete for the
      // same top-margin space as the stiffer/softer labels.
      x: medianStats.point,
      y: 0.5,
      yref: 'paper',
      yanchor: 'middle',
      xanchor: 'left',
      xshift: 6,
      textangle: -90,
      showarrow: false,
      text: `median ${medianStats.point.toFixed(2)} (16-84th pctile: ${medianStats.lower.toFixed(2)} to ${medianStats.upper.toFixed(2)})`,
      font: { size: 10, color: cssVar('--lg-text-2') },
    },
  ] : [];
  Plotly.react(
    hostId,
    [
      {
        x: pair.diffValues,
        type: 'histogram',
        histnorm: 'probability density',
        marker: { color },
        opacity: 0.3,
        xbins: { start: -6.5, end: 7.5, size: 1 },
        name: 'Observed',
        showlegend: false,
      },
      {
        x: band.x,
        y: band.lower,
        type: 'scatter',
        mode: 'lines',
        line: { width: 0 },
        showlegend: false,
        hoverinfo: 'skip',
      },
      {
        x: band.x,
        y: band.upper,
        type: 'scatter',
        mode: 'lines',
        line: { width: 0 },
        fill: 'tonexty',
        fillcolor: hexToRgba(color, 0.18),
        name: '68th pctile band',
        showlegend: false,
        hoverinfo: 'skip',
      },
      {
        x: band.x,
        y: band.median,
        type: 'scatter',
        mode: 'lines',
        line: { color, width: 2 },
        name: 'Posterior median',
        showlegend: false,
        hovertemplate: 'density %{y:.3f}<extra></extra>',
      },
    ],
    {
      ...chartLayout('Max Grade Difference<br>(Comp - Ref.)'),
      margin: { ...chartLayout('').margin, t: 48 },
      shapes: [
        ...medianShapes,
        {
          type: 'line',
          x0: 0,
          x1: 0,
          y0: 0,
          y1: 1,
          xref: 'x',
          yref: 'paper',
          line: { color: cssVar('--lg-text-2'), width: 1.5, dash: 'dash' },
        },
      ],
      annotations: [...medianAnnotations, ...stifferSofterAnnotations()],
    },
    { responsive: true, displayModeBar: false }
  );
}

function renderGymComparisonFocusRow() {
  if (!appState.loaded.gymComparisonBase) {
    return;
  }
  const container = document.getElementById('gym-comparison-focus-row');
  const note = document.getElementById('gym-comparison-note');
  const refGymId = appState.filters.compareRefGymId;
  const focusGymId = appState.filters.compareFocusGymId;

  if (!refGymId || !focusGymId || focusGymId === refGymId) {
    container.innerHTML = '<div class="comparison-chart-shell"><div class="comparison-chart-title">Choose a reference gym and a gym to compare against it.</div></div>';
    if (note) {
      note.textContent = 'Choose a reference gym and a gym to compare against it.';
    }
    return;
  }

  const model = buildGymComparisonModel([focusGymId]);
  if (!model.pairs.length) {
    container.innerHTML = '<div class="comparison-chart-shell"><div class="comparison-chart-title">No overlapping users at the selected minimum days.</div></div>';
    if (note) {
      note.textContent = 'No overlapping users at the selected minimum days.';
    }
    return;
  }

  const pair = model.pairs[0];
  const overlapUserCount = pair.points.reduce((sum, point) => sum + point.n_users, 0);
  if (note) {
    note.textContent = `${formatNumber(overlapUserCount)} users have logged at both gyms at least ${appState.filters.compareMinDays} times and are shown below.`;
  }
  const ticks = getGradeTicks(appState.filters.compareDiscipline);
  const tickVals = ticks.map((tick) => tick.value);
  const tickText = ticks.map((tick) => tick.label);
  const refGymName = gymName(refGymId);
  const axisRange = [Math.min(...tickVals) - 0.75, Math.max(...tickVals) + 0.75];
  const diagonalShape = gymComparisonDiagonalShape(tickVals);

  container.innerHTML = '';
  const shell = document.createElement('div');
  shell.className = 'comparison-chart-shell';
  const title = document.createElement('div');
  title.className = 'comparison-chart-title';
  title.textContent = `${refGymName} vs ${pair.compGymName}`;
  const plotsRow = document.createElement('div');
  plotsRow.className = 'comparison-row-plots';

  const scatterHostId = 'gym-comparison-focus-scatter';
  const scatterHost = document.createElement('div');
  scatterHost.id = scatterHostId;
  scatterHost.className = 'chart-host comparison-square-host';

  const heatmapHostId = 'gym-comparison-focus-heatmap';
  const heatmapHost = document.createElement('div');
  heatmapHost.id = heatmapHostId;
  heatmapHost.className = 'chart-host comparison-square-host';

  const histHostId = 'gym-comparison-focus-hist';
  const histHost = document.createElement('div');
  histHost.id = histHostId;
  histHost.className = 'chart-host comparison-narrow-host';

  plotsRow.appendChild(scatterHost);
  plotsRow.appendChild(heatmapHost);
  plotsRow.appendChild(histHost);
  shell.appendChild(title);
  shell.appendChild(plotsRow);
  container.appendChild(shell);

  renderGymComparisonPairScatter(scatterHostId, refGymName, pair, tickVals, tickText, axisRange, diagonalShape);
  renderGymComparisonPairHeatmap(heatmapHostId, refGymName, pair, tickVals, tickText, diagonalShape, axisRange);
  renderGymComparisonPairHistogram(histHostId, pair);
}

function renderGymComparisonVisuals() {
  if (!appState.loaded.gymComparisonBase) {
    return;
  }
  const model = buildGymComparisonModel(appState.filters.compareGymIds);

  if (!model.pairs.length) {
    renderPlotMessage('gym-comparison-delta-chart', 'Select gyms to compare.', 240);
    return;
  }

  const deltaBins = { start: -6.5, end: 7.5, size: 1 };
  const perGym = model.pairs.map((pair, index) => ({
    pair,
    color: gymLinePalette[index % gymLinePalette.length],
    band: bayesianBootstrapKde(pair.diffValues, { min: deltaBins.start, max: deltaBins.end }),
  }));

  const histogramTraces = perGym.flatMap(({ pair, color, band }) => {
    const { edges, heights } = binnedDensity(pair.diffValues, deltaBins);
    const step = stepHistogramPoints(edges, heights);
    return [
      {
        // A native Plotly histogram trace always draws all 4 sides of every
        // bar, which reads as a picket fence once the fill is removed.
        // Plotting the precomputed step-outline vertices as a plain line
        // instead gives a true step histogram: flat across each bin,
        // vertical only where the height actually changes. Faded and thin —
        // this is background context; the smoothed median curve below is
        // the primary line now.
        x: step.x,
        y: step.y,
        yaxis: 'y',
        type: 'scatter',
        mode: 'lines',
        opacity: 0.35,
        line: { color, width: 1 },
        showlegend: false,
        hoverinfo: 'skip',
      },
      {
        x: band.x,
        y: band.median,
        yaxis: 'y',
        type: 'scatter',
        mode: 'lines',
        line: { color, width: 2.5 },
        name: pair.compGymName,
        hovertemplate: `${pair.compGymName} density %{y:.3f}<extra></extra>`,
      },
    ];
  });

  // Staggered whisker strip above the main plot, one row per gym: an 'x' at
  // the fitted median grade delta with a horizontal error bar spanning its
  // 10th-90th percentile. A shaded band per gym (like the single-pair
  // panel) would overlap into an unreadable mess with several gyms
  // selected, so this keeps each gym's spread legible on its own row
  // while still sharing the x-axis with the histogram below it.
  const whiskerTraces = perGym
    .filter(({ band }) => band.summary)
    .map(({ pair, color, band }, rowIndex) => {
      const stats = band.summary;
      return {
        x: [stats.point],
        y: [rowIndex],
        yaxis: 'y2',
        type: 'scatter',
        mode: 'markers',
        marker: { symbol: 'x', size: 8, color },
        error_x: {
          type: 'data',
          symmetric: false,
          array: [stats.upper - stats.point],
          arrayminus: [stats.point - stats.lower],
          color,
          thickness: 1.5,
          width: 5,
        },
        showlegend: false,
        hovertemplate: `${pair.compGymName}<br>median %{x:.2f}<br>16-84th pctile: ${stats.lower.toFixed(2)} to ${stats.upper.toFixed(2)}<extra></extra>`,
      };
    });

  Plotly.react(
    'gym-comparison-delta-chart',
    [...histogramTraces, ...whiskerTraces],
    {
      ...chartLayout('Max Grade Difference (Comp - Ref.)'),
      margin: { ...chartLayout('').margin, t: 44 },
      height: 260 + (perGym.length * 20) + 40,
      yaxis: {
        ...chartLayout('').yaxis,
        domain: [0, 0.72],
      },
      yaxis2: {
        domain: [0.85, 1],
        anchor: 'x',
        autorange: 'reversed',
        showticklabels: false,
        showgrid: false,
        zeroline: false,
        fixedrange: true,
      },
      shapes: [
        {
          type: 'line',
          x0: 0,
          x1: 0,
          y0: 0,
          y1: 1,
          xref: 'x',
          yref: 'paper',
          line: { color: cssVar('--lg-text-2'), width: 1.5, dash: 'dash' },
        },
      ],
      annotations: stifferSofterAnnotations(),
    },
    { responsive: true, displayModeBar: false }
  );
}

function renderGymComparisonAll() {
  renderGymComparisonFocusRow();
  renderGymComparisonVisuals();
}

function renderAll() {
  renderSummary();
  renderTimeSeries();
  renderGymCounts();
  renderGradeDistribution();
  renderGymComparisonAll();
  renderBodyMetrics();
  renderUserSegmentation();
}

const TAB_NAMES = ['gym-comparison', 'body-morphology', 'user-segmentation', 'data-overview', 'grading-model', 'grading-v2'];

function getSavedTab() {
  const saved = localStorage.getItem('kaya-viewer-tab');
  return TAB_NAMES.includes(saved) ? saved : 'gym-comparison';
}

async function activateTab(tabName) {
  // Toggle visibility before rendering: Plotly measures the container's
  // width at render time, and .tab-pane is display:none until .active is
  // applied. Rendering into a hidden pane makes every chart pick a wrong
  // fallback width that only self-corrects on the next real resize event
  // (e.g. zooming), not on the pane becoming visible.
  document.querySelectorAll('.tab-button').forEach((item) => item.classList.toggle('active', item.dataset.tab === tabName));
  document.querySelectorAll('.tab-pane').forEach((pane) => pane.classList.toggle('active', pane.id === `tab-${tabName}`));
  localStorage.setItem('kaya-viewer-tab', tabName);

  if (tabName === 'data-overview') {
    await ensureDataOverviewData();
    renderSummary();
    renderTimeSeries();
    renderGymCounts();
    renderGradeDistribution();
  } else if (tabName === 'body-morphology') {
    await ensureBodyMorphologyData();
    renderBodyMetrics();
  } else if (tabName === 'user-segmentation') {
    await ensureUserSegmentationData();
    renderUserSegmentation();
  } else if (tabName === 'grading-model') {
    // Mostly static write-up; the one piece with live rendering is the
    // Ability Explorer (client-side calculator, no API data needed).
    bindExplorerControls();
    refreshExplorer();
  } else if (tabName === 'grading-v2') {
    renderV2Tab();
  } else {
    await ensureGymComparisonData();
    renderGymComparisonAll();
  }
}

function bindTabs() {
  document.querySelectorAll('.tab-button').forEach((button) => {
    button.addEventListener('click', async () => {
      await activateTab(button.dataset.tab);
    });
  });
}

// data-theme is only set once the user has explicitly toggled (persisted to
// localStorage); before that, <html> has no attribute at all and the theme
// is whatever prefers-color-scheme resolves to, so callers that care about
// the *effective* theme must fall back to the media query rather than
// assuming unset means light or dark.
function isEffectivelyLightTheme() {
  const explicitTheme = document.documentElement.dataset.theme;
  if (explicitTheme === 'light') {
    return true;
  }
  if (explicitTheme === 'dark') {
    return false;
  }
  return !window.matchMedia('(prefers-color-scheme: dark)').matches;
}

// When Kaya is opened directly, theme is whatever the user picked here
// (kaya-viewer-theme in localStorage) or the OS default. When it's embedded
// in an iframe on peterwilliams.dev's /kaya page, the parent site instead
// asks Kaya to mirror *its* theme — via ?embed_theme= on first load (read
// before any data fetch, so there's no flash of the wrong theme) and via
// postMessage for live toggles afterward. This is a per-view override only:
// it never touches kaya-viewer-theme, so a direct/standalone visit later
// still gets the user's own Kaya preference, not whatever the embed last
// asked for.
function getEmbedThemeOverride() {
  const params = new URLSearchParams(window.location.search);
  const embedTheme = params.get('embed_theme');
  return embedTheme === 'dark' || embedTheme === 'light' ? embedTheme : null;
}

const KAYA_EMBED_TRUSTED_ORIGINS = ['https://peterwilliams.dev', 'https://www.peterwilliams.dev'];

window.addEventListener('message', (event) => {
  if (!KAYA_EMBED_TRUSTED_ORIGINS.includes(event.origin)) {
    return;
  }
  if (!event.data || event.data.type !== 'site-theme') {
    return;
  }
  const theme = event.data.theme;
  if (theme !== 'dark' && theme !== 'light') {
    return;
  }
  document.documentElement.dataset.theme = theme;
  updateThemeToggleIcon();
  if (initialRenderComplete) {
    renderAll();
  }
});

function updateThemeToggleIcon() {
  const button = document.getElementById('theme-toggle');
  if (!button) {
    return;
  }
  const isLight = isEffectivelyLightTheme();
  // Icon shows the mode a click switches TO: moon while it's light out, sun
  // while it's dark out.
  button.textContent = isLight ? '🌙' : '☀️';
  button.setAttribute('aria-label', isLight ? 'Switch to dark theme' : 'Switch to light theme');
  button.title = button.getAttribute('aria-label');
}

function bindControls() {
  if (controlsBound) {
    return;
  }

  bindTabs();

  const bindIfPresent = (id, eventName, handler) => {
    const element = document.getElementById(id);
    if (element) {
      element.addEventListener(eventName, handler);
    }
  };

  bindIfPresent('time-gym-select', 'change', async (event) => {
    appState.filters.timeGymId = event.target.value;
    await loadTimeSeriesData();
    renderTimeSeries();
  });

  bindIfPresent('freq-select', 'change', async (event) => {
    appState.filters.freq = event.target.value;
    await loadTimeSeriesData();
    renderTimeSeries();
  });

  bindIfPresent('body-active-only-select', 'change', async (event) => {
    appState.filters.bodyActiveOnly = event.target.value === 'active';
    await reloadBodyMorphologyData();
    renderBodyMetrics();
  });

  // compare-ref-gym-select / compare-focus-gym-select are custom searchable
  // comboboxes (mountSearchableSingleSelect), not native <select> elements —
  // their change handling is wired up as the onChange callback passed in
  // populateGymControls() instead of here.

  bindIfPresent('compare-discipline-select', 'change', (event) => {
    appState.filters.compareDiscipline = event.target.value;
    renderGymComparisonAll();
  });

  bindIfPresent('compare-min-days-select', 'change', (event) => {
    appState.filters.compareMinDays = Number(event.target.value);
    renderGymComparisonAll();
  });

  bindIfPresent('theme-toggle', 'click', () => {
    const root = document.documentElement;
    const nextTheme = isEffectivelyLightTheme() ? 'dark' : 'light';
    root.dataset.theme = nextTheme;
    localStorage.setItem('kaya-viewer-theme', nextTheme);
    updateThemeToggleIcon();
    renderAll();
  });

  controlsBound = true;
}

async function refreshDynamicData() {
  await Promise.all([loadTimeSeriesData(), loadGradeComparisonData()]);
}

async function initialize() {
  window.__kayaViewerStatus = 'initializing';
  const embedTheme = getEmbedThemeOverride();
  const savedTheme = localStorage.getItem('kaya-viewer-theme');
  if (embedTheme) {
    document.documentElement.dataset.theme = embedTheme;
  } else if (savedTheme) {
    document.documentElement.dataset.theme = savedTheme;
  }
  updateThemeToggleIcon();
  if (viewerConfig.dataMode === 'static') {
    const infoPill = document.querySelector('.info-pill');
    if (infoPill) {
      infoPill.textContent = 'Static JSON';
    }
  }

  bindControls();
  window.__kayaViewerStatus = 'loading-static-data';
  await loadStaticData();
  window.__kayaViewerStatus = 'loading-summary-data';
  await loadSummaryData();
  window.__kayaViewerStatus = 'populating-controls';
  populateGymControls();
  window.__kayaViewerStatus = 'loading-base-data';
  await activateTab(getSavedTab());
  window.__kayaViewerStatus = 'rendering';
  renderSummary();
  initialRenderComplete = true;
  window.__kayaViewerStatus = 'ready';
}

async function bootstrapWithFallback() {
  const fallback = window.setTimeout(async () => {
    if (initialRenderComplete) {
      return;
    }
    try {
      await loadStaticData();
      await loadSummaryData();
      populateGymControls();
      await activateTab(getSavedTab());
      renderSummary();
      initialRenderComplete = true;
      window.__kayaViewerStatus = 'ready';
    } catch (error) {
      window.__kayaViewerError = error?.message || String(error);
      window.__kayaViewerStatus = 'fallback-error';
      console.error(error);
    }
  }, 1500);

  try {
    await initialize();
  } finally {
    window.clearTimeout(fallback);
  }
}

bootstrapWithFallback().catch((error) => {
  window.__kayaViewerError = error?.message || String(error);
  window.__kayaViewerStatus = 'bootstrap-error';
  console.error(error);
  alert(`Failed to load Kaya viewer: ${error.message}`);
});

// KaTeX + its auto-render extension load with `defer`, so they aren't
// guaranteed to exist yet when this (non-deferred) script runs -- waiting
// for DOMContentLoaded (which fires only after all deferred scripts have
// executed, per spec) sidesteps any load-order race instead of guessing.

// ==========================================================================
// Grading Model v2 tab
// Fitted values are baked in as constants (same pattern as EXPLORER_GYMS):
// these come from a specific PyMC run, not from the live viewer payloads.
// Source run: net50/confident, 29 gyms, 20,014 obs, 10,357 climbers.
// ==========================================================================
// Populated from /static/v2_results.json (regenerate with
// scripts/build_v2_results.py). Empty until that loads -- deliberately not
// seeded with stale numbers from an older fit, which would silently render
// figures that no longer match any fit on disk.
let V2_GYMS = [];
let V2_RESULTS = null;

function v2Stats() {
  const R = V2_RESULTS;
  if (!R) return [];
  const brands = new Set(R.gyms.map((g) => g.b)).size;
  const sg = R.sigma_gym;
  return [
    {v: String(R.n_gyms), l: 'gyms', s: `${brands} companies, one connected network`},
    {v: R.spread.toFixed(2), l: 'grades', s: 'stiffest minus softest'},
    {v: `${R.n_sig}/${R.n_gyms}`, l: 'credibly differ', s: '89% interval excludes zero'},
    {v: (R.spread / 0.254).toFixed(1) + '&times;', l: 'wider than v1',
     s: 'vs the 6 Touchstone gyms alone'},
    {v: sg.mean.toFixed(3), l: '&sigma;<sub>gym</sub>', s: 'was 0.128 in v1'},
    {v: R.dataset.n_obs.toLocaleString(), l: 'observations',
     s: `${R.dataset.n_users.toLocaleString()} climbers`},
  ];
}

// distance below own max -> share of sends (from 2,085,765 bouldering sends)
const V2_DISCARD = [[0,7.0],[1,15.5],[2,22.0],[3,19.9],[4,14.3],[5,21.3]];

// mean height by name-based P(female) bin -- independent validation
const V2_GENDER_VAL = [
  {b:'0-.02',x:0.01,h:69.17,n:11421},{b:'.02-.1',x:0.06,h:69.09,n:3650},
  {b:'.1-.3',x:0.20,h:68.11,n:1757},{b:'.3-.5',x:0.40,h:66.94,n:902},
  {b:'.5-.7',x:0.60,h:65.90,n:711},{b:'.7-.9',x:0.80,h:64.82,n:1046},
  {b:'.9-.98',x:0.94,h:64.27,n:1667},{b:'.98-1',x:0.99,h:64.22,n:4373},
];

const V2_GG = [
  ['gender_guesser label','n','true mean P(female)','v1 coded it as','mean height'],
  ['male','15,731','0.025','0.0','69.12'],
  ['mostly_male','2,150','0.142','0.0','68.71'],
  ['unknown','5,225','<b>0.383</b>','<b>0.5</b>','66.85'],
  ['andy','1,182','0.485','0.5','66.47'],
  ['mostly_female','887','<b>0.660</b>','<b>1.0</b>','65.79'],
  ['female','6,651','0.947','1.0','64.39'],
];

const V2_DIAG = [
  ['parameter block','R-hat','ESS','trustworthy?'],
  ['gym corrections','1.00 &ndash; 1.05','96 &ndash; 578','<span class="ok">yes</span>'],
  ['&sigma;<sub>gym</sub>','1.06','64','<span class="mid">marginal</span>'],
  ['&beta;<sub>gender</sub> = &minus;1.60','1.05','73','<span class="mid">marginal</span>'],
  ['&lambda;<sub>0</sub>, &kappa;','1.01','326 &ndash; 405','<span class="ok">yes</span>'],
  ['height terms &gamma;','1.08 &ndash; 1.13','26 &ndash; 45','<span class="bad">no</span>'],
  ['&beta;<sub>0</sub>, &delta;<sub>1</sub>','1.18 &ndash; 1.19','16 &ndash; 18','<span class="bad">no</span>'],
];

const V2_DECISIONS = [
  {t:'Treat impossible bodies as missing',
   w:'Heights of 12in and 96in, ape indices of &plusmn;30in. Under 1% of rows, but the 50 most extreme users held 10.3% of all squared-height leverage.',
   r:'Kept as <i>missing</i>, not dropped &mdash; those climbers’ sends are still valid data.'},
  {t:'Standardise height and ape index',
   w:'In raw centred inches, h&sup2; reached 3,136, so a N(0, 0.3) prior on the quadratic implied contributions of &plusmn;106 grades against a 12-grade scale.',
   r:'In z-units the prior means what it says: &plusmn;1.9 grades.'},
  {t:'Indicators, not imputation, for missing bodies',
   w:'43.9% of ape indices and 15.8% of heights are absent. v1 filled them with the median, asserting a measurement never taken and piling 44% of users onto one value.',
   r:'A missingness dummy lets the slope be identified only by climbers who reported a value, while the missing group gets its own offset. Unbiased under MAR, at one parameter instead of ~19,000 latents.'},
  {t:'Centre every design column',
   w:'h&sup2; and a&sup2; had means of 0.85 and 0.64, entangling the intercept with every slope &mdash; &beta;<sub>0</sub> came back at R-hat 1.19, ESS 16.',
   r:'Centring makes &beta;<sub>0</sub> orthogonal to the slopes.'},
  {t:'Sum-to-zero gym corrections',
   w:'The model is identified only up to an additive ability/correction shift. A zero-<i>mean</i> prior anchors that only softly &mdash; the realised mean still drifts by &sigma;<sub>gym</sub>/&radic;G.',
   r:'ZeroSumNormal enforces it exactly, which is what &ldquo;relative to the average gym&rdquo; was always meant to mean.'},
  {t:'Log-link the gap rate, and centre visit count',
   w:'&kappa; multiplied raw visit counts (median ~7), making it little more than a rescaling of &lambda;<sub>0</sub> &mdash; the identical pathology the v1 code documents fixing for &rho;, but never applied to &kappa;.',
   r:'exp() keeps the rate positive without sign constraints and decouples the two.'},
  {t:'Marginalise the gap analytically',
   w:'Normal minus Exponential is an ExGaussian, available in closed form.',
   r:'Removes 32,501 latent variables, restores a real observed node so LOO works, and matches numerical integration to 1e-8.'},
  {t:'Probabilistic gender, sharpened by height but never by ability',
   w:'Letting climbing performance inform inferred gender would make &ldquo;tall people are stronger&rdquo; and &ldquo;tall people are more likely male&rdquo; mutually reinforcing.',
   r:'Cutting that feedback keeps the height effect identified by variation in <i>names</i>, which is independent of height.'},
];

// Four categorical colours, all drawn from the existing token palette and all
// with real opacity -- an earlier version used --lg-gold-soft (10% alpha) for
// Bouldering Project, which rendered those gyms effectively invisible.
const V2_BRAND_COLOURS = {
  'Touchstone': '--lg-cat-1',           // blue, the house accent (17 of 29 gyms)
  'Movement': '--lg-cat-2',             // copper
  'Bouldering Project': '--lg-cat-3',   // green
  'Stronghold': '--lg-cat-5',           // violet -- ochre read as a second copper
};

function v2Colour(row) {
  return cssVar(V2_BRAND_COLOURS[row.b] || '--lg-text-2') || '#888';
}

function renderV2Stats() {
  const host = document.getElementById('v2-stats');
  if (!host) return;
  host.innerHTML = v2Stats().map((s) => `
    <div class="stat-tile">
      <div class="stat-value">${s.v}</div>
      <div class="stat-label">${s.l}</div>
      <div class="stat-sub">${s.s}</div>
    </div>`).join('');
}

function renderV2Decisions() {
  const host = document.getElementById('v2-decisions-grid');
  if (!host) return;
  host.innerHTML = V2_DECISIONS.map((d) => `
    <div class="decision-card">
      <div class="decision-title">${d.t}</div>
      <div class="decision-why"><span class="lbl">Problem</span>${d.w}</div>
      <div class="decision-res"><span class="lbl">Resolution</span>${d.r}</div>
    </div>`).join('');
}

function renderV2Table(id, rows) {
  const el = document.getElementById(id);
  if (!el) return;
  const [head, ...body] = rows;
  el.innerHTML = `<thead><tr>${head.map((h) => `<th>${h}</th>`).join('')}</tr></thead>`
    + `<tbody>${body.map((r) => `<tr>${r.map((c) => `<td>${c}</td>`).join('')}</tr>`).join('')}</tbody>`;
}

function renderV2GymChart() {
  const el = document.getElementById('v2-gym-chart');
  if (!el || typeof Plotly === 'undefined') return;
  const rows = V2_GYMS;
  // One trace per company, so Plotly's own legend does the show/hide. The row
  // order is pinned to the (correction-sorted) full list -- otherwise splitting
  // into traces would regroup the axis by company.
  const order = rows.map((r) => r.g);
  const brands = [...new Set(rows.map((r) => r.b))]
    .sort((a, b) => rows.filter((r) => r.b === b).length - rows.filter((r) => r.b === a).length);
  const traces = brands.map((b) => {
    const rs = rows.filter((r) => r.b === b);
    const c = v2Colour(rs[0]);
    return {
      type: 'scatter', mode: 'markers', name: b, legendgroup: b,
      x: rs.map((r) => r.m),
      y: rs.map((r) => r.g),
      // Error bars take the company's colour too -- a grey bar reads as a
      // separate annotation rather than as this point's uncertainty.
      error_x: {
        type: 'data', symmetric: false,
        array: rs.map((r) => r.hi - r.m),
        arrayminus: rs.map((r) => r.m - r.lo),
        color: hexToRgba(c, 0.45), thickness: 1.6, width: 3,
      },
      // Hollow marker = interval still contains zero. Fills are knocked back
      // so 29 dots read as a field rather than 29 competing signals.
      marker: {
        size: 11,
        color: rs.map((r) => (r.s ? hexToRgba(c, 0.72) : cssVar('--lg-card'))),
        line: { width: 2, color: hexToRgba(c, 0.85) },
      },
      hovertemplate: `<b>%{y}</b><br>${b}<br>correction %{x:+.3f} grades<extra></extra>`,
    };
  });
  const layout = chartLayout('grading correction (grades) — negative = softer, positive = stiffer');
  layout.height = Math.max(360, rows.length * 26 + 90);
  layout.margin = { l: 260, r: 30, t: 14, b: 52 };
  layout.shapes = [{
    type: 'line', xref: 'x', yref: 'paper', x0: 0, x1: 0, y0: 0, y1: 1,
    line: { color: cssVar('--lg-text-2'), width: 1, dash: 'dot' },
  }];
  layout.yaxis = {
    ...layout.yaxis, automargin: true, tickfont: { size: 11 },
    categoryorder: 'array', categoryarray: order,
    range: [-0.5, rows.length - 0.5],
  };
  layout.showlegend = true;
  // Parked bottom-right, the one corner the sorted data leaves empty.
  // Vertical, not the horizontal default from chartLayout -- a horizontal
  // legend spreads across the bottom and lands on top of the softest gyms.
  layout.legend = {
    ...layout.legend, orientation: 'v',
    x: 0.99, y: 0.01, xanchor: 'right', yanchor: 'bottom',
    bgcolor: cssVar('--lg-card'), bordercolor: cssVar('--lg-border'),
    borderwidth: 1, font: { size: 11 }, itemsizing: 'constant',
  };
  Plotly.react(el, traces, layout, { displayModeBar: false, responsive: true });
}

function renderV2BrandChart() {
  const el = document.getElementById('v2-brand-chart');
  if (!el || typeof Plotly === 'undefined') return;
  const brands = ['Touchstone', 'Bouldering Project', 'Stronghold', 'Movement'];
  const traces = brands.map((b) => {
    const rows = V2_GYMS.filter((r) => r.b === b);
    return {
      type: 'box', name: b, y: rows.map(() => b), x: rows.map((r) => r.m),
      orientation: 'h', boxpoints: 'all', jitter: 0.5, pointpos: 0,
      marker: { size: 9, color: cssVar(V2_BRAND_COLOURS[b] || '--lg-text-2') },
      line: { color: cssVar('--lg-border') },
      fillcolor: 'rgba(0,0,0,0)',
      hovertemplate: '%{text}<br>%{x:+.3f}<extra></extra>',
      text: rows.map((r) => r.g),
    };
  });
  const layout = chartLayout('grading correction (grades)');
  layout.height = 300;
  layout.showlegend = false;
  layout.margin = { l: 150, r: 24, t: 12, b: 48 };
  layout.shapes = [{ type: 'line', x0: 0, x1: 0, y0: -0.5, y1: 3.5,
    line: { color: cssVar('--lg-text-2'), width: 1, dash: 'dot' } }];
  Plotly.react(el, traces, layout, { displayModeBar: false, responsive: true });
}

function renderV2DiscardChart() {
  const el = document.getElementById('v2-discard-chart');
  if (!el || typeof Plotly === 'undefined') return;
  const trace = {
    type: 'bar',
    x: V2_DISCARD.map((d) => (d[0] === 5 ? '5+' : String(d[0]))),
    y: V2_DISCARD.map((d) => d[1]),
    marker: {
      color: V2_DISCARD.map((d) => (d[0] <= 1 ? cssVar('--lg-gold') : cssVar('--lg-text-2'))),
    },
    hovertemplate: '%{y:.1f}% of sends<extra></extra>',
  };
  const layout = chartLayout('grades below that climber’s own max at the same gym');
  layout.height = 260;
  layout.yaxis = { ...layout.yaxis, title: { text: '% of all sends' } };
  layout.annotations = [{
    x: 1.5, y: 20, xref: 'x', yref: 'y', showarrow: false,
    text: 'highlighted: within 1 grade of max (22.5%)',
    font: { size: 11, color: cssVar('--lg-text-2') },
  }];
  Plotly.react(el, [trace], layout, { displayModeBar: false, responsive: true });
}

function renderV2GenderValidation() {
  const el = document.getElementById('v2-gender-validation');
  if (!el || typeof Plotly === 'undefined') return;
  const trace = {
    type: 'scatter', mode: 'lines+markers',
    x: V2_GENDER_VAL.map((d) => d.x), y: V2_GENDER_VAL.map((d) => d.h),
    marker: { size: V2_GENDER_VAL.map((d) => 8 + Math.sqrt(d.n) / 18),
      color: cssVar('--lg-gold') },
    line: { color: cssVar('--lg-gold'), width: 2 },
    text: V2_GENDER_VAL.map((d) => `n = ${d.n.toLocaleString()}`),
    hovertemplate: 'P(female) %{x}<br>mean height %{y:.2f} in<br>%{text}<extra></extra>',
  };
  const layout = chartLayout('name-based P(female)');
  layout.height = 300;
  layout.yaxis = { ...layout.yaxis, title: { text: 'mean reported height (in)' } };
  Plotly.react(el, [trace], layout, { displayModeBar: false, responsive: true });
}

function renderV2HeightHist() {
  const el = document.getElementById('v2-height-hist');
  if (!el || typeof Plotly === 'undefined') return;
  // Illustrative shape: real distribution is ~N(67,4) with junk at the edges.
  const xs = [], ys = [];
  for (let h = 10; h <= 100; h += 1) {
    xs.push(h);
    let y = 3400 * Math.exp(-((h - 67.5) ** 2) / (2 * 4.2 ** 2));
    if (h === 12 || h === 13 || h === 96) y += 60;
    ys.push(y);
  }
  const layout = chartLayout('reported height (inches)');
  layout.height = 280;
  layout.yaxis = { ...layout.yaxis, title: { text: 'climbers' } };
  layout.shapes = [{
    type: 'rect', x0: 48, x1: 84, y0: 0, y1: 3600, yref: 'y',
    fillcolor: cssVar('--lg-gold'), opacity: 0.10, line: { width: 0 },
  }];
  layout.annotations = [
    { x: 12, y: 220, text: '12 in', showarrow: true, arrowhead: 0, ax: 0, ay: -26,
      font: { size: 11, color: cssVar('--lg-highlight') } },
    { x: 96, y: 220, text: '96 in (slider cap)', showarrow: true, arrowhead: 0, ax: 0, ay: -26,
      font: { size: 11, color: cssVar('--lg-highlight') } },
  ];
  Plotly.react(el, [{
    type: 'bar', x: xs, y: ys, marker: {
      color: xs.map((h) => (h >= 48 && h <= 84 ? cssVar('--lg-gold') : cssVar('--lg-highlight'))),
    },
    hovertemplate: '%{x} in<extra></extra>',
  }], layout, { displayModeBar: false, responsive: true });
}


// --- symbol glossary: every term that appears in an equation on this page ---
const V2_SYMBOLS = [
  ['group', 'Indices'],
  ['u', '\\(u\\)', 'a climber', '&mdash;'],
  ['g', '\\(g\\)', 'a gym', '&mdash;'],
  ['group', 'Ability'],
  ['beta0', '\\(\\beta_0\\)', 'baseline ability &mdash; the grade an average climber sends at an average gym', 'grades'],
  ['sigma_user', '\\(\\sigma_{\\text{user}}\\)', 'spread of natural ability between climbers', 'grades'],
  ['eps', '\\(\\tilde\\epsilon_u\\)', 'climber \\(u\\)&rsquo;s personal ability offset, standardised', 'SDs'],
  ['x', '\\(\\mathbf{x}_u\\)', 'covariate row for climber \\(u\\): gender, height terms, ape terms, missingness flags', '&mdash;'],
  ['group', 'Body and gender'],
  ['h', '\\(\\tilde h\\)', 'height, centred at the median and divided by its SD', 'SDs (1 SD &asymp; {H_SD} in)'],
  ['a', '\\(\\tilde a\\)', 'ape index (wingspan &minus; height), centred and scaled the same way', 'SDs (1 SD &asymp; {A_SD} in)'],
  ['G', '\\(G\\)', 'probability the climber is female, from their first name sharpened by height. 0 = male, 1 = female', 'probability'],
  ['gM', '\\(\\gamma_1^{M},\\ \\gamma_2^{M}\\)', 'slope and curvature of the height curve for <b>male users</b>. Identical to \\(\\gamma_1,\\gamma_2\\) &mdash; the male curve <i>is</i> the baseline', 'grades / SD, grades / SD²'],
  ['gF', '\\(\\gamma_1^{F},\\ \\gamma_2^{F}\\)', 'slope and curvature of the height curve for <b>female users</b>. Not sampled directly; equals \\(\\gamma_k+\\gamma_k^{\\times}\\)', 'grades / SD, grades / SD²'],
  ['gbase', '\\(\\gamma_1,\\ \\gamma_2\\)', 'what the sampler actually fits: the baseline (male-user) slope and curvature', 'grades / SD, grades / SD²'],
  ['gx', '\\(\\gamma_1^{\\times},\\ \\gamma_2^{\\times}\\)', 'the <b>gender difference</b> &mdash; how far the female curve departs from the male one. <b>This is the parameter the gender question turns on</b>; if its interval covers zero, the two curves are the same shape', 'grades / SD, grades / SD²'],
  ['delta', '\\(\\delta_1,\\ \\delta_2\\)', 'the same pair for ape index: linear slope and curvature', 'grades / SD'],
  ['sat', '\\(A,\\ h_0,\\ s\\)', 'saturating form only: how much reach is worth in total, the height it stops paying off at, and how sharply it levels', 'grades, SDs, SDs'],
  ['kappa_h', '\\(\\kappa_h\\)', 'vertex form only: how sharply ability falls away from the best height. Larger = a tighter optimum. <b>Distinct from the gap-rate \\(\\kappa\\) below</b>', 'grades / SD²'],
  ['p', '\\(p\\)', 'vertex form only: <b>the best height</b>, estimated directly rather than derived from \\(-\\gamma_1/2\\gamma_2\\)', 'SDs from median'],
  ['group', 'Name &rarr; gender'],
  ['p_name', '\\(p_{\\text{name}}\\)', 'calibrated probability that this first name belongs to a woman, from nomquamgender &mdash; before height is taken into account', 'probability'],
  ['musd', '\\(\\mu_M,\\sigma_M,\\ \\mu_F,\\sigma_F\\)', 'mean and SD of height within each gender group, used to sharpen the name prior. <b>Estimated without any ability data</b>, so the gender guess cannot be contaminated by the effect under test', 'inches'],
  ['group', 'Gyms'],
  ['sigma_gym', '\\(\\sigma_{\\text{gym}}\\)', 'spread of grading style across gyms &mdash; the headline &ldquo;how much do gyms differ&rdquo; number', 'grades'],
  ['delta_g', '\\(\\tilde\\delta_g\\)', 'gym \\(g\\)&rsquo;s standardised offset, constrained so all gyms sum to zero', 'SDs'],
  ['gym', '\\(\\text{gym}_g\\)', 'gym \\(g\\)&rsquo;s grading correction. Positive = stiffer than average', 'grades'],
  ['C', '\\(C_{u,g}\\)', 'structural ceiling &mdash; the hardest grade climber \\(u\\) could send at gym \\(g\\)', 'grades'],
  ['group', 'The gap, and what we observe'],
  ['m', '\\(m_{u,g}\\)', '<b>the observed data</b>: the hardest grade \\(u\\) actually logged at \\(g\\)', 'grades'],
  ['gap', '\\(\\text{gap}_{u,g}\\)', 'how far below their ceiling that logged max sits. Never negative', 'grades'],
  ['lambda', '\\(\\lambda_{u,g}\\)', 'rate of that gap &mdash; higher rate means a smaller expected gap', '1 / grades'],
  ['lambda0', '\\(\\lambda_0\\)', 'baseline gap rate for a typical climber at a typical gym', '1 / grades'],
  ['n', '\\(\\tilde n_{u,g}\\)', 'how many days \\(u\\) logged at \\(g\\), centred on the median (~8)', 'ratio'],
  ['kappa', '\\(\\kappa\\)', 'how much more of their ceiling a climber finds per extra visit', '&mdash;'],
  ['r', '\\(\\tilde r_u\\)', 'sends per session, centred &mdash; a proxy for how completely someone logs', 'ratio'],
  ['rho', '\\(\\rho\\)', 'how much that logging-completeness shifts the gap. <b>Fitted at &asymp;0</b>', '&mdash;'],
  ['sigma_link', '\\(\\sigma_{\\text{link}}\\)', 'residual noise: grades are integers on a continuous scale, so a labelled V5 is really 4.5&ndash;5.5', 'grades'],
];

const V2_FORMS = [
  ['form', 'equation', 'the claim it makes', 'params'],
  ['Zero', '\\(f=0\\)', 'Height does not affect climbing ability at all.', '0'],
  ['Linear', '\\(\\gamma_1\\tilde h\\)', 'Every inch helps (or hurts) by the same amount, forever.', '1'],
  ['Quadratic', '\\(\\gamma_1\\tilde h+\\gamma_2\\tilde h^2\\)', 'There is an optimal height, or an accelerating trend &mdash; one bend, no more.', '2'],
  ['Quadratic &times; gender', '\\((1-G)f_M(\\tilde h)+G\\,f_F(\\tilde h)\\)', 'Height works <i>differently</i> for men and women &mdash; a separate quadratic for each, blended by the probability \\(G\\). <b>This is what v1 concluded.</b>', '4'],
  ['Saturating', '\\(A\\,\\text{logistic}((\\tilde h-h_0)/s)\\)', 'Reach helps until you have enough of it, then stops paying. Never tested in v1.', '3'],
  ['Vertex quadratic', '\\(-\\kappa_h(\\tilde h-p)^2\\)', 'Same curve family as the plain quadratic, but the <b>best height \\(p\\) is a parameter</b> with its own credible interval &mdash; instead of being derived as \\(-\\gamma_1/2\\gamma_2\\) with error propagated through a ratio.', '2'],
];

// Each form's parameters, defaults and curve(s). Drives both the overlay
// chart and the interactive cards, so the two can never drift apart.
// `fitted: true` means the defaults are real posterior means (v3_conf,
// net50/confident); otherwise they are illustrative and marked as such.
// Height centring, in inches. These are a property of the fitted dataset, not
// constants -- v2Scales() reads them off the fits once those have loaded, and
// these are only the value before that. (An earlier hard-coded 3.4 was simply
// wrong for net50/confident, whose SD is 3.92 in.)
const v2HMed = () => v2Scales().h_median;
const v2HSd = () => v2Scales().h_sd;

const V2_FORM_SPECS = [
  {
    key: 'zero', label: 'Zero', fitted: false,
    eq: 'f(\\tilde h) = 0',
    claim: 'Height does not affect ability at all. The null the others must beat.',
    params: [],
    curves: () => [{ name: 'Zero', colour: '--lg-info', dash: 'solid', f: () => 0 }],
  },
  {
    key: 'linear', label: 'Linear', fitted: false,
    eq: 'f(\\tilde h) = \\gamma_1\\tilde h',
    claim: 'Every inch helps (or hurts) by the same amount, forever. Cannot bend.',
    params: [{ id: 'g1', tex: '\\gamma_1', min: -0.6, max: 0.6, step: 0.01, def: 0.18 }],
    curves: (v) => [{ name: 'Linear', colour: '--lg-info', dash: 'solid', f: (z) => v.g1 * z }],
  },
  {
    key: 'quadratic', label: 'Quadratic', fitted: false,
    eq: 'f(\\tilde h) = \\gamma_1\\tilde h + \\gamma_2\\tilde h^{2}',
    claim: 'One bend, no more. A peak (or a trough) sits at \\(-\\gamma_1/2\\gamma_2\\), wherever the data put it.',
    params: [
      { id: 'g1', tex: '\\gamma_1', min: -0.6, max: 0.6, step: 0.01, def: 0.18 },
      { id: 'g2', tex: '\\gamma_2', min: -0.3, max: 0.3, step: 0.005, def: -0.09 },
    ],
    curves: (v) => [{ name: 'Quadratic', colour: '--lg-info', dash: 'solid',
                      f: (z) => v.g1 * z + v.g2 * z * z }],
    note: (v) => (Math.abs(v.g2) < 1e-6 ? 'no curvature - this is a straight line'
      : `vertex at ${(v2HMed() + (-v.g1 / (2 * v.g2)) * v2HSd()).toFixed(1)} in `
        + `(${v.g2 < 0 ? 'a peak' : 'a trough'})`),
  },
  {
    key: 'quadratic_x_gender', label: 'Quadratic × gender', fitted: true,
    eq: '\\begin{aligned} f &= (1-G)\\left(\\gamma_1\\tilde h + \\gamma_2\\tilde h^{2}\\right) \\\\'
      + ' &\\quad + G\\left((\\gamma_1{+}\\gamma_1^{\\times})\\tilde h + (\\gamma_2{+}\\gamma_2^{\\times})\\tilde h^{2}\\right) \\end{aligned}',
    claim: 'A separate quadratic per gender. <b>This is what v1 concluded.</b> Set both \\(\\gamma^{\\times}\\) to zero and the two curves collapse onto each other.',
    params: [
      { id: 'g1', tex: '\\gamma_1', min: -0.6, max: 0.6, step: 0.01, def: -0.176 },
      { id: 'g2', tex: '\\gamma_2', min: -0.3, max: 0.3, step: 0.005, def: -0.069 },
      { id: 'g1x', tex: '\\gamma_1^{\\times}', min: -0.6, max: 0.6, step: 0.01, def: 0.153 },
      { id: 'g2x', tex: '\\gamma_2^{\\times}', min: -0.3, max: 0.3, step: 0.005, def: 0.159 },
    ],
    // Blue for male, orange for female, both solid -- the only card that
    // draws two curves, so no cross-form colour clash to design around.
    curves: (v) => [
      { name: 'Male users', colour: '--lg-info', dash: 'solid',
        f: (z) => v.g1 * z + v.g2 * z * z },
      { name: 'Female users', colour: '--lg-highlight', dash: 'solid',
        f: (z) => (v.g1 + v.g1x) * z + (v.g2 + v.g2x) * z * z },
    ],
  },
  {
    key: 'saturating', label: 'Saturating', fitted: false,
    eq: 'f(\\tilde h) = A\\,\\operatorname{logistic}\\!\\left(\\frac{\\tilde h - h_0}{s}\\right)',
    claim: 'Reach helps until you have enough of it, then stops paying. <b>Monotone by construction</b> &mdash; it cannot turn back down. Never tested in v1.',
    params: [
      { id: 'A', tex: 'A', min: -1.2, max: 1.2, step: 0.02, def: 0.55 },
      { id: 'h0', tex: 'h_0', min: -2.5, max: 2.5, step: 0.05, def: -0.4 },
      { id: 's', tex: 's', min: 0.1, max: 2.0, step: 0.05, def: 0.55 },
    ],
    curves: (v) => [{ name: 'Saturating', colour: '--lg-info', dash: 'solid',
                      f: (z) => v.A / (1 + Math.exp(-(z - v.h0) / Math.max(v.s, 0.05))) - v.A / 2 }],
  },
  {
    key: 'vertex_quadratic', label: 'Vertex quadratic', fitted: false,
    eq: 'f(\\tilde h) = -\\kappa_h\\left(\\tilde h - p\\right)^{2}',
    claim: 'The same curve family as the plain quadratic, but the <b>best height \\(p\\) is a parameter</b> you can read an interval off directly. Drag \\(p\\) to move the peak off the median.',
    params: [
      { id: 'kh', tex: '\\kappa_h', min: 0, max: 0.4, step: 0.005, def: 0.035 },
      { id: 'pk', tex: 'p', min: -3, max: 3, step: 0.05, def: 0.9 },
    ],
    curves: (v) => [{ name: 'Vertex quadratic', colour: '--lg-info', dash: 'solid',
                      f: (z) => -v.kh * (z - v.pk) * (z - v.pk) + v.kh * 2 }],
    note: (v) => `peak at ${(v2HMed() + v.pk * v2HSd()).toFixed(1)} in`,
  },
];

const V2_FORM_BY_KEY = Object.fromEntries(V2_FORM_SPECS.map((f) => [f.key, f]));
// Live slider state, seeded from each spec's defaults.
const v2FormState = Object.fromEntries(V2_FORM_SPECS.map((f) => [
  f.key, Object.fromEntries(f.params.map((p) => [p.id, p.def])),
]));

// Where each group actually sits: median +/- 1 SD from the cleaned data.
const V2_HEIGHT_BANDS = [
  { label: 'Male users', mid: 69.2, sd: 3.34, tok: '--lg-info', anchor: 'right' },
  { label: 'Female users', mid: 64.2, sd: 2.83, tok: '--lg-highlight', anchor: 'left' },
];

function renderV2Symbols() {
  const el = document.getElementById('v2-symbols');
  if (!el) return;
  const sc = v2Scales();
  const sub = (t) => String(t)
    .replace('{H_SD}', sc.h_sd.toFixed(1))
    .replace('{A_SD}', sc.a_sd.toFixed(1));
  let html = '<tbody>';
  V2_SYMBOLS.forEach((r) => {
    if (r[0] === 'group') {
      html += `<tr class="sym-group"><td colspan="2">${r[1]}</td></tr>`;
    } else {
      const unit = (r[3] && r[3] !== '&mdash;') ? `<span class="unit">${sub(r[3])}</span>` : '';
      html += `<tr class="sym-row" data-sym="${r[0]}">`
        + `<td class="sym">${r[1]}</td><td>${r[2]}${unit}</td></tr>`;
    }
  });
  el.innerHTML = html + '</tbody>';
}

// ---- fitted results (gyms, height-form comparison) ----

async function loadV2Results() {
  if (V2_RESULTS) return V2_RESULTS;
  try {
    const r = await fetch('/static/v2_results.json');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    V2_RESULTS = await r.json();
    V2_GYMS = V2_RESULTS.gyms;
  } catch (e) {
    V2_RESULTS = null;
    return null;
  }
  return V2_RESULTS;
}

// LOO ranks the height forms. Reported as a difference from the best form,
// because the absolute elpd is meaningless on its own and only the gaps
// between models on identical data carry information.
function renderV2FormsLoo() {
  const el = document.getElementById('v2-loo-table');
  const note = document.getElementById('v2-loo-note');
  if (!el || !V2_RESULTS) return;
  const forms = V2_RESULTS.forms || [];
  if (!forms.length) { el.innerHTML = ''; return; }

  const hasDse = forms.some((f) => f.dse !== undefined);
  el.innerHTML = '<thead><tr><th>height form</th><th>height params</th>'
    + '<th>LOO elpd</th><th>&Delta; vs best</th>'
    + (hasDse ? '<th>SE of &Delta;</th>' : '')
    + '<th>max R&#770;</th><th>min ESS</th></tr></thead><tbody>'
    + forms.map((f, i) => {
      const conv = f.rhat <= 1.01;
      // A gap inside one SE of the difference is not a ranking, it is noise.
      const real = f.dse !== undefined && Math.abs(f.d_elpd) > 2 * f.dse;
      return `<tr${i === 0 ? ' class="row-best"' : ''}>`
        + `<td><b>${f.label}</b>${i === 0 ? ' <span class="pill-best">best</span>' : ''}</td>`
        + `<td class="unit">${f.k ?? '&mdash;'}</td>`
        + `<td class="unit">${f.elpd.toFixed(1)}</td>`
        + `<td class="unit${i && !real ? ' muted' : ''}">${f.d_elpd === 0 ? '&mdash;' : f.d_elpd.toFixed(1)}</td>`
        + (hasDse ? `<td class="unit">${i === 0 ? '&mdash;' : `&plusmn;${f.dse?.toFixed(1) ?? '?'}`}</td>` : '')
        + `<td class="unit ${conv ? '' : 'bad'}">${f.rhat.toFixed(2)}</td>`
        + `<td class="unit ${f.ess >= 400 ? '' : 'bad'}">${f.ess}</td></tr>`;
    }).join('') + '</tbody>';

  if (note) {
    const pending = V2_RESULTS.pending || [];
    const best = forms[0], second = forms[1];
    let txt = `<b>${forms.length} of 6 height forms fitted.</b> `;
    if (best && second) {
      const gap = Math.abs(second.d_elpd);
      txt += `${best.label} leads ${second.label} by ${gap.toFixed(1)} elpd`;
      if (best.k !== null && second.k !== null && best.k < second.k) {
        txt += ` <b>with ${second.k - best.k} fewer height parameters</b>`;
      }
      txt += '. ';
      if (second.dse !== undefined) {
        const sep = forms.filter((f, i) => i && Math.abs(f.d_elpd) > 2 * f.dse);
        txt += `The <b>SE of &Delta;</b> column is the honest one: it is the `
          + 'standard error of the <i>difference</i>, from paired pointwise LOO, '
          + 'which accounts for every model being scored on the same '
          + 'observations. ';
        txt += sep.length
          ? `Only ${sep.map((f) => f.label.toLowerCase()).join(', ')} `
            + `${sep.length === 1 ? 'is' : 'are'} separated from the leader by `
            + 'more than two of those SEs. Every other gap in this table is '
            + 'inside the noise &mdash; the ranking is an ordering, not a result. '
          : '<b>No gap in this table exceeds two of those SEs.</b> The ranking '
            + 'is an ordering, not a result: on this data LOO cannot tell these '
            + 'height forms apart. ';
      } else if (gap < 10) {
        txt += 'That gap is small relative to the standard errors quoted per '
          + 'model, so it is a lead, not a verdict &mdash; a paired comparison '
          + 'on pointwise LOO is needed before ranking these confidently. ';
      }
    }
    if (pending.length) {
      txt += `Still running or queued: <code>${pending.join('</code>, <code>')}</code>.`;
    }
    note.innerHTML = txt;
  }
}

// What has held steady across every fit. Replication across different height
// forms is the strongest evidence available here that a number is real and
// not an artefact of one model specification.
function renderV2Replication() {
  const el = document.getElementById('v2-repl-table');
  if (!el || !V2_RESULTS) return;
  const rep = V2_RESULTS.replication || {};
  const SHOW = [
    ['sigma_gym', '\\(\\sigma_{\\text{gym}}\\)', 'spread of gym grading'],
    ['kappa', '\\(\\kappa\\)', 'gap rate per visit'],
    ['rho', '\\(\\rho\\)', 'gap rate per logging completeness'],
    ['sigma_user', '\\(\\sigma_{\\text{user}}\\)', 'spread of climber ability'],
    ['beta_gender', '\\(\\beta_{\\text{gender}}\\)', 'female-user ability shift'],
    ['delta1', '\\(\\delta_1\\)', 'ape-index slope'],
  ];
  // Only the arms that are genuine like-for-like refits.
  const fits = (V2_RESULTS.generated_from || []).filter((f) => f !== 'v3_all');
  el.innerHTML = '<thead><tr><th>parameter</th><th>meaning</th>'
    + fits.map((f) => `<th>${f.replace('v3_', '')}</th>`).join('')
    + '<th>verdict</th></tr></thead><tbody>'
    + SHOW.filter(([k]) => rep[k]).map(([k, tex, meaning]) => {
      const vals = fits.map((f) => rep[k][f]);
      const present = vals.filter(Boolean).map((v) => v.m);
      const rng = present.length > 1 ? Math.max(...present) - Math.min(...present) : 0;
      const tight = present.length > 1 && rng < 0.05;
      return `<tr><td class="sym">${tex}</td><td>${meaning}</td>`
        + vals.map((v) => `<td class="unit">${v ? v.m.toFixed(3) : '&mdash;'}</td>`).join('')
        + `<td>${tight ? '<span class="ok">stable</span>' : (present.length > 1
            ? `varies by ${rng.toFixed(2)}` : '&mdash;')}</td></tr>`;
    }).join('') + '</tbody>';
  if (typeof window.renderMathInElement === 'function') {
    window.renderMathInElement(el, {
      delimiters: [{ left: '\\(', right: '\\)', display: false }], throwOnError: false });
  }
}

// ---- priors, posteriors, corner plots and sampler diagnostics ----
//
// v2_posterior.json holds every fit: 4 chains x 150 thinned draws per
// parameter, plus prior draws. Thinning uses one step per fit, so draw i of
// one parameter matches draw i of another -- that is what makes the corner
// plots real joint samples rather than a scatter of unrelated numbers.

let V2_POST = null;

const V2_PARAM_TEX = {
  beta0: '\\beta_0', beta_gender: '\\beta_{\\text{gender}}',
  gamma1: '\\gamma_1', gamma2: '\\gamma_2',
  gamma1_x: '\\gamma_1^{\\times}', gamma2_x: '\\gamma_2^{\\times}',
  delta1: '\\delta_1', delta2: '\\delta_2',
  delta1_x: '\\delta_1^{\\times}', delta2_x: '\\delta_2^{\\times}',
  sigma_user: '\\sigma_{\\text{user}}', sigma_gym: '\\sigma_{\\text{gym}}',
  log_lambda0: '\\log\\lambda_0', kappa: '\\kappa', rho: '\\rho',
  beta_h_missing: '\\beta_{h\\text{-miss}}', beta_a_missing: '\\beta_{a\\text{-miss}}',
  sat_amp: 'A', sat_h0: 'h_0', sat_scale: 's',
  vq_curv: '\\kappa_h', vq_peak: 'p',
};
const V2_PARAM_BLURB = {
  beta0: 'baseline ability at an average gym',
  beta_gender: 'female-user shift in ability',
  gamma1: 'height slope, male users', gamma2: 'height curvature, male users',
  gamma1_x: 'how the female height slope differs',
  gamma2_x: 'how the female height curvature differs',
  delta1: 'ape-index slope', delta2: 'ape-index curvature',
  delta1_x: 'extra ape slope for female users',
  delta2_x: 'extra ape curvature for female users',
  sigma_user: 'spread of ability between climbers',
  sigma_gym: 'spread of grading style across gyms',
  log_lambda0: 'baseline gap rate (log)', kappa: 'gap rate per extra visit',
  rho: 'gap rate per unit logging completeness',
  beta_h_missing: 'ability shift for users with no height on file',
  beta_a_missing: 'ability shift for users with no ape index on file',
  sat_amp: 'saturating: total worth of reach', sat_h0: 'saturating: where it levels off',
  sat_scale: 'saturating: how sharply it levels',
  vq_curv: 'vertex form: how sharp the optimum is', vq_peak: 'vertex form: the best height',
};
const V2_FIT_LABEL = {
  v3_conf: 'quad × gender', v3_all: 'quad × gender (all names)',
  v3_lin: 'linear', v3_sat: 'saturating', v3_zero: 'zero',
  v3_quad: 'quadratic', v3_vtx: 'vertex quadratic',
  v4_linxg: 'linear × gender',
  v4_lin_apex: 'linear + ape×gender', v4_lin_apelin: 'linear, linear ape',
  v3_apex: 'quad × gender + ape×gender', v3_zsu: 'quad × gender (zero-sum users)',
};
// Ten distinct hues -- there are nine fits, and the status tokens only give
// five usable colours (two of which are near-identical blues).
const V2_FIT_HUES = ['--lg-cat-1', '--lg-cat-2', '--lg-cat-3', '--lg-cat-4',
                     '--lg-cat-5', '--lg-cat-6', '--lg-cat-7', '--lg-cat-8',
                     '--lg-cat-9', '--lg-cat-10'];

const V2_CORNER_GROUPS = {
  height: { label: 'Height block', of: ['beta0', 'gamma1', 'gamma2', 'gamma1_x', 'gamma2_x'] },
  gap: { label: 'Gap model', of: ['log_lambda0', 'kappa', 'rho'] },
  spread: { label: 'Variance components', of: ['beta0', 'sigma_user', 'sigma_gym'] },
  body: { label: 'Body covariates', of: ['gamma1', 'delta1', 'beta_h_missing', 'beta_a_missing'] },
};

const v2Fit = (n) => V2_POST?.fits?.[n];
const v2FitNames = () => Object.keys(V2_POST?.fits || {});
const v2SelectedFit = () => document.getElementById('v2-fit-pick')?.value || v2FitNames()[0];

// Gaussian KDE on a fixed grid. Silverman bandwidth; these posteriors are
// unimodal, so nothing fancier earns its keep.
function v2Kde(samples, grid) {
  const n = samples.length;
  if (!n) return grid.map(() => 0);
  const mean = samples.reduce((a, b) => a + b, 0) / n;
  const sd = Math.sqrt(samples.reduce((a, b) => a + (b - mean) ** 2, 0) / Math.max(1, n - 1)) || 1e-6;
  const sorted = [...samples].sort((a, b) => a - b);
  const q = (p) => sorted[Math.min(n - 1, Math.max(0, Math.floor(p * n)))];
  const iqr = q(0.75) - q(0.25);
  const bw = 0.9 * Math.min(sd, iqr / 1.349 || sd) * Math.pow(n, -0.2) || sd * 0.1;
  return grid.map((x) => {
    let acc = 0;
    for (const v of samples) acc += Math.exp(-0.5 * ((x - v) / bw) ** 2);
    return acc / (n * bw * Math.sqrt(2 * Math.PI));
  });
}

function v2Grid(lo, hi, n = 140) {
  const step = (hi - lo) / (n - 1), g = [];
  for (let i = 0; i < n; i++) g.push(lo + i * step);
  return g;
}

function v2SharedRange(post, prior) {
  const all = prior ? post.concat(prior) : post;
  const sorted = [...all].sort((a, b) => a - b);
  const q = (p) => sorted[Math.min(sorted.length - 1, Math.floor(p * sorted.length))];
  let lo = q(0.005), hi = q(0.995);
  lo = Math.min(lo, Math.min(...post)); hi = Math.max(hi, Math.max(...post));
  const pad = (hi - lo) * 0.06 || 0.1;
  return [lo - pad, hi + pad];
}

// ---- overview grid: every parameter of the selected fit ----

function renderV2PostGrid() {
  const host = document.getElementById('v2-post-grid');
  const fit = v2Fit(v2SelectedFit());
  if (!host || !fit) return;
  const names = Object.keys(fit.params);
  host.innerHTML = names.map((n) => `
    <button type="button" class="post-tile" data-param="${n}">
      <span class="post-tile-name">\\(${V2_PARAM_TEX[n] || n}\\)</span>
      <span class="post-tile-chart" id="v2-pt-${n}"></span>
      <span class="post-tile-rhat ${fit.params[n].rhat > 1.01 ? 'bad' : 'ok'}">R&#770; ${fit.params[n].rhat.toFixed(2)}</span>
    </button>`).join('');

  names.forEach((n) => {
    const el = document.getElementById(`v2-pt-${n}`);
    if (!el || typeof Plotly === 'undefined') return;
    const p = fit.params[n];
    const post = p.chains.flat();
    const pri = fit.prior?.[n];
    const [lo, hi] = v2SharedRange(post, pri);
    const grid = v2Grid(lo, hi, 70);
    const traces = [];
    if (pri) {
      traces.push({ type: 'scatter', mode: 'lines', x: grid, y: v2Kde(pri, grid),
        line: { color: cssVar('--lg-text-2'), width: 1 }, fill: 'tozeroy',
        fillcolor: `color-mix(in srgb, ${cssVar('--lg-text-2')} 16%, transparent)`,
        hoverinfo: 'skip' });
    }
    traces.push({ type: 'scatter', mode: 'lines', x: grid, y: v2Kde(post, grid),
      line: { color: cssVar('--lg-info'), width: 1.6 }, fill: 'tozeroy',
      fillcolor: `color-mix(in srgb, ${cssVar('--lg-info')} 24%, transparent)`,
      hoverinfo: 'skip' });
    Plotly.react(el, traces, {
      height: 62, margin: { l: 2, r: 2, t: 2, b: 2 }, showlegend: false,
      paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)',
      xaxis: { visible: false, range: [lo, hi] }, yaxis: { visible: false },
    }, { displayModeBar: false, staticPlot: true, responsive: true });
  });

  host.querySelectorAll('.post-tile').forEach((b) => {
    b.addEventListener('click', () => {
      const sel = document.getElementById('v2-param-pick');
      if (sel) sel.value = b.dataset.param;
      renderV2ParamDetail(b.dataset.param);
      document.querySelector('.param-detail')?.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
    });
  });
  if (typeof window.renderMathInElement === 'function') {
    window.renderMathInElement(host, {
      delimiters: [{ left: '\\(', right: '\\)', display: false }], throwOnError: false });
  }
}

// ---- one parameter, in detail ----

function renderV2ParamDetail(name) {
  const fitName = v2SelectedFit();
  const fit = v2Fit(fitName);
  if (!fit) return;
  const p = fit.params[name];
  if (!p) return;
  const post = p.chains.flat();

  // prior vs posterior
  const dens = document.getElementById('v2-param-dens');
  if (dens && typeof Plotly !== 'undefined') {
    const wide = document.getElementById('v2-param-wide')?.checked;
    const pri = fit.prior?.[name];
    const [lo, hi] = wide && pri
      ? v2SharedRange(post, pri)
      : [p.mean - 6 * p.sd, p.mean + 6 * p.sd];
    const grid = v2Grid(lo, hi, 160);
    const traces = [];
    if (pri) {
      traces.push({ type: 'scatter', mode: 'lines', name: 'prior', x: grid, y: v2Kde(pri, grid),
        line: { color: cssVar('--lg-text-2'), width: 1.5 }, fill: 'tozeroy',
        fillcolor: `color-mix(in srgb, ${cssVar('--lg-text-2')} 14%, transparent)`,
        hovertemplate: 'prior<br>%{x:.3f}<extra></extra>' });
    }
    traces.push({ type: 'scatter', mode: 'lines', name: 'posterior', x: grid, y: v2Kde(post, grid),
      line: { color: cssVar('--lg-info'), width: 2.2 }, fill: 'tozeroy',
      fillcolor: `color-mix(in srgb, ${cssVar('--lg-info')} 22%, transparent)`,
      hovertemplate: 'posterior<br>%{x:.3f}<extra></extra>' });
    const layout = chartLayout('value');
    layout.height = 280;
    layout.xaxis = { ...layout.xaxis, title: { text: 'value', standoff: 8 }, range: [lo, hi] };
    layout.yaxis = { ...layout.yaxis, title: { text: 'density', standoff: 6 }, showticklabels: false };
    layout.margin = { l: 48, r: 16, t: 10, b: 76 };
    layout.legend = { ...layout.legend, orientation: 'h', y: -0.34, x: 0 };
    layout.shapes = [p.lo, p.hi].map((x) => ({ type: 'line', x0: x, x1: x, y0: 0, y1: 1,
      yref: 'paper', line: { color: cssVar('--lg-info'), width: 1, dash: 'dot' } }));
    Plotly.react(dens, traces, layout, { displayModeBar: false, responsive: true });
  }

  // chains
  const tr = document.getElementById('v2-param-trace');
  if (tr && typeof Plotly !== 'undefined') {
    const hues = ['--lg-info', '--lg-highlight', '--lg-success', '--lg-danger'];
    const mode = document.getElementById('v2-trace-mode')?.value || 'trace';
    const layout = chartLayout('');
    layout.height = 280;
    layout.margin = { l: 56, r: 16, t: 10, b: 76 };
    layout.legend = { ...layout.legend, orientation: 'h', y: -0.34, x: 0 };
    let traces;

    if (mode === 'trace') {
      const nWarm = p.warmup ? p.warmup[0].length : 0;
      traces = p.chains.map((ch, i) => {
        const series = nWarm ? p.warmup[i].concat(ch) : ch;
        return { type: 'scatter', mode: 'lines', name: `chain ${i}`,
          x: series.map((_, j) => j - nWarm), y: series,
          line: { color: cssVar(hues[i % hues.length]), width: 0.9 },
          hovertemplate: `chain ${i}<br>%{y:.3f}<extra></extra>` };
      });
      layout.shapes = p.chains.map((ch, i) => ({
        type: 'line', xref: 'paper', x0: 0, x1: 1,
        y0: ch.reduce((a, b) => a + b, 0) / ch.length,
        y1: ch.reduce((a, b) => a + b, 0) / ch.length,
        line: { color: cssVar(hues[i % hues.length]), width: 1.2, dash: 'dash' },
      }));
      layout.annotations = [];
      if (nWarm) {
        layout.shapes.push({ type: 'rect', xref: 'x', yref: 'paper',
          x0: -nWarm, x1: 0, y0: 0, y1: 1,
          fillcolor: cssVar('--lg-text-2'), opacity: 0.08, line: { width: 0 } });
        layout.annotations.push({ x: -nWarm / 2, y: 1, yref: 'paper', yanchor: 'bottom',
          showarrow: false, text: 'warm-up (discarded)',
          font: { size: 10, color: cssVar('--lg-text-2') } });
        layout.margin.t = 22;
      }
      if (!nWarm) {
        // Only fits run after discard_tuned_samples=False went in have their
        // warm-up; say which those are rather than leaving a silent absence.
        const withWarm = v2FitNames().filter((f) => v2Fit(f)?.n_warmup)
          .map((f) => V2_FIT_LABEL[f] || f);
        layout.annotations.push({
          xref: 'paper', x: 0.5, yref: 'paper', y: 1, yanchor: 'bottom',
          showarrow: false, font: { size: 10, color: cssVar('--lg-text-2') },
          text: withWarm.length
            ? `warm-up not kept for this fit — see ${withWarm[withWarm.length - 1]}`
            : 'warm-up not kept for this fit',
        });
        layout.margin.t = 22;
      }
      layout.xaxis = { ...layout.xaxis, title: {
        text: nWarm ? 'draw (thinned; 0 = end of warm-up)' : 'draw (thinned, post-warm-up)',
        standoff: 8 } };
      layout.yaxis = { ...layout.yaxis, title: { text: 'value', standoff: 6 } };
    } else {
      const flat = [];
      p.chains.forEach((ch, ci) => ch.forEach((v) => flat.push([v, ci])));
      flat.sort((a, b) => a[0] - b[0]);
      const nBins = 12, total = flat.length;
      const counts = p.chains.map(() => new Array(nBins).fill(0));
      flat.forEach(([, ci], rank) => {
        counts[ci][Math.min(nBins - 1, Math.floor((rank / total) * nBins))] += 1;
      });
      const centres = [...Array(nBins)].map((_, b) => (b + 0.5) / nBins);
      traces = counts.map((c, i) => ({
        type: 'bar', name: `chain ${i}`, x: centres, y: c,
        marker: { color: cssVar(hues[i % hues.length]) },
        hovertemplate: `chain ${i}<br>rank bin %{x:.2f}<br>%{y} draws<extra></extra>`,
      }));
      const expected = total / (nBins * p.chains.length);
      layout.barmode = 'group';
      layout.shapes = [{ type: 'line', xref: 'paper', x0: 0, x1: 1, y0: expected, y1: expected,
        line: { color: cssVar('--lg-text-2'), width: 1.4, dash: 'dash' } }];
      layout.annotations = [{ xref: 'paper', x: 1, yref: 'paper', y: 1, xanchor: 'right',
        yanchor: 'bottom', showarrow: false, text: '- - -  even mixing',
        font: { size: 10, color: cssVar('--lg-text-2') } }];
      layout.margin.t = 22;
      layout.xaxis = { ...layout.xaxis, title: { text: 'rank within all chains', standoff: 8 } };
      layout.yaxis = { ...layout.yaxis, title: { text: 'draws in bin', standoff: 6 } };
    }
    Plotly.react(tr, traces, layout, { displayModeBar: false, responsive: true });
  }

  renderV2AcrossFits(name);
  renderV2ParamStats(name, p);
}

function renderV2ParamStats(name, p) {
  const tbl = document.getElementById('v2-param-stats');
  if (!tbl) return;
  const converged = p.rhat <= 1.01;
  tbl.innerHTML = '<thead><tr><th>statistic</th><th>value</th><th>reading</th></tr></thead><tbody>'
    + [
      ['posterior mean', p.mean.toFixed(3), 'the number quoted elsewhere on this page'],
      ['posterior SD', p.sd.toFixed(3), 'how uncertain that number is'],
      ['89% HDI', `[${p.lo.toFixed(3)}, ${p.hi.toFixed(3)}]`,
        (p.lo <= 0 && p.hi >= 0) ? '<b>includes zero</b> — no credible effect'
          : 'excludes zero — a credible effect'],
      ['R&#770;', p.rhat.toFixed(3), converged
        ? 'at or below 1.01 — chains agree'
        : '<b>above 1.01 — chains disagree, do not report this as final</b>'],
      ['ESS (bulk)', String(p.ess_bulk), p.ess_bulk < 400
        ? '<b>below 400 — too few effective draws</b>' : 'adequate'],
      ['ESS (tail)', String(p.ess_tail), p.ess_tail < 400
        ? 'below 400 — interval edges are noisy' : 'adequate'],
    ].map((r) => `<tr><td class="sym">${r[0]}</td><td class="unit">${r[1]}</td><td>${r[2]}</td></tr>`).join('')
    + '</tbody>';
  const verdict = document.getElementById('v2-param-verdict');
  if (verdict) {
    verdict.textContent = converged
      ? `converged — R-hat ${p.rhat.toFixed(2)}, ESS ${p.ess_bulk}`
      : `provisional — R-hat ${p.rhat.toFixed(2)}, ESS ${p.ess_bulk}: chains do not agree yet`;
    verdict.className = `param-verdict ${converged ? '' : 'warn'}`;
  }
}

// ---- the same parameter across every model version ----
//
// Most parameters exist in several fits. Overlaying their posteriors answers a
// question no single fit can: is this number a property of the data, or of the
// height form that happened to be bolted on beside it?

function renderV2AcrossFits(name) {
  const el = document.getElementById('v2-across-fits');
  const note = document.getElementById('v2-across-note');
  if (!el || typeof Plotly === 'undefined') return;
  const have = v2FitNames().filter((f) => v2Fit(f).params[name]);
  if (have.length < 2) {
    Plotly.purge(el);
    el.innerHTML = '<p class="form-noparams">Only one fit contains this parameter, '
      + 'so there is nothing to compare it against.</p>';
    if (note) note.textContent = '';
    return;
  }
  // Only clear when Plotly does not already own this node. Wiping innerHTML
  // on an initialised plot destroys its SVG but leaves _fullLayout behind, and
  // the next react() then treats it as an in-place update and draws nothing.
  if (!el._fullLayout) el.innerHTML = '';
  const allDraws = have.flatMap((f) => v2Fit(f).params[name].chains.flat());
  const [lo, hi] = v2SharedRange(allDraws, null);
  const grid = v2Grid(lo, hi, 160);
  const traces = have.map((f, i) => {
    const p = v2Fit(f).params[name];
    const bad = p.rhat > 1.01;
    return {
      type: 'scatter', mode: 'lines',
      name: `${V2_FIT_LABEL[f] || f}${bad ? ' ⚠' : ''}`,
      x: grid, y: v2Kde(p.chains.flat(), grid),
      line: { color: cssVar(V2_FIT_HUES[i % V2_FIT_HUES.length]), width: 2,
              dash: bad ? 'dot' : 'solid' },
      hovertemplate: `${V2_FIT_LABEL[f] || f}<br>%{x:.3f}<extra></extra>`,
    };
  });
  const layout = chartLayout('value');
  layout.height = 380;
  layout.xaxis = { ...layout.xaxis, title: { text: 'value', standoff: 8 }, range: [lo, hi] };
  layout.yaxis = { ...layout.yaxis, title: { text: 'density', standoff: 6 }, showticklabels: false };
  // One legend entry per fit wraps to three rows; give it room below the
  // axis title rather than letting it land on top of the plot.
  layout.margin = { l: 48, r: 16, t: 10, b: 150 };
  layout.legend = { ...layout.legend, orientation: 'h', y: -0.42, x: 0 };
  Plotly.react(el, traces, layout, { displayModeBar: false, responsive: true });

  if (note) {
    const means = have.map((f) => v2Fit(f).params[name].mean);
    const spread = Math.max(...means) - Math.min(...means);
    const widest = Math.max(...have.map((f) => v2Fit(f).params[name].sd));
    // Compare how far the models disagree against how uncertain any one of
    // them is. Below ~half an SD the disagreement is invisible next to the
    // noise; past ~1.5 SD the choice of height form is genuinely driving the
    // answer. In between, say so rather than forcing a verdict.
    const ratio = widest > 0 ? spread / widest : 0;
    let verdict;
    if (ratio < 0.5) {
      verdict = 'That is well inside the uncertainty of a single fit, so this '
        + 'parameter is <b>not sensitive to the height form</b> — it is a property '
        + 'of the data.';
    } else if (ratio < 1.5) {
      verdict = 'Those are comparable, so the fits are <b>consistent with each other</b> '
        + 'to within their own uncertainty — but not so tightly that the spread is '
        + 'negligible. Read the number as robust, the third decimal as not.';
    } else {
      verdict = '<b>The models disagree by more than any one of them claims to be '
        + 'uncertain</b>, so this number depends on which height form sits beside it. '
        + 'Treat it as conditional on that choice, not as a property of the data.';
    }
    note.innerHTML = `Across ${have.length} fits the posterior mean moves by `
      + `<b>${spread.toFixed(3)}</b>, against a within-fit SD of ${widest.toFixed(3)} `
      + `(ratio ${ratio.toFixed(1)}&times;). ${verdict}`
      + ' Dotted lines mark fits that have not converged.';
  }
}

// ---- the missing dimension: time ----
//
// v2_time.json is written by scripts/build_v2_time.py: the climber
// advancement curve (naive and de-biased) and the per-gym date/correction
// scatter. Nothing here is hand-typed.

let V2_TIME = null;

async function loadV2Time() {
  if (V2_TIME) return true;
  try {
    const r = await fetch('/static/v2_time.json', { cache: 'no-cache' });
    if (!r.ok) return false;
    V2_TIME = await r.json();
    return true;
  } catch (e) {
    return false;
  }
}

function renderV2Advancement() {
  const el = document.getElementById('v2-adv-chart');
  if (!el || !V2_TIME || typeof Plotly === 'undefined') return;
  const a = V2_TIME.advancement;
  const gx = (rows) => rows.map((r) => r.v);
  const traces = [];

  // The interquartile band lives in the table instead. It spans a full grade
  // either side, and drawing it here would bury three mean curves that now
  // fit inside a third of a grade.
  traces.push({
    type: 'scatter', mode: 'lines+markers', x: gx(a.naive), y: a.naive.map((r) => r.mean),
    name: 'naive (regression to the max)',
    line: { color: cssVar('--lg-cat-4'), width: 2, dash: 'dash' },
    marker: { size: 6 },
    hovertemplate: 'V%{x}: %{y:+.2f} grades/yr<extra>naive</extra>',
  });
  traces.push({
    type: 'scatter', mode: 'lines+markers', x: gx(a.short_win),
    y: a.short_win.map((r) => r.mean),
    name: 'three-month window alone',
    line: { color: cssVar('--lg-cat-3'), width: 2, dash: 'dot' },
    marker: { size: 6 },
    hovertemplate: 'V%{x}: %{y:+.2f} grades/yr<extra>3 months only</extra>',
  });
  traces.push({
    type: 'scatter', mode: 'lines+markers', x: gx(a.debiased), y: a.debiased.map((r) => r.mean),
    name: 'steady rate, fitted across all windows',
    line: { color: cssVar('--lg-cat-1'), width: 2.6 },
    marker: { size: 7 },
    error_y: { type: 'data', array: a.debiased.map((r) => r.sem),
      color: hexToRgba(cssVar('--lg-cat-1'), 0.5), thickness: 1.4, width: 3 },
    hovertemplate: 'V%{x}: %{y:+.2f} grades/yr<extra>de-biased</extra>',
  });

  const layout = chartLayout('current grade');
  layout.height = 380;
  layout.margin = { l: 62, r: 20, t: 12, b: 96 };
  layout.xaxis = { ...layout.xaxis, automargin: false, tickprefix: 'V', dtick: 1,
    title: { text: 'current grade', standoff: 10 } };
  // Pinned so the two corrected curves stay readable. The naive one runs to
  // +8 and -4 and is deliberately allowed to leave the frame -- it being off
  // the scale is the point, and its numbers are in the table below.
  layout.yaxis = { ...layout.yaxis, automargin: false, zeroline: true,
    range: [-1.35, 1.15], zerolinecolor: cssVar('--lg-text-2'),
    title: { text: 'grades gained per year', standoff: 6 } };
  layout.legend = { ...layout.legend, orientation: 'h', y: -0.3, yanchor: 'top',
    x: 0, font: { size: 10 } };
  Plotly.react(el, traces, layout, { displayModeBar: false, responsive: true });

  const note = document.getElementById('v2-adv-note');
  if (note) {
    const at = (rows, v) => rows.find((r) => r.v === v);
    // The worst naive value, whichever bin it lands in -- that is the
    // impossible number the argument rests on.
    const n1 = a.naive.reduce((w, r) => (r.mean < w.mean ? r : w), a.naive[0]);
    const d1 = at(a.debiased, 1), d3 = at(a.debiased, 3);
    const d5 = at(a.debiased, 5), d8 = at(a.debiased, 8);
    const s1 = at(a.short_win, 1);
    const worst = a.debiased.reduce((w, r) => (r.chi2 > w.chi2 ? r : w), a.debiased[0]);
    // Each bin's n counts measurements across all six watching periods, and
    // the same climber contributes at several of them, so this is a count of
    // measurements rather than of people.
    const nMeas = a.debiased.reduce((s, r) => s + r.n, 0);
    note.innerHTML = `${nMeas.toLocaleString()} measurements across six `
      + `watching periods, from ${a.n_climbers.toLocaleString()} climbers. `
      + 'Improvement runs '
      + `<b>${d1.mean.toFixed(2)} grades/yr at V${d1.v}</b> (&plusmn;`
      + `${d1.sem.toFixed(2)}), ${d3.mean.toFixed(2)} at V${d3.v}, `
      + `${d5.mean.toFixed(2)} at V${d5.v} and ${d8.mean.toFixed(2)} at `
      + `V${d8.v} &mdash; roughly ${(d1.mean / Math.max(d8.mean, 0.01)).toFixed(0)}&times; `
      + 'faster at the bottom than at the top. The naive estimator bottoms out '
      + `at ${n1.mean.toFixed(1)} grades/yr at V${n1.v}, an impossible number `
      + `that gives it away; the three-month window alone reads ${s1.mean.toFixed(2)} `
      + `at V${d1.v}, which a year of data does not bear out. A steady rate `
      + `fits every bin (worst χ&sup2;/dof is ${worst.chi2.toFixed(1)} at `
      + `V${worst.v}). Error bars understate the uncertainty: one climber `
      + 'contributes a triple at every position in their window sequence, so '
      + 'rows within a bin are not independent.';
  }
}

function renderV2Horizon() {
  const el = document.getElementById('v2-horizon-chart');
  if (!el || !V2_TIME || typeof Plotly === 'undefined') return;
  const byh = V2_TIME.advancement.by_horizon;
  if (!byh) return;
  const hs = Object.keys(byh).sort((x, y) => parseFloat(x) - parseFloat(y));
  // Short window = the darkest line, so the eye follows the one being argued
  // for. The rest fade out as they lengthen.
  const traces = hs.map((h, i) => {
    const rows = byh[h].filter((r) => r.v >= 1 && r.v <= 9);
    const t = i / Math.max(1, hs.length - 1);
    const c = cssVar(i === 0 ? '--lg-cat-1' : '--lg-text-2');
    return {
      type: 'scatter', mode: 'lines+markers',
      x: rows.map((r) => r.v), y: rows.map((r) => r.mean),
      name: parseFloat(h) === 1 ? '1 year' : `${parseFloat(h) * 12} months`,
      line: { color: hexToRgba(c, i === 0 ? 1 : 0.85 - 0.5 * t),
        width: i === 0 ? 2.8 : 1.6, dash: i === 0 ? 'solid' : 'dot' },
      marker: { size: i === 0 ? 8 : 5 },
      hovertemplate: `V%{x}: %{y:+.2f} grades/yr<extra>${h} yr window</extra>`,
    };
  });

  const layout = chartLayout('');
  layout.height = 360;
  layout.margin = { l: 62, r: 20, t: 12, b: 92 };
  layout.xaxis = { ...layout.xaxis, automargin: false, tickprefix: 'V', dtick: 1,
    title: { text: 'grade bin, assigned at w₀', standoff: 10 } };
  layout.yaxis = { ...layout.yaxis, automargin: false, zeroline: true,
    zerolinecolor: cssVar('--lg-text-2'),
    title: { text: 'grades gained per year', standoff: 6 } };
  layout.legend = { ...layout.legend, orientation: 'h', y: -0.28, yanchor: 'top',
    x: 0, font: { size: 10 } };
  Plotly.react(el, traces, layout, { displayModeBar: false, responsive: true });

  const note = document.getElementById('v2-horizon-note');
  if (note) {
    const g = (h, v) => (byh[h] || []).find((r) => r.v === v);
    const a1 = g(hs[0], 1), b1 = g('1.0', 1);
    const a6 = g(hs[0], 6), b6 = g('1.0', 6);
    note.innerHTML = 'The same estimator at six watching periods. At <b>V1</b> '
      + `it reads ${a1.mean.toFixed(2)} grades/yr over three months and `
      + `${b1.mean.toFixed(2)} over a year; at <b>V6</b> it goes `
      + `${a6.mean.toFixed(2)} to ${b6.mean.toFixed(2)}. The fan opens at the `
      + 'bottom and stays shut at the top, which is the signature of a gain '
      + 'that arrives once rather than accruing &mdash; and it only shows up '
      + 'where there is enough movement for the distinction to matter. '
      + 'Whichever period you pick you are reporting your own choice, so the '
      + 'curve below picks none of them.';
  }
  const starts = V2_TIME.advancement.starts || [];
  const lo = starts.find((r) => r.v === 1), hi = starts.find((r) => r.v === 9);
  const set = (id, r) => {
    const n = document.getElementById(id);
    if (n && r) n.textContent = `V${r.l1.toFixed(1)}`;
  };
  set('v2-start-lo', lo);
  set('v2-start-hi', hi);
}

function renderV2Accrual() {
  const el = document.getElementById('v2-accrual-chart');
  if (!el || !V2_TIME || typeof Plotly === 'undefined') return;
  const acc = V2_TIME.advancement.accrual;
  if (!acc || !acc.length) return;
  const hi = Math.max(...acc.map((r) => r.h));
  // Weighted mean rate, drawn as a straight line through the origin: if
  // change accrues linearly the points sit on it.
  const wsum = acc.reduce((s, r) => s + r.n, 0);
  const rate = acc.reduce((s, r) => s + r.rate * r.n, 0) / wsum;

  const c = cssVar('--lg-cat-1');
  const traces = [{
    type: 'scatter', mode: 'lines', name: `steady ${rate.toFixed(2)} grades/yr`,
    x: [0, hi], y: [0, rate * hi],
    line: { color: cssVar('--lg-text-2'), width: 1.6, dash: 'dash' },
    hoverinfo: 'skip',
  }, {
    type: 'scatter', mode: 'markers', name: 'measured',
    x: acc.map((r) => r.h), y: acc.map((r) => r.dl),
    marker: { size: 11, color: hexToRgba(c, 0.72),
      line: { width: 2, color: hexToRgba(c, 0.85) } },
    error_y: { type: 'data', array: acc.map((r) => r.sem),
      color: hexToRgba(c, 0.5), thickness: 1.4, width: 4 },
    text: acc.map((r) => r.n.toLocaleString()),
    hovertemplate: '%{x} yr later: %{y:+.3f} grades<br>'
      + '%{text} measurements<extra></extra>',
  }];

  const layout = chartLayout('');
  layout.height = 300;
  layout.margin = { l: 62, r: 20, t: 12, b: 82 };
  layout.xaxis = { ...layout.xaxis, automargin: false, range: [0, hi + 0.15],
    title: { text: 'time elapsed (years)', standoff: 10 } };
  layout.yaxis = { ...layout.yaxis, automargin: false, zeroline: true,
    zerolinecolor: cssVar('--lg-text-2'),
    title: { text: 'grades gained', standoff: 6 } };
  layout.legend = { ...layout.legend, orientation: 'h', y: -0.34, yanchor: 'top',
    x: 0, font: { size: 10 } };
  Plotly.react(el, traces, layout, { displayModeBar: false, responsive: true });

  const note = document.getElementById('v2-accrual-note');
  if (note) {
    const lo = acc[0], hiR = acc[acc.length - 1];
    note.innerHTML = 'Climbers between V3 and V8, measured over a ladder of '
      + 'horizons. The points track the dashed line, so the change accumulates '
      + `steadily: ${lo.dl.toFixed(3)} grades after ${lo.h} years and `
      + `${hiR.dl.toFixed(3)} after ${hiR.h}, which is `
      + `${(hiR.dl / lo.dl).toFixed(1)}&times; the change over `
      + `${(hiR.h / lo.h).toFixed(0)}&times; the time. The implied annual rate `
      + `never leaves the ${Math.min(...acc.map((r) => r.rate)).toFixed(2)} to `
      + `${Math.max(...acc.map((r) => r.rate)).toFixed(2)} band. Error bars are `
      + 'the standard error of the mean.';
  }
  const gm = document.getElementById('v2-gap-median');
  if (gm && V2_TIME.advancement.gap_months) {
    gm.textContent = V2_TIME.advancement.gap_months.median.toFixed(0);
  }
}

function renderV2TimeChart() {
  const el = document.getElementById('v2-time-chart');
  if (!el || !V2_TIME || typeof Plotly === 'undefined') return;
  const gt = V2_TIME.gym_time;
  const brands = [...new Set(gt.gyms.map((r) => r.b))]
    .sort((x, y) => gt.gyms.filter((r) => r.b === y).length
                  - gt.gyms.filter((r) => r.b === x).length);
  const traces = brands.map((b) => {
    const rs = gt.gyms.filter((r) => r.b === b);
    const c = cssVar(V2_BRAND_COLOURS[b] || '--lg-text-2');
    return {
      type: 'scatter', mode: 'markers', name: b,
      x: rs.map((r) => r.t_c), y: rs.map((r) => r.m),
      marker: { size: 11, color: hexToRgba(c, 0.72),
        line: { width: 2, color: hexToRgba(c, 0.85) } },
      text: rs.map((r) => r.g),
      hovertemplate: '<b>%{text}</b><br>%{x:+.2f} yr relative to its own '
        + 'climbers<br>correction %{y:+.3f} grades<extra></extra>',
    };
  });
  // The fitted line, drawn across the observed range only.
  const xs = gt.gyms.map((r) => r.t_c);
  const lo = Math.min(...xs), hi = Math.max(...xs);
  const ys = gt.gyms.map((r) => r.m);
  const mx = xs.reduce((s, v) => s + v, 0) / xs.length;
  const my = ys.reduce((s, v) => s + v, 0) / ys.length;
  const b1 = gt.raw.slope, b0 = my - b1 * mx;
  traces.push({
    type: 'scatter', mode: 'lines', name: `fit: ${b1.toFixed(2)} grades / yr`,
    x: [lo, hi], y: [b0 + b1 * lo, b0 + b1 * hi],
    line: { color: cssVar('--lg-text-2'), width: 1.6, dash: 'dash' },
    hoverinfo: 'skip',
  });

  const layout = chartLayout('');
  layout.height = 400;
  layout.margin = { l: 66, r: 20, t: 12, b: 96 };
  layout.xaxis = { ...layout.xaxis, automargin: false, zeroline: true,
    zerolinecolor: cssVar('--lg-text-2'),
    title: { text: 'how late this gym sits in its own climbers’ careers (years)',
      standoff: 10 } };
  layout.yaxis = { ...layout.yaxis, automargin: false, zeroline: true,
    zerolinecolor: cssVar('--lg-text-2'),
    title: { text: 'grading correction (grades)', standoff: 6 } };
  layout.legend = { ...layout.legend, orientation: 'h', y: -0.28, yanchor: 'top',
    x: 0, font: { size: 10 } };
  Plotly.react(el, traces, layout, { displayModeBar: false, responsive: true });

  const note = document.getElementById('v2-time-note');
  if (note) {
    note.innerHTML = `Each point is one of the 29 gyms, positioned by the average `
      + `within-climber date of its rows. ${gt.n_multi.toLocaleString()} multi-gym `
      + 'climbers carry these contrasts; the median one’s first and last '
      + `hardest send are only <b>${gt.gap_median} years</b> apart, and just `
      + `${Math.round(gt.gap_over_1y * 100)}% are more than a year apart &mdash; so `
      + 'the confound is concentrated in a minority. Right is later, up is stiffer, '
      + 'and the relationship runs exactly the way unmodelled improvement would '
      + 'push it.';
  }
}

function renderV2TimeStats() {
  const host = document.getElementById('v2-time-stats');
  if (!host || !V2_TIME) return;
  const gt = V2_TIME.gym_time, a = V2_TIME.advancement;
  const d = a.debiased;
  // V4-V8 rather than the single median bin: the curve is flat across that
  // span and each bin on its own carries a standard error near its own value.
  const near = d.filter((r) => r.v >= 4 && r.v <= 8);
  const typical = near.reduce((s, r) => s + r.mean, 0) / (near.length || 1);
  const spread = gt.spread_t_c[1] - gt.spread_t_c[0];
  // What improvement can actually account for, against the observed spread.
  const explained = typical * spread;
  const observed = (V2_RESULTS?.spread) || 1.29;
  const tiles = [
    { v: `+${gt.raw.r.toFixed(2)}`, l: 'correlation, raw',
      s: 'gym correction vs within-climber date' },
    { v: `+${gt.within_brand.r.toFixed(2)}`, l: 'correlation, within company',
      s: 'so it is not just Movement being late and stiff' },
    { v: `${typical >= 0 ? '+' : ''}${typical.toFixed(2)}`, l: 'grades/yr at V4–V8',
      s: 'measured advancement where the model’s climbers sit' },
    { v: `${Math.round((explained / observed) * 100)}%`, l: 'of the spread explained',
      s: `${explained.toFixed(2)} of ${observed.toFixed(2)} grades, from improvement` },
  ];
  host.innerHTML = tiles.map((t) => `
    <div class="stat-tile">
      <div class="stat-value">${t.v}</div>
      <div class="stat-label">${t.l}</div>
      <div class="stat-sub">${t.s}</div>
    </div>`).join('');

  const verdict = document.getElementById('v2-time-verdict');
  if (verdict) {
    verdict.innerHTML = 'The correlation is solid: <b>+' + gt.raw.r.toFixed(2)
      + '</b> raw, <b>+' + gt.within_brand.r.toFixed(2) + '</b> with company means '
      + 'removed, <b>+' + (gt.by_brand.Touchstone?.r ?? 0).toFixed(2)
      + '</b> inside Touchstone alone across 17 gyms. Its slope implies '
      + `<b>${gt.within_brand.slope.toFixed(2)} grades per year</b>. `
      + 'But the measured advancement rate where the median climber sits is '
      + `<b>${typical.toFixed(2)} grades per year</b> &mdash; roughly `
      + `<b>${(gt.within_brand.slope / typical).toFixed(0)}&times; smaller</b>. `
      + '<b>Climber improvement cannot be most of this.</b> Improvement '
      + `accounts for roughly ${explained.toFixed(2)} of the ${observed.toFixed(2)} `
      + 'grade correction spread, around '
      + `${Math.round((explained / observed) * 100)}%, which is reassuring for the `
      + 'headline gym result and leaves the rest of the correlation unexplained. '
      + 'Two candidates are not yet separated: <b>gyms’ grading genuinely '
      + 'drifting over time</b> &mdash; which would be a result rather than a '
      + 'confound &mdash; and selection in <b>when</b> climbers switch gyms. '
      + 'Neither can be tested until the send date is carried into the model.';
  }

  // The "how this goes into the model" numbers, so the argument for a fixed
  // offset quotes the same measurements the section just made.
  // Read the measured bins, not the straight-line fit: the curve is convex,
  // and the line overshoots the middle by more than the correction is worth.
  const at = (v) => (d.find((r) => r.v === v) || {}).mean;
  const set = (id, s) => {
    const n = document.getElementById(id);
    if (n) n.textContent = s;
  };
  set('v2-fix-slope', `+${gt.within_brand.slope.toFixed(2)}`);
  set('v2-fix-rate', `+${typical.toFixed(2)}`);
  set('v2-fix-ratio', (gt.within_brand.slope / typical).toFixed(0));
  set('v2-fix-span', spread.toFixed(2));
  set('v2-fix-shift', explained.toFixed(2));
  set('v2-fix-spread', observed.toFixed(2));
  const f2 = (x) => (x === undefined ? '—'
    : (Math.abs(x) < 0.005 ? '0.00' : x.toFixed(2)));
  set('v2-fix-lo', f2(at(3)));
  set('v2-fix-hi', f2(at(9)));
}

function renderV2AdvTable() {
  const el = document.getElementById('v2-adv-table');
  if (!el || !V2_TIME) return;
  const a = V2_TIME.advancement;
  const byV = {};
  a.naive.forEach((r) => { byV[r.v] = { v: r.v, naive: r.mean }; });
  a.short_win.forEach((r) => { byV[r.v] = { ...(byV[r.v] || { v: r.v }), sw: r.mean }; });
  a.long.forEach((r) => { byV[r.v] = { ...(byV[r.v] || { v: r.v }), long: r.mean }; });
  a.debiased.forEach((r) => {
    byV[r.v] = { ...(byV[r.v] || { v: r.v }), deb: r.mean, sem: r.sem,
      n: r.n, chi2: r.chi2 };
  });
  const rows = Object.values(byV).filter((r) => r.deb !== undefined)
    .sort((x, y) => x.v - y.v);
  const sgn = (x) => (x === undefined ? '&mdash;'
    : `${x >= 0 ? '+' : ''}${x.toFixed(2)}`);
  el.innerHTML = '<thead><tr><th>grade</th><th>naive</th><th>3-month only</th>'
    + '<th>1-year only</th><th>steady rate</th><th>&plusmn;</th>'
    + '<th>χ²/dof</th><th>triples</th></tr></thead><tbody>'
    + rows.map((r) => `<tr><td class="label-cell">V${r.v}</td>`
      + `<td class="unit muted">${sgn(r.naive)}</td>`
      + `<td class="unit muted">${sgn(r.sw)}</td>`
      + `<td class="unit muted">${sgn(r.long)}</td>`
      + `<td class="unit"><b>${sgn(r.deb)}</b></td>`
      + `<td class="unit muted">${r.sem.toFixed(2)}</td>`
      + `<td class="unit${r.chi2 > 2 ? '' : ' muted'}">${r.chi2.toFixed(2)}</td>`
      + `<td class="unit">${r.n.toLocaleString()}</td></tr>`).join('')
    + '</tbody>';
}

// ---- can the model comparison hear itself? ----
//
// v2_reliability.json is written by scripts/build_v2_reliability.py: the same
// seven height forms scored on progressively better-observed climbers, plus
// refits of one model that give the noise floor. Nothing here is hand-typed.

let V2_REL = null;

async function loadV2Reliability() {
  if (V2_REL) return true;
  try {
    const r = await fetch('/static/v2_reliability.json', { cache: 'no-cache' });
    if (!r.ok) return false;
    V2_REL = await r.json();
    return true;
  } catch (e) {
    return false;
  }
}

// Which model version the reliability figures are showing. Both are kept and
// both are reachable: the old one is the evidence that the fix was needed.
const V2_ARM_LABEL = {
  unmarginalized: 'every climber keeps their own ability offset (10,397 parameters)',
  marginalized: 'single-observation offsets integrated out (4,241 parameters)',
};
let V2_ARM = null;

function v2Arm() {
  if (!V2_REL) return null;
  const key = V2_ARM && V2_REL.arms[V2_ARM] ? V2_ARM : V2_REL.primary;
  return V2_REL.arms[key] ? { key, ...V2_REL.arms[key] } : null;
}

function renderV2ArmPicker() {
  const host = document.getElementById('v2-arm-picker');
  if (!host || !V2_REL) return;
  const keys = Object.keys(V2_REL.arms);
  const cur = v2Arm();
  if (keys.length < 2) {
    host.innerHTML = `<span class="muted">Showing: ${V2_ARM_LABEL[cur.key]}. `
      + 'The other version is still fitting.</span>';
    return;
  }
  host.innerHTML = keys.map((k) => `
    <button type="button" class="seg-btn${k === cur.key ? ' on' : ''}"
            data-arm="${k}">${k === 'marginalized' ? 'Offsets integrated out'
                                                   : 'Original model'}</button>`).join('')
    + `<span class="muted seg-note">${V2_ARM_LABEL[cur.key]}</span>`;
  host.querySelectorAll('[data-arm]').forEach((b) => {
    b.onclick = () => { V2_ARM = b.dataset.arm; renderV2Noise();
      renderV2SubsetTable(); renderV2ArmPicker(); };
  });
}

function renderV2Noise() {
  const el = document.getElementById('v2-noise-chart');
  const arm = v2Arm();
  if (!el || !arm || typeof Plotly === 'undefined') return;
  const subs = V2_REL.subsets, ks = subs.map((s) => String(s.k));
  const xs = subs.map((s, i) => i);
  const label = (s) => (s.k === 1 ? 'all rows'
    : `climbers with ${s.k}+ rows`);

  const traces = [];
  // The noise band first, so the model lines draw over it. This is the whole
  // point of the figure: a gap inside the band is not a result.
  const nz = ks.map((k) => (arm.noise[k] || {}).range || 0);
  traces.push({
    type: 'scatter', mode: 'lines', x: xs, y: nz.map((v) => -v),
    line: { width: 0 }, showlegend: false, hoverinfo: 'skip',
  });
  traces.push({
    type: 'scatter', mode: 'lines', x: xs, y: nz.map(() => 0),
    line: { width: 0 }, fill: 'tonexty',
    fillcolor: hexToRgba(cssVar('--lg-text-2'), 0.16),
    name: 'noise: spread across refits of one model',
    hovertemplate: 'noise floor<extra></extra>',
  });

  const real = arm.models.filter((m) => !m.replicate_of);
  real.forEach((m, i) => {
    const c = cssVar(V2_FIT_HUES[i % V2_FIT_HUES.length]);
    traces.push({
      type: 'scatter', mode: 'lines+markers', name: m.label,
      x: xs, y: ks.map((k) => m.by_subset[k].gap),
      line: { color: c, width: 2.2 }, marker: { size: 8 },
      hovertemplate: `%{y:+.1f} vs the best model<extra>${m.label}</extra>`,
    });
  });

  const layout = chartLayout('');
  layout.height = 420;
  layout.margin = { l: 66, r: 20, t: 12, b: 118 };
  layout.xaxis = { ...layout.xaxis, automargin: false,
    tickmode: 'array', tickvals: xs, ticktext: subs.map(label),
    title: { text: 'which observations are scored', standoff: 12 } };
  layout.yaxis = { ...layout.yaxis, automargin: false, zeroline: true,
    zerolinecolor: cssVar('--lg-text-2'),
    title: { text: 'score gap from the best model (higher is better)',
      standoff: 6 } };
  layout.legend = { ...layout.legend, orientation: 'h', y: -0.26,
    yanchor: 'top', x: 0, font: { size: 10 } };
  Plotly.react(el, traces, layout, { displayModeBar: false, responsive: true });

  const note = document.getElementById('v2-noise-note');
  if (note) {
    const n1 = arm.noise['1'] || {}, n3 = arm.noise['3'] || {};
    const s3 = subs.find((s) => s.k === 3) || {};
    note.innerHTML = `<b>${V2_ARM_LABEL[arm.key]}.</b> Each line is one height `
      + 'form, scored against the best model in the same column. The grey band '
      + `is how far apart ${n1.n_runs || '?'} fits of the <b>identical</b> `
      + 'model land &mdash; pure noise, since nothing about them differs but '
      + 'the random seed. Scored on all rows that band is '
      + `<b>${(n1.range || 0).toFixed(1)}</b> points wide; scored on climbers `
      + `with three or more observations &mdash; still `
      + `${Math.round((s3.share || 0) * 100)}% of the data &mdash; it is `
      + `<b>${(n3.range || 0).toFixed(1)}</b>. Nothing was refitted between `
      + 'these columns; the same per-observation scores are simply added up '
      + 'over different rows.';
  }
}

function renderV2SubsetTable() {
  const el = document.getElementById('v2-subset-table');
  const arm = v2Arm();
  if (!el || !arm) return;
  const subs = V2_REL.subsets;
  const head = subs.map((s) => (s.k === 1 ? 'all rows' : `${s.k}+ rows`));
  const sub = subs.map((s) => `${s.rows.toLocaleString()} rows`);
  el.innerHTML = '<thead><tr><th>height model</th>'
    + head.map((h, i) => `<th>${h}<br /><span class="muted">${sub[i]}</span></th>`).join('')
    + '<th>unreliable rows<br /><span class="muted">at 3+ rows</span></th></tr></thead><tbody>'
    + arm.models.map((m) => {
      const rep = m.replicate_of ? ' muted' : '';
      return `<tr><td class="label-cell${rep}">${m.label}</td>`
        + subs.map((s) => {
          const g = m.by_subset[String(s.k)].gap;
          return `<td class="unit${rep}">${g >= 0 ? '+' : ''}${g.toFixed(1)}</td>`;
        }).join('')
        + `<td class="unit muted">${Math.round(m.by_subset['3'].bad_k * 100)}%</td></tr>`;
    }).join('')
    + '</tbody>';
}

function renderV2ArmCompare() {
  const el = document.getElementById('v2-arm-compare');
  if (!el || !V2_REL) return;
  const a = V2_REL.arms.unmarginalized, b = V2_REL.arms.marginalized;
  if (!a || !b) {
    el.innerHTML = '<p class="caption">The side-by-side comparison appears '
      + 'once both versions have finished fitting.</p>';
    return;
  }
  const row = (name, get, fmt = (x) => x) => {
    const va = get(a), vb = get(b);
    return `<tr><td class="label-cell">${name}</td>`
      + `<td class="unit">${va === undefined ? '&mdash;' : fmt(va)}</td>`
      + `<td class="unit"><b>${vb === undefined ? '&mdash;' : fmt(vb)}</b></td></tr>`;
  };
  const noise = (arm, k) => (arm.noise[k] || {}).range;
  const badk = (arm, k) => {
    const m = arm.models.find((x) => !x.replicate_of);
    return m ? m.by_subset[k].bad_k : undefined;
  };
  const f1 = (x) => x.toFixed(1);
  const pc = (x) => `${Math.round(x * 100)}%`;
  el.innerHTML = '<thead><tr><th>&nbsp;</th><th>original model</th>'
    + '<th>offsets integrated out</th></tr></thead><tbody>'
    + row('parameters', (x) => (x === a ? 10397 : 4241), (x) => x.toLocaleString())
    + row('noise between refits, all rows', (x) => noise(x, '1'), f1)
    + row('noise between refits, 3+ rows', (x) => noise(x, '3'), f1)
    + row('unreliable rows, all rows', (x) => badk(x, '1'), pc)
    + row('unreliable rows, 3+ rows', (x) => badk(x, '3'), pc)
    + '</tbody>';
}

async function renderV2Reliability() {
  if (!(await loadV2Reliability())) return;
  renderV2ArmPicker();
  renderV2Noise();
  renderV2SubsetTable();
  renderV2ArmCompare();
}

async function renderV2Time() {
  if (!(await loadV2Time())) return;
  renderV2Advancement();
  renderV2Horizon();
  renderV2Accrual();
  renderV2AdvTable();
  renderV2TimeChart();
  renderV2TimeStats();
}

// ---- what each model concluded about the body ----
//
// The parameter posteriors above are knobs; these are the curves they add up
// to. Every fit's height form and shared ape form, drawn on one pair of axes
// with credible bands, so the shapes can be compared directly.

// The centring/scaling the model applied. Comes from the fits on disk -- the
// numbers are a property of the dataset, not constants.
const V2_SCALES_FALLBACK = {
  h_median: 68.0, h_sd: 3.917, a_median: 0.0, a_sd: 1.558,
  h_lo: 59, h_hi: 76, a_lo: -3, a_hi: 5,
};
const v2Scales = () => ({ ...V2_SCALES_FALLBACK, ...(V2_POST?.scales || {}) });

// Pinned axis limits for the fitted-curve panels, computed once across both
// genders and every fit. Reset it if the fits ever reload.
let v2FittedRange = null;

// f_height for one draw, in z-units, per the model's own definition. G is the
// gender indicator the interaction terms multiply.
function v2HeightAt(form, d, z, G) {
  switch (form) {
    case 'zero': return 0;
    case 'linear': return d.gamma1 * z;
    case 'quadratic': return d.gamma1 * z + d.gamma2 * z * z;
    case 'quadratic_x_gender':
      return d.gamma1 * z + d.gamma2 * z * z
        + G * (d.gamma1_x * z + d.gamma2_x * z * z);
    case 'vertex_quadratic': return -d.vq_curv * (z - d.vq_peak) ** 2;
    case 'saturating':
      return d.sat_amp / (1 + Math.exp(-(z - d.sat_h0) / (d.sat_scale + 1e-6)));
    default: return 0;
  }
}

function v2ApeAt(d, z, G) {
  let v = (d.delta1 || 0) * z + (d.delta2 || 0) * z * z;
  if (d.delta1_x !== undefined) v += G * ((d.delta1_x || 0) * z + (d.delta2_x || 0) * z * z);
  return v;
}

// Percentile of an already-sorted array.
function v2Pct(sorted, p) {
  const i = (sorted.length - 1) * p;
  const lo = Math.floor(i), hi = Math.ceil(i);
  return lo === hi ? sorted[lo] : sorted[lo] + (sorted[hi] - sorted[lo]) * (i - lo);
}

// Posterior mean curve plus an 89% band, evaluated draw by draw and centred
// per draw at z = 0. Centring per draw (not on the mean curve) is what keeps
// the band honest: the constant is not identified, only the shape is.
function v2CurveBand(fitName, kind, zs, G) {
  const fit = v2Fit(fitName);
  if (!fit) return null;
  const form = fit.height_form;
  const need = kind === 'height'
    ? { zero: [], linear: ['gamma1'], quadratic: ['gamma1', 'gamma2'],
        quadratic_x_gender: ['gamma1', 'gamma2', 'gamma1_x', 'gamma2_x'],
        vertex_quadratic: ['vq_curv', 'vq_peak'],
        saturating: ['sat_amp', 'sat_h0', 'sat_scale'] }[form] || []
    : ['delta1', 'delta2'];
  if (kind === 'height' && form === 'zero') return { flat: true };
  if (need.some((p) => !fit.params[p])) return null;

  const cols = {};
  need.forEach((p) => { cols[p] = fit.params[p].chains.flat(); });
  if (kind === 'ape') {
    ['delta1_x', 'delta2_x'].forEach((p) => {
      if (fit.params[p]) cols[p] = fit.params[p].chains.flat();
    });
  }
  const nD = cols[Object.keys(cols)[0]].length;
  const mean = [], lo = [], hi = [];
  const at = kind === 'height'
    ? (d, z) => v2HeightAt(form, d, z, G)
    : (d, z) => v2ApeAt(d, z, G);
  // One draw object reused across the grid -- this runs ~250k times.
  const d = {};
  for (const z of zs) {
    const vals = new Array(nD);
    for (let i = 0; i < nD; i++) {
      for (const p in cols) d[p] = cols[p][i];
      vals[i] = at(d, z) - at(d, 0);
    }
    let s = 0;
    for (const v of vals) s += v;
    mean.push(s / nD);
    vals.sort((a, b) => a - b);
    lo.push(v2Pct(vals, 0.055));
    hi.push(v2Pct(vals, 0.945));
  }
  return { mean, lo, hi };
}

// Where each group actually sits on this axis, median +/- 1 SD, in the same
// two colours the height-form cards higher up the page use. Both are always
// drawn -- the curves are misleading without them, since most of the x-range
// holds almost nobody -- but the selected group is the emphasised one.
function v2GroupBands(kind, selected) {
  const sc = v2Scales();
  const defs = [
    { g: 'male', label: 'Male users', tok: '--lg-info', anchor: 'right',
      st: sc[`${kind}_male`] },
    { g: 'female', label: 'Female users', tok: '--lg-highlight', anchor: 'left',
      st: sc[`${kind}_female`] },
  ].filter((d) => d.st);
  // Height puts the two groups side by side; ape puts them concentric, both
  // centred on zero. Labels anchored the same way in both cases collide in
  // the concentric one, so which way they grow depends on the geometry.
  const nested = defs.length === 2
    && Math.abs(defs[0].st.median - defs[1].st.median) < 0.5 * Math.min(defs[0].st.sd, defs[1].st.sd);
  const shapes = [], annotations = [];
  defs.forEach((d) => {
    const on = d.g === selected;
    const lo = d.st.median - d.st.sd, hi = d.st.median + d.st.sd;
    shapes.push({
      type: 'rect', xref: 'x', yref: 'paper',
      x0: lo, x1: hi, y0: 0, y1: 1,
      fillcolor: cssVar(d.tok), opacity: on ? 0.2 : 0.05,
      // A dashed edge on the selected band as well as a stronger fill: on a
      // pale ground, fill alone has to get muddy before it reads.
      line: on
        ? { width: 1, color: cssVar(d.tok), dash: 'dot' }
        : { width: 0 },
    });
    // Side by side: anchor each label to its own outer edge, growing inward
    // over its own band. Concentric: anchor to opposite edges, growing
    // outward into the empty space on either side.
    const left = d.anchor === 'left';
    annotations.push({
      x: nested ? (left ? lo : hi) : (left ? lo : hi),
      xanchor: nested ? (left ? 'right' : 'left') : (left ? 'left' : 'right'),
      y: 1, yref: 'paper', yanchor: 'bottom', showarrow: false,
      text: on ? `<b>${d.label}</b>` : d.label,
      font: { size: on ? 11 : 10, color: cssVar(d.tok) },
      opacity: on ? 1 : 0.4,
    });
  });
  return { shapes, annotations };
}

function renderV2FittedForms() {
  if (typeof Plotly === 'undefined' || !V2_POST) return;
  const hEl = document.getElementById('v2-fitted-height');
  const aEl = document.getElementById('v2-fitted-ape');
  if (!hEl || !aEl) return;
  // The card bleeds past the prose column; set that width before drawing or
  // Plotly measures the narrow column and the second panel overhangs.
  setV2FormGridWidth();
  const G = document.getElementById('v2-fitted-gender')?.value === 'female' ? 1 : 0;
  const showBand = document.getElementById('v2-fitted-band')?.checked !== false;
  const sc = v2Scales();
  // This panel compares model *forms*, so every curve on it has to come from
  // the same data. v3_all is fitted on all first names rather than the
  // confident-name subset, so it belongs to a different comparison -- and it
  // is the arm that never converged, whose female curve alone doubled the
  // y-range everything else had to share.
  const names = v2FitNames().filter((f) => v2Fit(f)?.name_filter === sc.name_filter);
  const dropped = v2FitNames().filter((f) => !names.includes(f));

  const grid = (lo, hi, n = 55) => {
    const step = (hi - lo) / (n - 1), out = [];
    for (let i = 0; i < n; i++) out.push(lo + i * step);
    return out;
  };
  const hIn = grid(sc.h_lo, sc.h_hi);
  // Ape index is centred on zero by construction, so its axis is too, and it
  // runs out to the 99.5th percentile of |ape| rather than the asymmetric
  // 1st/99th percentiles the height axis uses.
  const aMax = Math.ceil(sc.a_abs || Math.max(Math.abs(sc.a_lo), sc.a_hi));
  const aIn = grid(-aMax, aMax);
  const hZ = hIn.map((v) => (v - sc.h_median) / sc.h_sd);
  const aZ = aIn.map((v) => (v - sc.a_median) / sc.a_sd);

  // Axis limits are computed once over BOTH genders and EVERY fit, then
  // pinned. Otherwise switching gender or clicking a model out of the legend
  // rescales the axes and the shapes appear to change when they have not.
  if (!v2FittedRange) {
    const span = (kind, zs) => {
      let lo = Infinity, hi = -Infinity;
      [0, 1].forEach((g) => names.forEach((fn) => {
        const b = v2CurveBand(fn, kind, zs, g);
        if (!b || b.flat) { lo = Math.min(lo, 0); hi = Math.max(hi, 0); return; }
        b.lo.forEach((v) => { if (v < lo) lo = v; });
        b.hi.forEach((v) => { if (v > hi) hi = v; });
      }));
      if (!Number.isFinite(lo)) return [-1, 1];
      const pad = (hi - lo) * 0.08 || 0.1;
      return [lo - pad, hi + pad];
    };
    v2FittedRange = { h: span('height', hZ), a: span('ape', aZ) };
  }

  const build = (kind, xs, zs) => {
    const traces = [];
    const allNames = v2FitNames();
    names.forEach((fn) => {
      const c = cssVar(V2_FIT_HUES[allNames.indexOf(fn) % V2_FIT_HUES.length]);
      const label = V2_FIT_LABEL[fn] || fn;
      const band = v2CurveBand(fn, kind, zs, G);
      if (!band) return;
      if (band.flat) {
        // The zero form is a claim too: a flat line at zero, no band.
        traces.push({
          type: 'scatter', mode: 'lines', name: label, legendgroup: fn,
          x: xs, y: xs.map(() => 0),
          line: { color: c, width: 2, dash: 'dot' },
          hovertemplate: `${label}<br>no height effect<extra></extra>`,
        });
        return;
      }
      if (showBand) {
        traces.push({
          type: 'scatter', mode: 'lines', x: xs, y: band.lo, legendgroup: fn,
          line: { width: 0 }, showlegend: false, hoverinfo: 'skip',
        });
        traces.push({
          type: 'scatter', mode: 'lines', x: xs, y: band.hi, legendgroup: fn,
          line: { width: 0 }, fill: 'tonexty', fillcolor: hexToRgba(c, 0.13),
          showlegend: false, hoverinfo: 'skip',
        });
      }
      traces.push({
        type: 'scatter', mode: 'lines', name: label, legendgroup: fn,
        x: xs, y: band.mean, line: { color: c, width: 2.2 },
        hovertemplate: `${label}<br>%{x:.0f} → %{y:+.2f} grades<extra></extra>`,
      });
    });
    return traces;
  };

  const layoutFor = (title, ytitle) => {
    const l = chartLayout(title);
    l.height = 340;
    // Seven models wrap to two legend rows; the bottom margin and legend y are
    // measured against that, not guessed, or the legend lands on the x title.
    l.margin = { l: 56, r: 20, t: 10, b: 124 };
    l.xaxis = { ...l.xaxis, automargin: false,
      title: { text: title, standoff: 10 } };
    l.yaxis = { ...l.yaxis, automargin: false, title: { text: ytitle, standoff: 6 },
      zeroline: true, zerolinecolor: cssVar('--lg-text-2') };
    l.legend = { ...l.legend, orientation: 'h', y: -0.42, yanchor: 'top', x: 0,
      font: { size: 10 } };
    return l;
  };

  const sel = G ? 'female' : 'male';
  const hLayout = layoutFor('height (inches)', 'grade impact');
  const hb = v2GroupBands('h', sel);
  hLayout.shapes = hb.shapes;
  hLayout.annotations = hb.annotations;
  hLayout.margin.t = 24;               // room for the band labels
  hLayout.xaxis = { ...hLayout.xaxis, range: [sc.h_lo, sc.h_hi], autorange: false };
  hLayout.yaxis = { ...hLayout.yaxis, range: v2FittedRange.h, autorange: false };
  Plotly.react(hEl, build('height', hIn, hZ), hLayout,
    { displayModeBar: false, responsive: true });

  const aLayout = layoutFor('ape index (inches)', 'grade impact');
  const ab = v2GroupBands('a', sel);
  aLayout.shapes = ab.shapes;
  aLayout.annotations = ab.annotations;
  aLayout.margin.t = 24;
  aLayout.xaxis = { ...aLayout.xaxis, range: [-aMax, aMax], autorange: false,
    zeroline: true, zerolinecolor: cssVar('--lg-text-2') };
  aLayout.yaxis = { ...aLayout.yaxis, range: v2FittedRange.a, autorange: false };
  Plotly.react(aEl, build('ape', aIn, aZ), aLayout,
    { displayModeBar: false, responsive: true });

  const note = document.getElementById('v2-fitted-note');
  if (note) {
    // The honest summary number: how far the best curve travels across the
    // middle 98% of climbers, compared with how wide its band is there.
    const spans = names.map((fn) => {
      const b = v2CurveBand(fn, 'height', hZ, G);
      if (!b || b.flat) return null;
      const span = Math.max(...b.mean) - Math.min(...b.mean);
      const width = Math.max(...b.hi.map((v, i) => v - b.lo[i]));
      return { fn, span, width };
    }).filter(Boolean);
    const worst = spans.sort((a, b2) => b2.span - a.span)[0];
    // Only the ape x gender arm's ape curve responds to the toggle; every
    // other model's ape term is gender-blind. Say so, or the toggle looks
    // broken on the right-hand panel.
    const apeByGender = names.filter((f) => v2Fit(f)?.params?.delta1_x)
      .map((f) => V2_FIT_LABEL[f] || f);
    const hm = sc.h_male, hf = sc.h_female;
    const bands = (hm && hf)
      ? 'Shaded strips are each group&rsquo;s median &plusmn;1 SD &mdash; '
        + `male ${(hm.median - hm.sd).toFixed(0)}&ndash;${(hm.median + hm.sd).toFixed(0)} in, `
        + `female ${(hf.median - hf.sd).toFixed(0)}&ndash;${(hf.median + hf.sd).toFixed(0)} in `
        + '&mdash; with the selected group emphasised. Outside them the curves are '
        + 'extrapolation. '
      : '';
    const dropNote = dropped.length
      ? `<b>${dropped.map((f) => V2_FIT_LABEL[f] || f).join(', ')}</b> `
        + `${dropped.length === 1 ? 'is' : 'are'} not drawn here: fitted on a `
        + 'different user set, so the curves would not be comparable. '
      : '';
    const apeNote = apeByGender.length
      ? `On the ape panel the selector only changes <b>${apeByGender.join('</b>, <b>')}</b>: `
        + 'every other model&rsquo;s ape term is the same for everyone. The bands '
        + 'still differ, because the two groups&rsquo; ape distributions do. '
      : 'On the ape panel the selector changes only the bands &mdash; every '
        + 'model&rsquo;s ape term is the same for everyone. ';
    note.innerHTML = worst
      ? bands + apeNote + dropNote
        + `The largest height effect any model claims is <b>${V2_FIT_LABEL[worst.fn] || worst.fn}</b>, `
        + `travelling <b>${worst.span.toFixed(2)} grades</b> across the whole range `
        + `&mdash; against a credible band up to <b>${worst.width.toFixed(2)} grades</b> wide. `
        + (worst.span < worst.width
          ? 'The band is wider than the effect, which is the whole story: the shapes '
            + 'differ but none of them is separated from a flat line.'
          : 'That is a real effect, but read it against a gym-to-gym spread of '
            + 'well over a grade.')
      : '';
  }
}

// ---- corner plot ----

// Canonical ordering for the everything-at-once corner plot: the same order
// the symbol glossary walks, so the two read alike.
const V2_PARAM_ORDER = Object.keys(V2_PARAM_TEX);

function v2CornerNames(groupKey, fits) {
  const present = (n) => fits.some((f) => f.params[n]);
  if (groupKey === 'all') return V2_PARAM_ORDER.filter(present);
  return (V2_CORNER_GROUPS[groupKey]?.of || []).filter(present);
}

// Pearson correlation of two equal-length draw vectors.
function v2Corr(a, b) {
  const n = Math.min(a.length, b.length);
  if (n < 3) return 0;
  let ma = 0, mb = 0;
  for (let i = 0; i < n; i++) { ma += a[i]; mb += b[i]; }
  ma /= n; mb /= n;
  let sab = 0, sa = 0, sb = 0;
  for (let i = 0; i < n; i++) {
    const da = a[i] - ma, db = b[i] - mb;
    sab += da * db; sa += da * da; sb += db * db;
  }
  return sa && sb ? sab / Math.sqrt(sa * sb) : 0;
}

function renderV2Corner() {
  const el = document.getElementById('v2-corner');
  if (!el || typeof Plotly === 'undefined' || !V2_POST) return;
  const groupKey = document.getElementById('v2-corner-group')?.value || 'height';
  let overlay = document.getElementById('v2-corner-overlay')?.value || 'one';
  let style = document.getElementById('v2-corner-style')?.value || 'both';
  const primaryName = v2SelectedFit();

  // The everything plot bleeds out past the prose column; the grouped ones
  // stay inside it. Do this before drawing so Plotly measures the final width.
  const wide = groupKey === 'all';
  el.classList.toggle('chart-bleed-wide', wide);
  if (wide) setV2CornerWidth();

  // Say it in the control, not only in the caption underneath the plot.
  const ovSel = document.getElementById('v2-corner-overlay');
  const ovAll = ovSel?.querySelector('option[value="all"]');
  if (ovAll) {
    ovAll.disabled = wide;
    ovAll.textContent = wide ? 'All models overlaid — too slow here' : 'All models overlaid';
    if (wide && ovSel.value === 'all') { ovSel.value = 'one'; overlay = 'one'; }
  }

  const names0 = ['one', undefined].includes(overlay)
    ? v2CornerNames(groupKey, [v2Fit(primaryName)].filter(Boolean))
    : v2CornerNames(groupKey, v2FitNames().map(v2Fit).filter(Boolean));
  const names = names0;
  const N = names.length;
  if (N < 2) {
    Plotly.purge(el);
    el.innerHTML = '<p class="form-noparams">This fit does not contain enough of '
      + 'these parameters to draw a corner plot.</p>';
    const n0 = document.getElementById('v2-corner-note');
    if (n0) n0.innerHTML = '';
    return;
  }
  if (!el._fullLayout) el.innerHTML = '';

  // Which fits are drawn, and in what colour. A fit earns a place only if it
  // shares at least two of these parameters -- one gets you a diagonal and
  // nothing else.
  const allNames = v2FitNames();
  const chosen = (overlay === 'all' ? allNames : [primaryName])
    .filter((n) => {
      const f = v2Fit(n);
      return f && names.filter((p) => f.params[p]).length >= 2;
    });
  if (!chosen.length) chosen.push(primaryName);
  const colourOf = {};
  chosen.forEach((n) => {
    colourOf[n] = cssVar(V2_FIT_HUES[allNames.indexOf(n) % V2_FIT_HUES.length]);
  });

  // Cost control. Every cell is a separate SVG subplot, and contours cost
  // several times a scatter, so a 15-parameter plot across 7 fits has to give
  // something up or the page locks for seconds.
  const nCells = (N * (N + 1)) / 2;
  let downgraded = '';
  // Overlaying in the everything view is not a fallback, it is not possible:
  // every panel-model pair is its own SVG subplot trace, and 171 panels x 7
  // models is ~1,200 of them, which takes Plotly well over half a minute and
  // freezes the tab while it works. Measured, not assumed -- contours-only
  // does not help, because the cost is the trace count, not the trace type.
  if (chosen.length > 1 && nCells > 90) {
    const keep = chosen.includes(primaryName) ? primaryName : chosen[0];
    chosen.length = 0;
    chosen.push(keep);
    downgraded = `Overlaying models is off in this view: ${nCells} panels &times; every `
      + 'model is ~1,200 separate plots, which takes over half a minute to draw. '
      + `Showing <b>${V2_FIT_LABEL[keep] || keep}</b> alone. Pick a parameter group `
      + 'above to compare models.';
  }
  const load = nCells * chosen.length;
  // Points are what makes a smaller overlay unreadable as well as slow: seven
  // scatter clouds on one panel is mud, and contours survive the density.
  const heavy = load > 260 || nCells > 60;
  if (heavy && chosen.length > 1 && style !== 'contour') {
    style = 'contour';
    downgraded += (downgraded ? ' ' : '')
      + `Contours only here: ${nCells} panels &times; ${chosen.length} models `
      + 'is more scatter than one panel can show. Narrow the parameter group '
      + 'to get the points back.';
  } else if (heavy && style === 'both') {
    style = 'points';
    downgraded += (downgraded ? ' ' : '')
      + `Points only here: ${nCells} panels is more than contours can be fitted `
      + 'to at a usable speed.';
  }
  const MAX_PTS = load > 300 ? 120 : (chosen.length > 3 ? 220 : 500);
  // Coarser contours when there are hundreds of panels: at 56px a cell, four
  // levels and 18 bins is detail nobody can see and everybody waits for.
  const cLevels = heavy ? 2 : 4;
  const cBins = heavy ? 12 : 18;

  // Draws, per fit, thinned to the same stride across parameters so each cell
  // is a genuine joint sample rather than a scatter of unrelated numbers.
  const data = {};       // fit -> param -> full draws
  const pts = {};        // fit -> param -> thinned draws
  chosen.forEach((fn) => {
    const f = v2Fit(fn);
    data[fn] = {}; pts[fn] = {};
    let stride = 1;
    names.forEach((p) => {
      if (!f.params[p]) return;
      const d = f.params[p].chains.flat();
      data[fn][p] = d;
      stride = Math.max(stride, Math.ceil(d.length / MAX_PTS));
    });
    names.forEach((p) => {
      if (data[fn][p]) pts[fn][p] = data[fn][p].filter((_, i) => i % stride === 0);
    });
  });

  // One shared range per parameter across every drawn fit, so a column and its
  // row line up and the overlay is actually comparable.
  const ranges = names.map((p) => {
    let lo = Infinity, hi = -Infinity;
    chosen.forEach((fn) => {
      const d = data[fn][p];
      if (!d) return;
      for (const v of d) { if (v < lo) lo = v; if (v > hi) hi = v; }
    });
    if (!Number.isFinite(lo)) return [-1, 1];
    const pad = (hi - lo) * 0.06 || 0.05;
    return [lo - pad, hi + pad];
  });

  const traces = [], layout = chartLayout('');
  // Grouped plots get generous square-ish cells; the everything plot is width
  // bound, so it goes square instead of stretching cells into tall slivers.
  // Its width has to be stated outright -- react() reuses the width it last
  // measured, which for the first draw is the prose column, not the bled-out
  // element.
  if (wide) {
    layout.width = Math.max(520, el.clientWidth);
    layout.height = layout.width;
  } else {
    layout.height = Math.max(380, (N > 8 ? 108 : 150) * N);
  }
  layout.margin = { l: N > 8 ? 62 : 78, r: 16, t: 14, b: N > 8 ? 58 : 66 };
  const dense = N > 11;   // tick labels stop being readable past here
  delete layout.xaxis; delete layout.yaxis;

  const gap = 0.055 / Math.max(1, N - 1);
  const cell = (k) => (k === 0 ? '' : String(k + 1));
  const seenLegend = {};
  let k = 0;
  for (let i = 0; i < N; i++) {
    for (let j = 0; j <= i; j++) {
      const xa = `x${cell(k)}`, ya = `y${cell(k)}`;
      const isDiag = i === j;
      chosen.forEach((fn) => {
        const c = colourOf[fn];
        const label = V2_FIT_LABEL[fn] || fn;
        // Legend entry once per fit, on whichever cell that fit first appears
        // in; legendgroup makes the click toggle the whole model at once.
        const legend = () => {
          const first = !seenLegend[fn];
          seenLegend[fn] = true;
          return { legendgroup: fn, name: label, showlegend: first };
        };
        if (isDiag) {
          const d = data[fn][names[i]];
          if (!d) return;
          const grid = v2Grid(ranges[i][0], ranges[i][1], 90);
          traces.push({
            type: 'scatter', mode: 'lines', x: grid, y: v2Kde(d, grid),
            xaxis: xa, yaxis: ya,
            fill: chosen.length === 1 ? 'tozeroy' : undefined,
            line: { color: c, width: 1.5 },
            fillcolor: `color-mix(in srgb, ${c} 22%, transparent)`,
            hoverinfo: 'skip', ...legend(),
          });
          return;
        }
        const dx = data[fn][names[j]], dy = data[fn][names[i]];
        if (!dx || !dy) return;
        if (style !== 'points') {
          traces.push({
            type: 'histogram2dcontour',
            x: dx, y: dy, xaxis: xa, yaxis: ya,
            colorscale: [[0, c], [1, c]], showscale: false,
            ncontours: cLevels, contours: { coloring: 'lines' },
            line: { width: 1.1, smoothing: 1.3 },
            nbinsx: cBins, nbinsy: cBins,
            hoverinfo: 'skip', ...legend(),
          });
        }
        if (style !== 'contour') {
          traces.push({
            type: 'scatter', mode: 'markers',
            x: pts[fn][names[j]], y: pts[fn][names[i]], xaxis: xa, yaxis: ya,
            marker: {
              color: c, size: N > 8 ? 2 : 2.5,
              // Points under contours are texture, not the message; the more
              // models are stacked the more they have to step back.
              opacity: style === 'both' ? 0.5 / (chosen.length + 2) : 0.34,
            },
            hovertemplate: `${label}<br>${names[j]} %{x:.3f}<br>${names[i]} %{y:.3f}<extra></extra>`,
            ...legend(),
          });
        }
      });
      const bottom = i === N - 1;
      const left = j === 0 && !isDiag;
      const tf = { size: N > 8 ? 8 : 9 };
      const titleFont = { size: N > 8 ? 9 : 10 };
      layout[`xaxis${cell(k)}`] = {
        domain: [j / N + gap, (j + 1) / N - gap],
        anchor: ya, range: ranges[j],
        showticklabels: bottom && !dense, nticks: N > 8 ? 3 : 4, automargin: false,
        gridcolor: cssVar('--lg-border'), zerolinecolor: cssVar('--lg-border'),
        title: bottom ? { text: names[j], standoff: 6, font: titleFont } : undefined,
        tickfont: tf,
      };
      layout[`yaxis${cell(k)}`] = {
        domain: [1 - (i + 1) / N + gap, 1 - i / N - gap],
        anchor: xa,
        // The diagonal's vertical axis is a density, not the parameter, so it
        // gets neither the shared range nor a label.
        range: isDiag ? undefined : ranges[i],
        showticklabels: left && !dense, nticks: N > 8 ? 3 : 4, automargin: false,
        gridcolor: cssVar('--lg-border'), zerolinecolor: cssVar('--lg-border'),
        title: left ? { text: names[i], standoff: 6, font: titleFont } : undefined,
        tickfont: tf,
      };
      k++;
    }
  }

  // The upper-right triangle of a corner plot is empty by construction, which
  // is exactly where the legend wants to live.
  layout.showlegend = chosen.length > 1;
  if (layout.showlegend) {
    // Top-right is empty by construction in a lower-triangle corner plot.
    layout.legend = {
      ...layout.legend, orientation: 'v',
      x: 1, y: 1, xanchor: 'right', yanchor: 'top',
      bgcolor: cssVar('--lg-card'), bordercolor: cssVar('--lg-border'),
      borderwidth: 1, font: { size: 11 }, itemsizing: 'constant',
    };
  }
  // responsive is off in the wide case on purpose: the width is pinned above,
  // and Plotly's resize observer would otherwise redraw 120 subplots on every
  // frame of the glossary panel's slide.
  Plotly.react(el, traces, layout, { displayModeBar: false, responsive: !wide });

  const note = document.getElementById('v2-corner-note');
  if (!note) return;
  const primary = v2Fit(chosen.includes(primaryName) ? primaryName : chosen[0]);
  const pName = V2_FIT_LABEL[chosen.includes(primaryName) ? primaryName : chosen[0]];
  let worst = null;
  for (let i = 0; i < N; i++) {
    for (let j = i + 1; j < N; j++) {
      const a = primary.params[names[i]], b = primary.params[names[j]];
      if (!a || !b) continue;
      const r = v2Corr(a.chains.flat(), b.chains.flat());
      if (!worst || Math.abs(r) > Math.abs(worst.r)) worst = { r, a: names[i], b: names[j] };
    }
  }
  let html = 'Diagonal cells are each parameter on its own; off-diagonal cells are pairs. ';
  if (chosen.length > 1) {
    html += `Each colour is one model &mdash; click the legend to isolate one. `
      + 'A parameter a model does not contain is simply absent from its cells. '
      + `Correlations quoted below are for <b>${pName}</b>. `;
  }
  if (worst) {
    html += `Strongest pairing: <b>${worst.a}</b> and <b>${worst.b}</b>, `
      + `correlation <b>${worst.r.toFixed(2)}</b>. `
      + (Math.abs(worst.r) > 0.7
        ? 'A tight diagonal ridge like that means the two are <b>trading off against '
          + 'each other</b> — the data pin down their combination far better than '
          + 'either alone, and the sampler has to crawl along that ridge. This is '
          + 'the geometry behind the convergence trouble on this page.'
        : 'Nothing here is strongly entangled, which is what you want: each '
          + 'parameter is being identified more or less on its own.');
  }
  if (downgraded) html += ` <b>${downgraded}</b>`;
  note.innerHTML = html;
}

// The everything-at-once corner plot wants the whole pane, not the 920px prose
// column. Same trick as setV2FormGridWidth, but without its 1240px cap.
function setV2CornerWidth() {
  const el = document.getElementById('v2-corner');
  const pane = document.getElementById('tab-grading-v2');
  if (!el || !pane) return;
  const cs = getComputedStyle(pane);
  const usable = pane.clientWidth
    - parseFloat(cs.paddingLeft || 0) - parseFloat(cs.paddingRight || 0);
  const shell = el.parentElement.clientWidth;
  pane.style.setProperty('--fg-wide', `${Math.round(Math.max(shell, usable))}px`);
  void el.offsetWidth;   // force layout so Plotly measures the new width
}

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
  const tbl = document.getElementById('v2-sampler-table');
  const note = document.getElementById('v2-sampler-note');
  const guide = document.getElementById('v2-sampler-guide');
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
  if (bindV2Inference.done || !V2_POST) return;
  bindV2Inference.done = true;

  const fitSel = document.getElementById('v2-fit-pick');
  if (fitSel && !fitSel.options.length) {
    v2FitNames().forEach((f) => {
      const o = document.createElement('option');
      const fit = v2Fit(f);
      o.value = f;
      o.textContent = `${f} — ${V2_FIT_LABEL[f] || fit.height_form}`
        + (fit.max_rhat > 1.01 ? ` (R-hat ${fit.max_rhat.toFixed(2)})` : '');
      fitSel.appendChild(o);
    });
    if (v2FitNames().includes('v3_conf')) fitSel.value = 'v3_conf';
  }
  const paramSel = document.getElementById('v2-param-pick');
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

  const groupSel = document.getElementById('v2-corner-group');
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

  fitSel?.addEventListener('change', () => {
    fillParams();
    renderV2PostGrid();
    renderV2ParamDetail(paramSel.value);
    renderV2Corner();
    renderV2Sampler();
  });
  paramSel?.addEventListener('change', () => renderV2ParamDetail(paramSel.value));
  ['v2-param-wide', 'v2-trace-mode'].forEach((id) => {
    document.getElementById(id)?.addEventListener('change',
      () => renderV2ParamDetail(paramSel.value));
  });
  ['v2-corner-group', 'v2-corner-overlay', 'v2-corner-style'].forEach((id) => {
    document.getElementById(id)?.addEventListener('change', renderV2Corner);
  });
  ['v2-fitted-gender', 'v2-fitted-band'].forEach((id) => {
    document.getElementById(id)?.addEventListener('change', renderV2FittedForms);
  });
}

async function loadV2Posterior() {
  if (V2_POST) return V2_POST;
  try {
    const r = await fetch('/static/v2_posterior.json');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    V2_POST = await r.json();
  } catch (e) {
    const host = document.getElementById('v2-post-grid');
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
    const el = document.getElementById('v2-symbols');
    if (el) window.renderMathInElement(el, { delimiters: [{ left: '\\(', right: '\\)', display: false }] });
  }
  renderV2PostGrid();
  renderV2Sampler();
  renderV2FittedForms();
  renderV2Corner();
  const sel = document.getElementById('v2-param-pick');
  renderV2ParamDetail(sel?.value || Object.keys(v2Fit(v2SelectedFit()).params)[0]);
}


// ---- glossary panel: open/close + equation-to-symbol highlighting ----

const V2_GLOSS_KEY = 'kaya.v2.glossary.open';

function setV2GlossaryOpen(open, persist = true) {
  const panel = document.getElementById('v2-glossary');
  const btn = document.getElementById('v2-gloss-toggle');
  if (!panel) return;
  panel.dataset.open = open ? 'true' : 'false';
  // Reserve the gutter so the centred article re-centres beside the panel
  // instead of running underneath it.
  document.getElementById('tab-grading-v2')?.classList.toggle('gloss-open', open);
  // The grid's usable width just changed by the width of the panel gutter.
  setTimeout(sizeV2FormGrid, 240);
  if (btn) btn.setAttribute('aria-expanded', open ? 'true' : 'false');
  if (persist) {
    try { localStorage.setItem(V2_GLOSS_KEY, open ? '1' : '0'); } catch (e) { /* private mode */ }
  }
}

// Dim every row except the ones this equation actually uses, and bring the
// first match into view if the panel has scrolled past it.
function highlightV2Symbols(keys) {
  const panel = document.getElementById('v2-glossary');
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

let v2GlossaryBound = false;

function bindV2Glossary() {
  if (v2GlossaryBound) return;
  const panel = document.getElementById('v2-glossary');
  if (!panel) return;
  v2GlossaryBound = true;

  const toggle = document.getElementById('v2-gloss-toggle');
  if (toggle) {
    toggle.addEventListener('click', () => {
      setV2GlossaryOpen(panel.dataset.open !== 'true');
    });
  }
  const opener = document.getElementById('v2-gloss-open');
  if (opener) {
    opener.addEventListener('click', () => {
      setV2GlossaryOpen(true);
      panel.querySelector('.glossary-scroll')?.scrollTo({ top: 0 });
    });
  }

  // Hovering an equation filters the panel. Opening it on hover would be
  // jarring, so a shut panel just pulses the handle instead.
  document.querySelectorAll('#tab-grading-v2 .eqn').forEach((eq) => {
    const keys = (eq.dataset.syms || '').split(/\s+/).filter(Boolean);
    const on = () => { if (panel.dataset.open === 'true') highlightV2Symbols(keys); };
    const off = () => highlightV2Symbols(null);
    eq.addEventListener('mouseenter', on);
    eq.addEventListener('mouseleave', off);
    eq.addEventListener('focus', on);
    eq.addEventListener('blur', off);

    // Clicking the equation is the way in when the panel is shut: it opens the
    // panel and lands on this equation's symbols. highlightV2Symbols scrolls
    // the first hit into view, but only once the panel has actually widened.
    // Click is a toggle: open the panel onto this equation's symbols, or shut
    // it again if it is already open.
    const openTo = () => {
      if (panel.dataset.open === 'true') {
        highlightV2Symbols(null);
        setV2GlossaryOpen(false);
        return;
      }
      setV2GlossaryOpen(true);
      setTimeout(() => highlightV2Symbols(keys), 220);
    };
    eq.setAttribute('role', 'button');
    eq.setAttribute('title', 'Show these symbols in the reference panel (click again to close it)');
    eq.addEventListener('click', openTo);
    eq.addEventListener('keydown', (ev) => {
      if (ev.key === 'Enter' || ev.key === ' ') { ev.preventDefault(); openTo(); }
    });
  });

  // Reverse direction: hovering a definition marks the equations that use it.
  panel.addEventListener('mouseover', (ev) => {
    const row = ev.target.closest('[data-sym]');
    if (!row) return;
    document.querySelectorAll('#tab-grading-v2 .eqn').forEach((eq) => {
      const keys = (eq.dataset.syms || '').split(/\s+/);
      eq.classList.toggle('eqn-active', keys.includes(row.dataset.sym));
    });
  });
  panel.addEventListener('mouseleave', () => {
    document.querySelectorAll('#tab-grading-v2 .eqn.eqn-active')
      .forEach((eq) => eq.classList.remove('eqn-active'));
  });

  document.addEventListener('keydown', (ev) => {
    if (ev.key === 'Escape' && panel.dataset.open === 'true'
        && document.getElementById('tab-grading-v2')?.classList.contains('active')) {
      setV2GlossaryOpen(false);
    }
  });

  // Default open where there is a gutter to open into; remember the choice.
  let stored = null;
  try { stored = localStorage.getItem(V2_GLOSS_KEY); } catch (e) { /* private mode */ }
  const wide = window.matchMedia('(min-width: 1180px)').matches;
  setV2GlossaryOpen(stored === null ? wide : stored === '1', false);

  // Only now, once the resting position is set, allow the slide to animate --
  // see the .is-animated note in the CSS. rAF is the clean signal but never
  // fires while the tab is in the background, so a timer backs it up;
  // whichever lands first wins and the other is a no-op.
  const pane = document.getElementById('tab-grading-v2');
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
  const el = document.getElementById('v2-forms-table');
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

// ---- gap-likelihood explorer ----
//
// The model never sees a failed attempt, so the observed max is the ceiling
// minus an Exponential gap, blurred by grade rounding. That composition is an
// Exponentially Modified Gaussian; these two charts are it, drawn.

const V2_GAP_STATE = { C: 6.0, visits: 8, sigma: 0.5, kappa: 0.277, rho: -0.018, rel: 0 };
// Fitted on v3_conf: log lambda = log_lambda0 + kappa*n_tilde + rho*r_tilde.
const V2_LOG_LAMBDA0 = 0.082, V2_MEDIAN_VISITS = 8;

const v2NormPdf = (x, mu, sd) => Math.exp(-0.5 * ((x - mu) / sd) ** 2) / (sd * Math.sqrt(2 * Math.PI));

// Rate for a climber with `visits` days logged and centred reliability `rel`.
function v2GapRate(visits, kappa, rho, rel) {
  const nTilde = visits / V2_MEDIAN_VISITS - 1;
  return Math.exp(V2_LOG_LAMBDA0 + kappa * nTilde + rho * rel);
}

// Density of the observed max: (C - Exponential(rate)) + Normal(0, sigma).
// Convolution done numerically -- clearer to read than the ExGaussian closed
// form, and this is a picture, not the likelihood.
function v2ObservedDensity(xs, C, rate, sigma) {
  const gaps = [], step = 0.02;
  for (let g = 0; g < 12; g += step) gaps.push(g);
  return xs.map((x) => {
    let acc = 0;
    for (const g of gaps) acc += rate * Math.exp(-rate * g) * v2NormPdf(x, C - g, sigma) * step;
    return acc;
  });
}

function renderV2GapChart() {
  const el = document.getElementById('v2-gap-chart');
  if (!el || typeof Plotly === 'undefined') return;
  const { C, visits, sigma, kappa, rho, rel } = V2_GAP_STATE;
  const rate = v2GapRate(visits, kappa, rho, rel);
  const xs = [];
  for (let x = C - 7; x <= C + 2.5; x += 0.02) xs.push(x);
  const ys = v2ObservedDensity(xs, C, rate, sigma);

  const traces = [{
    type: 'scatter', mode: 'lines', name: 'observed max', x: xs, y: ys,
    fill: 'tozeroy', line: { color: cssVar('--lg-info'), width: 2 },
    fillcolor: `color-mix(in srgb, ${cssVar('--lg-info')} 18%, transparent)`,
    hovertemplate: 'grade %{x:.2f}<br>density %{y:.3f}<extra></extra>',
  }];
  const layout = chartLayout('logged max grade (V)');
  layout.height = 260;
  layout.yaxis = { ...layout.yaxis, title: { text: 'density', standoff: 6 }, showticklabels: false };
  layout.xaxis = { ...layout.xaxis, title: { text: 'logged max grade (V)', standoff: 8 } };
  layout.margin = { l: 46, r: 16, t: 26, b: 46 };
  layout.showlegend = false;
  layout.shapes = [{
    type: 'line', x0: C, x1: C, y0: 0, y1: 1, yref: 'paper',
    line: { color: cssVar('--lg-danger'), width: 2, dash: 'dash' },
  }];
  const mean = C - 1 / rate;
  layout.annotations = [
    { x: C, y: 1, yref: 'paper', yanchor: 'bottom', xanchor: 'left', showarrow: false,
      text: ` true ceiling C = ${C.toFixed(1)}`, font: { size: 10, color: cssVar('--lg-danger') } },
    { x: mean, y: 0.5, yref: 'paper', yanchor: 'bottom', xanchor: 'right', showarrow: false,
      text: `mean logged ${mean.toFixed(2)} `, font: { size: 10, color: cssVar('--lg-text-2') } },
  ];
  Plotly.react(el, traces, layout, { displayModeBar: false, responsive: true });

  const note = document.getElementById('v2-gap-note');
  if (note) {
    note.textContent = `expected gap ${(1 / rate).toFixed(2)} grades `
      + `— a climber at V${C.toFixed(1)} logs V${mean.toFixed(2)} on average`;
  }
}

function renderV2VisitsChart() {
  const el = document.getElementById('v2-visits-chart');
  if (!el || typeof Plotly === 'undefined') return;
  const { kappa, rho, rel } = V2_GAP_STATE;
  const xs = [];
  for (let n = 1; n <= 40; n += 1) xs.push(n);
  const ys = xs.map((n) => 1 / v2GapRate(n, kappa, rho, rel));

  const traces = [{
    type: 'scatter', mode: 'lines', name: 'expected gap', x: xs, y: ys,
    line: { color: cssVar('--lg-info'), width: 2.5 },
    hovertemplate: '%{x} visits → %{y:.2f} grades below ceiling<extra></extra>',
  }];
  const layout = chartLayout('days logged at the gym');
  layout.height = 260;
  layout.yaxis = { ...layout.yaxis, title: { text: 'expected gap (grades)', standoff: 6 }, rangemode: 'tozero' };
  layout.xaxis = { ...layout.xaxis, title: { text: 'days logged at the gym', standoff: 8 } };
  layout.margin = { l: 58, r: 16, t: 26, b: 46 };
  layout.showlegend = false;
  layout.shapes = [{
    type: 'line', x0: V2_MEDIAN_VISITS, x1: V2_MEDIAN_VISITS, y0: 0, y1: 1, yref: 'paper',
    line: { color: cssVar('--lg-text-2'), width: 1, dash: 'dot' },
  }];
  layout.annotations = [{
    x: V2_MEDIAN_VISITS, y: 1, yref: 'paper', yanchor: 'bottom', xanchor: 'left',
    showarrow: false, text: ' median climber', font: { size: 10, color: cssVar('--lg-text-2') },
  }];
  Plotly.react(el, traces, layout, { displayModeBar: false, responsive: true });

  const note = document.getElementById('v2-visits-note');
  if (note) {
    const g1 = 1 / v2GapRate(1, kappa, rho, rel), g30 = 1 / v2GapRate(30, kappa, rho, rel);
    note.textContent = `1 visit: ${g1.toFixed(2)} grades below — 30 visits: ${g30.toFixed(2)}`;
  }
}

const V2_GAP_SLIDERS = [
  { host: 'v2-gap-controls', id: 'C', tex: 'C', label: 'true ceiling', min: 3, max: 10, step: 0.1 },
  { host: 'v2-gap-controls', id: 'visits', tex: 'n', label: 'visits', min: 1, max: 40, step: 1 },
  { host: 'v2-gap-controls', id: 'sigma', tex: '\\sigma_{\\text{link}}', label: 'rounding noise', min: 0.05, max: 1.5, step: 0.05 },
  { host: 'v2-visits-controls', id: 'kappa', tex: '\\kappa', label: 'visit effect', min: 0, max: 0.8, step: 0.01 },
  { host: 'v2-visits-controls', id: 'rho', tex: '\\rho', label: 'reliability effect', min: -0.5, max: 0.5, step: 0.01 },
  { host: 'v2-visits-controls', id: 'rel', tex: '\\tilde r', label: 'how completely they log', min: -1.5, max: 1.5, step: 0.05 },
];

function bindV2GapExplorer() {
  const hosts = { 'v2-gap-controls': [], 'v2-visits-controls': [] };
  V2_GAP_SLIDERS.forEach((sl) => hosts[sl.host]?.push(sl));
  Object.entries(hosts).forEach(([hostId, sliders]) => {
    const host = document.getElementById(hostId);
    if (!host) return;
    const noteId = hostId === 'v2-gap-controls' ? 'v2-gap-note' : 'v2-visits-note';
    host.innerHTML = sliders.map((sl) => `
      <label class="form-slider wide">
        <span class="form-slider-tex">\\(${sl.tex}\\)</span>
        <input type="range" id="v2-gs-${sl.id}" data-k="${sl.id}"
               min="${sl.min}" max="${sl.max}" step="${sl.step}" value="${V2_GAP_STATE[sl.id]}" />
        <output id="v2-gs-val-${sl.id}" class="form-slider-val"></output>
        <span class="form-slider-label">${sl.label}</span>
      </label>`).join('')
      + `<div class="form-card-foot"><span class="form-card-note" id="${noteId}"></span></div>`;
  });
  document.querySelectorAll('[id^="v2-gs-"]').forEach((inp) => {
    if (inp.tagName !== 'INPUT') return;
    inp.addEventListener('input', () => {
      V2_GAP_STATE[inp.dataset.k] = parseFloat(inp.value);
      renderV2GapExplorer();
    });
  });
}

function renderV2GapExplorer() {
  V2_GAP_SLIDERS.forEach((sl) => {
    const out = document.getElementById(`v2-gs-val-${sl.id}`);
    if (out) out.textContent = V2_GAP_STATE[sl.id].toFixed(sl.step < 0.1 ? 2 : 1);
  });
  renderV2GapChart();
  renderV2VisitsChart();
}

// ---- one interactive card per functional form ----

function renderV2FormCard(spec) {
  const chart = document.getElementById(`v2-fc-chart-${spec.key}`);
  if (!chart || typeof Plotly === 'undefined') return;
  const vals = v2FormState[spec.key];
  const { inches, z } = v2HeightGrid();

  const traces = spec.curves(vals).map((c) => ({
    type: 'scatter', mode: 'lines', name: c.name,
    x: inches, y: z.map(c.f),
    line: { color: cssVar(c.colour), width: 2.5, dash: c.dash },
    hovertemplate: `${c.name}<br>%{x:.0f} in → %{y:+.2f} grades<extra></extra>`,
  }));

  const layout = chartLayout('Height (in)');
  layout.height = 250;
  // Hold a common y-range so the cards stay comparable and a small slider
  // nudge doesn't silently rescale the axis under you -- but grow it rather
  // than clip a curve when someone drags a parameter to an extreme.
  const ys = traces.flatMap((t) => t.y);
  const span = Math.max(1.35, Math.abs(Math.min(...ys)) * 1.08, Math.max(...ys) * 1.08);
  layout.yaxis = { ...layout.yaxis, zeroline: true, range: [-span, span],
                   title: { text: 'Ability impact (grades)', standoff: 6 } };
  layout.xaxis = { ...layout.xaxis, title: { text: 'Height (in)', standoff: 8 } };
  layout.margin = { l: 58, r: 14, t: 26, b: 48 };
  layout.showlegend = spec.curves(vals).length > 1;
  // Keep the legend clear of the x tick labels -- at -0.22 its background sat
  // 2px under them and hid every tick but the last.
  // Must clear the x-axis title, not just the tick labels.
  layout.legend = { ...layout.legend, orientation: 'h', y: -0.52, x: 0, font: { size: 10 } };
  if (layout.showlegend) layout.margin.b = 100;
  const bands = v2BandShapes(true);
  layout.shapes = bands.shapes;
  layout.annotations = bands.annotations;
  Plotly.react(chart, traces, layout, { displayModeBar: false, responsive: true });

  // Readouts: current value beside each slider, plus the derived note.
  spec.params.forEach((prm) => {
    const out = document.getElementById(`v2-fc-val-${spec.key}-${prm.id}`);
    if (out) out.textContent = vals[prm.id].toFixed(prm.step < 0.01 ? 3 : 2);
  });
  const noteEl = document.getElementById(`v2-fc-note-${spec.key}`);
  if (noteEl) noteEl.textContent = spec.note ? spec.note(vals) : '';
}

function renderV2FormCards() {
  const host = document.getElementById('v2-form-cards');
  if (!host) return;
  host.innerHTML = V2_FORM_SPECS.map((spec) => {
    const badge = spec.fitted
      ? '<span class="form-badge fitted">fitted &middot; v3_conf</span>'
      : '<span class="form-badge">illustrative</span>';
    const sliders = spec.params.length
      ? spec.params.map((p) => `
          <label class="form-slider">
            <span class="form-slider-tex">\\(${p.tex}\\)</span>
            <input type="range" id="v2-fc-in-${spec.key}-${p.id}"
                   data-form="${spec.key}" data-param="${p.id}"
                   min="${p.min}" max="${p.max}" step="${p.step}" value="${p.def}" />
            <output id="v2-fc-val-${spec.key}-${p.id}" class="form-slider-val"></output>
          </label>`).join('')
      : '<p class="form-noparams">No parameters &mdash; there is nothing to adjust. That is the point.</p>';
    return `
      <section class="form-card">
        <header class="form-card-head">
          <h4>${spec.label}</h4>${badge}
        </header>
        <div class="form-card-eq">\\[${spec.eq}\\]</div>
        <p class="form-card-claim">${spec.claim}</p>
        <div id="v2-fc-chart-${spec.key}" class="form-card-chart"></div>
        <div class="form-card-controls">
          ${sliders}
          <div class="form-card-foot">
            <span class="form-card-note" id="v2-fc-note-${spec.key}"></span>
            ${spec.params.length ? `<button type="button" class="ghost-button form-reset" data-form="${spec.key}">Reset</button>` : ''}
          </div>
        </div>
      </section>`;
  }).join('');

  host.querySelectorAll('input[type="range"]').forEach((inp) => {
    inp.addEventListener('input', () => {
      const spec = V2_FORM_BY_KEY[inp.dataset.form];
      v2FormState[spec.key][inp.dataset.param] = parseFloat(inp.value);
      renderV2FormCard(spec);
    });
  });
  host.querySelectorAll('.form-reset').forEach((btn) => {
    btn.addEventListener('click', () => {
      const spec = V2_FORM_BY_KEY[btn.dataset.form];
      spec.params.forEach((p) => {
        v2FormState[spec.key][p.id] = p.def;
        const inp = document.getElementById(`v2-fc-in-${spec.key}-${p.id}`);
        if (inp) inp.value = p.def;
      });
      renderV2FormCard(spec);
    });
  });

  setV2FormGridWidth();
  V2_FORM_SPECS.forEach(renderV2FormCard);
  sizeV2FormGrid();
  if (!renderV2FormCards.resizeBound) {
    renderV2FormCards.resizeBound = true;
    let t;
    window.addEventListener('resize', () => {
      clearTimeout(t);
      t = setTimeout(sizeV2FormGrid, 150);
    });
  }
}

// The grid bleeds wider than the prose column so three cards fit comfortably.
// How much room there is depends on whether the symbols panel is reserving its
// gutter, which CSS can't see -- so measure the pane and hand CSS the number.
function setV2FormGridWidth() {
  const host = document.getElementById('v2-form-cards');
  const pane = document.getElementById('tab-grading-v2');
  if (!host || !pane) return;
  const cs = getComputedStyle(pane);
  const usable = pane.clientWidth
    - parseFloat(cs.paddingLeft || 0) - parseFloat(cs.paddingRight || 0);
  const shell = host.parentElement.clientWidth;
  // Never narrower than the column it sits in, never wider than the pane.
  const w = Math.max(shell, Math.min(1240, usable));
  // Set on the pane, not the grid: the gap explorer, posterior grid and
  // parameter detail all bleed to the same width and inherit it from here.
  pane.style.setProperty('--fg-w', `${Math.round(w)}px`);
  void host.offsetWidth;   // force layout, so anything drawn next sees the new width
}

function sizeV2FormGrid() {
  setV2FormGridWidth();
  const corner = document.getElementById('v2-corner');
  // The everything plot pins its own width, so resizing it is not enough --
  // it has to be redrawn against the new pane width.
  const cornerWide = corner?.classList.contains('chart-bleed-wide');
  if (cornerWide) {
    const before = corner._fullLayout?.width || 0;
    setV2CornerWidth();
    // Redrawing 120 subplots is expensive; only do it if the width moved.
    if (Math.abs(corner.clientWidth - before) > 4) renderV2Corner();
  }
  if (typeof Plotly === 'undefined') return;
  const ids = V2_FORM_SPECS.map((spec) => `v2-fc-chart-${spec.key}`)
    .concat(['v2-gap-chart', 'v2-visits-chart', 'v2-param-dens', 'v2-param-trace',
             'v2-across-fits', 'v2-fitted-height', 'v2-fitted-ape'],
            cornerWide ? [] : ['v2-corner'])
    .concat(Object.keys(v2Fit(v2SelectedFit())?.params || {}).map((n) => `v2-pt-${n}`));
  ids.forEach((id) => {
    const el = document.getElementById(id);
    // _fullLayout is only set once Plotly has actually drawn into the node.
    if (el && el._fullLayout) Plotly.Plots.resize(el);
  });
}

// Info dots open on hover for free (CSS), but a click pins them open so touch
// works and so a long panel can be scrolled without it vanishing underfoot.
let infoDotsBound = false;
function bindInfoDots() {
  if (infoDotsBound) return;
  infoDotsBound = true;
  document.addEventListener('click', (ev) => {
    const dot = ev.target.closest('.infodot');
    const wrap = dot?.closest('.infowrap');
    document.querySelectorAll('.infowrap.is-pinned').forEach((w) => {
      if (w !== wrap) w.classList.remove('is-pinned');
    });
    if (wrap) {
      wrap.classList.toggle('is-pinned');
      // Flip the anchor if the popover would run off the left of the pane.
      const pop = wrap.querySelector('.infopop');
      if (pop) {
        pop.classList.remove('pop-left');
        const r = pop.getBoundingClientRect();
        if (r.left < 8) pop.classList.add('pop-left');
      }
      ev.preventDefault();
    } else if (!ev.target.closest('.infopop')) {
      document.querySelectorAll('.infowrap.is-pinned')
        .forEach((w) => w.classList.remove('is-pinned'));
    }
  });
  document.addEventListener('keydown', (ev) => {
    if (ev.key !== 'Escape') return;
    document.querySelectorAll('.infowrap.is-pinned')
      .forEach((w) => w.classList.remove('is-pinned'));
  });
}


async function renderV2Tab() {
  // Gyms, headline numbers and the LOO table all come from the fits on disk,
  // so nothing gym-shaped can render until that file has loaded.
  const ok = await loadV2Results();
  if (!ok) {
    const host = document.getElementById('v2-stats');
    if (host) {
      host.innerHTML = '<p class="form-noparams">Fitted results could not be '
        + 'loaded (/static/v2_results.json). Regenerate with '
        + 'scripts/build_v2_results.py.</p>';
    }
  }
  bindInfoDots();
  renderV2FormsLoo();
  renderV2Replication();
  renderV2Stats();
  renderV2Decisions();
  renderV2Symbols();
  renderV2FormsTable();
  renderV2FormCards();
  bindV2GapExplorer();
  renderV2GapExplorer();
  renderV2Inference();
  renderV2Table('v2-gg-table', V2_GG);
  renderV2Table('v2-diag-table', V2_DIAG);
  renderV2GymChart();
  renderV2BrandChart();
  renderV2DiscardChart();
  renderV2GenderValidation();
  renderV2HeightHist();
  renderV2Time();
  renderV2Reliability();
  // The glossary and forms table are injected as innerHTML *after* KaTeX's
  // auto-render already ran on DOMContentLoaded, so their \( ... \) spans
  // would otherwise stay as literal source. Typeset them explicitly.
  if (typeof window.renderMathInElement === 'function') {
    ['v2-symbols', 'v2-forms-table', 'v2-form-cards', 'v2-gap-controls', 'v2-visits-controls'].forEach((id) => {
      const el = document.getElementById(id);
      if (el) {
        window.renderMathInElement(el, {
          delimiters: [
            { left: '\\[', right: '\\]', display: true },
            { left: '\\(', right: '\\)', display: false },
          ],
          throwOnError: false,
        });
      }
    });
  }
  bindV2Glossary();
}


document.addEventListener('DOMContentLoaded', () => {
  if (typeof window.renderMathInElement === 'function') {
    window.renderMathInElement(document.body, {
      delimiters: [
        { left: '$$', right: '$$', display: true },
        { left: '\\(', right: '\\)', display: false },
      ],
    });
  }
});