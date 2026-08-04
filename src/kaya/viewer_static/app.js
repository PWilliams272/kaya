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
  // effect for male-coded climbers (gamma1, gamma2 both include zero); a
  // real quadratic effect for female-coded climbers, but its vertex falls
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
    name: state.genderCode === 1 ? 'Female-coded' : 'Male-coded',
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
    bindV2Controls();
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
const V2_GYMS = [
  {i:'279',g:'Touchstone Sacramento Pipeworks',b:'Touchstone',m:-0.791,lo:-0.879,hi:-0.686,s:true},
  {i:'285',g:'Touchstone The Studio',b:'Touchstone',m:-0.422,lo:-0.529,hi:-0.322,s:true},
  {i:'270',g:'Touchstone Great Western Power Company',b:'Touchstone',m:-0.327,lo:-0.422,hi:-0.225,s:true},
  {i:'944',g:'Bouldering Project - Salt Lake',b:'Bouldering Project',m:-0.277,lo:-0.411,hi:-0.130,s:true},
  {i:'292',g:'Touchstone Team Training Center',b:'Touchstone',m:-0.269,lo:-0.394,hi:-0.106,s:true},
  {i:'38',g:'Movement San Francisco',b:'Movement',m:-0.234,lo:-0.302,hi:-0.169,s:true},
  {i:'1100',g:'Touchstone Class 5',b:'Touchstone',m:-0.196,lo:-0.269,hi:-0.116,s:true},
  {i:'261',g:'Touchstone Verdigo Boulders',b:'Touchstone',m:-0.152,lo:-0.212,hi:-0.094,s:true},
  {i:'277',g:'Touchstone Diablo Rock Gym',b:'Touchstone',m:-0.117,lo:-0.195,hi:-0.032,s:true},
  {i:'1049',g:'Touchstone The Oaks',b:'Touchstone',m:-0.103,lo:-0.176,hi:-0.041,s:true},
  {i:'901',g:'Touchstone The Post',b:'Touchstone',m:-0.032,lo:-0.098,hi:0.034,s:false},
  {i:'257',g:'Touchstone LA Boulders',b:'Touchstone',m:-0.009,lo:-0.067,hi:0.051,s:false},
  {i:'1178',g:'Touchstone Hyperion',b:'Touchstone',m:0.016,lo:-0.034,hi:0.076,s:false},
  {i:'67',g:'Touchstone Mission Cliffs',b:'Touchstone',m:0.018,lo:-0.054,hi:0.093,s:false},
  {i:'122',g:'Touchstone Hollywood Boulders',b:'Touchstone',m:0.020,lo:-0.040,hi:0.078,s:false},
  {i:'293',g:'Touchstone Berkeley Ironworks',b:'Touchstone',m:0.020,lo:-0.047,hi:0.089,s:false},
  {i:'381',g:'Bouldering Project - Fremont/Upper Walls',b:'Bouldering Project',m:0.022,lo:-0.058,hi:0.112,s:false},
  {i:'260',g:'Touchstone Cliffs of Id',b:'Touchstone',m:0.058,lo:-0.005,hi:0.115,s:false},
  {i:'1102',g:'The Stronghold Lincoln Heights',b:'Stronghold',m:0.106,lo:-0.025,hi:0.233,s:false},
  {i:'1140',g:'Bouldering Project - University District',b:'Bouldering Project',m:0.112,lo:0.040,hi:0.198,s:true},
  {i:'413',g:'Touchstone Pacific Pipe',b:'Touchstone',m:0.135,lo:0.092,hi:0.182,s:true},
  {i:'1101',g:'The Stronghold Echo Park',b:'Stronghold',m:0.144,lo:0.024,hi:0.269,s:true},
  {i:'10',g:'Bouldering Project - Poplar',b:'Bouldering Project',m:0.167,lo:0.080,hi:0.240,s:true},
  {i:'104',g:'Movement Fountain Valley',b:'Movement',m:0.249,lo:0.154,hi:0.330,s:true},
  {i:'100',g:'Movement Sunnyvale',b:'Movement',m:0.280,lo:0.222,hi:0.335,s:true},
  {i:'51',g:'Touchstone Dogpatch Boulders',b:'Touchstone',m:0.339,lo:0.288,hi:0.393,s:true},
  {i:'1183',g:'Movement Mountain View',b:'Movement',m:0.379,lo:0.335,hi:0.426,s:true},
  {i:'103',g:'Movement Santa Clara',b:'Movement',m:0.397,lo:0.356,hi:0.442,s:true},
  {i:'49',g:'Movement Belmont',b:'Movement',m:0.465,lo:0.391,hi:0.530,s:true},
];

const V2_STATS = [
  {v:'29', l:'gyms', s:'4 companies, one connected network'},
  {v:'1.26', l:'grades', s:'stiffest minus softest'},
  {v:'20/29', l:'credibly differ', s:'89% interval excludes zero'},
  {v:'4.9&times;', l:'wider than v1', s:'vs the 6 Touchstone gyms alone'},
  {v:'0.282', l:'&sigma;<sub>gym</sub>', s:'was 0.128 in v1'},
  {v:'20,014', l:'observations', s:'10,357 climbers'},
];

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
  'Touchstone': '--lg-gold',            // blue, the house accent (17 of 29 gyms)
  'Movement': '--lg-highlight',         // copper
  'Bouldering Project': '--lg-success',  // green
  'Stronghold': '--lg-warning',          // ochre
};

function v2Colour(row, mode) {
  if (mode === 'brand') return cssVar(V2_BRAND_COLOURS[row.b] || '--lg-text-2') || '#888';
  if (mode === 'sig') return row.s ? cssVar('--lg-gold') : cssVar('--lg-text-2');
  return row.m >= 0 ? cssVar('--lg-highlight') : cssVar('--lg-gold');
}

function renderV2Stats() {
  const host = document.getElementById('v2-stats');
  if (!host) return;
  host.innerHTML = V2_STATS.map((s) => `
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
  const mode = document.getElementById('v2-colour-by')?.value || 'brand';
  const brand = document.getElementById('v2-filter-brand')?.value || '';
  const onlySig = document.getElementById('v2-only-sig')?.checked;
  let rows = V2_GYMS.filter((r) => (!brand || r.b === brand) && (!onlySig || r.s));
  const trace = {
    type: 'scatter', mode: 'markers', orientation: 'h',
    x: rows.map((r) => r.m),
    y: rows.map((r) => r.g),
    error_x: {
      type: 'data', symmetric: false,
      array: rows.map((r) => r.hi - r.m),
      arrayminus: rows.map((r) => r.m - r.lo),
      color: cssVar('--lg-text-2'), thickness: 1.6, width: 3, opacity: 0.75,
    },
    marker: {
      size: 11,
      color: rows.map((r) => (r.s ? v2Colour(r, mode) : cssVar('--lg-card'))),
      line: { width: 2, color: rows.map((r) => v2Colour(r, mode)) },
    },
    hovertemplate: '<b>%{y}</b><br>correction %{x:+.3f} grades<extra></extra>',
  };
  const layout = chartLayout('grading correction (grades) — negative = softer, positive = stiffer');
  layout.height = Math.max(360, rows.length * 26 + 90);
  layout.margin = { l: 260, r: 30, t: 14, b: 52 };
  layout.shapes = [{
    type: 'line', x0: 0, x1: 0, y0: -0.5, y1: rows.length - 0.5,
    line: { color: cssVar('--lg-text-2'), width: 1, dash: 'dot' },
  }];
  layout.yaxis = { ...layout.yaxis, automargin: true, tickfont: { size: 11 } };
  Plotly.react(el, [trace], layout, { displayModeBar: false, responsive: true });
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
  ['\\(u\\)', 'a climber', '&mdash;'],
  ['\\(g\\)', 'a gym', '&mdash;'],
  ['group', 'Ability'],
  ['\\(\\beta_0\\)', 'baseline ability &mdash; the grade an average climber sends at an average gym', 'grades'],
  ['\\(\\sigma_{\\text{user}}\\)', 'spread of natural ability between climbers', 'grades'],
  ['\\(\\tilde\\epsilon_u\\)', 'climber \\(u\\)&rsquo;s personal ability offset, standardised', 'SDs'],
  ['\\(\\mathbf{x}_u\\)', 'covariate row for climber \\(u\\): gender, height terms, ape terms, missingness flags', '&mdash;'],
  ['group', 'Body and gender'],
  ['\\(\\tilde h\\)', 'height, centred at the median and divided by its SD', 'SDs (1 SD &asymp; 3.4 in)'],
  ['\\(\\tilde a\\)', 'ape index (wingspan &minus; height), centred and scaled the same way', 'SDs (1 SD &asymp; 2.6 in)'],
  ['\\(G\\)', 'probability the climber is female, from their first name sharpened by height. 0 = male, 1 = female', 'probability'],
  ['\\(\\gamma_1^{M},\\ \\gamma_2^{M}\\)', 'slope and curvature of the <b>male-coded</b> height curve. Identical to \\(\\gamma_1,\\gamma_2\\) &mdash; the male curve <i>is</i> the baseline', 'grades / SD, grades / SD²'],
  ['\\(\\gamma_1^{F},\\ \\gamma_2^{F}\\)', 'slope and curvature of the <b>female-coded</b> height curve. Not sampled directly; equals \\(\\gamma_k+\\gamma_k^{\\times}\\)', 'grades / SD, grades / SD²'],
  ['\\(\\gamma_1,\\ \\gamma_2\\)', 'what the sampler actually fits: the baseline (male-coded) slope and curvature', 'grades / SD, grades / SD²'],
  ['\\(\\gamma_1^{\\times},\\ \\gamma_2^{\\times}\\)', 'the <b>gender difference</b> &mdash; how far the female curve departs from the male one. <b>This is the parameter the gender question turns on</b>; if its interval covers zero, the two curves are the same shape', 'grades / SD, grades / SD²'],
  ['\\(\\delta_1,\\ \\delta_2\\)', 'the same pair for ape index: linear slope and curvature', 'grades / SD'],
  ['\\(A,\\ h_0,\\ s\\)', 'saturating form only: how much reach is worth in total, the height it stops paying off at, and how sharply it levels', 'grades, SDs, SDs'],
  ['\\(\\kappa_h\\)', 'vertex form only: how sharply ability falls away from the best height. Larger = a tighter optimum. <b>Distinct from the gap-rate \\(\\kappa\\) below</b>', 'grades / SD²'],
  ['\\(p\\)', 'vertex form only: <b>the best height</b>, estimated directly rather than derived from \\(-\\gamma_1/2\\gamma_2\\)', 'SDs from median'],
  ['group', 'Gyms'],
  ['\\(\\sigma_{\\text{gym}}\\)', 'spread of grading style across gyms &mdash; the headline &ldquo;how much do gyms differ&rdquo; number', 'grades'],
  ['\\(\\tilde\\delta_g\\)', 'gym \\(g\\)&rsquo;s standardised offset, constrained so all gyms sum to zero', 'SDs'],
  ['\\(\\text{gym}_g\\)', 'gym \\(g\\)&rsquo;s grading correction. Positive = stiffer than average', 'grades'],
  ['\\(C_{u,g}\\)', 'structural ceiling &mdash; the hardest grade climber \\(u\\) could send at gym \\(g\\)', 'grades'],
  ['group', 'The gap, and what we observe'],
  ['\\(m_{u,g}\\)', '<b>the observed data</b>: the hardest grade \\(u\\) actually logged at \\(g\\)', 'grades'],
  ['\\(\\text{gap}_{u,g}\\)', 'how far below their ceiling that logged max sits. Never negative', 'grades'],
  ['\\(\\lambda_{u,g}\\)', 'rate of that gap &mdash; higher rate means a smaller expected gap', '1 / grades'],
  ['\\(\\lambda_0\\)', 'baseline gap rate for a typical climber at a typical gym', '1 / grades'],
  ['\\(\\tilde n_{u,g}\\)', 'how many days \\(u\\) logged at \\(g\\), centred on the median (~8)', 'ratio'],
  ['\\(\\kappa\\)', 'how much more of their ceiling a climber finds per extra visit', '&mdash;'],
  ['\\(\\tilde r_u\\)', 'sends per session, centred &mdash; a proxy for how completely someone logs', 'ratio'],
  ['\\(\\rho\\)', 'how much that logging-completeness shifts the gap. <b>Fitted at &asymp;0</b>', '&mdash;'],
  ['\\(\\sigma_{\\text{link}}\\)', 'residual noise: grades are integers on a continuous scale, so a labelled V5 is really 4.5&ndash;5.5', 'grades'],
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

// Illustrative coefficients -- chosen to make each shape legible, NOT fitted.
const V2_FORM_SHAPES = {
  zero: () => 0,
  linear: (z) => 0.18 * z,
  quadratic: (z) => 0.10 * z - 0.16 * z * z,
  saturating: (z) => 0.55 / (1 + Math.exp(-(z + 0.4) / 0.55)) - 0.28,
  vertex_quadratic: (z) => -0.035 * (z - 0.9) * (z - 0.9) + 0.28,   // peak at ~71 in, deliberately above the median
};
const V2_HMED = 68.0, V2_HSD = 3.4;   // inches, from the cleaned data

function renderV2Symbols() {
  const el = document.getElementById('v2-symbols');
  if (!el) return;
  let html = '<thead><tr><th>symbol</th><th>meaning</th><th>units</th></tr></thead><tbody>';
  V2_SYMBOLS.forEach((r) => {
    if (r[0] === 'group') {
      html += `<tr class="sym-group"><td colspan="3">${r[1]}</td></tr>`;
    } else {
      html += `<tr><td class="sym">${r[0]}</td><td>${r[1]}</td><td class="unit">${r[2]}</td></tr>`;
    }
  });
  el.innerHTML = html + '</tbody>';
}

function renderV2FormsTable() {
  const el = document.getElementById('v2-forms-table');
  if (!el) return;
  const [h, ...body] = V2_FORMS;
  el.innerHTML = `<thead><tr>${h.map((x) => `<th>${x}</th>`).join('')}</tr></thead><tbody>`
    + body.map((r) => `<tr><td><b>${r[0]}</b></td><td class="sym">${r[1]}</td><td>${r[2]}</td><td class="unit">${r[3]}</td></tr>`).join('')
    + '</tbody>';
}

function renderV2FormsChart() {
  const el = document.getElementById('v2-forms-chart');
  if (!el || typeof Plotly === 'undefined') return;
  const pick = document.getElementById('v2-form-pick')?.value || 'all';
  const showDist = document.getElementById('v2-form-dist')?.checked;
  const inches = [];
  for (let h = 58; h <= 78; h += 0.25) inches.push(h);
  const z = inches.map((h) => (h - V2_HMED) / V2_HSD);

  const defs = [
    ['zero', 'Zero', '--lg-text-2', 'dot'],
    ['linear', 'Linear', '--lg-info', 'solid'],
    ['quadratic', 'Quadratic', '--lg-success', 'solid'],
    ['saturating', 'Saturating', '--lg-danger', 'solid'],
    ['vertex_quadratic', 'Vertex quadratic', '--lg-warning', 'dashdot'],
  ];
  const traces = [];
  defs.forEach(([key, label, tok, dash]) => {
    if (pick !== 'all' && pick !== key) return;
    traces.push({
      type: 'scatter', mode: 'lines', name: label,
      x: inches, y: z.map(V2_FORM_SHAPES[key]),
      line: { color: cssVar(tok), width: 2.5, dash },
      hovertemplate: `${label}<br>%{x:.0f} in → %{y:+.2f} grades<extra></extra>`,
    });
  });
  if (pick === 'all' || pick === 'quadratic_x_gender') {
    // Two curves, because that is exactly what this form asserts.
    traces.push({
      type: 'scatter', mode: 'lines', name: 'Quad × gender (male-coded)',
      x: inches, y: z.map((v) => 0.18 * v - 0.09 * v * v),
      line: { color: cssVar('--lg-highlight'), width: 2.5 },
      hovertemplate: 'male-coded<br>%{x:.0f} in → %{y:+.2f}<extra></extra>',
    });
    traces.push({
      type: 'scatter', mode: 'lines', name: 'Quad × gender (female-coded)',
      x: inches, y: z.map((v) => (0.18 - 0.34) * v + (-0.09 + 0.02) * v * v),
      line: { color: cssVar('--lg-highlight'), width: 2.5, dash: 'dash' },
      hovertemplate: 'female-coded<br>%{x:.0f} in → %{y:+.2f}<extra></extra>',
    });
  }

  const layout = chartLayout('height (inches)');
  layout.height = 380;
  layout.yaxis = { ...layout.yaxis, title: { text: 'ability offset (grades)' }, zeroline: true };
  layout.legend = { ...layout.legend, orientation: 'h', y: -0.42, x: 0, font: { size: 11 } };
  layout.margin = { l: 62, r: 24, t: 26, b: 130 };
  layout.height = 430;
  layout.shapes = [];
  layout.annotations = [];
  if (showDist) {
    // Where each group actually sits -- median +/- 1 SD from the cleaned data.
    layout.shapes.push(
      { type: 'rect', xref: 'x', yref: 'paper', x0: 69.2 - 3.34, x1: 69.2 + 3.34,
        y0: 0, y1: 1, fillcolor: cssVar('--lg-gold'), opacity: 0.07, line: { width: 0 } },
      { type: 'rect', xref: 'x', yref: 'paper', x0: 64.2 - 2.83, x1: 64.2 + 2.83,
        y0: 0, y1: 1, fillcolor: cssVar('--lg-highlight'), opacity: 0.07, line: { width: 0 } },
    );
    layout.annotations.push(
      { x: 69.2, y: 1, yref: 'paper', yanchor: 'bottom', showarrow: false,
        text: 'male-coded ±1 SD', font: { size: 10, color: cssVar('--lg-gold') } },
      { x: 64.2, y: 1, yref: 'paper', yanchor: 'bottom', showarrow: false,
        text: 'female-coded ±1 SD', font: { size: 10, color: cssVar('--lg-highlight') } },
    );
  }
  Plotly.react(el, traces, layout, { displayModeBar: false, responsive: true });
}

let v2Bound = false;
function bindV2Controls() {
  if (v2Bound) return;
  const sel = document.getElementById('v2-filter-brand');
  if (sel && sel.options.length <= 1) {
    [...new Set(V2_GYMS.map((r) => r.b))].sort().forEach((b) => {
      const o = document.createElement('option');
      o.value = b; o.textContent = b; sel.appendChild(o);
    });
  }
  ['v2-colour-by', 'v2-filter-brand', 'v2-only-sig'].forEach((id) => {
    document.getElementById(id)?.addEventListener('change', renderV2GymChart);
  });
  ['v2-form-pick', 'v2-form-dist'].forEach((id) => {
    document.getElementById(id)?.addEventListener('change', renderV2FormsChart);
  });
  v2Bound = true;
}

function renderV2Tab() {
  renderV2Stats();
  renderV2Decisions();
  renderV2Symbols();
  renderV2FormsTable();
  renderV2FormsChart();
  renderV2Table('v2-gg-table', V2_GG);
  renderV2Table('v2-diag-table', V2_DIAG);
  renderV2GymChart();
  renderV2BrandChart();
  renderV2DiscardChart();
  renderV2GenderValidation();
  renderV2HeightHist();
  // The glossary and forms table are injected as innerHTML *after* KaTeX's
  // auto-render already ran on DOMContentLoaded, so their \( ... \) spans
  // would otherwise stay as literal source. Typeset them explicitly.
  if (typeof window.renderMathInElement === 'function') {
    ['v2-symbols', 'v2-forms-table'].forEach((id) => {
      const el = document.getElementById(id);
      if (el) {
        window.renderMathInElement(el, {
          delimiters: [{ left: '\\(', right: '\\)', display: false }],
          throwOnError: false,
        });
      }
    });
  }
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