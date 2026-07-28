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
    input.placeholder = state.selectedValues.length ? '' : placeholder;
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
      'Choose gyms'
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
  const total = rows.reduce((sum, row) => sum + (row.send_count || 0), 0);
  return rows.map((row) => ({ ...row, density: total ? row.send_count / total : 0 }));
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
  document.getElementById('local-db-path').textContent = summary.local_db_path || 'Unknown';
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
      fill: 'tozeroy',
      fillcolor: 'rgba(37,99,235,0.12)',
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
        title: 'All gyms sends',
      },
      yaxis2: {
        title: appState.filters.timeGymId ? `${gymName(appState.filters.timeGymId)} sends` : '',
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
        title: 'Share of sends',
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
        title: 'Share of sends',
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
  const audience = appState.filters.bodyActiveOnly ? 'Active users only' : 'All users';
  document.getElementById('body-morphology-note').textContent = `${audience} for the grade panels. Each pair is a scatter (exact bucket counts, plus a GAM-fit mean curve + 68% CI band per gender) next to a combined density heatmap (both genders, same plot) — click "Male"/"Female" in either panel's legend to isolate one. Height and ape-index histograms stay on the original all-users, male-users, female-users split, with height and ape values clipped to the notebook ranges and bubble diameter scaled by sqrt(users in each exact bucket).`;
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
  document.getElementById('segment-sample-count').textContent = formatNumber(payload.sample_size);
  document.getElementById('segment-note').textContent = `${payload.criteria_text || ''} Corner plots are shown on a log scale with notebook-style thresholds.`;

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
    margin: { l: 54, r: 16, t: 40, b: 48 },
    showlegend: true,
    legend: { ...chartLayout('').legend, y: 1.03 },
    grid: { rows: gridSize, columns: gridSize, pattern: 'independent' },
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
      x: medianStats.point,
      y: 1,
      yref: 'paper',
      yanchor: 'bottom',
      xanchor: 'center',
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
      ...chartLayout('Grade delta (comp - ref)'),
      margin: { ...chartLayout('Grade delta (comp - ref)').margin, t: 34 },
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
      annotations: medianAnnotations,
    },
    { responsive: true, displayModeBar: false }
  );
}

function renderGymComparisonFocusRow() {
  if (!appState.loaded.gymComparisonBase) {
    return;
  }
  const container = document.getElementById('gym-comparison-focus-row');
  const refGymId = appState.filters.compareRefGymId;
  const focusGymId = appState.filters.compareFocusGymId;

  if (!refGymId || !focusGymId || focusGymId === refGymId) {
    container.innerHTML = '<div class="comparison-chart-shell"><div class="comparison-chart-title">Choose a reference gym and a gym to compare against it.</div></div>';
    return;
  }

  const model = buildGymComparisonModel([focusGymId]);
  if (!model.pairs.length) {
    container.innerHTML = '<div class="comparison-chart-shell"><div class="comparison-chart-title">No overlapping users at the selected minimum days.</div></div>';
    return;
  }

  const pair = model.pairs[0];
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
      ...chartLayout(`Max grade at comparison gym - ${gymName(appState.filters.compareRefGymId)}`),
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
    },
    { responsive: true, displayModeBar: false }
  );
}

function renderGymComparisonNote() {
  const note = document.getElementById('gym-comparison-note');
  if (!note) {
    return;
  }
  note.textContent = 'Median and 10th-90th percentile lines show the spread of observed grade deltas, not a clean measurement of the '
    + 'true grading gap between gyms. A person\'s max logged grade at a gym is a noisy stand-in for their real ceiling there — it '
    + 'depends on how many times they climbed at that gym and how consistently they log sends, both of which vary by person and '
    + 'aren\'t accounted for here.';
}

function renderGymComparisonAll() {
  renderGymComparisonNote();
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

const TAB_NAMES = ['gym-comparison', 'body-morphology', 'user-segmentation', 'data-overview'];

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
  const savedTheme = localStorage.getItem('kaya-viewer-theme');
  if (savedTheme) {
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