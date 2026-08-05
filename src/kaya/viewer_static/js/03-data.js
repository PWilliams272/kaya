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

