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

