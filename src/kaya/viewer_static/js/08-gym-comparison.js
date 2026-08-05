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

