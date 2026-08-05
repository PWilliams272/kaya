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

