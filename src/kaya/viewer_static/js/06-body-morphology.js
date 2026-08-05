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

