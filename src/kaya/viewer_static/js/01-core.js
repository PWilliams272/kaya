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

