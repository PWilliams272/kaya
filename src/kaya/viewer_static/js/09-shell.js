function renderAll() {
  renderSummary();
  renderTimeSeries();
  renderGymCounts();
  renderGradeDistribution();
  renderGymComparisonAll();
  renderBodyMetrics();
  renderUserSegmentation();
}

const TAB_NAMES = ['gym-comparison', 'body-morphology', 'user-segmentation', 'data-overview',
  'grading-findings', 'grading-current', 'grading-model', 'grading-v2'];

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
    // The directory's model columns come from the explainer payload. Awaited
    // rather than fired and forgotten so the table is not drawn twice, and
    // tolerated when absent -- the gym list stands on its own without it.
    if (typeof loadV2Results === 'function') await loadV2Results();
    bindGymDirectory();
    renderGymDirectory();
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
  } else if (tabName === 'grading-findings') {
    // The presentation cut. Same payloads as the two pages below it, so
    // switching between any of them refetches nothing.
    renderFindingsTab();
  } else if (tabName === 'run-log') {
    // The lab notebook. Its own payload, shared with nothing, so it costs one
    // fetch the first time and nothing after.
    await renderV2Runlog();
  } else if (tabName === 'grading-current') {
    // The current write-up. Shares every payload with the archived v2 notes,
    // so switching between them refetches nothing.
    renderCurrentTab();
  } else if (tabName === 'grading-v2') {
    renderV2Tab();
  } else {
    await ensureGymComparisonData();
    renderGymComparisonAll();
  }

  // Last, and outside every branch: the contents rail is derived from the DOM
  // this pane just produced, so it has to be built after the pane is populated
  // rather than alongside it. Tabs that are not article pages produce an empty
  // tree and the rail hides itself.
  if (typeof renderPageNav === 'function') renderPageNav(tabName);
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

// Wait for every script in the document before starting.
//
// The bootstrap restores the last-used tab, which calls that tab's renderer --
// and the explainer renderers (`renderV2Tab`, `renderCurrentTab`) live in
// scripts that load AFTER this one. In the original single app.js they were
// function declarations in the same file, so hoisting made them visible here.
// Across separate classic scripts it does not, and restoring a saved explainer
// tab threw `renderCurrentTab is not defined` before the later scripts had been
// evaluated. DOMContentLoaded fires once they all have, which is exactly the
// guarantee the file split lost.
function startViewer() {
  bootstrapWithFallback().catch((error) => {
    window.__kayaViewerError = error?.message || String(error);
    window.__kayaViewerStatus = 'bootstrap-error';
    console.error(error);
    alert(`Failed to load Kaya viewer: ${error.message}`);
  });
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', startViewer, { once: true });
} else {
  startViewer();
}

