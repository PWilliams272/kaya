# Kaya Local Viewer — Architecture Notes For Website Integration

Context: this doc describes the local Kaya data viewer in this repo. It's being handed to whichever agent is modernizing the main Flask website, because the interactivity/snappiness/multi-page feel of this viewer is the reference the owner wants to bring into that site. The Kaya viewer itself will become one component/section of the larger site, not the whole thing.

## What This Is

A local-only interactive dashboard for exploring Kaya climbing gym data (send histories, grade distributions, body-metric correlations, gym-vs-gym grading comparisons). Currently runs standalone via `uvicorn kaya.viewer_app:app`, reading from a local SQLite mirror of the production data.

## Stack

- **Backend**: FastAPI + Uvicorn (Python 3.11+). Not Flask.
- **Frontend**: vanilla JavaScript — no framework (no React/Vue/etc), no build step, no npm/bundler. Plotly.js loaded from CDN for all charting.
- **Styling**: plain CSS with custom properties for theming (light/dark), no preprocessor, no Tailwind.
- **Data**: local SQLite via a shared `KayaDataAccessor` class (also used by the production data-pull pipeline), backed by S3 for the live/canonical history.

## File Layout

```
src/kaya/viewer_app.py          FastAPI app: routes, static mounts, middleware, Jinja2Templates
src/kaya/viewer_payloads.py     Builds chart-ready JSON payloads from the data layer (~660 lines)
src/kaya/viewer_templates/
  base.html                     Shell only: head, topbar, tab bar, includes, docked glossary (~53 lines)
  tabs/<tab>.html               One fragment per tab — the unit of navigation and of the payload
  tabs/grading-v2/<section>.html    The two explainer tabs split again by article section,
  tabs/grading-model/<section>.html since one tab's article is longer than all four dashboards
src/kaya/viewer_static/
  app.js                        All client logic: fetching, rendering, custom UI widgets (~6000 lines)
  app.css                       Component styles (~530 lines)
  research.css                  Article components for the explainer tabs (~1180 lines)
  tokens.css                    Design tokens (CSS custom properties) + light/dark theme
  v2_*.json                     Precomputed evidence for the Grading Model v2 article
```

There is no `index.html`. The page is rendered from `base.html` through
`Jinja2Templates`; a static copy alongside it would be a second thing to hand-edit.
`app.js` is still one file — splitting it into per-tab modules is the remaining
half of this work, and it carries real regression risk because the whole file
shares one flat scope.

`viewer_payloads.py` is framework-agnostic — it's a plain Python class that takes no FastAPI dependency, so it can be reused as-is behind a Flask view if you go that route.

## UX Patterns Worth Carrying Over

1. **Single-page app via tab toggling, not real routing.** Six tabs — four dashboards (Gym Comparison, Height and Wingspan, User Analysis, Data Overview) and two explainers (Grading Model, Grading Model v2) — each a `<section class="tab-pane">` assembled from its own template fragment. Switching tabs toggles a CSS class (`display:none` ↔ `flex`) and fetches/renders that tab's data — no page reload, feels instant. Tradeoff: no URL sync today (see Limits below).

2. **Design tokens for theming.** `tokens.css` defines `--lg-bg`, `--lg-text`, `--lg-card`, etc. on `:root`, overridden under `:root[data-theme='light']`. Switching theme is just flipping that attribute — every component restyles for free. Toggle is a compact 32px icon button (🌙/☀️) in the topbar, persisted to `localStorage`.

3. **Custom lightweight interactive widgets, no component library.** Two are worth studying directly if the new site wants similar polish:
   - A searchable single-select "combobox" (`mountSearchableSingleSelect` in app.js): click to open a dropdown, type to filter by substring *anywhere* in the label (native `<select>` typeahead only matches prefixes), click outside or Escape to close.
   - A multi-select "pill" picker (`mountSearchableMultiSelect`): same search/filter dropdown, but picks render as removable pill/tag chips instead of checkboxes; Backspace on an empty search field removes the last pill.
   Both share a small set of helpers (`comboFilteredOptions`, `renderComboOptionsList`, `bindComboOutsideClose`) — the "click-outside-to-close" and "mousedown+preventDefault so option clicks don't get eaten by input blur" tricks are the fiddly parts worth reusing verbatim.

4. **Plotly.js for all charts** — line, bar, histogram, scatter, heatmap. A few non-obvious techniques in there if the new site wants similar statistical polish:
   - Bayesian-bootstrapped kernel density estimates computed client-side in JS, layered over raw histograms, to show honest uncertainty on small samples without assuming a parametric (Gaussian) shape.
   - Hand-rolled "step histogram" outlines — Plotly's native histogram trace always draws all 4 sides of every bar, so a true unfilled step outline is built manually as a line trace with precomputed vertices.
   - A scatter-matrix "corner plot" built from Plotly's `grid` layout with per-cell axis domains, shared axes, and cross-cell reference lines/shapes.

5. **State lives in one `appState` object** (filters, loaded data, mounted widget handles), not scattered across the DOM. Only `theme` and the active tab persist across reloads (via `localStorage`); all other filter selections reset on refresh today.

6. **No-cache middleware** (`viewer_app.py`): every response gets `Cache-Control: no-store, no-cache, must-revalidate`. This was a deliberate fix for local dev — the browser was repeatedly serving stale JSON/JS during active iteration. **This should not carry over to production as-is** — it exists purely to avoid that specific local dev-loop pain and would hurt real-world performance on a public site.

7. **Responsive layout via flexbox shrink, not heavy media querying.** Multi-panel rows use `flex: 1 1 <basis>` with a `min-width`/`max-width` band so panels shrink together to fit before falling back to a single "stack" breakpoint, rather than wrapping awkwardly at arbitrary widths.

## Integration Options

The Kaya viewer runs as its own FastAPI process today, fully separate from the Flask site. To fold it in as one component of the larger site, roughly in order of effort:

- **(a) Reverse-proxy it.** Keep this as an independent FastAPI service, proxy a path or subdomain to it from the Flask app / web server config. Least code change, keeps the two stacks independent.
- **(b) Port the routes into Flask views.** `viewer_payloads.py` has no FastAPI dependency — it's just a plain class returning dicts/DataFrames-as-records — so the actual route handlers in `viewer_app.py` are the only FastAPI-specific code to reimplement (a handful of thin `@app.get(...)` functions). Static frontend files need no changes.
- **(c) Mount as an ASGI sub-app inside Flask.** More involved WSGI/ASGI bridging; probably not worth it unless there's a strong reason to keep one process.

The frontend (`app.js`/`app.css`/`tokens.css` plus the templates) has no framework lock-in — it only depends on relative `/viewer-data/...` fetch endpoints (and, in development only, `/api/...`). The Jinja templates are plain `{% include %}` composition with no FastAPI-specific tags, so they port to any Jinja-capable backend, and the static assets need no changes at all. Note the `/api/...` router is registered only when `KAYA_VIEWER_ENV` is not `production`: those endpoints query SQLite per request and fit GAMs on the fly, which is exactly what the deployed viewer must not do. In production the page runs in `static` data mode and reads `/viewer-data/*.json`. `tokens.css` in particular is a good candidate to extract as a shared design-token file across the whole site if consistent theming across components is a goal.

## Current Limits To Know About

- **No URL/routing sync.** Tab and filter state aren't reflected in the URL, so deep links, browser back/forward, and bookmarking don't work. Worth fixing if this becomes a real page on the site.
- **One large JS file.** `app.js` is ~2100 lines with some repetition between similar chart-rendering functions; fine for a local tool, would benefit from modularization before scaling further.
- **No auth, no deployment story.** Built purely for local exploration against a local SQLite file — nothing here handles multi-user access, auth, or a production data path.
