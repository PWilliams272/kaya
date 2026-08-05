# Kaya Viewer — Project Status (as of 2026-07-28)

Written for other agents picking up work in this repo. Covers everything built
across the local viewer, its precompute pipeline, and its production Lambda —
from initial development through a live, working deployment. Where this
overlaps with other docs in the repo (`VIEWER_HANDOFF.md`,
`KAYA_VIEWER_DESIGN_HANDOFF.md`, `DATA_STORAGE_NOTES.md`,
`lambda_deployment/VIEWER_CACHE_RUNBOOK.md`), those stay the source of truth
for their specific topics (UI architecture, design tokens, storage strategy,
Lambda deploy commands, respectively) — this doc is the narrative connecting
all of it, plus what isn't written down elsewhere.

## What this is

An interactive dashboard for exploring Kaya climbing-gym data: send histories,
grade distributions, body-metric correlations (height/ape-index vs. grade),
and gym-vs-gym grading comparisons. FastAPI + vanilla JS frontend
(no framework, no build step, Plotly.js from CDN). See `VIEWER_HANDOFF.md`
for the detailed architecture/UX writeup — this section is just orientation.

Six tabs, of two kinds — the split matters, because they follow different
conventions and should not borrow each other's components:

| Kind | Tabs | Convention |
| --- | --- | --- |
| Dashboard — a tool for exploring data | Gym Comparison, Height and Wingspan, User Analysis, Data Overview | `viewer-app` skill; no article components |
| Explainer — an argument about a method | Grading Model, Grading Model v2 | `research-page` / `writeup` skills |

Where this doc says "the four tabs" below, it means the four dashboard tabs —
that narrative predates the two explainer tabs.

## 1. Viewer application work

- Extensive bug-fixing pass across all four dashboard tabs: chart colors, corner-plot
  rendering, grade-tick generation, height/ape-index filtering and
  formatting, caching-driven staleness, layout overflow, theme toggle
  styling, tab persistence across reloads.
- **Gym Comparison** redesigned: searchable single/multi-select "pill"
  pickers (`mountSearchableSingleSelect`/`mountSearchableMultiSelect` in
  `app.js`) replacing native `<select>`s with substring search. Naive
  statistics replaced with **Bayesian-bootstrap KDE** histograms and 2D
  density heatmaps, computed client-side — deliberately chosen over
  SEM-style intervals because SEM shrinks with sample size in a way that
  misrepresents genuine small-sample uncertainty as false precision. 68%
  interval width, matching one standard deviation under normality, chosen
  after explicit discussion of 68 vs. 80 vs. 95%.
- **Body Morphology** redesigned: scatter + combined (both-genders-overlaid,
  legend-toggleable) density heatmap per grade/height/ape-index combination,
  plus a **GAM-fitted trend curve** (`pygam`, `viewer_payloads.py`'s
  `_fit_gam_curve`/`_build_gam_curves`) using `gridsearch` over a log-spaced
  smoothing-penalty range specifically to avoid a handful of sparse/extreme
  points dominating the fit — this was caught and fixed after a real
  instance of one outlier fully controlling the curve's shape.
- Finalized a hardcoded color system (`#518AE6` male / `#F039F3` female,
  `genderBaseColors` in `app.js`), applied consistently across scatter,
  heatmap, and GAM-line colors via one shared derivation function
  (`buildGenderColorSet`) — this replaced an earlier in-app color-picker UI
  that was removed once the final colors were locked in.

## 2. Performance

- Root-caused and fixed a severe regression in Gym Comparison: an uncached,
  row-wise `.apply()` over the full 1.83M-row sends table. Fixed via
  instance-level caching (keyed on `KayaDataAccessor._local_db_version()`)
  plus vectorization (`np.select`/`.map()` instead of per-row Python).
  Verified byte-identical output vs. the old logic before/after; ~12s cold /
  0.24s warm afterward.

## 3. Precompute architecture

Rationale: daily data updates + a live production site shouldn't mean every
page view re-queries SQLite. Three of the four dashboard tabs need no live filtering at
all (checked directly against every `fetchViewerData` call site in
`app.js`); Gym Comparison's apparent "liveness" is entirely client-side JS
combining over one precomputed base dataset, not a server-side query per
selection.

- `ViewerPayloadBuilder.write_static_artifacts()` (already existed) writes
  all chart-ready JSON to `data/viewer_payloads/latest/` locally. Added
  missing `active_only=False` body-metrics variants so the Audience toggle
  works fully from static data (previously only `active_only=True` was
  precomputed).
- Added `upload_static_artifacts_to_s3()` + a `--upload-to-s3` flag on
  `build_viewer_payloads.py`, publishing to `kaya/viewer-cache/...json` in
  S3 (bucket `my-kaya-data-545009868532-us-east-2`).
- `index.html`'s `kaya-viewer-data-mode` meta tag now defaults to `static`
  (was `api`) — the whole app loads from precomputed JSON by default;
  `?dataMode=api` remains a live-debug escape hatch.

## 4. Design retheme

Driven by handoff docs from the sibling `aws_flask_site` agent (working on
the main site's redesign), reconciled across several rounds — the first two
handoff passes were prose-only and missed real details (shape/elevation,
then a background-color and sidebar-removal correction) before a complete
component reference (`src/kaya/viewer_static/design-system-reference.html`)
became the actual source of truth. Full detail in
`KAYA_VIEWER_DESIGN_HANDOFF.md`. Summary of what changed:

- `tokens.css` remapped to the site's canonical light-first blue/copper
  palette (`#1976d2` primary / `#b8752e` copper secondary), IBM Plex Sans
  embedded via `@font-face`, radius scale tightened (max 6px), shadows
  flattened, tabs switched from pill-shaped to underline-style.
- Persistent sidebar removed (page content moved into the topbar and Data
  Overview tab); a background/container color-inversion bug and several
  grey-should-be-blue interactive states (pills, focus rings, dropdown
  hover) fixed against the reference file.
- Fixed a hardcoded `data-theme="dark"` default that fought the site's
  Auto-by-default (`prefers-color-scheme`) behavior, plus the resulting
  wrong-icon-on-first-load bug in the theme toggle.

## 5. Production hardening (`viewer_app.py`)

Single env var, `KAYA_VIEWER_ENV=production`, gates three things that were
previously always-on dev conveniences:
- `reload=True` in `main()` (production is expected to invoke `uvicorn`
  directly via systemd anyway, bypassing `main()`, but it's safe either way)
- CORS: `allow_origins`/`allow_credentials` driven by
  `KAYA_VIEWER_ALLOWED_ORIGINS` (comma-separated) instead of a permanent
  wildcard
- The no-cache-everything middleware (a deliberate local-dev-loop hack,
  explicitly *not* meant to ship to production — see `VIEWER_HANDOFF.md`)

Verified via a side-by-side dev-mode vs. production-mode server comparison;
local dev behavior is unchanged when the env var is unset.

## 6. Data storage architecture

Triggered by a round of grounded diagnostic questions from another agent
about SQLite vs. Parquet/DuckDB. Answered with real measurements, not
guesses (file size 213MB→416MB→518MB over the session, zero secondary
indexes found, default non-WAL journal mode, 12s/0.24s cold/warm query
benchmarks). Decision: **hybrid, not a rewrite**.

- **Local SQLite hardening** (`db_manager.py`): `PRAGMA journal_mode=WAL` in
  `get_engine()` (was default `delete` mode, which blocks concurrent readers
  during a write); indexes on `sends(gym_id)` and `sends(date)` — the only
  two columns ever used in a SQL `WHERE` clause
  (`KayaDataAccessor._build_db_filters`). Deliberately did *not* index
  `grade` (never filtered in SQL — classification happens in pandas after
  the read) or `user_id` (only ever inside `COUNT(DISTINCT ...)`).
- **Curated Parquet layer**: satisfies an outstanding item in
  `DATA_STORAGE_NOTES.md` ("define and build the curated parquet layer").
  `KayaDataAccessor.write_curated_month_parquet(year_month)` rebuilds one
  month's partition from the local DB and publishes it to
  `kaya/curated/sends/year=YYYY/month=MM/data.parquet`. Backfilled all 67
  historical months via `src/kaya/backfill_curated_parquet.py` — 1,830,280
  rows, verified to exactly match the local table's row count; 61.1MB total
  vs. 518MB for the equivalent SQLite file (~8.5x smaller, Parquet's
  columnar compression).
- **Deliberately deferred**: replacing SQLite as the query engine with
  DuckDB-over-Parquet. The 213MB→416MB jump looks like it's mostly catching
  up on a one-time historical backfill, not confirmed organic growth — this
  would be real engineering work to solve a cost curve that isn't confirmed
  to bite yet. **Explicit trigger to revisit**: once there's a few weeks of
  post-backfill daily runs, check the actual growth rate and how close the
  Lambda's snapshot download/upload time is getting to its timeout. Local
  SQLite stays the query engine (upsert semantics, SQL-side filtering); the
  curated Parquet layer is for growth-safe S3 publishing and ad hoc DuckDB
  access from `notebooks/`, not a replacement for the query layer.

## 7. `kaya-viewer-cache` Lambda — live in production

A new, isolated Lambda on its own daily EventBridge schedule, deliberately
**separate from `kaya-data-updater`** (different failure mode, different
deploy path — matches this pipeline's existing preference for decomposition
over bundling unrelated concerns into one function).

**What it does each run** (`src/kaya/build_viewer_cache_lambda.py`):
1. Downloads the persisted full-history SQLite snapshot from
   `kaya/materialized/kaya_data.db`.
2. Incrementally syncs the last 3 `run_date=` partitions on top
   (`sync_latest_s3_to_local_db`) — buffer past "1 day" in case a run is
   missed; reports which `YYYY-MM` months the new rows actually fall into.
3. Rebuilds only the touched curated Parquet month-partition(s).
4. Rebuilds all viewer JSON artifacts and uploads them to
   `kaya/viewer-cache/...`.
5. Re-uploads the refreshed SQLite snapshot for next run.

**Why container image, not zip**: `pandas`/`numpy`/`scipy`/`pygam` are well
past the 250MB zip-deploy limit `kaya-data-updater`'s workflow has to stay
under. Separate lean requirements file
(`lambda_deployment/viewer_cache_requirements.txt`) — no `requests`/`tqdm`/
`psycopg2`/`matplotlib`/`fastapi`/`uvicorn`, none of which this batch job
needs.

**Live AWS resources** (all created, region `us-east-2`, account
`545009868532`):
- ECR repo: `kaya-viewer-cache`
- IAM role: `kaya-viewer-cache-lambda-role` (least-privilege: read
  `kaya/raw/*` + `kaya/state/*`; read+write `kaya/materialized/*` and
  `kaya/curated/sends/*`; write `kaya/viewer-cache/*`; basic Lambda
  execution for CloudWatch logs)
- Lambda function: `kaya-viewer-cache` (900s timeout — Lambda's hard
  ceiling; 3008MB memory; 3008MB ephemeral storage — the SQLite snapshot
  alone is ~520MB, well past the 512MB default)
- EventBridge rule: `kaya-viewer-cache-daily`, `cron(0 12 * * ? *)` (noon
  UTC) — 9 hours after `kaya-data-updater`'s `run-every-day` rule
  (`cron(0 3 * * ? *)`, confirmed via `aws events list-targets-by-rule`),
  since that rule only *dispatches* per-gym SQS jobs and actual ingestion
  completes asynchronously over an unmeasured window afterward
- GitHub Actions deploy: `.github/workflows/deploy-viewer-cache-lambda.yml`
  (manual `workflow_dispatch` only, builds/pushes/updates), wired to a
  `VIEWER_CACHE_LAMBDA_FUNCTION_NAME` repo variable and an extended IAM
  policy on the `kaya-app` user (identified via CloudTrail — GitHub never
  exposes secret values once set, even to repo admins). Confirmed working
  via a real successful `workflow_dispatch` run.

**Full runbook, including all `aws`/`docker` commands and IAM policy JSON,
already executed and recorded as the actual configuration**: see
`lambda_deployment/VIEWER_CACHE_RUNBOOK.md`.

### Bugs found only by actually deploying (all fixed, worth knowing about)

1. **SQLite version mismatch.** AWS's `public.ecr.aws/lambda/python:3.11`
   base image bundles `libsqlite3` 3.7.17 (2013) — `INSERT ... ON CONFLICT
   DO UPDATE` needs >= 3.24 (2018), and failed with `near "ON": syntax
   error` on a real invoke despite working fine on any local machine with a
   modern system SQLite. Fixed in `db_manager.py`'s SQLite upsert branch by
   switching to `INSERT OR REPLACE` (supported since essentially any SQLite
   version), equivalent here since every upserted record always carries the
   full row.
2. **`numpy` tried to build from source** inside the (compiler-less) base
   image. Root cause: an unpinned/loose `numpy` constraint let pip resolve a
   version with no prebuilt wheel for the exact environment. Fixed by
   pinning `numpy==1.26.4` in `viewer_cache_requirements.txt` and adding
   `--only-binary=numpy,scipy,pandas,pyarrow` to the Dockerfile's `pip
   install`, so any future mismatch like this fails fast and clearly.
3. **Docker Buildx attestation manifests break `CreateFunction`.** Modern
   Buildx attaches a provenance/SBOM attestation manifest by default;
   Lambda's `CreateFunction`/`UpdateFunctionCode` reject that image format
   outright. Fixed by always building with `--provenance=false --sbom=false`.
4. **Apple Silicon architecture mismatch.** `docker build` defaults to the
   host architecture (arm64 on Apple Silicon); Lambda here runs `x86_64`.
   Fixed by always building with `--platform linux/amd64`.

### Verified end-to-end (2026-07-28)

Manual invoke: 18,406 rows synced, July 2026 curated Parquet partition
rebuilt, all 461 viewer JSON files rebuilt and uploaded, SQLite snapshot
re-uploaded (523,243,520 bytes, up from the 518,217,728-byte bootstrap).
10m35s duration, 2,577MB memory used of 3,008MB configured — real but not
huge margin under the 900s ceiling; if a future run ever approaches that
limit, the fix has to be making the job faster, since 900s is Lambda's
maximum regardless of configuration.

A `workflow_dispatch` run through the actual GitHub Actions workflow also
succeeded (one harmless Node.js 20→24 deprecation warning, no functional
issue).

## 8. Git history note

The entire `feature/python-data-access-layer` branch — everything above —
had never been pushed to GitHub until this was discovered while trying to
trigger the CI workflow (`workflow_dispatch` requires the workflow file to
exist on the repo's default branch, which GitHub couldn't see it on). Pushed
the branch, then merged it into `main` (both with explicit owner
confirmation first, since pushing/merging affects shared state). `main` is
now at the merge commit containing all of this work.

## 9. Reusable UI pattern — the docked reference panel (added 2026-08-04)

**Moved.** The full spec now lives in `docs/docked-reference-panel.md`, as a
standalone, self-contained document — behaviour contract, style spec, markup,
CSS, JS, four traps, accessibility checklist, and the info-dot sibling pattern.
It carries the code inline so another repo can implement the pattern without
reading this codebase. That file is the one to hand to anyone reusing this;
keep it updated when the panel changes.

Short version: a panel fixed to the right gutter that stays in view while
scrolling, collapses to a slim vertical handle, remembers its state, and is
two-way hover-linked to the prose — hovering a referring element filters the
panel to the entries it uses and dims the rest; hovering an entry marks the
elements that use it; clicking a referring element toggles the panel open onto
its entries or shut again. Linking is by plain string keys (`data-syms` on the
element, `data-sym` on the row), so it generalises past equations to table
column dictionaries, chart legends, or ID→name lookups.

The two traps that dominate the work, and which recur elsewhere in this
codebase: (1) a CSS transition that starts while its container is
`display: none` sticks in `playState: "running"` and pins the property,
outranking even inline styles — gate transitions behind a class JS adds after
the resting position is set, with a `setTimeout` backstop because
`requestAnimationFrame` never fires in a backgrounded tab; (2) a fixed panel
must push content, not overlay it — `padding-right` on the pane so a
`margin: 0 auto` column re-centres. Both are written up properly in the doc.

Reference implementation: `src/kaya/viewer_static/` — `index.html`
(`<aside id="v2-glossary">`), `research.css`
(`/* ==== docked symbol glossary ==== */`), `app.js` (`V2_SYMBOLS`,
`setV2GlossaryOpen` / `highlightV2Symbols` / `bindV2Glossary` / `bindInfoDots`).

## What's next

- **Time is not in the grading model at all** (found 2026-08-04). Gym
  corrections correlate **+0.61** with when their climbers logged (+0.55
  within-brand, so it is not just Movement being later and stiffer). Measured
  climber advancement, de-biased for regression to the max, is only
  +0.06 to +0.17 grades/yr around V5&ndash;V6 where the median climber sits
  &mdash; an order of magnitude below the +0.68 grades/yr the correlation
  implies. So improvement explains a slice of it and something else explains
  the rest. Scripts: `scripts/gym_time_confound.py`,
  `scripts/climber_advancement.py`. Any drift term must be
  **grade-dependent**, not a constant. Prerequisite: `prepare_base_data`
  drops the send date from the observation table and needs to carry it.
- **A climber-advancement write-up** of its own is wanted eventually &mdash;
  how fast people move up, and how that slows with grade.
- **Reusing the docked panel** (section 9) — the owner has asked for similar
  implementations elsewhere; the two traps above are the whole difficulty.
- **Widen the gym network in the grading model** (owner request, 2026-08-04).
  Every fit so far runs on the `net50` network — 29 gyms, of which 17 are
  Touchstone, 6 Movement, 4 Bouldering Project and only 2 Stronghold. The
  scrape already covers 89 gyms (`src/kaya/config/gyms_to_update.json`:
  32 Movement, 18 Touchstone, 12 Bouldering Project, 11 Hangar, 6 Sender One,
  6 Mesa Rim, 2 Rockreation, 2 Stronghold) and `networks.json` already
  defines a looser `net20` with 68 of them. Wanted: more Stronghold, more
  Movement, more Bouldering Project, plus other chains, so per-company
  contrasts rest on more than two gyms in the thin cases. The cheap first
  move is refitting on `net20`; the expensive one is adding gyms to the
  scrape roster. Note both the shared-user linkage threshold and runtime
  scale with gym count.
- **Nothing urgent outstanding from the original plan** — precompute, S3
  publishing, production hardening, storage hardening, and the Lambda
  deployment are all done and verified live.
- **Worth checking**: the EventBridge rule's first fully *automatic*
  (non-manual) firing, to confirm it runs cleanly unattended.
- **Blocked, not on either agent**: systemd unit / DNS / nginx /
  `auth_request` / TLS for actually reverse-proxying this into the live
  website — needs real EC2 host access neither session has had. See the
  deployment section of `KAYA_VIEWER_DESIGN_HANDOFF.md` for the prepared
  checklist.
- **Deliberately deferred with a stated trigger**: the SQLite→DuckDB/Parquet
  query-engine question (see section 6 above).
- **Cosmetic, no urgency**: bump `actions/checkout` and
  `aws-actions/configure-aws-credentials` past their Node 20 deprecation
  warning.
