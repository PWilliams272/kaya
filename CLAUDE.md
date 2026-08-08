# kaya

Pulls and analyzes climbing data from the Kaya platform, and serves the interactive viewer at
`kaya.peterwilliams.dev`. Python 3.11, pandas/NumPy/SciPy, SQLAlchemy, boto3, FastAPI + Plotly.

This repo is the reference implementation for the workspace's viewer-app pattern — `garmin`,
`aws_monitor`, `evolution_sim`, and `project_registry` all copy its architecture. Changing the
viewer's structural conventions here has downstream consequences.

## Architecture

- `src/kaya/data_puller.py` — Kaya API boundary. Provider-specific request/auth logic stays here.
- `src/kaya/update_data_script.py` — pull orchestration; the Lambda updater's entrypoint.
- `src/kaya/s3_storage.py`, `data_access.py` — S3-backed raw/state storage and the read layer.
  Analytical data lives in S3, never in the website RDS.
- `src/kaya/analysis.py` — transformations over pulled data.
- `src/kaya/grading_model.py`, `grading_model_v2.py`, `marginal_v2.py` — Bayesian grade-consensus
  modelling. `v2` is current; `v1` is kept for comparison, not for new work. `grading_model_v2`
  is the PyMC build, `marginal_v2` an independent NumPy implementation of the same likelihood —
  `scripts/check_pymc_marginal.py` asserts they agree, and **any term added to one goes in both**.
- `src/kaya/advancement.py` — the climber-advancement offset: a known number of grades added to
  each ceiling for when that send happened, relative to the climber's own other sends. **Fixed,
  never fitted** — a free parameter has nothing to separate it from the gym corrections and
  absorbed 3.4× its true value in simulation. `runs/base_bouldering.pkl` must carry
  `max_send_date` for it to apply; `build_model_v2(advancement=True)` raises rather than
  silently doing nothing.
- `src/kaya/viewer_payloads.py`, `build_viewer_payloads.py` — precompute each page's full response
  JSON offline. The viewer reads these; it does not compute on request.
- `src/kaya/build_viewer_cache_lambda.py` — the `kaya-viewer-cache` Lambda (container image, daily
  EventBridge) that materializes the viewer's SQLite mirror and syncs it to S3.
- `src/kaya/viewer_app.py` — the FastAPI viewer. Renders `viewer_templates/base.html` through
  `Jinja2Templates` and serves `viewer_static/`. There is no `index.html` — the page is one
  template per tab (`viewer_templates/tabs/`), with the two explainer tabs split again by
  article section. Add a tab by adding a fragment and an `{% include %}`, not by growing a file.
  The `/api/*` routes are development-only; production registers `/`, `/prelim` and the static
  mounts, and nothing else.
- `src/kaya/viewer_copy.py`, `viewer_content/*.json` — author-editable page copy, added blocks,
  review notes and chart layout for `/prelim`, stored as git-tracked JSON and rendered
  server-side. An added block keeps its STRUCTURE in `_blocks.json` and its WORDS in
  `_copy.json` under the same id, so new and drafted blocks share one edit-and-sanitise path.
  Deleting a **drafted** block cannot be a removal — the template would put it back — so it is
  recorded as a hidden key in `_hidden.json`: dropped entirely in production, struck through with
  a restore control in development. Deleting an **added** block removes its record instead.
  `@claude ...@` written anywhere in the copy is an inline note to the agent: marked in
  development, **stripped in production**, and listed by `viewer_copy.inline_notes('prelim')` —
  read that at the start of a session on this page, alongside `prelim_notes.json`.
  **Every write path is development-only** (`POST /api/prelim-{copy,blocks,notes,layout,hidden}` on `dev_api`) because
  `kaya.peterwilliams.dev` is public and unauthenticated — a live page that can rewrite its own
  text is a defacement vector. Stored copy renders unescaped, so it goes through the allowlist
  sanitiser on the way in *and* on the way out; the files are meant to be hand-edited too.
  Notes never render in production. Chart heights and dragged label positions do.
- `src/kaya/viewer_static/js/` — the client, 28 **classic** scripts (not ES modules) sharing one
  global scope. **Load order in `base.html` is load-bearing** — `09-shell.js` boots at top level
  before the `v2/` files evaluate. Numeric prefixes encode that; don't reorder or drop one.
  `v2/17`–`21` belong to `/prelim` and are loaded by `prelim.html`, not `base.html`; `20`/`21`
  (edit and review mode) are behind `{% if editable %}` and never ship to production.
- `src/kaya/viewer_static/tokens.css` — design tokens. **Source of truth for the whole workspace**;
  other repos sync from here. Do not edit a downstream copy. See `scripts/sync-design-tokens.sh`
  in `system-overview`.

Runtime shape: two Lambdas (S3-backed updater with SQS per-gym fanout and a DLQ; plus the viewer
cache builder) feed S3; the viewer on the EC2 host reads precomputed output. The old VPC + RDS
Lambda design is retired — do not treat it as current state.

## Run / validate

```bash
source .venv/bin/activate && PYTHONPATH=src python -m kaya.update_data_script   # data pull
source .venv/bin/activate && python -m kaya.viewer_app                          # viewer locally
source .venv/bin/activate && python -m pytest                                   # tests
source .venv/bin/activate && python -m ruff check .                             # lint
source .venv/bin/activate && python -m mypy                                     # types
```

All three gates are green — keep them that way. Install them with `pip install -e ".[dev]"`.

Coverage is **thin and deliberately scoped**: `tests/` covers the viewer's request surface and the
payload builder's caching, i.e. the invariants that broke silently before (computing routes leaking
into production, a cache that never cached). There is no coverage of the data pull, the S3 layer, or
the model. For anything outside `tests/`, still verify by running the affected entrypoint.

`mypy` passes because `db_manager`, `grading_model_v2`, and `data_puller` are on an
`ignore_errors` list in `pyproject.toml` — they have real findings. Take a module off that list when
you annotate it; don't add new ones.

Dependencies are split: core is what the pull and the deployed viewer import. `pymc`/`arviz` are in
the `modelling` extra, `pygam` in `payloads`, `matplotlib` in `notebooks`, `psycopg2-binary` in
`rds`. `pip install -e ".[all]"` for a full local environment.

## Deploy

**`main` deploys nothing.** It runs CI and that is all. Shipping is a separate, deliberate act:
merge to `prod`, or dispatch the workflow at a chosen ref. This reconciles the repo with the
workspace-wide `prod`-deploy-only model, which it used to be the sole exception to (changed
2026-08-07).

| Workflow | Deploys | Trigger |
| --- | --- | --- |
| `ci.yml` | nothing — ruff, mypy, pytest | every branch except `prod`, and every PR |
| `deploy-lambda.yml` | updater Lambda | push to **`prod`**, or `workflow_dispatch` |
| `deploy-viewer-app.yml` | viewer to EC2 (systemd `kaya-viewer.service`, port `8010`) | push to **`prod`**, or `workflow_dispatch` |
| `deploy-viewer-cache-lambda.yml` | `kaya-viewer-cache` Lambda | `workflow_dispatch` only |

The two deploy workflows are **path-filtered**, so a `prod` push only ships the surface it
touched — a viewer copy change no longer redeploys the data pipeline. Both filters are
allowlists derived from each entrypoint's transitive imports, and an allowlist goes stale
silently (the deploy just stops firing). `tests/test_deploy_workflows.py` recomputes the import
closure from source and fails if a filter, or the Lambda's packaging list, no longer covers it.
**Add a module the handler imports, and that test tells you to list it — don't skip it.**

The Lambda zip ships only the handler's closure (~400KB), not the whole `src/kaya/` tree (6.7MB);
`viewer_static/` in particular has never been reachable from the updater.

## Rules

- `kaya.peterwilliams.dev` is **deliberately public, no auth gate**. An nginx `auth_request` gate was
  built, deployed, verified, then explicitly reversed on 2026-07-28 by the site owner. Do not
  reintroduce it, and do not reintroduce `SESSION_COOKIE_DOMAIN` — it caused a real login bug.
- Viewer routes read precomputed payloads only. Never fit a model or hit the Kaya API on request.
- Exploratory work stays in `notebooks/`/`figs/`. Only curated outputs cross to the website.
- Don't edit `data/`, `runs/`, or generated figures unless the task is explicitly about regenerating them.
- Preserve provenance: record whether a file is source, derived, or publishable output.
- Intermittent Kaya API failures send gyms to the DLQ — that's expected, not necessarily a bug.

## Where to look for more

- `README.md` — setup and env vars.
- `KAYA_HANDOFF.md` — repo-local handoff and history.
- `VIEWER_HANDOFF.md`, `VIEWER_PROJECT_STATUS.md`, `VIEWER_APP_DEPLOY_RUNBOOK.md` — viewer build/deploy.
- `KAYA_VIEWER_DESIGN_HANDOFF.md`, `viewer_static/design-system-reference.html` — design system.
- `BAYESIAN_GRADING_MODEL.md` — the grading model's methodology.
- `docs/run-plan.md` — which fits are queued, what question each one settles, and what is
  deliberately not being run. Decision-driven: nothing downstream of an open question runs.
- `docs/inference-toolkit.md` — portable reference on choosing and diagnosing a sampler:
  what R-hat and ESS measure, affine invariance, nested sampling, and which fix each
  diagnostic signature points at. Written to travel to another repo; keep it self-contained.
- `DATA_STORAGE_NOTES.md` — storage decisions; `system-overview/data-storage-migration.md` for the
  cross-repo picture.
