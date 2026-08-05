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
  modelling. `v2` is current; `v1` is kept for comparison, not for new work.
- `src/kaya/viewer_payloads.py`, `build_viewer_payloads.py` — precompute each page's full response
  JSON offline. The viewer reads these; it does not compute on request.
- `src/kaya/build_viewer_cache_lambda.py` — the `kaya-viewer-cache` Lambda (container image, daily
  EventBridge) that materializes the viewer's SQLite mirror and syncs it to S3.
- `src/kaya/viewer_app.py` — the FastAPI viewer. Renders `viewer_templates/base.html` through
  `Jinja2Templates` and serves `viewer_static/`. There is no `index.html` — the page is one
  template per tab (`viewer_templates/tabs/`), with the two explainer tabs split again by
  article section. Add a tab by adding a fragment and an `{% include %}`, not by growing a file.
  The `/api/*` routes are development-only; production registers only `/` and the static mounts.
- `src/kaya/viewer_static/js/` — the client, 16 **classic** scripts (not ES modules) sharing one
  global scope. **Load order in `base.html` is load-bearing** — `09-shell.js` boots at top level
  before the `v2/` files evaluate. Numeric prefixes encode that; don't reorder or drop one.
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

Three workflows, and their triggers differ — check before assuming a push is inert:

| Workflow | Deploys | Trigger |
| --- | --- | --- |
| `deploy-lambda.yml` | updater Lambda | push to `main` **or** `prod` |
| `deploy-viewer-app.yml` | viewer to EC2 (systemd `kaya-viewer.service`, port `8010`) | push to `main` |
| `deploy-viewer-cache-lambda.yml` | `kaya-viewer-cache` Lambda | `workflow_dispatch` only |

**A push to `main` deploys the viewer and the updater Lambda.** That diverges from the workspace's
`prod`-deploy-only branch model and is not yet reconciled — treat `main` as a deploy branch here
until it is.

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
- `DATA_STORAGE_NOTES.md` — storage decisions; `system-overview/data-storage-migration.md` for the
  cross-repo picture.
