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
- `src/kaya/viewer_app.py` — the FastAPI viewer. Serves `viewer_static/`.
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
source .venv/bin/activate && python -m compileall src                           # syntax check
```

No `tests/` directory yet — adding one is tracked cleanup work, so prefer verifying changes by
running the affected entrypoint rather than assuming coverage exists.

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
