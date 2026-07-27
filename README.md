# Kaya

Kaya owns the Kaya climbing data pull and transformation pipeline. The live updater now runs as an S3-backed, SQS-fanout Lambda workflow rather than the older VPC-plus-RDS design.

## Branching Model

- `prod`: protected production branch. Treat this as the exact release line for the live production Lambda.
- `main`: protected integration branch. Completed work lands here first and can deploy to a dev environment once the dev AWS resources exist.
- `feature/*`: short-lived branches off `main` for normal development.
- `hotfix/*`: short-lived branches off `prod` for urgent production repairs.

The current long-lived `dev` branch is behind `main` and should not remain the primary development branch. Prefer `main` plus short-lived feature branches.

## Promotion Flow

1. Branch from `main` into `feature/*`.
2. Open a PR back into `main`.
3. Let `main` deploy to the dev Lambda environment.
4. Validate there.
5. Promote the same commit into `prod`.
6. Let `prod` deploy to production.
7. Tag the production release.

## Deployment Workflow

The GitHub Actions workflow in `.github/workflows/deploy-lambda.yml` supports two environments:

- pushes to `prod` deploy to production
- pushes to `main` deploy to dev when a dev Lambda function name has been configured
- manual `workflow_dispatch` runs can target either environment

Use GitHub Environments named `dev` and `prod` with environment-scoped variables:

- `LAMBDA_FUNCTION_NAME`
- `AWS_REGION` (optional; defaults to `us-east-2`)

Production can keep using `kaya-data-updater`. Dev should point at a separate function such as `kaya-data-updater-dev`.

## Recommended AWS Environment Split

For reliable testing, maintain separate dev and prod resources for the operational path:

- Lambda function
- SQS main queue
- SQS DLQ
- EventBridge schedule or manual trigger path
- S3 raw/state storage
- Secrets or other environment configuration

Separate buckets are preferable. Separate top-level prefixes are acceptable if you want to keep the setup lighter at first.

## Current Gap

Only the production Lambda currently exists in AWS. Before relying on `main` deployments, create the dev environment resources and set the `dev` GitHub Environment variables accordingly.

## Validation

Local validation:

```bash
source .venv/bin/activate && python -m compileall src
```

Primary local run path for the updater:

```bash
source .venv/bin/activate && PYTHONPATH=src python -m kaya.update_data_script
```

## Current AWS Runtime

The production runtime is currently a single Lambda function plus queueing and S3 storage:

- Lambda function: `kaya-data-updater`
- EventBridge rule: `run-every-day`
- Main queue: `kaya-gym-update-jobs`
- DLQ: `kaya-gym-update-jobs-dlq`
- Primary bucket: `my-kaya-data-545009868532-us-east-2`
- Primary prefix: `kaya`
- Region: `us-east-2`

Important current-state notes:

- the live Lambda is no longer VPC-attached
- the live write path is S3 plus state files, not the old shared RDS path
- the queue event source mapping runs with batch size `1` and limited concurrency
- production is the only verified deployed environment today; the workflow supports `dev`, but the separate dev AWS resources still need to exist before relying on that path

The GitHub deployment workflow at `.github/workflows/deploy-lambda.yml` packages `src/kaya/` and deploys `kaya/update_data_script.lambda_handler`.

## Lambda Entry Modes

`src/kaya/update_data_script.py` supports three practical entry modes:

1. Direct update mode.
	Pass a `gym_id` or `gym_ids` payload and the Lambda updates those gyms immediately.
2. Dispatch mode.
	Pass `dispatch=true` and the Lambda enqueues one SQS message per target gym.
3. Worker mode.
	When invoked by SQS with `Records`, the Lambda processes queued jobs one at a time.

The steady-state production flow is:

1. EventBridge invokes the Lambda with `dispatch=true`.
2. The Lambda enqueues one job per configured gym.
3. SQS re-invokes the same Lambda in worker mode.
4. Successful jobs write raw send batches to S3 and update the per-gym state file.
5. Terminal per-gym failures are sent to the DLQ and do not block the next day’s run.

Updater behavior worth knowing:

- `mode='incremental'` stops when it hits previously seen sends for that gym
- `mode='full'` is for complete pulls or backfill-style refreshes
- `storage_backend` can be `auto`, `db`, or `s3`
- `max_offset_retries` now defaults to `3`
- DLQ handling is application-driven first: worker failures are explicitly forwarded to `KAYA_SQS_DLQ_URL`, and the queue redrive policy is only the infra safety net

Configured gyms come from `src/kaya/config/gyms_to_update.json` through `load_gyms_config()` in `src/kaya/gym_config.py`.

## Python Data Access

The repo now includes a unified Python accessor for the main current storage surfaces:

- local SQLite `sends`
- legacy AWS DB `sends`
- S3 raw send batches
- S3 per-gym state files

Example:

```python
from kaya import KayaDataAccessor

accessor = KayaDataAccessor()

state_df = accessor.read_state()
recent_raw_df = accessor.read_sends(
	source='s3_raw',
	gym_ids=[103],
	run_dates=['2026-07-26'],
	max_objects=5,
)
sync_result = accessor.sync_s3_sends_to_local_db(
	gym_ids=[103],
	max_objects=5,
)
```

Use `source='local_db'`, `source='aws_db'`, `source='s3_raw'`, or `source='s3_backfill'` explicitly when you want full control.

The most important public surface is:

```python
from kaya import KayaDataAccessor
```

Common `KayaDataAccessor` entrypoints:

- `read_sends()` reads raw send rows from local SQLite, the legacy AWS DB, live S3 raw partitions, or the historical S3 backfill
- `summarize_sends()` returns total sends, unique users, unique gyms, and date bounds
- `list_gyms()` returns gym metadata and send counts
- `sends_time_series()` builds daily, weekly, or monthly counts
- `grade_distribution()` returns chart-ready grouped grade counts
- `read_state()` reads the per-gym S3 frontier files
- `read_user_profiles()` builds cached user-level profiles for viewer and analysis work
- `list_raw_objects()` lists matching raw S3 objects before reading or syncing them
- `sync_s3_sends_to_local_db()` upserts selected S3 raw objects into local SQLite
- `sync_latest_s3_to_local_db()` syncs the newest live run-date partitions into local SQLite

Important source semantics:

- `local_db`: local SQLite history at `~/.kaya/kaya_data.db`
- `aws_db`: legacy shared AWS database path, still readable if `AWS_DB_URL` exists, but no longer the preferred write target
- `s3_raw`: live raw incremental S3 partitions under `kaya/raw/sends/run_date=.../gym_id=.../run_id=.../`
- `s3_backfill`: historical export path under `kaya/raw/sends/source=rds-backfill/`

The local SQLite path is:

```text
~/.kaya/kaya_data.db
```

Inside Lambda, the temporary local SQLite path resolves under:

```text
/tmp/.kaya/kaya_data.db
```

## S3 Layout And State Files

The current raw incremental path looks like:

```text
kaya/raw/sends/run_date=YYYY-MM-DD/gym_id=<gym_id>/run_id=<run_id>/batch-00000.jsonl.gz
```

Historical RDS backfill lives under:

```text
kaya/raw/sends/source=rds-backfill/
```

Per-gym frontier state lives under:

```text
kaya/state/gym_id=<gym_id>.json
```

Those state files currently include:

- `schema_version`
- `gym_id`
- `run_id`
- `last_successful_run_at`
- `total_written`
- `recent_send_ids`

`recent_send_ids` is the incremental frontier used to decide when an incremental pull has reached previously seen data for a gym. It is not a full history table.

Additional helpers now available:

- `summarize_sends()` for KPI-style counts and date bounds
- `list_gyms()` for gym lists and send counts
- `sends_time_series()` for daily, weekly, or monthly activity
- `grade_distribution()` for chart-ready grade counts
- `read_user_profiles()` for cached user-level body metrics and usage summaries
- `sync_latest_s3_to_local_db()` for pulling the newest S3 run partitions into local SQLite
- `python -m kaya.build_viewer_payloads` for materializing chart-ready viewer JSON artifacts

## Local Sync

Example sync commands:

```bash
source .venv/bin/activate && PYTHONPATH=src python -m kaya.sync_local_data --latest-run-dates 1
```

```bash
source .venv/bin/activate && PYTHONPATH=src python -m kaya.sync_local_data --run-date 2026-07-26 --gym-id 103
```

The sync CLI now defaults to the Kaya project S3 bucket, the `kaya/` prefix, the `admin` AWS profile, and `us-east-2` locally, so you do not need to export `KAYA_S3_BUCKET` just to run the common local sync path. Use `--bucket`, `--prefix`, `--aws-profile`, or `--aws-region` if you want to override that behavior.

Useful local sync examples:

```bash
source .venv/bin/activate && PYTHONPATH=src python -m kaya.sync_local_data --latest-run-dates 3
```

```bash
source .venv/bin/activate && PYTHONPATH=src python -m kaya.sync_local_data --export-name rds-backfill --max-objects 25
```

The sync path currently drops S3-only metadata columns such as `run_date`, `run_id`, `s3_key`, and `source` before upserting into the historical local `sends` schema.

## Secrets And Tokens

This repo does not store Kaya API credentials in git.

The code expects these runtime token names:

- `KAYA_API_TOKEN`
- `KAYA_REFRESH_TOKEN`

The helper module is `src/kaya/secrets.py`:

- `load_secrets()` loads tokens from local `.env` when running locally
- `load_secrets(force_aws=True)` or Lambda execution loads them from AWS Secrets Manager
- `write_secrets()` updates either the local `.env` file or the AWS secret, depending on runtime

AWS token resolution depends on:

- `KAYA_API_TOKENS_SECRET_NAME`
- `AWS_REGION`

Practical rules:

- locally, check the repo-adjacent `.env` path used by `python-dotenv`
- in AWS, check the Secrets Manager secret referenced by `KAYA_API_TOKENS_SECRET_NAME`
- do not paste live token values into docs, notebooks, or committed config

## Environment Variables That Matter

Current important runtime variables:

- `KAYA_S3_BUCKET`: enables the S3-backed updater path
- `KAYA_S3_PREFIX`: defaults to `kaya`
- `KAYA_S3_STATE_MAX_SEND_IDS`: optional cap for stored frontier send IDs
- `KAYA_SQS_QUEUE_URL`: required for dispatch mode
- `KAYA_SQS_DLQ_URL`: optional but strongly recommended for terminal worker failures
- `KAYA_API_TOKENS_SECRET_NAME`: AWS secret name for Kaya API tokens
- `AWS_REGION` or `AWS_DEFAULT_REGION`: AWS region selection
- `AWS_PROFILE`: local AWS profile, commonly `admin`
- `AWS_DB_URL`: legacy AWS database read path
- `AWS_DB_SCHEMA`: legacy AWS DB schema, still used when reading old DB surfaces
- `LOCAL_DB_URL`: optional override for the local SQLite connection string

If `KAYA_S3_BUCKET` is absent, local S3-backed commands will not work unless the CLI sets the defaults for you, as `python -m kaya.sync_local_data` does.

## Local Viewer

A local-only interactive viewer is now available on top of the local SQLite database.

Run it with:

```bash
source .venv/bin/activate && PYTHONPATH=src python -m kaya.viewer_app
```

Then open:

```text
http://127.0.0.1:8000
```

The first version includes:

- activity KPIs
- sends-over-time chart
- grade distribution chart
- top gyms chart
- climber height and ape-index distributions
- max-boulder-grade vs height or ape-index bubble charts
- recent S3 state preview from the current state files

The local viewer follows the dark-first card-and-token visual direction from the provided style guide while staying fully local and Python-backed for now.

## Prebuilt Viewer Data

You can materialize the current viewer payloads into static JSON artifacts with:

```bash
source .venv/bin/activate && PYTHONPATH=src python -m kaya.build_viewer_payloads
```

By default this writes to:

```text
data/viewer_payloads/latest/
```

The viewer can then run in static-data mode and load those JSON files instead of the live API routes:

```text
http://127.0.0.1:8000/?dataMode=static
```

Current static artifacts cover:

- summary
- gyms list
- state preview
- top gyms
- daily, weekly, and monthly time series
- separate bouldering and routes grade distributions
- separate bouldering and routes body-metric payloads

The current static mode intentionally does not precompute every gym/date filter combination, so the gym and date controls are disabled in that mode.

Recent viewer-specific notes:

- the user-segmentation tab now follows notebook-style metrics based on `n_sends`, `n_sesh`, and `n_sends_per_sesh`
- session counts are day-normalized before segmentation, so `n_sesh` means unique climbing dates rather than raw timestamp rows
- the body morphology page now has a toggle between all users and active users for the grade scatter panels
- morphology histograms remain on the original `All Users`, `Male`, `Female` split

## Recommended Handoff Checklist

If another agent picks this repo up cold, the fastest reliable order is:

1. Read `README.md`, `KAYA_HANDOFF.md`, and `DATA_STORAGE_NOTES.md`.
2. Confirm whether the task is about production Lambda behavior, local analysis, or the viewer.
3. For production questions, start with `src/kaya/update_data_script.py`, `src/kaya/data_puller.py`, and `.github/workflows/deploy-lambda.yml`.
4. For local data questions, start with `src/kaya/data_access.py` and `src/kaya/sync_local_data.py`.
5. For viewer questions, start with `src/kaya/viewer_app.py`, `src/kaya/viewer_payloads.py`, and `src/kaya/viewer_static/`.
6. Assume S3 is the source of truth for current live raw/history ingestion unless a task explicitly calls for the legacy DB path.

## Current Direction

The repo direction is now:

- raw operational history in S3
- local exploration through SQLite plus `KayaDataAccessor`
- curated or viewer-facing outputs generated from those surfaces
- no renewed dependence on the old VPC-plus-RDS write path unless a future task explicitly requires it
