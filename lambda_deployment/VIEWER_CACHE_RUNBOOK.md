# Viewer-cache Lambda — deployment runbook

**Status: fully live as of 2026-07-28.** The `kaya-viewer-cache` Lambda, its
IAM role, the ECR repo, and the `kaya-viewer-cache-daily` EventBridge rule
all exist; a manual invoke completed successfully end-to-end (sync → curated
Parquet → viewer JSON → both S3 uploads); and the GitHub Actions deploy
workflow (step 6 below) is wired up too, including extending `kaya-app`'s
IAM policy for ECR + the new function. Everything in this doc has already
been run once and is recorded here as the actual configuration, not just a
plan. The only thing not yet done is exercising an actual deploy *through*
the GitHub Actions workflow itself (today's image updates were all pushed
locally) — worth a first real `workflow_dispatch` run to confirm the wiring.

This is the concrete, copy-pasteable sequence used to turn on the daily
viewer-cache precompute job: a new, isolated Lambda (container image) on an
EventBridge schedule, separate from `kaya-data-updater`, syncing a persisted
SQLite snapshot from S3 rather than a fresh full-history rebuild every run.

Code involved:
- `src/kaya/build_viewer_cache_lambda.py` — the handler
- `lambda_deployment/viewer_cache.Dockerfile` + `viewer_cache_requirements.txt`
- `.github/workflows/deploy-viewer-cache-lambda.yml` — builds the image, pushes
  to ECR, updates the Lambda's code (does **not** create the function itself)

## What the handler actually does, each run

1. Download the persisted full-history SQLite snapshot from
   `s3://<bucket>/kaya/materialized/kaya_data.db` into `/tmp`.
2. Incrementally sync the last 3 `run_date=` partitions from
   `kaya/raw/sends/...` on top of it (`sync_latest_s3_to_local_db`) — a small
   buffer past "1 day" in case a run is missed or delayed. Returns which
   `YYYY-MM` months the newly-synced rows actually fall into.
3. For each touched month, rebuild that month's curated Parquet partition
   (`write_curated_month_parquet`) — a fresh `SELECT ... WHERE date >= / <`
   query against the local DB (fast: `date` is indexed) written to
   `kaya/curated/sends/year=YYYY/month=MM/data.parquet`. Only the touched
   month(s) are rewritten, not the full history — this is the piece that
   satisfies DATA_STORAGE_NOTES.md's outstanding "define and build the
   curated parquet layer" task, and unlocks DuckDB-over-Parquet for the
   notebooks in `notebooks/`.
4. Rebuild all static viewer artifacts (`write_static_artifacts()`) into `/tmp`.
5. Upload them to `s3://<bucket>/kaya/viewer-cache/...` (additive, same prefix
   used by today's one-time manual upload).
6. Upload the refreshed SQLite back to `kaya/materialized/kaya_data.db`, so
   the next run's download step has the update baked in.

Steps 1 and 6 are why this needs read+write on a `kaya/materialized/*` key.
Step 3 needs write on `kaya/curated/sends/*` too. This deliberately does
**not** remove the full-file SQLite round-trip's linear-with-size cost (steps
1 and 6) — only the newly-added curated Parquet layer is partition-
incremental. Revisit once there's a few weeks of post-backfill data to
measure real daily growth against how close the snapshot download/upload
time is getting to the Lambda timeout; if it's trending toward mattering,
replacing the SQLite-snapshot step with a Parquet-partition rebuild of the
local query copy is the next move, and the curated files from step 3 are
already sitting there ready to be consumed that way.

## One-time setup, in order

### 1. Create the ECR repository

```bash
aws ecr create-repository \
  --repository-name kaya-viewer-cache \
  --region us-east-2 \
  --image-scanning-configuration scanOnPush=true
```

### 2. Create the Lambda's execution role

Trust policy (`viewer-cache-trust-policy.json`):

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "lambda.amazonaws.com" },
    "Action": "sts:AssumeRole"
  }]
}
```

Permissions policy (`viewer-cache-permissions-policy.json`) — replace
`my-kaya-data-545009868532-us-east-2` if the bucket ever changes:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListScopedPrefixes",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::my-kaya-data-545009868532-us-east-2",
      "Condition": {
        "StringLike": {
          "s3:prefix": [
            "kaya/raw/*",
            "kaya/state/*",
            "kaya/materialized/*",
            "kaya/viewer-cache/*",
            "kaya/curated/*"
          ]
        }
      }
    },
    {
      "Sid": "ReadRawAndState",
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": [
        "arn:aws:s3:::my-kaya-data-545009868532-us-east-2/kaya/raw/*",
        "arn:aws:s3:::my-kaya-data-545009868532-us-east-2/kaya/state/*"
      ]
    },
    {
      "Sid": "ReadWriteMaterializedSnapshot",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject"],
      "Resource": "arn:aws:s3:::my-kaya-data-545009868532-us-east-2/kaya/materialized/*"
    },
    {
      "Sid": "WriteCuratedParquet",
      "Effect": "Allow",
      "Action": "s3:PutObject",
      "Resource": "arn:aws:s3:::my-kaya-data-545009868532-us-east-2/kaya/curated/sends/*"
    },
    {
      "Sid": "WriteViewerCache",
      "Effect": "Allow",
      "Action": "s3:PutObject",
      "Resource": "arn:aws:s3:::my-kaya-data-545009868532-us-east-2/kaya/viewer-cache/*"
    }
  ]
}
```

```bash
aws iam create-role \
  --role-name kaya-viewer-cache-lambda-role \
  --assume-role-policy-document file://viewer-cache-trust-policy.json

aws iam put-role-policy \
  --role-name kaya-viewer-cache-lambda-role \
  --policy-name kaya-viewer-cache-s3-access \
  --policy-document file://viewer-cache-permissions-policy.json

aws iam attach-role-policy \
  --role-name kaya-viewer-cache-lambda-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
```

### 3. Bootstrap the materialized-DB snapshot

Seeds `kaya/materialized/kaya_data.db` from a known-complete local DB, so the
Lambda's first real run isn't starting from only 3 days of data. Run once,
locally, with the `admin` AWS profile (same one used for today's manual
`kaya/viewer-cache/` upload — confirmed to have write access; the pipeline's
own `.env` credentials do not):

```bash
AWS_PROFILE=admin aws s3 cp \
  ~/.kaya/kaya_data.db \
  s3://my-kaya-data-545009868532-us-east-2/kaya/materialized/kaya_data.db
```

### 4. Build and push the first image, then create the function

`aws lambda update-function-code` (what the GitHub Actions workflow runs)
requires the function to already exist, so the very first image has to be
pushed and the function created manually:

```bash
aws ecr get-login-password --region us-east-2 | \
  docker login --username AWS --password-stdin <account-id>.dkr.ecr.us-east-2.amazonaws.com

# --platform linux/amd64 matters on Apple Silicon dev machines — without it,
# Docker builds for arm64 by default. --provenance=false --sbom=false matters
# everywhere: modern Buildx attaches an attestation manifest by default, and
# Lambda's CreateFunction rejects that image format outright
# ("InvalidParameterValueException: ... media type ... is not supported").
docker build --platform linux/amd64 --provenance=false --sbom=false \
  -f lambda_deployment/viewer_cache.Dockerfile \
  -t <account-id>.dkr.ecr.us-east-2.amazonaws.com/kaya-viewer-cache:bootstrap .
docker push <account-id>.dkr.ecr.us-east-2.amazonaws.com/kaya-viewer-cache:bootstrap

aws lambda create-function \
  --function-name kaya-viewer-cache \
  --package-type Image \
  --code ImageUri=<account-id>.dkr.ecr.us-east-2.amazonaws.com/kaya-viewer-cache:bootstrap \
  --role arn:aws:iam::<account-id>:role/kaya-viewer-cache-lambda-role \
  --timeout 900 \
  --memory-size 3008 \
  --ephemeral-storage Size=3008 \
  --environment "Variables={KAYA_S3_BUCKET=my-kaya-data-545009868532-us-east-2,KAYA_S3_PREFIX=kaya}"
```

**Actual first-run numbers** (2026-07-28, ~18K new rows synced): 10m35s
duration, 2,577MB memory used. `--timeout 900` is Lambda's hard ceiling — that
run used about 70% of it, so there's real but not huge margin. If a future
run ever approaches 900s (larger daily volume, more months touched at once),
the fix is making the job itself faster, not raising the timeout further —
there's nowhere higher to go.

`--memory-size 1024` (the original starting guess) is **not enough** —
Lambda's CPU allocation scales with configured memory, and at 1024MB the
`write_static_artifacts()` step (GAM fits, Bayesian bootstrap, ~461 files)
was still running when the original 300s timeout hit. 3008MB comfortably
covers the observed 2,577MB peak with room to spare.

`--ephemeral-storage Size=3008` (default is 512MB) is required — the
materialized SQLite snapshot alone is ~520MB, which doesn't fit in the
default `/tmp`.

### 5. Create the EventBridge scheduled rule

Once per day, after `kaya-data-updater`'s own schedule has run (check its
existing EventBridge rule for its trigger time and pick something after it,
so the raw sends this job syncs already reflect that day's ingestion):

```bash
aws events put-rule \
  --name kaya-viewer-cache-daily \
  --schedule-expression "cron(0 12 * * ? *)" \
  --state ENABLED

aws lambda add-permission \
  --function-name kaya-viewer-cache \
  --statement-id AllowEventBridgeInvoke \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn arn:aws:events:us-east-2:<account-id>:rule/kaya-viewer-cache-daily

aws events put-targets \
  --rule kaya-viewer-cache-daily \
  --targets "Id"="1","Arn"="arn:aws:lambda:us-east-2:<account-id>:function:kaya-viewer-cache"
```

`cron(0 12 * * ? *)` = noon UTC, used as-is (not just a placeholder) — checked
`kaya-data-updater`'s actual rule (`run-every-day`, `cron(0 3 * * ? *)`) and
picked a 9-hour buffer, since that rule only *dispatches* per-gym SQS jobs at
3am; actual ingestion completes asynchronously over some window afterward
that wasn't measured precisely, so the buffer is deliberately generous.

### 6. Wire up the GitHub Actions workflow — done, 2026-07-28

`vars.VIEWER_CACHE_LAMBDA_FUNCTION_NAME` = `kaya-viewer-cache` was added as a
repository variable via the GitHub web UI (Settings → Secrets and variables
→ Actions → Variables). `VIEWER_CACHE_ECR_REPOSITORY` wasn't needed — the
workflow's default already matches.

`secrets.AWS_ACCESS_KEY_ID` / `secrets.AWS_SECRET_ACCESS_KEY` turned out to
belong to the `kaya-app` IAM user — identified via CloudTrail (`kaya-app`
shows up calling `UpdateFunctionCode` on `kaya-data-updater`, matching
`deploy-lambda.yml`'s known behavior), not by inspecting the GitHub secret
directly (GitHub never exposes secret values, even to repo admins, once
set). `kaya-app`'s existing inline policy (`KayaLambdaDeploymentPolicy`)
only covered `kaya-data-updater` and had zero ECR permissions at all — the
old zip-based workflow never needed any. Extended that same inline policy
(via the `admin` AWS profile) with an equivalent Lambda-deploy statement
scoped to `kaya-viewer-cache`, plus `ecr:GetAuthorizationToken` (account-wide
— this specific action doesn't support resource scoping) and the repo-scoped
push actions (`BatchCheckLayerAvailability`, `PutImage`,
`InitiateLayerUpload`, `UploadLayerPart`, `CompleteLayerUpload`,
`BatchGetImage`) limited to the `kaya-viewer-cache` ECR repo. The original
`kaya-data-updater` statement was left untouched — no regression risk to the
existing deploy path.

Future deploys are now just: run the "Deploy Viewer Cache Lambda" workflow
via `workflow_dispatch` (not yet actually exercised end-to-end through
GitHub Actions — today's deploys were all done locally via `docker build`/
`push`/`aws lambda update-function-code`; worth a first real run through the
workflow to confirm the wiring before relying on it).

## Bugs found only by actually deploying (both already fixed in code)

Neither of these showed up in any local testing — both are specific to the
Lambda's exact runtime environment.

1. **`INSERT ... ON CONFLICT DO UPDATE` failed with `near "ON": syntax
   error`.** AWS's `public.ecr.aws/lambda/python:3.11` base image bundles
   `libsqlite3` version **3.7.17** (from 2013) — the SQLite upsert clause
   needs >= 3.24 (2018). A local Mac's Python typically links a far newer
   system SQLite (3.53.x as of this writing), so this was invisible until
   the code actually ran inside the Lambda. Fixed in `db_manager.py`'s
   SQLite branch of `write_dataframe()` by switching to `INSERT OR REPLACE`
   (a SQLite extension supported since essentially any version), which is
   equivalent here since every upserted record always carries the full row.
2. **`numpy` tried to build from source and failed** (`Unknown compiler(s):
   [gcc, clang, ...]`) inside the base image, which has no C compiler.
   Root cause: pip resolving an unpinned/loose `numpy` constraint pulled in
   a version without a prebuilt wheel for this exact environment as a
   nested build-time dependency of another package, rather than using an
   existing wheel. Fixed by pinning `numpy==1.26.4` (matching what's
   already proven working locally) and adding `--only-binary=numpy,scipy,
   pandas,pyarrow` to the Dockerfile's `pip install`, so any future
   resolution mismatch like this fails fast and clearly instead of silently
   attempting a doomed source build.

## Verifying a run

```bash
aws lambda invoke --function-name kaya-viewer-cache /tmp/out.json && cat /tmp/out.json
```

Check `artifacts_uploaded.files_uploaded` is close to 461 (today's manual
upload count) and `sync.rows_written` is a small, plausible number (a few
days of new sends, not near-zero every day forever, and not huge either).
