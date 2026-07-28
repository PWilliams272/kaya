# Viewer-cache Lambda — deployment runbook

Everything in this doc is **prepared but not executed**. It's the concrete,
copy-pasteable sequence for turning on the daily viewer-cache precompute job,
decided as: a new, isolated Lambda (container image) on an EventBridge
schedule, separate from `kaya-data-updater`, syncing a persisted SQLite
snapshot from S3 rather than a fresh full-history rebuild every run.

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

docker build -f lambda_deployment/viewer_cache.Dockerfile \
  -t <account-id>.dkr.ecr.us-east-2.amazonaws.com/kaya-viewer-cache:bootstrap .
docker push <account-id>.dkr.ecr.us-east-2.amazonaws.com/kaya-viewer-cache:bootstrap

aws lambda create-function \
  --function-name kaya-viewer-cache \
  --package-type Image \
  --code ImageUri=<account-id>.dkr.ecr.us-east-2.amazonaws.com/kaya-viewer-cache:bootstrap \
  --role arn:aws:iam::<account-id>:role/kaya-viewer-cache-lambda-role \
  --timeout 300 \
  --memory-size 1024 \
  --environment "Variables={KAYA_S3_BUCKET=my-kaya-data-545009868532-us-east-2,KAYA_S3_PREFIX=kaya}"
```

`--timeout 300` (5 min) and `--memory-size 1024` (1GB) are starting points —
today's full local `write_static_artifacts()` run took roughly a minute plus
the sync/upload steps, so this leaves real headroom; adjust after watching
actual CloudWatch duration/memory-used on the first few real runs.

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

`cron(0 12 * * ? *)` is a placeholder (noon UTC) — replace with whatever time
actually follows `kaya-data-updater`'s schedule.

### 6. Wire up the GitHub Actions workflow

Set these as repo (or environment-scoped) variables so
`deploy-viewer-cache-lambda.yml` knows what to deploy to:

- `vars.VIEWER_CACHE_LAMBDA_FUNCTION_NAME` = `kaya-viewer-cache`
- `vars.VIEWER_CACHE_ECR_REPOSITORY` = `kaya-viewer-cache` (optional, this is
  already the default)

The workflow reuses `secrets.AWS_ACCESS_KEY_ID` / `secrets.AWS_SECRET_ACCESS_KEY`
— confirm whether that's the same credential `deploy-lambda.yml` already uses
or a separate one, and either way make sure it has `ecr:GetAuthorizationToken`,
`ecr:BatchCheckLayerAvailability`, `ecr:InitiateLayerUpload`,
`ecr:UploadLayerPart`, `ecr:CompleteLayerUpload`, `ecr:PutImage`, and
`ecr:BatchGetImage` scoped to the `kaya-viewer-cache` repo, plus
`lambda:UpdateFunctionCode` scoped to the `kaya-viewer-cache` function. This
is a deploy-time credential, distinct from the Lambda's own runtime role in
step 2 above.

After that, future deploys are just: run the "Deploy Viewer Cache Lambda"
workflow via `workflow_dispatch`.

## Verifying a run

```bash
aws lambda invoke --function-name kaya-viewer-cache /tmp/out.json && cat /tmp/out.json
```

Check `artifacts_uploaded.files_uploaded` is close to 461 (today's manual
upload count) and `sync.rows_written` is a small, plausible number (a few
days of new sends, not near-zero every day forever, and not huge either).
