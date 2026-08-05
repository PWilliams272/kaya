# Data Storage Notes

Use this file for Kaya storage and deployment decisions while working only in the `kaya` repo.

## Target Direction

- Kaya analytical history should move out of the shared website RDS
- default target storage is private or controlled S3-backed analytical files
- RDS should be used only if a real relational requirement appears later

## Current State

- `kaya-data-updater` now writes incremental raw sends and per-gym frontier state to S3
- the Lambda is no longer VPC-attached and no longer needs the old NAT-backed egress path
- the daily EventBridge rule dispatches per-gym SQS jobs instead of running the full pull in one invocation
- terminal per-gym failures are stored in `kaya-gym-update-jobs-dlq` and do not block the next day's run
- historical RDS data has been backfilled to S3 under `kaya/raw/sends/source=rds-backfill/`

## Storage Rules

- prefer parquet as the main analytical format
- use JSON for small cached viewer payloads only
- avoid keeping large append-only scrape history in RDS
- if site-facing content only needs charts or filtered outputs, publish curated files rather than growing the website database

## Suggested S3 Layout

- `s3://<kaya-bucket>/raw/<gym-or-source>/<date-partition>/...`
- `s3://<kaya-bucket>/curated/sends/year=YYYY/month=MM/...parquet`
- `s3://<kaya-bucket>/viewer-cache/...json`

## Lambda And Networking

- the live Kaya Lambda now runs without VPC attachment
- keep the Lambda out of a VPC unless a future private dependency appears that truly requires it
- the current runtime dependency set is public Kaya API access, Secrets Manager, S3, and SQS

## Viewer Pattern

- let the website backend read curated S3-backed datasets and serve filtered subsets or aggregates
- for scatter plots, histograms, and grade filters, prefer backend analytical querying over parquet rather than storing everything in Postgres

## Fit Inputs Under `runs/`

Provenance for the two files every `scripts/build_v2_*.py` and `scripts/run_*.py` entrypoint reads.
Nothing in this repo writes either one, so "just re-run it" is not a recovery path:

| File | Provenance | Tracked? | Why |
| --- | --- | --- | --- |
| `runs/networks.json` | **source** — hand-maintained gym-network definitions (`la6`, `net50`, `net100`, …) | yes | 3.5KB, no generator exists, and seven scripts fail without it. Losing it means no payload can be rebuilt. |
| `runs/base_bouldering.pkl` | **derived** — pickled bouldering-send snapshot built from the S3 history | no (`runs/*.pkl`) | 8MB binary; rebuildable from S3, and a pickle is the wrong thing to put in git |

Everything else under `runs/` is fit output: `*.nc` traces (~600MB each), `logs/`, and `traces/` are
all ignored.

## Local Data Access

- use DuckDB over local parquet copies or synced S3 exports for tabular browsing and SQL-style filtering
- DBeaver can be used against DuckDB if you want a familiar UI
- do not rely on `kaya.sends` in the shared RDS as the long-term analysis interface

## Current Operational Tasks

1. Inspect DLQ entries after daily runs and decide whether any failed gyms need manual replay
2. Define and build the curated parquet layer on top of the raw S3 history
3. Update downstream analysis or website-facing outputs to read curated files instead of RDS
4. Add any desired DLQ replay or alerting workflow without changing the daily dispatch path
