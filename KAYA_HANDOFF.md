# Kaya Repo Handoff

Use this file when working in the `kaya` repo by itself with no visibility into the other repos.

## What This Repo Owns

- Kaya data pull and update logic
- Kaya analytical data shaping and derived outputs
- Any curated datasets, figures, or viewer-ready outputs that may later be published through the website

## What This Repo Does Not Own

- The public website shell
- Long-term website route design
- Cross-repo AWS planning history outside the notes captured here

If a change affects how the website consumes Kaya outputs, call out the expected update needed in `aws_flask_site` rather than assuming both repos are open.

## Current Situation

- The live `kaya-data-updater` Lambda now runs without VPC attachment.
- The live updater now uses S3-backed raw send storage plus per-gym state files rather than the older DB-backed write path.
- Daily execution now uses an EventBridge dispatch step plus per-gym SQS fanout workers.

Practical implication:

- Kaya is no longer blocked by the old NAT-backed Lambda subnet design.
- The main operational follow-up is monitoring or replaying terminally failed gyms from the DLQ when needed.

## Verified AWS Operator Setup On This Machine

- Preferred operator profile: `--profile admin --region us-east-2`
- Admin CLI identity: `arn:aws:iam::545009868532:user/pw-admin-cli`
- AWS account: `545009868532`
- Region: `us-east-2`

Use `admin` for:

- Lambda config inspection and updates
- IAM inspection
- VPC and route-table inspection
- future S3 storage migration work

## Current Verified Repo Direction

- Kaya analytical history should move out of the shared website RDS.
- Default target storage is S3-backed analytical files.
- Website-facing use should default to curated artifacts, figures, or S3-backed analytical datasets rather than direct DB-backed runtime coupling.

## Current Live-State Summary

- historical RDS centerpiece was `kaya.sends`, and that history has now been backfilled to S3
- live incremental state now lives under `kaya/state/` and live raw sends land under `kaya/raw/sends/run_date=.../`
- live queueing uses `kaya-gym-update-jobs` plus DLQ `kaya-gym-update-jobs-dlq`
- the daily EventBridge rule `run-every-day` and the Lambda SQS event source mapping are both enabled in steady state

## What The Next Agent Should Assume

- Garmin has already completed the main AWS cleanup, and Kaya has now completed the same high-priority runtime migration away from VPC-plus-RDS.
- Do not treat NAT removal as a future possibility. It already happened, and Kaya no longer depends on that path.
- The current focus is operational hardening: curated outputs, DLQ triage, and any desired replay or alerting flow.

## Recommended Immediate Work Order

1. Inspect the current DLQ contents and classify the known failed gyms.
2. Decide whether to add a manual replay path or alerting for DLQ entries.
3. Build curated parquet or viewer-facing datasets from the S3 raw history.
4. Update downstream analysis or website-facing outputs to consume curated files rather than the shared RDS table.

## Suggested S3 Direction

- `s3://<kaya-bucket>/raw/<gym-or-source>/<date-partition>/...`
- `s3://<kaya-bucket>/curated/sends/year=YYYY/month=MM/...parquet`
- `s3://<kaya-bucket>/viewer-cache/...json`

Use parquet as the default analytical format. Keep JSON for small cache or viewer payloads only.

## What To Avoid

- Do not deepen Kaya’s dependence on the shared RDS.
- Do not rebuild the old VPC-plus-NAT path unless a truly unavoidable private-resource dependency appears.
- Do not mix exploratory notebook work with deploy-path refactors in the same first change.

## Good First Deliverable

Produce a short repo-local operational brief covering:

- current live Lambda inputs and outputs
- the steady-state daily dispatch and DLQ flow
- the current DLQ contents and failure patterns
- the next curated-data step for downstream consumers