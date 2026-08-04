# Copilot Instructions For kaya

## Repo Summary

- Repo name: `kaya`
- Purpose: Pull and analyze data from the Kaya climbing platform.
- Role in larger system: Owns Kaya data collection, transformation, and exploratory analysis. Only a curated subset should ever be exposed on the public website.
- Website surface: No direct runtime integration was confirmed in the website app factory during this review. Public-facing outputs are likely writeups, selected figures, or curated data products.
- Target `v2` integration mode: Generated artifacts, figures, and writeups by default; add a live package boundary only if a specific site feature truly needs it.
- Sync boundary: Treat this repo as the source of truth for data logic and analysis. If website content is derived from this repo, publish or copy only the selected outputs, not the full exploratory surface.

## Run And Validation

- Main local run command: `source .venv/bin/activate && PYTHONPATH=src python -m kaya.update_data_script`
- Main validation command: `source .venv/bin/activate && python -m compileall src`
- Deployment processes: `.github/workflows/deploy-lambda.yml` deploys the Lambda updater on `prod`. If public-facing site content changes, document how the selected output moves into `aws_flask_site`.

## AWS CLI Access

- AWS CLI is already installed on this machine.
- Verified primary AWS account ID: `545009868532`
- Verified default region for current work: `us-east-2`
- Do not create new credentials in this repo. Use the existing local AWS CLI profiles from `~/.aws/config` and `~/.aws/credentials`.
- Prefer `--profile admin --region us-east-2` for cross-project AWS management, IAM changes, Lambda configuration changes, VPC or route-table inspection, and bucket-policy inspection.
- Use `--profile personal --region us-east-2` only as a fallback read profile when needed.

Verified useful profiles on this machine:

- `admin`
- `personal`

Verified admin CLI identity:

- `arn:aws:iam::545009868532:user/pw-admin-cli`

Verified Kaya-relevant AWS resources:

- Lambda function: `kaya-data-updater`
- Current live Lambda pattern: non-VPC S3-backed updater with EventBridge dispatch, SQS per-gym fanout, and DLQ-backed terminal failure capture

Safe AWS CLI verification commands:

- `aws sts get-caller-identity --profile admin --region us-east-2`
- `aws lambda get-function-configuration --function-name kaya-data-updater --profile admin --region us-east-2`
- `aws logs describe-log-streams --log-group-name /aws/lambda/kaya-data-updater --order-by LastEventTime --descending --max-items 5 --profile admin --region us-east-2`

Access rules:

- Keep secrets out of git and out of markdown notes.
- Do not paste access keys, secret keys, session tokens, or raw `.env` values into the repo.
- Prefer CLI inspection over console clicking when tracing Lambda, logs, VPC attachment, or future S3 outputs.

## Branch Workflow

- Default stable branch: `main`
- Integration branch: `dev`
- Production branch: `prod`
- Release tagging rule: Tag release commits on `main` as `vX.Y.Z`; use `prod` only for release-ready states.

## Important Paths

- Current repo-only handoff: `KAYA_HANDOFF.md`
- Package root: `src/kaya/`
- Data update workflow: `src/kaya/update_data_script.py`
- Analysis layer: `src/kaya/analysis.py`
- Configuration files: `src/kaya/config/`
- Deployment-related files: `lambda_deployment/`
- Lambda deployment workflow: `.github/workflows/deploy-lambda.yml`
- Generated or exploratory files to avoid editing casually: `data/`, `notebooks/`, ad hoc figures under `figs/`

## Safety Rules

- Do not assume every notebook, plot, or dataset should be exposed on the website.
- Keep exploratory analysis separate from curated outputs intended for publishing.
- When adding public-facing functionality, define the export boundary clearly: package API, generated artifact, or copied writeup.
- Preserve data provenance and document whether a file is source, derived, or publishable output.
- Prefer AWS CLI or GitHub Actions over manual console work for Lambda deployment or inspection.
- Do not treat the old VPC-plus-RDS Lambda shape as the current state; the live updater now runs on the S3-plus-SQS path.

## Architecture Notes

- Main framework or stack: Python 3.11 package with pandas, NumPy, SciPy, SQLAlchemy, boto3, and notebooks.
- Key integrations: Kaya data pulls, S3-backed raw/state storage, SQS fanout, optional Lambda deployment helpers, and possible downstream website writeups.
- Known fragile areas: sparse top-level docs, unclear website handoff path, mixed exploratory versus production-adjacent code, and intermittent Kaya API failures that can send gyms to the DLQ.

## Data Storage Rules

- Default target storage for Kaya analytical data is S3-backed files rather than the shared website RDS.
- The live Lambda no longer needs VPC attachment; keep it that way unless a future private dependency truly requires it.
- Use RDS only if a concrete relational need appears, not as the default sink for scrape history.
- Prefer parquet or similarly query-friendly files for viewer-facing analytical datasets.

## Documentation Standards

- Public functions and classes should have docstrings.
- Type hints are expected for public APIs and multi-step data workflows.
- Example usage should live in the README, docs, or targeted module docstrings.
- When adding a new analysis workflow, document whether it is exploratory, repeatable, or intended to feed a public output.

## V2 Coordination Rules

- Default to artifact handoff for public site use: writeups, tables, figures, or curated JSON outputs.
- Do not add direct website runtime embedding unless a concrete `v2` feature needs live Kaya-backed behavior.
- If a website page derives from this repo, document exactly which script or workflow produced the published output.
- Preferred future viewer pattern: S3-backed analytical files plus backend filtering or query layers, with DuckDB-style local table exploration instead of relying on shared RDS tables.
- Treat Kaya as the next AWS migration priority after Garmin because the shared NAT path has been removed and the current deployed Lambda still appears to rely on the older VPC-plus-RDS design.

## Cleanup Priorities

- Priority 1: Clarify the public-output boundary between this repo and `aws_flask_site`.
- Priority 2: Add any desired DLQ replay or alerting workflow now that daily S3-backed pulls are live.
- Priority 3: Fill in the top-level docs and standardized validation guidance.
- Priority 4: Separate exploratory notebooks from stable, reusable data workflows more clearly.

## Coordination With `system-overview`

- `system-overview` (`/Users/peterwilliams/projects/system-overview`) is the cross-repo coordination hub — its docs (`repo-inventory.md`, `system-overview.md`, `AGENT_HANDOFF.md`) are the source of truth for how this repo fits into the overall workspace (deploy targets, ports, IAM, integration mode, priorities).
- After any change here with cross-repo relevance — new deploy target, new port/subdomain, new AWS resource, new integration mode, a significant scope or status change — report it back so `system-overview` can be updated. If you can, make the edit directly in the relevant `system-overview` doc(s); otherwise leave the user a short note of what changed so they can relay it.
- Before starting work that could plausibly conflict with what `system-overview` has documented for this repo (a different port/subdomain than assigned, a new instance profile instead of extending the shared role, a deploy pattern that diverges from the established `kaya` pattern, etc.), flag it and tell the user to check with the `system-overview` agent before proceeding, rather than assuming and continuing.