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
