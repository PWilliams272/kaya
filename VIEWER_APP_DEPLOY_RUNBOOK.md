# Viewer app deploy — EC2 push, runbook

Covers `.github/workflows/deploy-viewer-app.yml`, which automates pushing
code changes to the already-running viewer app on EC2. It does **not** set
up the app itself — that was bootstrapped by hand (code at
`/home/ubuntu/kaya_viewer`, venv at `/home/ubuntu/kaya_viewer/.venv`,
running under systemd as `kaya-viewer.service` on loopback port 8010). This
workflow only automates the "push new code" step: rsync, `pip install -e .`,
`systemctl restart`.

## What it does, each run

1. Rsyncs `src/`, `pyproject.toml`, `README.md` to `/home/ubuntu/kaya_viewer/`
   via `easingthemes/ssh-deploy` (the same action `aws_flask_site`'s
   Push-to-EC2 workflow already uses for this host, confirmed directly from
   its run logs — mirrored here for consistency rather than a hand-rolled
   `ssh-agent` + raw `rsync`/`ssh` sequence).
2. Default rsync args (`-rlgoDzvc -i`, **no `--delete`**) are used
   deliberately as-is. `--delete` was left off on purpose: the sync target,
   `/home/ubuntu/kaya_viewer/`, also holds `.venv/` and anything else
   provisioned outside this rsync's three sources — `--delete` scopes to
   directories actually transferred (so `.venv/` specifically wouldn't be at
   risk even with it on), but there's no upside to opting into deletion
   semantics on a production host for this deploy, so it stays off, matching
   the sibling repo's own default.
3. `SCRIPT_AFTER` runs `pip install -e .` (idempotent, cheap even when
   nothing changed) and `sudo systemctl restart kaya-viewer.service` on the
   remote host after the transfer completes.

## Trigger — decided: push to `main`

Push to `main` + manual `workflow_dispatch`. `aws_flask_site`'s own
Push-to-EC2 workflow instead triggers on push to a dedicated `prod` branch,
separate from `main` — confirmed via the other agent this is "a reasonable,
different pattern, not wrong, just asymmetric," not something kaya needed to
match. Explicit call (2026-07-28): stay on push-to-`main` for now: every
push to `main` deploys. A `main`/`prod` branch split like `aws_flask_site`'s
is a possible future direction, not committed to.

## Secrets required (none set as of this writing)

- `KAYA_VIEWER_DEPLOY_KEY` — a dedicated ed25519 private key generated
  specifically for this deploy (not shared with `aws_flask_site`'s own
  `EC2_SSH_KEY`, deliberately — GitHub secrets don't cross repos anyway, so
  "reuse" would only ever mean copying the same key material into a second
  secret; a separate key keeps this consistent with the project's existing
  pattern of narrowly-scoped, per-purpose credentials, e.g. the viewer-cache
  Lambda's own dedicated IAM role rather than reusing broader credentials).
  Its public half needs to be appended to `~/.ssh/authorized_keys` for the
  `ubuntu` user on the host before this workflow can succeed.
- `KAYA_VIEWER_HOST` — the host's public IP.

## The host IP *will* go stale — this is not hypothetical

**There is no Elastic IP on this host.** Confirmed directly: the exact
failure that broke `aws_flask_site`'s own deploy on 2026-07-28 was a stale
`HOST_DNS` secret after a resize changed the instance's public IP, causing a
silent SSH timeout with no obviously-related error message. The planned (but
not yet scheduled, per [[kaya_viewer_deploy_infra]] memory / earlier agent
report) `t3.medium` resize will do the exact same thing to
`KAYA_VIEWER_HOST` unless it's updated at the same time.

**When any resize happens on this host, update `KAYA_VIEWER_HOST` as part of
that change** — not as an afterthought once a deploy mysteriously times out.
Current value (confirmed live as of 2026-07-28 via
`aws ec2 describe-instances`): `3.16.167.114`.

## Viewer-cache sync timer (host-side, outside this workflow)

Separate from the above: the live site reads its JSON from a local static
directory (`data/viewer_payloads/latest/`, FastAPI's `/viewer-data` mount),
not live from S3. Found and fixed 2026-08-03 — that directory had been
populated once by hand and nothing kept it in sync with the daily
`kaya-viewer-cache` Lambda's S3 output (`kaya/viewer-cache/`), so the site
had been silently serving a stale 2026-07-28 snapshot.

Fixed with a systemd timer installed directly on the host (via SSH, not
through this GitHub Actions workflow — deliberately decoupled so code
deploys and data refreshes don't block each other):

- `kaya-viewer-cache-sync.service` / `.timer` — unit files live in this repo
  at `lambda_deployment/host_systemd/`, but are **not** deployed by any
  workflow; they were copied to `/etc/systemd/system/` on the host by hand
  and enabled with `systemctl enable --now kaya-viewer-cache-sync.timer`.
- Runs `python -m kaya.sync_viewer_cache_to_host`
  (`src/kaya/sync_viewer_cache_to_host.py`) daily at 13:00 UTC (an hour
  after the Lambda's noon UTC run), plus once ~5 minutes after any host
  reboot. Uses the host's existing `ec2-kaya-viewer-role` IAM instance
  profile (already scoped read-only to `kaya/viewer-cache/*`) — no
  credentials to provision.
- **If this host is ever rebuilt**, redeploy by hand: `pip install -e .`
  already makes `kaya.sync_viewer_cache_to_host` importable (it's a normal
  module under `src/kaya/`), so just copy the two unit files from
  `lambda_deployment/host_systemd/` to `/etc/systemd/system/`, `systemctl
  daemon-reload`, then `systemctl enable --now kaya-viewer-cache-sync.timer`.
- Check it's alive: `systemctl list-timers kaya-viewer-cache-sync.timer` and
  `journalctl -u kaya-viewer-cache-sync.service -n 20`.
