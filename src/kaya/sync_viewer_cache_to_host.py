"""Pull the current kaya/viewer-cache/ JSON payloads from S3 down onto this
host's local disk, at the exact path the running kaya-viewer.service reads
from (VIEWER_ARTIFACTS_DIR's non-Lambda branch: data/viewer_payloads/latest/
relative to the repo root).

This closes the one manual step identified while debugging the 2026-08-03
"live site still shows the gap" incident: the daily kaya-viewer-cache Lambda
already rebuilds and uploads this JSON to S3 every day, but nothing was ever
pulling it back down onto the EC2 host that actually serves it -- the
directory had been populated once by hand (2026-07-28) and never touched
again. Deployed as a systemd timer on the host (see
kaya-viewer-cache-sync.service/.timer in this same directory's sibling
lambda_deployment/ layout, or the VIEWER_APP_DEPLOY_RUNBOOK.md section on
this), independent of the GitHub Actions code-deploy path -- code and data
refresh on separate schedules and neither should block the other.

The host's IAM instance profile (ec2-kaya-viewer-role) already carries a
kaya-viewer-cache-readonly policy scoped to exactly this S3 prefix, so no
credentials need to be provisioned for this to run.

Run directly:
    .venv/bin/python -m kaya.sync_viewer_cache_to_host
"""
import logging
import os

from kaya.s3_storage import get_s3_bucket, get_s3_client, get_s3_prefix
from kaya.viewer_payloads import VIEWER_ARTIFACTS_DIR

logger = logging.getLogger(__name__)


def sync_viewer_cache_to_host() -> dict:
    bucket = get_s3_bucket()
    prefix = f'{get_s3_prefix()}/viewer-cache/'
    client = get_s3_client()
    dest = VIEWER_ARTIFACTS_DIR
    dest.mkdir(parents=True, exist_ok=True)

    paginator = client.get_paginator('list_objects_v2')
    remote_rel_keys = set()
    downloaded = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get('Contents', []):
            key = item['Key']
            rel = key[len(prefix):]
            if not rel:
                continue
            remote_rel_keys.add(rel)
            local_path = dest / rel
            local_path.parent.mkdir(parents=True, exist_ok=True)
            client.download_file(bucket, key, str(local_path))
            downloaded += 1

    deleted = 0
    for local_file in dest.rglob('*'):
        if local_file.is_file():
            rel = str(local_file.relative_to(dest))
            if rel not in remote_rel_keys:
                local_file.unlink()
                deleted += 1

    result = {
        'bucket': bucket,
        'prefix': prefix,
        'dest': str(dest),
        'downloaded': downloaded,
        'deleted': deleted,
        'total_remote': len(remote_rel_keys),
    }
    logger.info('viewer-cache sync: %s', result)
    return result


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    os.environ.setdefault('AWS_REGION', 'us-east-2')
    print(sync_viewer_cache_to_host())


if __name__ == '__main__':
    main()
