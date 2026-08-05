import argparse
import json
from pathlib import Path

from kaya.viewer_payloads import VIEWER_ARTIFACTS_DIR, ViewerPayloadBuilder


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Materialize Kaya viewer payloads into static JSON artifacts.'
    )
    parser.add_argument(
        '--output-dir',
        default=str(VIEWER_ARTIFACTS_DIR),
        help='Directory where the viewer JSON artifacts should be written.',
    )
    parser.add_argument(
        '--upload-to-s3',
        action='store_true',
        help='After writing local artifacts, also upload them to S3 (requires KAYA_S3_BUCKET).',
    )
    parser.add_argument(
        '--s3-prefix',
        default=None,
        help='Override the S3 key prefix used for the upload (defaults to "<KAYA_S3_PREFIX>/viewer-cache").',
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()
    builder = ViewerPayloadBuilder()
    result = builder.write_static_artifacts(output_dir=output_dir)
    if args.upload_to_s3:
        result['s3_upload'] = builder.upload_static_artifacts_to_s3(
            output_dir=output_dir,
            s3_prefix=args.s3_prefix,
        )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
