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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()
    result = ViewerPayloadBuilder().write_static_artifacts(output_dir=output_dir)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()