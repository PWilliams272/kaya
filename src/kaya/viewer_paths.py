"""Where a fit's trace and result JSON live.

`runs/` is the durable archive; the job scratch directory holds fits that are
still being copied out of a batch and is deleted when the job is. Each lookup
resolves to `runs/` when the file is there and falls back to scratch, so a
batch that is mid-copy does not break the build scripts.

build_v2_results.py, build_v2_posteriors.py and build_v2_time.py each carry
their own copy of these four functions. This module is the shared version; the
older three have not been migrated onto it.
"""
import fnmatch
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNS = ROOT / 'runs'
SCRATCH = Path(os.environ.get('CLAUDE_JOB_DIR', '')) / 'tmp' \
    if os.environ.get('CLAUDE_JOB_DIR') else RUNS


def _pick(*candidates: Path) -> Path:
    for p in candidates:
        if p.exists():
            return p
    return candidates[0]


def data_file(fname: str) -> Path:
    """base_bouldering.pkl, networks.json, csv inputs."""
    return _pick(RUNS / fname, SCRATCH / fname)


def trace_file(name: str) -> Path:
    return _pick(RUNS / 'traces' / f'idata_{name}.nc', SCRATCH / f'idata_{name}.nc')


def result_file(name: str) -> Path:
    return _pick(RUNS / 'results' / f'result_{name}.json',
                 SCRATCH / f'result_{name}.json')


def trace_names(*globs: str) -> list[str]:
    """Fit names that have BOTH a trace and a result, from either location."""
    names = set()
    for d in ({RUNS / 'traces', SCRATCH}):
        if not d.is_dir():
            continue
        for p in d.glob('idata_*.nc'):
            n = p.stem[len('idata_'):]
            if any(fnmatch.fnmatch(n, g) for g in globs):
                names.add(n)
    return sorted(n for n in names if result_file(n).exists())
