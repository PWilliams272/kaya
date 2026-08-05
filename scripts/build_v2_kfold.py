"""Add up the grouped k-fold folds into one score per model.

Each fold wrote the per-row log predictive density for the climbers it held
out. Every row of the dataset is held out exactly once across the folds, so
concatenating them gives one score per row -- directly comparable between
models, because every model saw the same folds.

Two things this reports that leave-one-out cannot:

* **The honest generalisation gap.** Leave-one-out predicts one more send from
  a climber the model has already seen; this predicts a climber it has never
  seen. The difference between the two is what the per-climber offsets were
  actually buying.
* **A difference with a usable error bar.** As in build_v2_vs_null.py, the
  error on a gap between two models comes from differencing per row first --
  the two models find the same climbers hard, and that shared difficulty
  cancels.

Writes src/kaya/viewer_static/v2_kfold.json.

Run from the repo root.
"""
import json
import warnings
from collections import defaultdict
from pathlib import Path

warnings.filterwarnings('ignore')
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / 'runs' / 'results'
OUT = ROOT / 'src' / 'kaya' / 'viewer_static' / 'v2_kfold.json'

LABEL = {
    'v3_lin': 'straight line', 'v3_quad': 'plain curve',
    'v3_sat': 'saturating curve', 'v3_conf': 'curve, differs by gender',
    'v4_linxg': 'straight line, differs by gender', 'v3_zero': 'no height term',
}
REFERENCE = 'v3_zero'


def load():
    """Per-model row scores, plus whether every fold actually landed."""
    rows, meta = defaultdict(dict), defaultdict(list)
    for f in sorted(RESULTS.glob('kfold_*_fold*.json')):
        d = json.loads(f.read_text())
        name = d['name']
        meta[name].append({'fold': d['fold'], 'max_rhat': d.get('max_rhat'),
                           'elapsed_min': d.get('elapsed_min'),
                           'n_rows': d.get('n_scored'),
                           'dropped': d.get('n_dropped_unseen_gym', 0)})
        for i, v in zip(d['row_index'], d['elpd_i']):
            rows[name][int(i)] = float(v)
    return rows, meta


def main():
    rows, meta = load()
    if not rows:
        print('no kfold results yet -- run: '
              'scripts/run_batch.py --batch kfold')
        OUT.write_text(json.dumps({'models': [], 'reference': REFERENCE}))
        return

    # Only rows every model scored can be compared. A row dropped by one model
    # (its gym unseen in that fold's training set) has to be dropped for all,
    # or the totals are added over different denominators.
    common = set.intersection(*(set(v) for v in rows.values()))
    common = np.array(sorted(common))
    print(f'{len(rows)} models, {len(common):,} rows scored by all of them')
    for name, m in sorted(meta.items()):
        folds = sorted(x['fold'] for x in m)
        worst = max((x['max_rhat'] or 0) for x in m)
        drop = sum(x['dropped'] or 0 for x in m)
        print(f'  {name:<12} folds {folds}  worst R-hat {worst:.3f}'
              + (f'  {drop} rows dropped' if drop else ''))

    score = {n: np.array([rows[n][i] for i in common]) for n in rows}
    ref = score.get(REFERENCE)
    models = []
    for name, s in sorted(score.items(), key=lambda kv: -kv[1].sum()):
        row = {'name': name, 'label': LABEL.get(name, name),
               'elpd': round(float(s.sum()), 2),
               'per_row': round(float(s.mean()), 4),
               'n_folds': len(meta[name]),
               'worst_rhat': round(max((x['max_rhat'] or 0) for x in meta[name]), 3)}
        if ref is not None and name != REFERENCE:
            d = s - ref
            se = float(np.std(d, ddof=1) * np.sqrt(len(d)))
            row.update(vs_null=round(float(d.sum()), 2), se=round(se, 2),
                       z=round(float(d.sum() / se), 2) if se > 0 else None)
        models.append(row)

    print(f'\nheld-out score, and the gap from "{LABEL.get(REFERENCE)}"')
    print(f'{"height form":>34}{"elpd":>12}{"per row":>10}'
          f'{"vs no height":>16}{"z":>7}')
    for m in models:
        z = m.get('z')
        print(f'{m["label"]:>34}{m["elpd"]:>12.1f}{m["per_row"]:>10.3f}'
              + (f'{m["vs_null"]:>+11.1f} +/-{m["se"]:<4.1f}{z:>7.1f}'
                 if 'vs_null' in m else f'{"reference":>16}{"":>7}'))

    OUT.write_text(json.dumps({'models': models, 'reference': REFERENCE,
                               'n_rows': int(len(common))},
                              separators=(',', ':')))
    print(f'\nwrote {OUT}')


if __name__ == '__main__':
    main()
