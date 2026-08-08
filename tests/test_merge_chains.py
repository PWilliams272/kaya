"""Merging chains from separate runs, and the three ways it goes wrong quietly.

The premise of an incremental sweep is that 4 chains run tomorrow are worth
exactly as much as 4 run today. That is true -- chains are independent by
construction -- but only if the merge is done right, and every way of doing it
wrong produces a plausible-looking trace rather than an error.
"""
import sys
from pathlib import Path

import numpy as np
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))

az = pytest.importorskip('arviz')
mc = pytest.importorskip('merge_chains')

FIT_ARGS = '{"chains": 4, "height_form": "linear", "tune": 2000}'


def trace(n_chains=2, n_draws=200, seed='1', args=FIT_ARGS, offset=0.0, rng_seed=0):
    rng = np.random.default_rng(rng_seed)
    x = rng.standard_normal((n_chains, n_draws)) + offset
    idata = az.from_dict(posterior={'beta0': x, 'sigma_gym': np.abs(x) + 1})
    idata.posterior.attrs['kaya_seed'] = seed
    if args is not None:
        idata.posterior.attrs['kaya_fit_args'] = args
    return idata


def test_merging_reproduces_the_original_diagnostics():
    """The claim the whole approach rests on, checked end to end."""
    rng = np.random.default_rng(3)
    full = az.from_dict(posterior={'beta0': rng.standard_normal((4, 500))})
    a, b = full.isel(chain=[0, 1]), full.isel(chain=[2, 3])
    merged = mc.merge([a, b])
    assert merged.posterior.sizes['chain'] == 4
    for stat in (az.rhat, az.ess):
        want = float(np.asarray(stat(full, var_names=['beta0'])['beta0'].values))
        got = float(np.asarray(stat(merged, var_names=['beta0'])['beta0'].values))
        assert abs(want - got) < 1e-9, f'{stat.__name__} changed on merge'


def test_chain_ids_are_renumbered_so_none_are_lost():
    """Separate runs both label their chains 0..n-1.

    Concatenating on colliding ids makes xarray ALIGN rather than append, which
    drops chains silently -- the trace still loads and still has a posterior.
    """
    merged = mc.merge([trace(n_chains=2, rng_seed=1), trace(n_chains=3, rng_seed=2)])
    assert merged.posterior.sizes['chain'] == 5
    assert list(merged.posterior.chain.values) == [0, 1, 2, 3, 4]


def test_the_same_seed_twice_is_refused():
    """Same seed = same chains. Merged, between-chain variance is zero, so
    R-hat reads 1.000 however badly the sampler actually mixed."""
    problems = mc.check_compatible([trace(seed='7'), trace(seed='7')],
                                   ['a', 'b'], allow_unseeded=False)
    assert any('same chains' in p for p in problems)


def test_unseeded_traces_are_refused_unless_overridden():
    pair = [trace(seed='unset'), trace(seed='unset')]
    assert mc.check_compatible(pair, ['a', 'b'], allow_unseeded=False)
    assert not mc.check_compatible(pair, ['a', 'b'], allow_unseeded=True)


def test_different_models_are_refused():
    """Merging two posteriors gives a number that is neither of them."""
    other = '{"chains": 4, "height_form": "saturating", "tune": 2000}'
    problems = mc.check_compatible(
        [trace(seed='1'), trace(seed='2', args=other)], ['a', 'b'],
        allow_unseeded=False)
    assert any('fitted differently' in p for p in problems)


def test_differing_draw_counts_are_refused():
    """xarray would pad the short one with NaN rather than complain."""
    problems = mc.check_compatible(
        [trace(seed='1', n_draws=200), trace(seed='2', n_draws=100)],
        ['a', 'b'], allow_unseeded=False)
    assert any('draw counts' in p for p in problems)


def test_the_draws_flag_alone_does_not_block_a_merge():
    """A top-up may legitimately be launched with a different --draws value;
    the guard on draw COUNT above is what actually protects the merge."""
    assert 'draws' in mc.IGNORED_ARGS
    a = '{"chains": 4, "height_form": "linear", "tune": 2000, "draws": 1000}'
    b = '{"chains": 4, "height_form": "linear", "tune": 2000, "draws": 2000}'
    assert not mc.check_compatible(
        [trace(seed='1', args=a), trace(seed='2', args=b)], ['a', 'b'],
        allow_unseeded=False)


def test_a_compatible_pair_passes_cleanly():
    assert mc.check_compatible([trace(seed='1'), trace(seed='2')],
                               ['a', 'b'], allow_unseeded=False) == []
