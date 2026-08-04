"""Pull per-chain draws for every scalar parameter out of the v3_conf trace,
plus matching prior draws, and write a compact JSON the viewer can plot.

Kept deliberately small: 200 draws/chain is plenty for a density curve or a
trace plot at screen resolution, and keeps the payload well under a megabyte.
"""
import json, numpy as np, arviz as az

TRACE = '/Users/peterwilliams/.claude/jobs/e4f1b508/tmp/idata_v3_conf.nc'
OUT = '/Users/peterwilliams/projects/kaya/src/kaya/viewer_static/v2_posterior.json'
THIN_TO = 200

SCALARS = ['beta0', 'beta_gender', 'gamma1', 'gamma2', 'gamma1_x', 'gamma2_x',
           'delta1', 'delta2', 'sigma_user', 'sigma_gym', 'log_lambda0',
           'kappa', 'rho', 'beta_h_missing', 'beta_a_missing']

idata = az.from_netcdf(TRACE)
post = idata.posterior
print('chains', post.sizes['chain'], 'draws', post.sizes['draw'])

summ = az.summary(idata, var_names=SCALARS, hdi_prob=0.89)

out = {'n_chains': int(post.sizes['chain']), 'n_draws': int(post.sizes['draw']),
       'thin_to': THIN_TO, 'params': {}}

for name in SCALARS:
    if name not in post:
        print('  missing', name); continue
    arr = post[name].values                      # (chain, draw)
    step = max(1, arr.shape[1] // THIN_TO)
    thin = arr[:, ::step][:, :THIN_TO]
    r = summ.loc[name]
    out['params'][name] = {
        'chains': [[round(float(v), 5) for v in ch] for ch in thin],
        'mean': round(float(r['mean']), 4), 'sd': round(float(r['sd']), 4),
        'lo': round(float(r['hdi_5.5%']), 4), 'hi': round(float(r['hdi_94.5%']), 4),
        'rhat': round(float(r['r_hat']), 3),
        'ess_bulk': int(r['ess_bulk']), 'ess_tail': int(r['ess_tail']),
    }

# Sampler diagnostics -- what the chains were doing, not just where they landed.
stats = idata.sample_stats
diag = {}
for k, label in [('diverging', 'divergences'), ('tree_depth', 'tree_depth'),
                 ('step_size', 'step_size'), ('n_steps', 'n_steps'),
                 ('acceptance_rate', 'accept')]:
    if k in stats:
        v = stats[k].values
        diag[label] = {'per_chain_mean': [round(float(c.mean()), 4) for c in v],
                       'overall_mean': round(float(v.mean()), 4),
                       'max': round(float(v.max()), 4)}
if 'diverging' in stats:
    diag['divergences']['total'] = int(stats['diverging'].values.sum())
out['sample_stats'] = diag
out['sampling_minutes'] = None

with open(OUT, 'w') as f:
    json.dump(out, f, separators=(',', ':'))
import os
print('wrote', OUT, round(os.path.getsize(OUT)/1024, 1), 'KB')
print('params:', len(out['params']))
