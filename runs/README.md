# Fit archive

Traces and result summaries for the grading-model v2 fits. Kept here rather
than in a scratch directory because each trace is 30-60 minutes of sampling
and the scratch directory is deleted with the job that made it.

Gitignored (the `.nc` files are ~600 MB each) but durable on disk.

## Naming

| suffix | meaning |
|---|---|
| *(none)* | **unmarginalized** — every climber carries their own ability offset (10,397 parameters). The original model. |
| `_marg` | **marginalized** — the offsets of climbers with a single observation are integrated out exactly (4,241 parameters). |

Both are kept deliberately. The marginalized model is expected to be the
better one, but "expected" is not "shown", and the comparison between them is
itself a result: the unmarginalized fits are what produced the 31.1-elpd
run-to-run noise and the 8,400 effective parameters, and there is no way to
report that the fix worked without the thing it was fixing.

## What each fit is

| name | height form | notes |
|---|---|---|
| `v3_zero` | none | the null: does height matter at all |
| `v3_lin` | straight line | |
| `v3_quad` | quadratic | |
| `v3_sat` | saturating | |
| `v3_vtx` | vertex-parameterised quadratic | did not converge (max R-hat 1.34) |
| `v3_conf` | quadratic x gender | the form used elsewhere on the write-up page |
| `v4_linxg` | straight line x gender | |
| `v4_lin_b` … `v4_lin_e` | straight line | **refits of `v3_lin`**, identical settings, different seed. The spread among them is the measurement's noise floor. |
| `v3_apex`, `v4_lin_apex`, `v4_lin_apelin` | — | ape-index variants |
| `v3_all`, `v3_zsu` | — | dataset and parameterisation variants |
