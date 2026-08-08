"""The deploy path filters must keep covering what each entrypoint imports.

`deploy-lambda.yml` and `deploy-viewer-app.yml` both build from `src/kaya/`.
They are path-filtered so the updater Lambda and the viewer have independent
deploy streams — otherwise a viewer copy edit redeploys the data pipeline and a
puller fix restarts the web host.

Those filters are **allowlists**, and an allowlist fails silently in the worst
direction: add a module the handler imports, forget to list it, and the deploy
just stops firing. No error, no failed run, nothing in the Actions tab —
production simply drifts behind `main` until somebody notices. That is the
exact failure these tests exist to prevent, so they recompute each entrypoint's
transitive import closure from the source and assert the workflow still covers
it.

The Lambda's packaging step carries a second copy of the same list (it copies
named modules rather than the whole tree), so that is checked against the
closure too. Three places, one truth.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / 'src' / 'kaya'
WORKFLOWS = ROOT / '.github' / 'workflows'

LAMBDA_WF = WORKFLOWS / 'deploy-lambda.yml'
VIEWER_WF = WORKFLOWS / 'deploy-viewer-app.yml'

# `kaya/__init__.py` is a root of every closure, not a leaf bolted on at the
# end. It has to ship for `kaya/update_data_script.lambda_handler` to resolve
# at all, AND it re-exports `KayaDataAccessor`, so importing *anything* from
# the package executes `from kaya.data_access import ...` first.
#
# Treating it as a leaf is not hypothetical: the first cut of this split
# shipped six modules without `data_access`, and the Lambda would have died at
# cold start on that re-export — a package that imports cleanly in the repo and
# not in the zip. Seed the traversal with it so the re-export is followed.
ALWAYS_ROOTS = {'__init__'}


def _local_imports(module: str) -> set[str]:
    """Names inside the `kaya` package that `module` imports directly."""
    path = SRC / f'{module}.py'
    if not path.exists():
        return set()
    found: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.ImportFrom) and node.module:
            if node.module == 'kaya':
                found.update(alias.name for alias in node.names)
            elif node.module.startswith('kaya.'):
                found.add(node.module.split('.')[1])
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith('kaya.'):
                    found.add(alias.name.split('.')[1])
    # `from kaya import X` may name a submodule or a symbol re-exported by
    # __init__; keep only the ones that are real modules on disk.
    return {f for f in found if (SRC / f'{f}.py').exists()}


def import_closure(root: str) -> set[str]:
    """Every kaya module reachable from `root` or from the package __init__."""
    seen: set[str] = set()
    stack = [root, *ALWAYS_ROOTS]
    while stack:
        module = stack.pop()
        if module in seen:
            continue
        seen.add(module)
        stack.extend(_local_imports(module) - seen)
    return seen


def workflow_paths(workflow: Path) -> list[str]:
    # `on` is parsed by PyYAML as the boolean True (YAML 1.1 treats `on` as a
    # truthy keyword), so look it up by both spellings rather than assuming.
    spec = yaml.safe_load(workflow.read_text())
    triggers = spec.get('on', spec.get(True))
    assert triggers, f'{workflow.name} has no trigger block'
    return list(triggers['push']['paths'])


def covered_modules(paths: list[str]) -> set[str]:
    """The src/kaya/*.py entries a path filter lists."""
    out = set()
    for entry in paths:
        m = re.fullmatch(r'src/kaya/(\w+)\.py', entry)
        if m:
            out.add(m.group(1))
    return out


@pytest.mark.parametrize('workflow, entrypoint', [
    (LAMBDA_WF, 'update_data_script'),
    (VIEWER_WF, 'viewer_app'),
])
def test_path_filter_covers_every_module_the_entrypoint_imports(workflow, entrypoint):
    needed = import_closure(entrypoint)
    listed = covered_modules(workflow_paths(workflow))
    missing = needed - listed
    assert not missing, (
        f'{workflow.name} would not redeploy when these change, even though '
        f'{entrypoint}.py imports them (directly or transitively): '
        f'{sorted(missing)}. Add them under `paths:`.'
    )


def test_the_lambda_package_step_copies_exactly_its_import_closure():
    """The zip must contain the closure — no less, and nothing extra."""
    block = re.search(r'for m in (.+?); do', _lambda_build_script(), re.S)
    assert block, 'could not find the module copy loop in deploy-lambda.yml'
    packaged = set(block.group(1).replace('\\\n', ' ').split())
    needed = import_closure('update_data_script')

    assert not (needed - packaged), (
        f'the Lambda zip is missing modules its handler imports: '
        f'{sorted(needed - packaged)} — it will fail at import time on deploy.'
    )
    assert not (packaged - needed), (
        f'the Lambda zip carries modules nothing reachable from the handler '
        f'imports: {sorted(packaged - needed)}. Drop them, or the zip grows '
        f'without anyone deciding it should.'
    )


def _lambda_build_script() -> str:
    """The shell that actually assembles the zip — commands only, no prose.

    Read from the parsed YAML rather than by slicing the raw file: the comments
    above the step legitimately mention viewer_static (explaining why it was
    removed), and a substring search over the file text matches that comment
    and fails on the explanation for the fix.
    """
    spec = yaml.safe_load(LAMBDA_WF.read_text())
    steps = spec['jobs']['deploy']['steps']
    script = '\n'.join(
        step['run'] for step in steps
        if 'run' in step and 'lambda_build' in step['run']
    )
    assert script, 'no lambda_build assembly step found in deploy-lambda.yml'
    return script


def test_the_viewer_assets_stay_out_of_the_lambda_package():
    """viewer_static/ is 5.2MB and the updater has never read a byte of it."""
    script = _lambda_build_script()
    for asset in ('viewer_static', 'viewer_templates', 'viewer_app'):
        assert asset not in script, (
            f'{asset} is back in the Lambda package. It inflates the zip and '
            f'makes every viewer change look like a data-pipeline change.'
        )


def test_the_shared_modules_appear_in_both_filters():
    """A change to genuinely shared code has to move both surfaces."""
    shared = import_closure('update_data_script') & import_closure('viewer_app')
    shared -= ALWAYS_ROOTS
    lam = covered_modules(workflow_paths(LAMBDA_WF))
    vw = covered_modules(workflow_paths(VIEWER_WF))
    for module in sorted(shared):
        assert module in lam and module in vw, (
            f'{module}.py is imported by both entrypoints but is filtered into '
            f'only one deploy stream — the other would silently run stale code.'
        )


@pytest.mark.parametrize('workflow', [LAMBDA_WF, VIEWER_WF])
def test_each_filtered_workflow_keeps_a_manual_escape_hatch(workflow):
    """A wrong filter must never be unrecoverable without editing CI."""
    spec = yaml.safe_load(workflow.read_text())
    triggers = spec.get('on', spec.get(True))
    assert 'workflow_dispatch' in triggers, (
        f'{workflow.name} is path-filtered with no workflow_dispatch. If a '
        f'filter is wrong there would be no way to deploy without a commit.'
    )


@pytest.mark.parametrize('workflow', [LAMBDA_WF, VIEWER_WF])
def test_main_does_not_deploy_anything(workflow):
    """`main` is for integrating work, not shipping it.

    Both of these used to fire on a push to `main`, so there was no way to
    merge without also deploying — a docs commit and a pipeline rewrite reached
    production by the identical route. Shipping is now an explicit act: merge
    to `prod`, or dispatch the workflow.
    """
    spec = yaml.safe_load(workflow.read_text())
    triggers = spec.get('on', spec.get(True))
    branches = triggers['push']['branches']
    assert 'main' not in branches, (
        f'{workflow.name} deploys on a push to main. That makes every merge a '
        f'production release. Deploy from prod, or by workflow_dispatch.'
    )
    assert branches == ['prod'], (
        f'{workflow.name} deploys from {branches}; expected only prod.'
    )


def test_main_is_covered_by_ci_even_though_it_no_longer_deploys():
    """Removing the deploy from main must not leave main unchecked."""
    ci = WORKFLOWS / 'ci.yml'
    assert ci.exists(), (
        'main no longer deploys, so nothing would run the test suite on it '
        'without ci.yml.'
    )
    spec = yaml.safe_load(ci.read_text())
    triggers = spec.get('on', spec.get(True))
    push = triggers['push'] or {}
    # branches-ignore rather than an allowlist: every branch is checked except
    # the deploy branch, so a new feature branch is covered without an edit.
    assert 'main' not in (push.get('branches-ignore') or []), (
        'ci.yml explicitly skips main, which is the one branch that must be '
        'green before it can be merged to prod.'
    )
    steps = ' '.join(
        step.get('run', '') for step in spec['jobs']['check']['steps']
    )
    for gate in ('ruff', 'mypy', 'pytest'):
        assert gate in steps, f'ci.yml does not run {gate}'


@pytest.mark.parametrize('workflow', [LAMBDA_WF, VIEWER_WF])
def test_the_workflow_file_triggers_its_own_redeploy(workflow):
    """Editing a deploy workflow should be deployable by that workflow."""
    paths = workflow_paths(workflow)
    assert f'.github/workflows/{workflow.name}' in paths, (
        f'{workflow.name} does not list itself under `paths:`, so a fix to the '
        f'deploy itself would not take effect until some other file changed.'
    )
