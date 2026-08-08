"""Tests for the viewer's request surface.

The rule these exist to hold: viewer routes serve precomputed payloads and never
fit a model or query a database while a request is open. That rule was violated
silently for a while — `/api/charts/body-metrics` ran a cross-validated GAM
(generalized additive model) gridsearch per request — so it is worth a test
rather than a comment.

Driven through the ASGI interface directly rather than `fastapi.testclient`,
which needs `httpx` that this project does not otherwise depend on.
"""
import asyncio
import importlib
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _load_viewer_app(env: Optional[str]):
    """Import `kaya.viewer_app` fresh under a given KAYA_VIEWER_ENV.

    The production/development split is decided at import time, so the module
    cache has to be dropped between the two cases.
    """
    os.environ.pop('KAYA_VIEWER_ENV', None)
    if env is not None:
        os.environ['KAYA_VIEWER_ENV'] = env
    for name in [n for n in sys.modules if n == 'kaya' or n.startswith('kaya.')]:
        del sys.modules[name]
    return importlib.import_module('kaya.viewer_app')


def _get(app: Any, path: str) -> int:
    """GET `path` against an ASGI app, returning the response status code."""
    scope: Dict[str, Any] = {
        'type': 'http',
        'asgi': {'version': '3.0'},
        'http_version': '1.1',
        'method': 'GET',
        'scheme': 'http',
        'path': path,
        'raw_path': path.encode(),
        'query_string': b'',
        'root_path': '',
        'headers': [(b'host', b'testserver')],
        'client': ('127.0.0.1', 1),
        'server': ('testserver', 80),
    }
    captured: Dict[str, int] = {}

    async def receive() -> Dict[str, Any]:
        return {'type': 'http.request', 'body': b'', 'more_body': False}

    async def send(message: Dict[str, Any]) -> None:
        if message['type'] == 'http.response.start':
            captured['status'] = message['status']

    asyncio.run(app(scope, receive, send))
    return captured['status']


def _get_body(app: Any, path: str) -> tuple:
    """GET `path`, returning (status, decoded body). Used where the RENDERED
    output is the thing under test, not just the status code."""
    scope: Dict[str, Any] = {
        'type': 'http',
        'asgi': {'version': '3.0'},
        'http_version': '1.1',
        'method': 'GET',
        'scheme': 'http',
        'path': path,
        'raw_path': path.encode(),
        'query_string': b'',
        'root_path': '',
        'headers': [(b'host', b'testserver')],
        'client': ('127.0.0.1', 1),
        'server': ('testserver', 80),
    }
    captured: Dict[str, Any] = {'body': b''}

    async def receive() -> Dict[str, Any]:
        return {'type': 'http.request', 'body': b'', 'more_body': False}

    async def send(message: Dict[str, Any]) -> None:
        if message['type'] == 'http.response.start':
            captured['status'] = message['status']
        elif message['type'] == 'http.response.body':
            captured['body'] += message.get('body', b'')

    asyncio.run(app(scope, receive, send))
    return captured['status'], captured['body'].decode('utf-8', 'replace')


COMPUTING_ROUTES = [
    '/api/summary',
    '/api/gyms',
    '/api/charts/time-series',
    '/api/charts/grade-distribution',
    '/api/charts/top-gyms',
    '/api/charts/body-metrics',
    '/api/charts/user-segmentation',
    '/api/charts/gym-comparison-base',
    '/api/state-preview',
]


@pytest.mark.parametrize('path', COMPUTING_ROUTES)
def test_computing_routes_absent_in_production(path: str) -> None:
    """No route that computes on request may exist in production."""
    module = _load_viewer_app('production')
    assert _get(module.app, path) == 404, (
        f'{path} is reachable in production. Routes that query SQLite or fit a '
        f'model must stay on the development-only router.'
    )


def test_index_is_served_in_production() -> None:
    module = _load_viewer_app('production')
    assert _get(module.app, '/') == 200


def test_prelim_is_served_in_production() -> None:
    """The working-notes page is a real production route, not a dev-only one.

    Added 2026-08-07 as the second top-level route this viewer has had. It
    reads a precomputed payload like every other route -- the rule it must not
    break is "never fit a model or query a database on request", not "only
    ever serve one page".
    """
    module = _load_viewer_app('production')
    assert _get(module.app, '/prelim') == 200


def test_prelim_does_not_pull_the_main_bundle() -> None:
    """The whole reason /prelim is its own route is that it stays small.

    `/` renders four tabs into one document and its Findings tab alone fetches
    v2_posterior.json at ~2.9MB. If this page starts including base.html's
    script list or research.css, that saving is silently gone and nobody finds
    out from a passing test suite.
    """
    template = (ROOT / 'src' / 'kaya' / 'viewer_templates' / 'prelim.html').read_text()
    # Parse the tags, do not substring-search the file. A bare `'research.css'
    # not in template` matches this test's own reason for existing, written in
    # the template's header comment -- the same way the Lambda packaging test
    # once matched the word "viewer_static" in its own explanation.
    styles = re.findall(r'<link[^>]+rel="stylesheet"[^>]+href="([^"]+)"', template)
    # KaTeX is allowed: the equations here are meant to be typeset identically
    # to the Findings tab's, and base.html loads this exact URL. research.css
    # is the thing being kept out -- it carries the whole four-tab shell.
    allowed_styles = {
        '/static/tokens.css',
        '/static/prelim.css',
        'https://cdn.jsdelivr.net/npm/katex@0.16.9/dist/katex.min.css',
    }
    assert set(styles) <= allowed_styles, (
        f'prelim.html grew a stylesheet: {styles}. research.css in particular '
        'carries the whole four-tab article shell this page does not use.')
    scripts = re.findall(r'<script src="(/static/[^"]+)"', template)
    assert set(scripts) <= {'/static/js/01-core.js',
                            '/static/js/v2/17-prelim.js',
                            '/static/js/v2/18-ability.js',
                            '/static/js/v2/19-ability-sim.js',
                            # Dev-only; gated behind `{% if editable %}` and
                            # never sent to production. See the edit-mode tests.
                            '/static/js/v2/20-edit.js',
                            '/static/js/v2/21-review.js'}, (
        f'prelim.html grew a script dependency: {scripts}. Every file added '
        'here ships to a page whose point is being cheap.')


def test_prelim_reads_only_its_own_payload() -> None:
    """One payload, and it is the small one."""
    static = ROOT / 'src' / 'kaya' / 'viewer_static' / 'js' / 'v2'
    fetched = set()
    for name in ('17-prelim.js', '18-ability.js', '19-ability-sim.js'):
        fetched |= set(re.findall(r"fetch\('(/static/[^']+)'",
                                  (static / name).read_text()))
    assert fetched == {'/static/v2_prelim.json'}, (
        f'/prelim fetches {fetched}; it must read v2_prelim.json alone')


def test_computing_routes_present_in_development() -> None:
    """The dev loop keeps them — the gating is by environment, not deletion."""
    module = _load_viewer_app(None)
    assert _get(module.app, '/api/gyms') == 200


def test_production_cors_is_not_wildcard() -> None:
    """An empty allow-list used to fall through to `['*']` in production."""
    module = _load_viewer_app('production')
    assert module.ALLOWED_ORIGINS, 'production must not fall back to a wildcard origin'
    assert '*' not in module.ALLOWED_ORIGINS


def test_viewer_port_matches_the_assigned_port() -> None:
    """8010 is kaya's port in the workspace registry; 8000 is the main site's."""
    module = _load_viewer_app(None)
    assert module.VIEWER_PORT == 8010


# --- editable page copy -------------------------------------------------
#
# The rule these exist to hold: the SAVE path is development-only. The deployed
# viewer is public and has no auth gate, so a write endpoint on it would let
# anyone rewrite the page. That is a property of route registration, which is
# exactly the kind of thing that breaks silently.


def _post(app: Any, path: str, body: bytes) -> Dict[str, Any]:
    """POST JSON to an ASGI app, returning its status and response body."""
    scope: Dict[str, Any] = {
        'type': 'http',
        'asgi': {'version': '3.0'},
        'http_version': '1.1',
        'method': 'POST',
        'scheme': 'http',
        'path': path,
        'raw_path': path.encode(),
        'query_string': b'',
        'root_path': '',
        'headers': [
            (b'host', b'testserver'),
            (b'content-type', b'application/json'),
            (b'content-length', str(len(body)).encode()),
        ],
        'client': ('127.0.0.1', 1),
        'server': ('testserver', 80),
    }
    captured: Dict[str, Any] = {'body': b''}

    async def receive() -> Dict[str, Any]:
        return {'type': 'http.request', 'body': body, 'more_body': False}

    async def send(message: Dict[str, Any]) -> None:
        if message['type'] == 'http.response.start':
            captured['status'] = message['status']
        elif message['type'] == 'http.response.body':
            captured['body'] += message.get('body', b'')

    asyncio.run(app(scope, receive, send))
    return captured


def test_the_copy_save_route_does_not_exist_in_production() -> None:
    """The whole safety story for edit mode is this assertion.

    kaya.peterwilliams.dev is deliberately public with no auth gate. A save
    endpoint there would let any visitor rewrite the page's text, so it lives
    on the dev-only router and must stay there.
    """
    prod = _load_viewer_app('production')
    body = b'{"updates": {"ab-lede": "defaced"}}'
    assert _post(prod.app, '/api/prelim-copy', body)['status'] == 404


def test_the_copy_save_route_exists_in_development(tmp_path, monkeypatch) -> None:
    # Pointed at a temp dir: this route WRITES, and the real store is a
    # git-tracked file holding the author's actual words.
    dev = _load_viewer_app('development')
    monkeypatch.setattr(dev.viewer_copy, 'CONTENT_DIR', tmp_path)
    assert _post(dev.app, '/api/prelim-copy', b'{"updates": {}}')['status'] == 200


def test_production_renders_no_edit_affordance() -> None:
    """No toolbar and no editor script in the deployed page's actual HTML.

    Asserted against the RENDERED response, not against the template source: a
    template-text check cannot tell an `{% if editable %}` branch that is taken
    from one that is not, which is the only thing that matters here. Shipping
    the button would be an invitation with no lock behind it -- the route it
    posts to does not exist in production.
    """
    prod = _load_viewer_app('production')
    status, body = _get_body(prod.app, '/prelim')
    assert status == 200
    for marker in ('pm-edit-bar', '20-edit.js'):
        assert marker not in body, f'production /prelim still renders {marker}'
    # `data-copy` DOES render in production, and should: it is an inert
    # attribute, and it is how the stored copy gets into the page at all.
    # Without the toolbar and the editor script it grants nothing.
    assert 'data-copy=' in body


def test_development_renders_the_edit_affordance() -> None:
    """The gate is by environment, not by deletion -- it works locally."""
    dev = _load_viewer_app('development')
    status, body = _get_body(dev.app, '/prelim')
    assert status == 200
    assert 'pm-edit-bar' in body
    assert '/static/js/v2/20-edit.js' in body
    assert body.count('data-copy=') >= 15


def test_every_editable_block_has_a_unique_key() -> None:
    """Two blocks sharing a key would silently overwrite each other on save."""
    template = (ROOT / 'src' / 'kaya' / 'viewer_templates' / 'prelim.html').read_text()
    keys = re.findall(r"\{%\s*call ed\('([^']+)'", template)
    assert keys, 'no editable blocks found; the ed() macro is unused'
    assert len(keys) == len(set(keys)), (
        f'duplicate copy keys: {sorted(k for k in set(keys) if keys.count(k) > 1)}')


@pytest.mark.parametrize('route', ['/api/prelim-notes', '/api/prelim-layout'])
def test_review_write_routes_do_not_exist_in_production(route: str) -> None:
    """Same boundary as the copy route: no write endpoint on a public page."""
    prod = _load_viewer_app('production')
    assert _post(prod.app, route, b'{}')['status'] == 404


def test_production_ships_no_review_notes() -> None:
    """Notes are messages ABOUT the page, not part of it.

    Chart layout is different and deliberately does render in production: once
    a figure has been sized, that is the figure.
    """
    prod = _load_viewer_app('production')
    _, body = _get_body(prod.app, '/prelim')
    assert 'PM_INITIAL_NOTES = []' in body or 'PM_INITIAL_NOTES' not in body
    assert 'pm-note-add' not in body


def test_the_block_route_does_not_exist_in_production() -> None:
    prod = _load_viewer_app('production')
    assert _post(prod.app, '/api/prelim-blocks', b'{"blocks": []}')['status'] == 404


def test_added_blocks_render_in_production() -> None:
    """Unlike notes, an added block IS page content and must ship.

    Only the ability to add one is development-only. Rendered through the same
    `slot()` macro in both environments, with its text coming from the copy
    store, so it goes through the sanitiser like every other block.
    """
    prod = _load_viewer_app('production')
    _, body = _get_body(prod.app, '/prelim')
    assert 'slot(' not in body, 'the macro leaked into the output'
    # The template must call slot() after every editable block and every chart,
    # or an added block would have nowhere to render.
    template = (ROOT / 'src' / 'kaya' / 'viewer_templates' / 'prelim.html').read_text()
    charts = set(re.findall(r'<div id="(ab-[a-z-]+)" class="pm-chart', template))
    slotted = set(re.findall(r"\{\{ slot\('([^']+)'\) \}\}", template))
    assert charts <= slotted, f'charts with no insertion slot: {charts - slotted}'


def test_the_hidden_route_does_not_exist_in_production() -> None:
    prod = _load_viewer_app('production')
    assert _post(prod.app, '/api/prelim-hidden', b'{"hidden": []}')['status'] == 404


def test_a_deleted_drafted_block_is_gone_in_production(monkeypatch) -> None:
    """Deleting drafted copy hides it; production must render none of it.

    The dev page still renders it, struck through, so it can be restored --
    the template is the only copy of those words, and a delete with no way
    back would lose them.
    """
    template = (ROOT / 'src' / 'kaya' / 'viewer_templates' / 'prelim.html').read_text()
    key = re.findall(r"\{%\s*call ed\('([^']+)'", template)[0]

    # Patched on the FRESHLY imported module: _load_viewer_app drops the whole
    # `kaya` package from sys.modules, so a patch applied before it is thrown
    # away with the old module object.
    prod = _load_viewer_app('production')
    monkeypatch.setattr(prod.viewer_copy, 'load_hidden', lambda page: [key])
    _, prod_body = _get_body(prod.app, '/prelim')
    assert f'data-copy="{key}"' not in prod_body

    dev = _load_viewer_app('development')
    monkeypatch.setattr(dev.viewer_copy, 'load_hidden', lambda page: [key])
    _, dev_body = _get_body(dev.app, '/prelim')
    assert f'data-copy="{key}"' in dev_body
    assert 'pm-deleted' in dev_body


def test_an_inline_note_never_reaches_the_public_page(monkeypatch) -> None:
    """`@claude ...@` is a message to the agent, not copy.

    Development marks it so it cannot be missed; production removes it, so a
    note forgotten in a paragraph is not a note published to the internet.
    """
    template = (ROOT / 'src' / 'kaya' / 'viewer_templates' / 'prelim.html').read_text()
    key = re.findall(r"\{%\s*call ed\('([^']+)'", template)[0]
    stored = {key: 'Kept words. @claude SECRETNOTE@ More kept words.'}

    prod = _load_viewer_app('production')
    monkeypatch.setattr(prod.viewer_copy, 'load_copy', lambda page: dict(stored))
    _, prod_body = _get_body(prod.app, '/prelim')
    assert 'SECRETNOTE' not in prod_body
    assert 'Kept words.' in prod_body

    dev = _load_viewer_app('development')
    monkeypatch.setattr(dev.viewer_copy, 'load_copy', lambda page: dict(stored))
    _, dev_body = _get_body(dev.app, '/prelim')
    assert 'pm-inline-note">SECRETNOTE</mark>' in dev_body
