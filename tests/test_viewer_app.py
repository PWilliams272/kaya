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
import sys
from typing import Any, Dict, Optional

import pytest


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
