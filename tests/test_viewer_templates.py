"""Tests for the templated viewer shell.

`index.html` used to be one 2,891-line file. It is now `base.html` plus a
fragment per tab, and the two explainer tabs split again by article section.
The risk that introduces is a fragment silently failing to be included — the
page would still render, just missing a tab — so these assert the whole page
comes back through the template.
"""
import asyncio
from pathlib import Path
from typing import Any, Dict, Tuple

import pytest

from kaya import viewer_app

TABS = [
    'tab-gym-comparison',
    'tab-body-morphology',
    'tab-user-segmentation',
    'tab-data-overview',
    'tab-grading-model',
    'tab-grading-v2',
]


def _render_index() -> Tuple[int, str]:
    """GET / against the ASGI app, returning (status, body)."""
    path = '/'
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

    asyncio.run(viewer_app.app(scope, receive, send))
    return captured['status'], captured['body'].decode('utf-8')


@pytest.fixture(scope='module')
def page() -> str:
    status, body = _render_index()
    assert status == 200
    return body


@pytest.mark.parametrize('tab_id', TABS)
def test_every_tab_is_included(page: str, tab_id: str) -> None:
    """A dropped `{% include %}` would lose a whole tab without erroring."""
    assert f'id="{tab_id}"' in page, f'{tab_id} missing — check its include in base.html'


def test_tab_buttons_match_tab_panes(page: str) -> None:
    import re

    buttons = set(re.findall(r'data-tab="([a-z0-9-]+)"', page))
    panes = {t[len('tab-'):] for t in re.findall(r'id="(tab-[a-z0-9-]+)"', page)}
    assert buttons == panes, f'nav and panes disagree: {buttons ^ panes}'


def test_no_unrendered_jinja_syntax(page: str) -> None:
    """A misspelled include path renders literally rather than raising."""
    assert '{%' not in page
    assert '{{' not in page


def test_shell_elements_survive_the_split(page: str) -> None:
    assert '<title>Kaya Data Analyzer</title>' in page
    assert 'kaya-viewer-data-mode' in page
    assert 'id="v2-glossary"' in page, 'docked parameter panel lost'
    assert '/static/js/01-core.js' in page, 'the first script must still load'


def test_explainer_components_stay_out_of_the_dashboards(page: str) -> None:
    """The dashboard/explainer boundary, asserted rather than assumed.

    Dashboards are tools for exploring data; explainers are arguments about a
    method. Article components belong only to the latter.
    """
    import re

    article_markers = ('article-shell', 'class="finding', 'class="eqn', 'verdict-pill')
    dashboards = ['gym-comparison', 'body-morphology', 'user-segmentation', 'data-overview']
    for slug in dashboards:
        match = re.search(
            rf'<section id="tab-{slug}".*?(?=<section id="tab-|</main>)', page, re.S
        )
        assert match, f'could not locate the {slug} pane'
        pane = match.group(0)
        for marker in article_markers:
            assert marker not in pane, (
                f'{slug} is a dashboard and must not carry {marker!r}'
            )


def test_no_template_file_is_oversized() -> None:
    """~600 lines per file is the target the split exists to hold."""
    template_dir = Path(viewer_app.TEMPLATE_DIR)
    oversized = {
        str(path.relative_to(template_dir)): path.read_text(encoding='utf-8').count('\n')
        for path in template_dir.rglob('*.html')
        if path.read_text(encoding='utf-8').count('\n') > 700
    }
    assert not oversized, f'split these further: {oversized}'


def test_static_index_html_is_gone() -> None:
    """One source of truth for the page, not a template and a stale copy."""
    assert not (Path(viewer_app.STATIC_DIR) / 'index.html').exists()


# --- the JavaScript split -------------------------------------------------
#
# app.js was one 6,074-line file. It is now 16 classic scripts sharing one
# global scope, each a verbatim slice of the original. Because they are classic
# scripts and not modules, LOAD ORDER IS LOAD-BEARING: 09-shell.js calls
# bootstrapWithFallback() at top level before the v2 files are evaluated,
# exactly as the single file did. These tests exist to catch a shuffled,
# missing, or orphaned script — none of which would raise on their own.

def _script_srcs(page: str) -> list:
    import re

    return re.findall(r'<script src="/static/(js/[^"?]+)', page)


def test_every_referenced_script_exists(page: str) -> None:
    static = Path(viewer_app.STATIC_DIR)
    missing = [src for src in _script_srcs(page) if not (static / src).is_file()]
    assert not missing, f'base.html references scripts that do not exist: {missing}'


def test_no_script_is_orphaned(page: str) -> None:
    """A file nobody loads is dead code that still looks live."""
    static = Path(viewer_app.STATIC_DIR)
    on_disk = {
        str(path.relative_to(static)) for path in (static / 'js').rglob('*.js')
    }
    referenced = set(_script_srcs(page))
    assert on_disk == referenced, f'not loaded by base.html: {sorted(on_disk - referenced)}'


def test_scripts_load_in_numeric_order(page: str) -> None:
    """The numeric prefixes encode a real dependency order, not a preference."""
    srcs = _script_srcs(page)
    top_level = [s for s in srcs if s.count('/') == 1]
    v2 = [s for s in srcs if s.startswith('js/v2/')]
    assert top_level == sorted(top_level), 'top-level scripts are out of order'
    assert v2 == sorted(v2), 'v2 scripts are out of order'
    assert srcs == top_level + v2, 'the v2 scripts must load after the shell'


def test_no_script_is_oversized() -> None:
    static = Path(viewer_app.STATIC_DIR)
    oversized = {
        str(path.relative_to(static)): path.read_text(encoding='utf-8').count('\n')
        for path in (static / 'js').rglob('*.js')
        if path.read_text(encoding='utf-8').count('\n') > 700
    }
    assert not oversized, f'split these further: {oversized}'


def test_monolithic_app_js_is_gone() -> None:
    assert not (Path(viewer_app.STATIC_DIR) / 'app.js').exists()
