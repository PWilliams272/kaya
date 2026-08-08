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
    'tab-grading-current',
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
    """A file nobody loads is dead code that still looks live.

    Checked across EVERY template, not just base.html. Since 2026-08-07 the
    viewer serves a second top-level page (/prelim) which loads its own single
    script and deliberately none of the `/` bundle, so "referenced by
    base.html" stopped being the same question as "referenced at all".
    """
    static = Path(viewer_app.STATIC_DIR)
    on_disk = {
        str(path.relative_to(static)) for path in (static / 'js').rglob('*.js')
    }
    templates = Path(viewer_app.TEMPLATE_DIR)
    referenced = set()
    for tpl in templates.rglob('*.html'):
        referenced |= set(_script_srcs(tpl.read_text()))
    assert on_disk <= referenced, (
        f'loaded by no template at all: {sorted(on_disk - referenced)}')


def test_scripts_load_in_numeric_order(page: str) -> None:
    """The numeric prefixes encode a real dependency order, not a preference.

    Groups load shell -> v2 -> current -> findings, because each depends on the
    one before: both explainer pages call the v2 renderers with their own
    element ids rather than duplicating them, and the findings page reuses
    helpers defined in the current page's driver.
    """
    srcs = _script_srcs(page)
    groups = ['js/', 'js/v2/', 'js/current/', 'js/findings/']
    expected: list = []
    for prefix in groups:
        members = [s for s in srcs if s.startswith(prefix) and s.count('/') == prefix.count('/')]
        assert members == sorted(members), f'{prefix} scripts are out of order'
        expected += members
    assert srcs == expected, (
        'script groups must load shell -> v2 -> current -> findings; '
        f'got {srcs}'
    )


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


# --- the current Grading Model page vs its archives ---------------------
#
# The current page is a distillation: it states the settled conclusions, and the
# full working record stays on the archived tabs. That only holds if the
# archives keep rendering, and if the new page keeps its own id namespace —
# two panes cannot both own `v2-*`.

GM_SECTIONS = [
    'gm-headline',
    'gm-model',
    'gm-gyms',
    'gm-height',
    'gm-time',
    'gm-cv',
    'gm-marginal',
    'gm-samplers',
    'gm-negatives',
    'gm-diagnostics',
    'gm-provenance',
]


GF_SECTIONS = [
    'gf-headline',
    'gf-gyms',
    'gf-model',
    'gf-body',
    'gf-time',
    'gf-evidence',
]


@pytest.mark.parametrize('section_id', GM_SECTIONS)
def test_current_page_sections_present(page: str, section_id: str) -> None:
    assert f'id="{section_id}"' in page


@pytest.mark.parametrize('section_id', GF_SECTIONS)
def test_findings_page_sections_present(page: str, section_id: str) -> None:
    assert f'id="{section_id}"' in page


def test_findings_page_is_shorter_than_the_detailed_one(page: str) -> None:
    """The whole premise of a second page is that it is a shorter read.

    Not a style preference: if the presentation cut grows to match the detailed
    one there is no reason for it to exist, and two full-length pages quoting
    the same fit is a maintenance trap rather than a feature.
    """
    import re

    def pane(slug: str) -> str:
        m = re.search(rf'<section id="tab-{slug}".*?(?=<section id="tab-|</main>)',
                      page, re.S)
        assert m, f'no pane for {slug}'
        return m.group(0)

    findings, detail = pane('grading-findings'), pane('grading-current')
    assert len(findings) < 0.75 * len(detail), (
        f'findings pane is {len(findings)} chars against the detailed '
        f"page's {len(detail)} — it has stopped being a distillation")


def test_findings_page_keeps_the_headliners(page: str) -> None:
    """Equations, sliders, both headline results, and the honest caveats."""
    import re

    m = re.search(r'<section id="tab-grading-findings".*?(?=<section id="tab-|</main>)',
                  page, re.S)
    assert m
    body = m.group(0)
    assert body.count('class="eqn"') >= 5, 'the model must keep its equations'
    assert 'id="gf-form-cards"' in body, 'the functional forms need their sliders'
    assert 'id="gf-gym-chart"' in body, 'gym comparison is a headline result'
    assert 'id="gf-fitted-height"' in body, 'height/ape is a headline result'
    assert 'id="gf-adv-chart"' in body, 'improvement over time must survive'
    assert 'id="gf-glossary"' in body, 'equations need their docked symbol panel'
    # A presentation cut that dropped every caveat would be a brochure.
    assert body.count('verdict-pill') >= 5, 'the open and null results must survive'


def test_findings_tab_is_registered_in_the_shell() -> None:
    """A pane nobody can reach renders nothing and fails no other test."""
    shell = (Path(viewer_app.STATIC_DIR) / 'js' / '09-shell.js').read_text()
    assert "'grading-findings'" in shell, 'tab is not in TAB_NAMES'
    assert 'renderFindingsTab()' in shell, 'tab activates no renderer'


def test_current_page_does_not_reuse_archived_ids(page: str) -> None:
    """Duplicate ids silently break getElementById — this repo has been bitten."""
    import re

    all_ids = re.findall(r'id="([^"]+)"', page)
    duplicates = {i for i in all_ids if all_ids.count(i) > 1}
    assert not duplicates, f'duplicate element ids: {sorted(duplicates)}'


def test_both_archives_are_labelled_as_archives(page: str) -> None:
    import re

    for slug in ['grading-model', 'grading-v2']:
        button = re.search(rf'data-tab="{slug}"[^>]*>(.*?)</button>', page)
        assert button, f'no tab button for {slug}'
        assert 'Archive' in button.group(1), f'{slug} tab is not labelled as an archive'
        pane = re.search(rf'<section id="tab-{slug}".*?(?=<section id="tab-|</main>)', page, re.S)
        assert pane and 'archive-banner' in pane.group(0), f'{slug} pane has no archive banner'


def test_current_tab_is_not_labelled_as_an_archive(page: str) -> None:
    import re

    button = re.search(r'data-tab="grading-current"[^>]*>(.*?)</button>', page)
    assert button and 'Archive' not in button.group(1)
    assert 'tab-archived' not in button.group(0)


def test_current_page_carries_verdicts_and_equations(page: str) -> None:
    """A distillation that dropped the negatives would not be a distillation."""
    import re

    pane = re.search(r'<section id="tab-grading-current".*?(?=<section id="tab-|</main>)', page, re.S)
    assert pane
    body = pane.group(0)
    assert body.count('verdict-pill') >= 8, 'negative results must survive as verdicts'
    assert body.count('class="eqn"') >= 5, 'the model must keep its functional form'
    assert 'id="gm-glossary"' in body, 'equations need their docked parameter panel'
    assert 'scripts/build_v2_results.py' in body, 'provenance citations must survive'


def test_bootstrap_waits_for_every_script() -> None:
    """The saved-tab restore calls renderers defined in later scripts.

    In the single app.js these were hoisted function declarations in the same
    file. Across separate classic scripts they are not, and calling
    `bootstrapWithFallback()` at top level threw `renderCurrentTab is not
    defined` whenever the last-used tab was an explainer. DOMContentLoaded is
    the gate that restores the guarantee.
    """
    shell = (Path(viewer_app.STATIC_DIR) / 'js' / '09-shell.js').read_text(encoding='utf-8')
    assert "addEventListener('DOMContentLoaded', startViewer" in shell
    for line in shell.splitlines():
        assert not line.startswith('bootstrapWithFallback('), (
            'bootstrap must not run before the later scripts are evaluated'
        )


def test_v2_renderers_are_not_hardcoded_to_the_archive_pane() -> None:
    """Both explainer panes share these renderers, so ids resolve through v2El.

    A reintroduced literal `v2-` id would draw the current Grading Model tab's
    figure into the archived pane, or into nothing at all.
    """
    import re

    static = Path(viewer_app.STATIC_DIR)
    offenders = {}
    for path in sorted((static / 'js' / 'v2').glob('*.js')):
        text = path.read_text(encoding='utf-8')
        hits = [
            m.group(0)
            for m in re.finditer(r"""getElementById\(\s*['"`]v2-|id="v2-|#tab-grading-v2""", text)
        ]
        # The namespace's own default is the one legitimate literal.
        hits = [h for h in hits if h not in ("V2_NS = 'v2-'",)]
        if hits:
            offenders[path.name] = hits
    assert not offenders, f'hardcoded archive ids: {offenders}'


# --- the gym directory on Data Overview -------------------------------------

def test_data_overview_carries_the_gym_directory(page: str) -> None:
    """The full gym list, its controls, and the renderer that fills it."""
    for element_id in ['gym-directory-table', 'gym-directory-search',
                       'gym-directory-scope', 'gym-directory-note',
                       'gym-directory-foot']:
        assert f'id="{element_id}"' in page, f'missing {element_id}'
    shell = (Path(viewer_app.STATIC_DIR) / 'js' / '09-shell.js').read_text()
    assert 'renderGymDirectory()' in shell, 'directory is never rendered'
    assert 'bindGymDirectory()' in shell, 'directory controls are never bound'


# --- the independent-sampler section ----------------------------------------

def test_current_page_carries_the_emcee_section(page: str) -> None:
    """Every element 11-emcee.js writes into has to exist on the page.

    The renderer hides its whole section when the payload is missing, which is
    the right behaviour for an un-built payload and exactly the wrong failure
    mode for a typo in an id: the section would silently disappear and nothing
    would say why.
    """
    for element_id in ['gm-emcee-run', 'gm-emcee-param', 'gm-emcee-verdict',
                       'gm-emcee-trace', 'gm-emcee-dens', 'gm-emcee-runmean',
                       'gm-emcee-acf', 'gm-emcee-walkers', 'gm-emcee-table',
                       'gm-emcee-discrepancy', 'gm-emcee-gyms',
                       'gm-emcee-converged', 'gm-emcee-gap-note',
                       'gm-emcee-gyms-note']:
        assert f'id="{element_id}"' in page, f'missing {element_id}'
    # The cross-sampler table moved onto this page with it; it used to exist
    # only on the archived pane.
    assert 'id="gm-crosssampler-table"' in page
    assert 'id="gm-crosssampler-note"' in page


def test_emcee_renderer_is_wired_into_the_current_tab() -> None:
    """A section nobody calls renders as an empty shell, not an error."""
    js = (Path(viewer_app.STATIC_DIR) / 'js' / 'current'
          / '01-grading-current.js').read_text()
    assert 'renderV2Emcee()' in js, 'the emcee section is never rendered'
    assert 'renderV2Samplers()' in js, 'the cross-sampler table is never filled'


def test_callout_bolds_are_not_all_block_level() -> None:
    """`.callout b` as a bare selector broke every mid-sentence bold.

    Callouts open with a bolded lead that reads as a heading, so that one is
    block. A bare `.callout b` extended it to emphasis inside a sentence and
    shredded the paragraph into one line per bold phrase.
    """
    css = (Path(viewer_app.STATIC_DIR) / 'research.css').read_text()
    assert '.callout b {' not in css, 'the bare selector is back'
    assert '.callout > div > b:first-child' in css
