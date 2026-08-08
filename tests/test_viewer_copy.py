"""Tests for the editable-copy store and its sanitiser.

Stored copy is rendered UNESCAPED -- that is the point, it carries the author's
emphasis and inline maths markup -- so the sanitiser is the only thing standing
between the JSON file and the rendered page. It is an allowlist, and these
tests pin what it lets through.

The other half of the safety story (the save route existing only outside
production) is in test_viewer_app.py, because it is a property of routing.
"""
import json

import pytest

from kaya import viewer_copy

# --- the sanitiser ------------------------------------------------------

@pytest.mark.parametrize('raw,expected', [
    # Inline markup an author actually needs survives untouched.
    ('<b>bold</b> and <i>ital</i>', '<b>bold</b> and <i>ital</i>'),
    ('V6<sub>link</sub>', 'V6<sub>link</sub>'),
    ('line<br>break', 'line<br>break'),
    ('<code>kappa</code>', '<code>kappa</code>'),
    # Unknown tags are UNWRAPPED, not deleted: losing the author's words to a
    # stray tag is worse than losing the tag.
    ('<div class="x">kept</div>', 'kept'),
    ('<h1>heading text</h1>', 'heading text'),
    # Attributes do not survive, so no event handlers can.
    ('<b onclick="evil()">x</b>', '<b>x</b>'),
    ('<span style="position:fixed">x</span>', '<span>x</span>'),
    # Script-like tags lose their CONTENTS too -- unwrapping them would print
    # the code as visible page text.
    ('<script>alert(1)</script>after', 'after'),
    ('<style>body{display:none}</style>after', 'after'),
    ('<iframe src="//evil"></iframe>after', 'after'),
    # Bare text is escaped, so copy can contain angle brackets safely.
    ('a < b & c', 'a &lt; b &amp; c'),
    # Unbalanced markup is closed rather than allowed to leak into the page.
    ('<b>unclosed', '<b>unclosed</b>'),
    ('</b>stray', 'stray'),
    ('', ''),
])
def test_sanitiser_allowlist(raw: str, expected: str) -> None:
    assert viewer_copy.sanitize_html(raw) == expected


@pytest.mark.parametrize('href,kept', [
    ('https://example.com/x', True),
    ('http://example.com', True),
    ('mailto:a@b.c', True),
    ('/local/path', True),
    ('#anchor', True),
    ('javascript:alert(1)', False),
    ('JaVaScRiPt:alert(1)', False),
    ('data:text/html;base64,PHNjcmlwdD4=', False),
    ('vbscript:msgbox', False),
])
def test_only_safe_link_schemes_survive(href: str, kept: bool) -> None:
    """`href` is the one attribute that survives, so its scheme is checked."""
    out = viewer_copy.sanitize_html(f'<a href="{href}">t</a>')
    assert out.startswith('<a href=' if kept else '<a>'), out
    assert 'javascript' not in out.lower()
    assert 'data:' not in out.lower()


def test_a_quote_cannot_break_out_of_the_href_attribute() -> None:
    """The classic attribute-injection shape, defused by escaping.

    The payload's own quotes become `&quot;`, so `onmouseover=...` ends up
    INSIDE the href value as inert text rather than becoming a second
    attribute. What is asserted is therefore the tag's shape -- exactly two
    real quote characters, i.e. one attribute -- not the absence of the word.
    """
    out = viewer_copy.sanitize_html('<a href=\'/x" onmouseover="evil()\'>t</a>')
    assert out.count('"') == 2, out
    assert '&quot;' in out, out
    assert out.startswith('<a href="') and out.endswith('>t</a>')


# --- the store ----------------------------------------------------------

@pytest.fixture()
def store(tmp_path, monkeypatch):
    """Point the store at a temp dir, so tests never touch the real copy."""
    monkeypatch.setattr(viewer_copy, 'CONTENT_DIR', tmp_path)
    return tmp_path


def test_save_then_load_round_trips(store) -> None:
    viewer_copy.save_copy('demo', {'ab-lede': '<b>hello</b>'})
    assert viewer_copy.load_copy('demo') == {'ab-lede': '<b>hello</b>'}


def test_save_merges_rather_than_replacing(store) -> None:
    """One edited block must not wipe every other block on the page."""
    viewer_copy.save_copy('demo', {'one': 'first', 'two': 'second'})
    viewer_copy.save_copy('demo', {'two': 'changed'})
    assert viewer_copy.load_copy('demo') == {'one': 'first', 'two': 'changed'}


def test_an_empty_value_reverts_to_the_template_default(store) -> None:
    """Deleting the key, not storing an empty string: an empty string would
    pin the block blank forever, with no way back to the drafted copy."""
    viewer_copy.save_copy('demo', {'one': 'text'})
    viewer_copy.save_copy('demo', {'one': '   '})
    assert viewer_copy.load_copy('demo') == {}


def test_saved_html_is_sanitised_on_the_way_in(store) -> None:
    viewer_copy.save_copy('demo', {'one': '<b>ok</b><script>bad()</script>'})
    assert json.loads((store / 'demo_copy.json').read_text())['one'] == '<b>ok</b>'


def test_hand_edited_html_is_sanitised_on_the_way_out(store) -> None:
    """The file is meant to be hand-editable, and a hand edit skips save_copy.

    So load also sanitises -- otherwise pasting a <script> into the JSON would
    put it straight onto a public page.
    """
    (store / 'demo_copy.json').write_text(
        json.dumps({'one': '<img src=x onerror=alert(1)>text'}))
    assert viewer_copy.load_copy('demo') == {'one': 'text'}


@pytest.mark.parametrize('bad_key', ['Has Space', 'UPPER', '../escape', 'a' * 65, ''])
def test_bad_keys_are_rejected(store, bad_key: str) -> None:
    with pytest.raises(ValueError):
        viewer_copy.save_copy('demo', {bad_key: 'x'})


def test_a_key_cannot_escape_the_content_directory() -> None:
    with pytest.raises(ValueError):
        viewer_copy.copy_path('../../etc/passwd')


def test_oversized_copy_is_rejected_not_truncated(store) -> None:
    with pytest.raises(ValueError):
        viewer_copy.save_copy('demo', {'one': 'x' * (viewer_copy.MAX_VALUE_CHARS + 1)})


def test_non_string_values_are_rejected(store) -> None:
    with pytest.raises(ValueError):
        viewer_copy.save_copy('demo', {'one': {'nested': 'object'}})


def test_a_corrupt_store_reads_as_empty_rather_than_crashing(store) -> None:
    """A half-written or hand-broken file must not take the page down."""
    (store / 'demo_copy.json').write_text('{not json')
    assert viewer_copy.load_copy('demo') == {}


def test_a_missing_store_reads_as_empty(store) -> None:
    assert viewer_copy.load_copy('never-written') == {}


def test_the_store_is_written_sorted_for_readable_diffs(store) -> None:
    """It is committed like source, so its diffs have to be readable."""
    viewer_copy.save_copy('demo', {'zeta': 'z', 'alpha': 'a'})
    body = (store / 'demo_copy.json').read_text()
    assert body.index('"alpha"') < body.index('"zeta"')
    assert body.endswith('\n')


def test_the_real_prelim_store_is_valid() -> None:
    """The committed file must parse, or the page silently loses all its copy."""
    path = viewer_copy.copy_path('prelim')
    assert path.exists(), f'{path} is missing; the page falls back to drafts'
    assert isinstance(json.loads(path.read_text()), dict)


# --- review notes -------------------------------------------------------

def test_notes_round_trip(store) -> None:
    viewer_copy.save_notes('demo', [
        {'id': 'n1', 'anchor': 'ab-landscape', 'text': 'move the C label left'}])
    assert viewer_copy.load_notes('demo') == [
        {'id': 'n1', 'anchor': 'ab-landscape',
         'text': 'move the C label left', 'done': False}]


def test_saving_notes_replaces_rather_than_merging(store) -> None:
    """Notes are managed as a set by the page, so a merge makes deletion
    impossible to express. Copy merges; notes do not."""
    viewer_copy.save_notes('demo', [{'id': 'n1', 'anchor': 'a', 'text': 'one'},
                                    {'id': 'n2', 'anchor': 'a', 'text': 'two'}])
    viewer_copy.save_notes('demo', [{'id': 'n1', 'anchor': 'a', 'text': 'one'}])
    assert [n['id'] for n in viewer_copy.load_notes('demo')] == ['n1']


def test_note_text_is_stored_raw_not_escaped(store) -> None:
    """It is only ever shown in a textarea value, so escaping here would just
    make `&lt;` appear in the author's own note next time they opened it."""
    viewer_copy.save_notes('demo', [
        {'id': 'n1', 'anchor': 'a', 'text': 'use < instead of &'}])
    assert viewer_copy.load_notes('demo')[0]['text'] == 'use < instead of &'


def test_one_malformed_note_does_not_discard_the_others(store) -> None:
    """Losing every note to one bad record is the wrong failure mode."""
    saved = viewer_copy.save_notes('demo', [
        {'id': 'n1', 'anchor': 'a', 'text': 'kept'},
        {'id': 'BAD ID', 'anchor': 'a', 'text': 'dropped'},
        'not even a dict',
        {'id': 'n3', 'anchor': 'a', 'text': '   '},          # empty -> dropped
        {'id': 'n4', 'anchor': 'a', 'text': 'also kept'},
    ])
    assert [n['id'] for n in saved] == ['n1', 'n4']


def test_too_many_notes_is_rejected(store) -> None:
    with pytest.raises(ValueError):
        viewer_copy.save_notes('demo', [
            {'id': f'n{i}', 'anchor': 'a', 'text': 'x'}
            for i in range(viewer_copy.MAX_NOTES + 1)])


# --- chart layout -------------------------------------------------------

def test_layout_round_trips_height_and_moves(store) -> None:
    viewer_copy.save_layout('demo', {
        'ab-landscape': {'height': 340, 'moves': {'annotation:0': {'x': 6.9, 'y': 0.5}}}})
    assert viewer_copy.load_layout('demo') == {
        'ab-landscape': {'height': 340, 'moves': {'annotation:0': {'x': 6.9, 'y': 0.5}}}}


def test_layout_merges_so_a_resize_keeps_label_positions(store) -> None:
    viewer_copy.save_layout('demo', {'c1': {'moves': {'annotation:0': {'x': 1.0}}}})
    viewer_copy.save_layout('demo', {'c1': {'height': 400}})
    got = viewer_copy.load_layout('demo')['c1']
    assert got['height'] == 400
    assert got['moves'] == {'annotation:0': {'x': 1.0}}


@pytest.mark.parametrize('height', [10, 5000, 'tall', None])
def test_out_of_range_heights_are_dropped(store, height) -> None:
    """Dropped, not clamped: an out-of-range height is how the page asks for
    the default back (the reset button sends 0)."""
    viewer_copy.save_layout('demo', {'c1': {'height': height}})
    assert 'height' not in viewer_copy.load_layout('demo').get('c1', {})


def test_layout_stores_numbers_only(store) -> None:
    """This store can move a label. It must never be able to introduce one,
    or rename one -- that is what the copy store is for, and that sanitises."""
    viewer_copy.save_layout('demo', {'c1': {'moves': {
        'annotation:0': {'x': 1.0, 'text': '<script>x</script>', 'bgcolor': 'red'}}}})
    assert viewer_copy.load_layout('demo')['c1']['moves'] == {'annotation:0': {'x': 1.0}}


def test_layout_rejects_a_non_object(store) -> None:
    with pytest.raises(ValueError):
        viewer_copy.save_layout('demo', ['not', 'an', 'object'])


def test_the_real_review_stores_are_valid() -> None:
    """Committed like source; unparseable means the page silently loses them."""
    for name, kind in (('prelim_notes.json', list), ('prelim_layout.json', dict)):
        path = viewer_copy.CONTENT_DIR / name
        assert path.exists(), f'{path} is missing'
        assert isinstance(json.loads(path.read_text()), kind)


# --- author-added blocks ------------------------------------------------

def test_blocks_round_trip(store) -> None:
    viewer_copy.save_blocks('demo', [
        {'id': 'b1', 'anchor': 'ab-landscape', 'kind': 'h3', 'order': 0}])
    assert viewer_copy.load_blocks('demo') == [
        {'id': 'b1', 'anchor': 'ab-landscape', 'kind': 'h3', 'order': 0}]


def test_a_block_holds_no_text(store) -> None:
    """Structure here, words in the copy store under the same id.

    That split is what lets a new block be edited, sanitised and saved by
    exactly the same path as a drafted one -- and it is why there is no way to
    inject markup through this route.
    """
    saved = viewer_copy.save_blocks('demo', [{
        'id': 'b1', 'anchor': 'a', 'kind': 'p',
        'text': '<script>alert(1)</script>', 'style': 'position:fixed'}])
    assert saved == [{'id': 'b1', 'anchor': 'a', 'kind': 'p', 'order': 0}]


def test_blocks_sort_by_anchor_then_order(store) -> None:
    """Two blocks under one anchor must come back in the order they were made."""
    viewer_copy.save_blocks('demo', [
        {'id': 'b2', 'anchor': 'z', 'kind': 'p', 'order': 1},
        {'id': 'b1', 'anchor': 'z', 'kind': 'h3', 'order': 0},
        {'id': 'b3', 'anchor': 'a', 'kind': 'p', 'order': 0},
    ])
    assert [b['id'] for b in viewer_copy.load_blocks('demo')] == ['b3', 'b1', 'b2']


@pytest.mark.parametrize('kind', ['script', 'div', 'iframe', 'h1', ''])
def test_only_prose_element_kinds_are_allowed(store, kind: str) -> None:
    """The kind becomes a tag name in the template, so it is an allowlist."""
    saved = viewer_copy.save_blocks('demo', [
        {'id': 'b1', 'anchor': 'a', 'kind': kind}])
    assert saved == []


def test_duplicate_block_ids_are_collapsed(store) -> None:
    """Two blocks with one id would render the same copy twice."""
    saved = viewer_copy.save_blocks('demo', [
        {'id': 'b1', 'anchor': 'a', 'kind': 'p', 'order': 0},
        {'id': 'b1', 'anchor': 'b', 'kind': 'h3', 'order': 0},
    ])
    assert len(saved) == 1


def test_blocks_group_by_anchor_for_the_template(store) -> None:
    viewer_copy.save_blocks('demo', [
        {'id': 'b1', 'anchor': 'a', 'kind': 'p', 'order': 0},
        {'id': 'b2', 'anchor': 'a', 'kind': 'p', 'order': 1},
        {'id': 'b3', 'anchor': 'z', 'kind': 'h3', 'order': 0},
    ])
    grouped = viewer_copy.blocks_by_anchor('demo')
    assert sorted(grouped) == ['a', 'z']
    assert [b['id'] for b in grouped['a']] == ['b1', 'b2']


def test_saving_blocks_replaces_so_deletion_works(store) -> None:
    viewer_copy.save_blocks('demo', [{'id': 'b1', 'anchor': 'a', 'kind': 'p'},
                                     {'id': 'b2', 'anchor': 'a', 'kind': 'p'}])
    viewer_copy.save_blocks('demo', [{'id': 'b1', 'anchor': 'a', 'kind': 'p'}])
    assert [b['id'] for b in viewer_copy.load_blocks('demo')] == ['b1']


def test_too_many_blocks_is_rejected(store) -> None:
    with pytest.raises(ValueError):
        viewer_copy.save_blocks('demo', [
            {'id': f'b{i}', 'anchor': 'a', 'kind': 'p'}
            for i in range(viewer_copy.MAX_BLOCKS + 1)])


def test_the_real_blocks_store_is_valid() -> None:
    path = viewer_copy.CONTENT_DIR / 'prelim_blocks.json'
    assert path.exists()
    assert isinstance(json.loads(path.read_text()), list)


# --- deleted drafted blocks ---------------------------------------------
#
# Drafted copy lives in the template, so deleting it cannot be a removal --
# the next render would put it straight back. It is recorded as a hidden key
# instead, and the template drops those in production.

def test_hidden_round_trips(store) -> None:
    viewer_copy.save_hidden('demo', ['ab-lede', 'ab-gap'])
    assert viewer_copy.load_hidden('demo') == ['ab-gap', 'ab-lede']


def test_saving_hidden_replaces_so_restore_works(store) -> None:
    """Wholesale, not a merge -- otherwise nothing could ever be un-deleted."""
    viewer_copy.save_hidden('demo', ['ab-lede', 'ab-gap'])
    viewer_copy.save_hidden('demo', ['ab-gap'])
    assert viewer_copy.load_hidden('demo') == ['ab-gap']


def test_hidden_keys_are_deduplicated(store) -> None:
    assert viewer_copy.save_hidden('demo', ['a', 'a', 'a']) == ['a']


@pytest.mark.parametrize('key', ['../../etc/passwd', 'Ab-Lede', '', 'a b', 'a/b'])
def test_a_hidden_key_that_is_not_a_key_is_dropped(store, key: str) -> None:
    """Same shape rule as everywhere else: these become selectors and paths."""
    assert viewer_copy.save_hidden('demo', [key]) == []


def test_hidden_must_be_a_list(store) -> None:
    with pytest.raises(ValueError):
        viewer_copy.save_hidden('demo', {'ab-lede': True})


def test_too_many_hidden_keys_is_rejected(store) -> None:
    with pytest.raises(ValueError):
        viewer_copy.save_hidden('demo', [f'k{i}' for i in range(viewer_copy.MAX_KEYS + 1)])


def test_missing_hidden_store_reads_as_nothing_deleted(store) -> None:
    """The failure mode has to be 'everything shows', never 'page is empty'."""
    assert viewer_copy.load_hidden('demo') == []


def test_the_real_hidden_store_is_valid() -> None:
    path = viewer_copy.CONTENT_DIR / 'prelim_hidden.json'
    assert path.exists()
    assert isinstance(json.loads(path.read_text()), list)


# --- inline @claude notes -----------------------------------------------

def test_an_inline_note_is_marked_for_the_author(store) -> None:
    viewer_copy.save_copy('demo', {'ab-lede': 'Text. @claude tighten this@ More.'})
    out = viewer_copy.render_copy('demo', keep_notes=True)['ab-lede']
    assert out == 'Text. <mark class="pm-inline-note">tighten this</mark> More.'


def test_an_inline_note_is_stripped_for_the_public_page(store) -> None:
    """The guarantee that makes it safe to leave one lying around."""
    viewer_copy.save_copy('demo', {'ab-lede': 'Text. @claude tighten this@ More.'})
    assert viewer_copy.render_copy('demo', keep_notes=False)['ab-lede'] == 'Text. More.'


def test_a_note_spanning_markup_is_still_found(store) -> None:
    viewer_copy.save_copy('demo', {'ab-lede': 'a @claude make <b>this</b> bold@ b'})
    assert '<mark' in viewer_copy.render_copy('demo', keep_notes=True)['ab-lede']
    assert 'make' not in viewer_copy.render_copy('demo', keep_notes=False)['ab-lede']


def test_notes_are_listed_with_the_block_they_are_in(store) -> None:
    """One call tells the next session what was asked, and about what."""
    viewer_copy.save_copy('demo', {
        'ab-lede': 'x @claude first@ y @claude second@',
        'ab-gap': 'z @claude third@',
    })
    assert viewer_copy.inline_notes('demo') == [
        ('ab-gap', 'third'), ('ab-lede', 'first'), ('ab-lede', 'second')]


def test_copy_without_a_note_is_untouched(store) -> None:
    """An @ in ordinary prose must not eat the rest of the sentence."""
    viewer_copy.save_copy('demo', {'ab-lede': 'mail me at a@b.com, or not'})
    for keep in (True, False):
        assert viewer_copy.render_copy('demo', keep_notes=keep)['ab-lede'] == (
            'mail me at a@b.com, or not')
