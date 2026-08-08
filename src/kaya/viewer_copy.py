"""Editable page copy: a small git-tracked store the viewer renders from.

WHY THIS EXISTS. The write-ups on this site are drafted by an agent and then
reworded by their author, and round-tripping every wording change through a
source edit is the slow part. This lets the copy be edited in the page itself
and saved somewhere durable, so the next session reads the current words rather
than the draft ones.

WHERE THE COPY LIVES. `viewer_content/*_copy.json`, one file per page, keyed by
the `data-copy` attribute on the element it fills. Plain JSON, sorted keys, one
line per entry -- it is meant to be read, diffed and hand-edited like any other
source file, and it is committed like one.

THE SAFETY BOUNDARY, WHICH IS THE IMPORTANT PART.

  * Saving is DEVELOPMENT ONLY. The route lives on `dev_api`, which
    `viewer_app` registers only when KAYA_VIEWER_ENV != production, so the
    deployed site has no write endpoint at all. kaya.peterwilliams.dev is
    deliberately public and unauthenticated; a public page that can rewrite its
    own text on request is a defacement vector, not a feature.
  * Stored copy is RENDERED UNESCAPED, because the point is to keep the
    author's emphasis and inline maths markup. So everything is put through the
    allowlist sanitiser below on the way IN -- at save time, once, rather than
    trusting the file at render time. A hand-edit that slips a `<script>` into
    the JSON would still be rendered, which is why `sanitize_html` is also
    applied when the store is loaded.

The sanitiser is an allowlist and drops anything it does not recognise: no
attributes survive except `href` on a link, and no URL scheme survives except
http, https, mailto and same-document/relative.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from html import escape
from html.parser import HTMLParser
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.parse import urlparse

CONTENT_DIR = Path(__file__).with_name('viewer_content')

#: Inline markup an author legitimately needs. Block tags are deliberately
#: absent: the element being edited already IS the block, and letting copy
#: introduce its own would let it break the page layout.
ALLOWED_TAGS = frozenset({
    'a', 'b', 'strong', 'i', 'em', 'code', 'sub', 'sup', 'br', 'span', 'small',
})
VOID_TAGS = frozenset({'br'})
ALLOWED_SCHEMES = frozenset({'http', 'https', 'mailto'})

#: Tags whose CONTENTS go too, not just the tag. An unknown tag is unwrapped so
#: its words survive, but the body of a <script> is code, not words -- unwrap it
#: and `alert(1)` shows up as visible page text.
DROP_CONTENT_TAGS = frozenset({
    'script', 'style', 'iframe', 'object', 'embed', 'template', 'noscript',
})

KEY_RE = re.compile(r'^[a-z0-9][a-z0-9-]{0,63}$')

#: Generous for a paragraph, small enough that a runaway paste cannot bloat the
#: page. Exceeding it is an error, not a silent truncation.
MAX_VALUE_CHARS = 20_000
MAX_KEYS = 400


class _Sanitiser(HTMLParser):
    """Rebuild a fragment from an allowlist, keeping text and dropping the rest.

    An unknown tag is unwrapped rather than deleted -- `<div>hello</div>`
    becomes `hello`, not nothing -- because losing an author's words to a stray
    tag is worse than losing the tag. Comments, declarations and processing
    instructions are dropped outright.
    """

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.out: List[str] = []
        self.open_tags: List[str] = []
        self.suppress = 0

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, str | None]]) -> None:
        if tag in DROP_CONTENT_TAGS:
            self.suppress += 1
            return
        if self.suppress or tag not in ALLOWED_TAGS:
            return
        if tag in VOID_TAGS:
            self.out.append(f'<{tag}>')
            return
        href = self._href(tag, attrs)
        self.out.append(f'<{tag} href="{href}">' if href else f'<{tag}>')
        self.open_tags.append(tag)

    def handle_startendtag(self, tag: str, attrs: List[Tuple[str, str | None]]) -> None:
        if not self.suppress and tag in VOID_TAGS:
            self.out.append(f'<{tag}>')

    def handle_endtag(self, tag: str) -> None:
        if tag in DROP_CONTENT_TAGS:
            self.suppress = max(0, self.suppress - 1)
            return
        if self.suppress or tag not in ALLOWED_TAGS or tag in VOID_TAGS:
            return
        # Close only if it is actually open, so a stray `</b>` cannot unbalance
        # the fragment and leak markup into whatever follows it on the page.
        if tag in self.open_tags:
            while self.open_tags:
                popped = self.open_tags.pop()
                self.out.append(f'</{popped}>')
                if popped == tag:
                    break

    def handle_data(self, data: str) -> None:
        if self.suppress:
            return
        self.out.append(escape(data, quote=False))

    def close_all(self) -> str:
        while self.open_tags:
            self.out.append(f'</{self.open_tags.pop()}>')
        return ''.join(self.out)

    @staticmethod
    def _href(tag: str, attrs: List[Tuple[str, str | None]]) -> str | None:
        """The one attribute that survives, and only on a link."""
        if tag != 'a':
            return None
        for name, value in attrs:
            if name.lower() != 'href' or not value:
                continue
            raw = value.strip()
            # Same-document and site-relative links carry no scheme and are
            # safe; anything with a scheme must be on the allowlist, which is
            # what keeps `javascript:` out.
            if raw.startswith(('#', '/')):
                return escape(raw, quote=True)
            parsed = urlparse(raw)
            if parsed.scheme.lower() in ALLOWED_SCHEMES:
                return escape(raw, quote=True)
            return None
        return None


def sanitize_html(fragment: str) -> str:
    """Reduce a fragment to allowlisted inline markup and escaped text."""
    parser = _Sanitiser()
    parser.feed(fragment)
    parser.close()
    return parser.close_all()


def copy_path(page: str) -> Path:
    """Store file for a page. `page` is a slug, not a caller-supplied path."""
    if not KEY_RE.match(page):
        raise ValueError(f'not a page slug: {page!r}')
    return CONTENT_DIR / f'{page}_copy.json'


def load_copy(page: str) -> Dict[str, str]:
    """Stored copy for a page, sanitised. Missing or unreadable reads as empty.

    Sanitised on the way out as well as in: the file is hand-editable, and a
    hand edit does not go through the save path.
    """
    path = copy_path(page)
    try:
        raw = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(raw, dict):
        return {}
    return {
        str(k): sanitize_html(str(v))
        for k, v in raw.items()
        if KEY_RE.match(str(k))
    }


def save_copy(page: str, updates: Dict[str, str]) -> Dict[str, str]:
    """Merge sanitised updates into a page's store and write it atomically.

    An empty value DELETES its key, which is how a block is reverted to the
    default written in the template rather than pinned to an empty string.
    """
    if not isinstance(updates, dict):
        raise ValueError('updates must be an object')

    merged = load_copy(page)
    for key, value in updates.items():
        if not KEY_RE.match(str(key)):
            raise ValueError(f'not a copy key: {key!r}')
        if not isinstance(value, str):
            raise ValueError(f'copy for {key!r} must be a string')
        if len(value) > MAX_VALUE_CHARS:
            raise ValueError(f'copy for {key!r} is over {MAX_VALUE_CHARS} chars')
        clean = sanitize_html(value).strip()
        if clean:
            merged[key] = clean
        else:
            merged.pop(key, None)

    if len(merged) > MAX_KEYS:
        raise ValueError(f'more than {MAX_KEYS} copy keys')

    _write_json(copy_path(page), merged)
    return merged


# --- review notes -------------------------------------------------------
#
# Sticky notes the author drops on the page to say what to change. They are
# NOT page content: they never render in production, and they exist so a
# request lands in a file the next session reads, instead of in a chat message
# that has to be re-explained against a page nobody can see.

ANCHOR_RE = re.compile(r'^[a-z0-9][a-z0-9:_-]{0,80}$')
MAX_NOTE_CHARS = 4_000
MAX_NOTES = 300


def _clean_note(raw: object) -> Dict[str, object] | None:
    """One note, validated. Anything malformed is dropped rather than raised:
    a single bad note must not cost the author every other note in the file."""
    if not isinstance(raw, dict):
        return None
    note_id = str(raw.get('id', ''))
    anchor = str(raw.get('anchor', ''))
    text = str(raw.get('text', ''))
    if not ANCHOR_RE.match(note_id) or not ANCHOR_RE.match(anchor):
        return None
    if not text.strip():
        return None
    return {
        'id': note_id,
        'anchor': anchor,
        # Stored RAW, not escaped. A note is plain text and every path that
        # shows it treats it as text -- `textarea.value` in the browser,
        # `|tojson` into the script that seeds it -- so escaping here would
        # only make `&lt;` appear in the author's own note the second time
        # they opened it. Nothing renders a note as HTML.
        'text': text[:MAX_NOTE_CHARS],
        'done': bool(raw.get('done', False)),
    }


def load_notes(page: str) -> List[Dict[str, object]]:
    path = copy_path(page).with_name(f'{page}_notes.json')
    try:
        raw = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(raw, list):
        return []
    return [n for n in (_clean_note(r) for r in raw) if n is not None]


def save_notes(page: str, notes: object) -> List[Dict[str, object]]:
    """Replace the note list wholesale -- unlike copy, which merges.

    Notes are added and deleted as a set by the page, so a merge would make
    deletion impossible to express.
    """
    if not isinstance(notes, list):
        raise ValueError('notes must be a list')
    if len(notes) > MAX_NOTES:
        raise ValueError(f'more than {MAX_NOTES} notes')
    clean = [n for n in (_clean_note(r) for r in notes) if n is not None]
    _write_json(copy_path(page).with_name(f'{page}_notes.json'), clean)
    return clean


# --- chart layout -------------------------------------------------------
#
# Per-chart height, and the positions of any annotation or shape the author
# dragged. Numbers only: this store can move a label, never introduce one.

MIN_CHART_PX = 120
MAX_CHART_PX = 1600
MAX_MOVES = 60


def _clean_layout(raw: object) -> Dict[str, Dict[str, object]]:
    out: Dict[str, Dict[str, object]] = {}
    if not isinstance(raw, dict):
        return out
    for chart, spec in raw.items():
        if not ANCHOR_RE.match(str(chart)) or not isinstance(spec, dict):
            continue
        entry: Dict[str, object] = {}
        height = spec.get('height')
        if isinstance(height, (int, float)) and MIN_CHART_PX <= height <= MAX_CHART_PX:
            entry['height'] = int(height)
        moves = spec.get('moves')
        if isinstance(moves, dict):
            clean_moves: Dict[str, Dict[str, float]] = {}
            for target, coords in list(moves.items())[:MAX_MOVES]:
                if not ANCHOR_RE.match(str(target)) or not isinstance(coords, dict):
                    continue
                nums = {
                    str(k): float(v) for k, v in coords.items()
                    if isinstance(v, (int, float)) and str(k) in
                    {'x', 'y', 'x0', 'x1', 'y0', 'y1'}
                }
                if nums:
                    clean_moves[str(target)] = nums
            if clean_moves:
                entry['moves'] = clean_moves
        if entry:
            out[str(chart)] = entry
    return out


def load_layout(page: str) -> Dict[str, Dict[str, object]]:
    path = copy_path(page).with_name(f'{page}_layout.json')
    try:
        return _clean_layout(json.loads(path.read_text(encoding='utf-8')))
    except (OSError, json.JSONDecodeError):
        return {}


def save_layout(page: str, updates: object) -> Dict[str, Dict[str, object]]:
    """Merge per-chart layout. Merging, not replacing: resizing one chart must
    not discard the label positions someone set on another."""
    if not isinstance(updates, dict):
        raise ValueError('layout must be an object')
    merged = load_layout(page)
    for chart, spec in _clean_layout(updates).items():
        entry = merged.setdefault(chart, {})
        if 'height' in spec:
            entry['height'] = spec['height']
        if 'moves' in spec:
            existing = entry.get('moves')
            moves = dict(existing) if isinstance(existing, dict) else {}
            new_moves = spec['moves']
            if isinstance(new_moves, dict):
                moves.update(new_moves)
            entry['moves'] = moves
    _write_json(copy_path(page).with_name(f'{page}_layout.json'), merged)
    return merged


def _write_json(path: Path, data: object) -> None:
    """Write-then-replace, shared by every store here.

    An interrupted write must not leave a half-written file: every loader
    treats unparseable JSON as empty, so a torn write would silently erase the
    author's copy on the next page load.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(data, indent=2, sort_keys=True, ensure_ascii=False) + '\n'
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix='.copy-', suffix='.json')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as handle:
            handle.write(body)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


# --- author-added blocks ------------------------------------------------
#
# Headings and paragraphs the author inserts into the page, which the drafted
# template knows nothing about.
#
# SPLIT ON PURPOSE: this file holds only STRUCTURE -- id, where it goes, what
# kind of element it is. The block's WORDS live in the copy store under the
# same id, so a new block is edited, sanitised and saved by exactly the same
# path as a drafted one. Two stores, one source of truth for each thing; the
# alternative was text in two places, which goes wrong the first time someone
# edits a block and then moves it.
#
# Unlike notes, blocks ARE page content and render in production.

BLOCK_KINDS = frozenset({'p', 'h3', 'h4'})
MAX_BLOCKS = 200


def _clean_block(raw: object) -> Dict[str, object] | None:
    if not isinstance(raw, dict):
        return None
    block_id = str(raw.get('id', ''))
    anchor = str(raw.get('anchor', ''))
    kind = str(raw.get('kind', 'p'))
    if not KEY_RE.match(block_id) or not ANCHOR_RE.match(anchor):
        return None
    if kind not in BLOCK_KINDS:
        return None
    try:
        order = int(raw.get('order', 0))
    except (TypeError, ValueError):
        order = 0
    return {'id': block_id, 'anchor': anchor, 'kind': kind, 'order': order}


def load_blocks(page: str) -> List[Dict[str, object]]:
    path = copy_path(page).with_name(f'{page}_blocks.json')
    try:
        raw = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(raw, list):
        return []
    clean = [b for b in (_clean_block(r) for r in raw) if b is not None]
    # Stable: anchor groups keep template order, and `order` sequences the
    # blocks sharing one anchor. `_clean_block` has already coerced both, so
    # the cast is a typing formality rather than a guard.
    def _sort_key(block: Dict[str, object]) -> Tuple[str, int]:
        return (str(block['anchor']), int(str(block['order'])))

    clean.sort(key=_sort_key)
    return clean


def save_blocks(page: str, blocks: object) -> List[Dict[str, object]]:
    """Replace the block list wholesale, like notes: the page manages them as a
    set, so a merge would make deletion and reordering impossible to express."""
    if not isinstance(blocks, list):
        raise ValueError('blocks must be a list')
    if len(blocks) > MAX_BLOCKS:
        raise ValueError(f'more than {MAX_BLOCKS} blocks')
    clean = [b for b in (_clean_block(r) for r in blocks) if b is not None]
    seen = set()
    unique = []
    for block in clean:
        if block['id'] in seen:
            continue          # a duplicate id would render the same copy twice
        seen.add(block['id'])
        unique.append(block)
    _write_json(copy_path(page).with_name(f'{page}_blocks.json'), unique)
    return unique


def blocks_by_anchor(page: str) -> Dict[str, List[Dict[str, object]]]:
    """Blocks grouped for the template, which renders them after their anchor."""
    grouped: Dict[str, List[Dict[str, object]]] = {}
    for block in load_blocks(page):
        grouped.setdefault(str(block['anchor']), []).append(block)
    return grouped


# --- hidden blocks ------------------------------------------------------
#
# Deleting a DRAFTED block -- one written into the template -- cannot mean
# removing it, because the template is the source and would put it straight
# back on the next render. So deletion is recorded as a hidden set instead.
#
# Hidden blocks are not rendered at all in production. In development they
# still render, struck through and dimmed, with a restore control: a delete
# with no way back is a trap, and there is nowhere else the author could go to
# undo it.
#
# Blocks the author ADDED are deleted properly, via save_blocks -- there is a
# real record to remove, so hiding one would just leave litter in the file.


def load_hidden(page: str) -> List[str]:
    path = copy_path(page).with_name(f'{page}_hidden.json')
    try:
        raw = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(raw, list):
        return []
    return sorted({str(k) for k in raw if KEY_RE.match(str(k))})


def save_hidden(page: str, keys: object) -> List[str]:
    """Replace the hidden set. Wholesale, so un-hiding is expressible."""
    if not isinstance(keys, list):
        raise ValueError('hidden must be a list of keys')
    if len(keys) > MAX_KEYS:
        raise ValueError(f'more than {MAX_KEYS} hidden keys')
    clean = sorted({str(k) for k in keys if KEY_RE.match(str(k))})
    _write_json(copy_path(page).with_name(f'{page}_hidden.json'), clean)
    return clean


# --- inline notes to the agent ------------------------------------------
#
# `@claude move this above the equations@` written anywhere in the copy. The
# sticky notes are for a comment ABOUT a block; this is for a comment placed at
# an exact spot INSIDE the words, which is where most "not this sentence, that
# one" feedback actually belongs.
#
# Two guarantees make it safe to leave one lying around:
#   * production STRIPS them, so a forgotten note cannot ship to a public page;
#   * development MARKS them, so they are impossible to miss while editing.
# The transform runs after sanitising, so the <mark> it inserts is ours, not
# something copy could have smuggled in.

INLINE_NOTE_RE = re.compile(r'@(claude|done)\b(.*?)@', re.DOTALL | re.IGNORECASE)


def _note_state(word: str) -> str:
    return 'done' if word.lower() == 'done' else 'open'


def strip_inline_notes(fragment: str) -> str:
    """Remove agent notes, and the double space left where one was."""
    return re.sub(r'  +', ' ', INLINE_NOTE_RE.sub('', fragment)).strip()


def mark_inline_notes(fragment: str) -> str:
    """Wrap agent notes so they stand out from the copy around them."""
    return INLINE_NOTE_RE.sub(
        lambda m: (f'<mark class="pm-inline-note" data-state="{_note_state(m.group(1))}">'
                   f'{m.group(2).strip()}</mark>'),
        fragment,
    )


def render_copy(page: str, *, keep_notes: bool) -> Dict[str, str]:
    """Stored copy prepared for rendering: notes marked, or notes removed."""
    transform = mark_inline_notes if keep_notes else strip_inline_notes
    return {k: transform(v) for k, v in load_copy(page).items()}


def inline_notes(page: str) -> List[Tuple[str, str, str]]:
    """Every agent note in a page's copy, as (block key, state, note text).

    The point of entry for the next session: one call says what the author
    asked for, which block they asked it about, and whether it has been
    handled yet. Read the `open` ones; leave the `done` ones alone -- they are
    waiting on the author to check the change and delete the note.
    """
    found: List[Tuple[str, str, str]] = []
    for key, value in sorted(load_copy(page).items()):
        found.extend(
            (key, _note_state(m.group(1)), m.group(2).strip())
            for m in INLINE_NOTE_RE.finditer(value)
        )
    return found


def resolve_inline_notes(page: str, key: str | None = None) -> int:
    """Mark notes handled: `@claude ...@` becomes `@done ...@`. Returns the count.

    Call this AFTER making the change the note asked for. The note does not go
    away -- it turns green and reads "handled", which is the author's cue that
    the change is ready to look at. Deleting it is their call, not ours: we
    cannot tell whether they liked the result.
    """
    resolved = 0

    def flip(m: 're.Match[str]') -> str:
        nonlocal resolved
        if _note_state(m.group(1)) == 'done':
            return m.group(0)
        resolved += 1
        return f'@done{m.group(2)}@'

    updates = {}
    for k, value in load_copy(page).items():
        if key is not None and k != key:
            continue
        flipped = INLINE_NOTE_RE.sub(flip, value)
        if flipped != value:
            updates[k] = flipped
    if updates:
        save_copy(page, updates)
    return resolved
