# The page-editing toolkit — handoff

**What it is.** A way to edit a server-rendered write-up *in the page itself* and have the edit
persist as a git-tracked source file. Reword a paragraph, add a heading, delete a drafted section,
resize a chart, drag a chart label, leave a sticky note, or leave an inline note addressed to the
agent — all in the browser, all saved to JSON that the next session reads.

**Why it exists.** The write-ups on this site are drafted by an agent and then reworded by their
author. Round-tripping every wording change through "describe the change → agent edits the template
→ reload" is the slow part. This closes that loop: the author edits directly, the agent reads what
they wrote.

**Status.** Built, verified end to end in the browser, committed on `feature/conform-audit-fixes`
(`6a4dfb1`). Live on one page (`/prelim` in the `kaya` repo). All three gates green.

**Where it's going.** Extraction into a shared Python package — see [Extraction plan](#extraction-plan)
at the end. This document is written to travel with it, so it repeats context a `kaya` reader
already has.

---

## 1. The working agreement (read this first if you are the agent)

This is the part that is easy to get wrong, because it is a protocol between two people rather than
a piece of code.

**At the start of any session that touches an edited page**, read both note channels. They are
separate on purpose:

```bash
source .venv/bin/activate && PYTHONPATH=src python -c "
from kaya import viewer_copy as vc
for key, state, text in vc.inline_notes('prelim'):
    print(f'{state:5} {key:16} {text}')
"
cat src/kaya/viewer_content/prelim_notes.json    # sticky notes, anchored to a block or chart
```

| Channel | Shape | Use |
| --- | --- | --- |
| **Inline note** | `@claude ...@` written *inside* the prose | "not this sentence, that one" — feedback at an exact spot |
| **Sticky note** | a card anchored to a block or a chart, with a done flag | feedback *about* a block or a figure as a whole |

**After you make the change a note asked for**, mark it handled — do not delete it:

```bash
PYTHONPATH=src python -c "
from kaya import viewer_copy as vc
print(vc.resolve_inline_notes('prelim'), 'notes marked handled')      # all open notes
print(vc.resolve_inline_notes('prelim', key='ab-lede'), 'in one block')  # or just one block
"
```

`@claude ...@` becomes `@done ...@`, which renders **green with a ✓ handled prefix** instead of
orange. That colour change is the entire signal: orange means nobody has looked at it, green means
the change is made and is waiting to be checked.

**The author deletes the note, not you.** You cannot tell whether they liked the result. Leaving it
green is how they find the diff to review. If you disagree with a note, say so in your reply and
leave it *open* — silently resolving a note you did not act on destroys the only record of the ask.

Sticky notes have the same shape: a `done` toggle the author flips, and a delete button that is
theirs to press.

**Never let a note reach production.** Both kinds are stripped server-side in production, and there
are tests pinning that. Do not add a code path that renders them without that guard.

---

## 2. What the author can do

| Action | How | Persisted to |
| --- | --- | --- |
| Reword any text block | **Edit copy** → click into the text → **Save** (or ⌘S) | `*_copy.json` |
| Add a paragraph or heading | **Review tools** → **+ text** / **+ section** on any block | `*_blocks.json` (structure) + `*_copy.json` (words) |
| Delete an **added** block | **delete** on that block | removed from `*_blocks.json` |
| Delete a **drafted** block | **delete** on that block → strikes through, **restore** to undo | `*_hidden.json` |
| Leave a note about a block or chart | **+ note** | `*_notes.json` |
| Leave a note inside the prose | type `@claude ...@` while editing | `*_copy.json` |
| Resize a chart | drag its bottom edge | `*_layout.json` |
| Move a chart label or annotation | drag it (Plotly `config.edits`) | `*_layout.json` |

Everything is behind two toolbar buttons, both rendered only outside production.

---

## 3. The safety boundary

**This is the load-bearing constraint. Do not weaken it without a conversation.**

`kaya.peterwilliams.dev` is deliberately public and unauthenticated — an nginx auth gate was built,
deployed, verified, then explicitly reversed by the site owner. A public page that can rewrite its
own text on request is a defacement vector, not a feature.

So:

1. **Every write route lives on the dev-only router.** `viewer_app` registers `dev_api` only when
   `KAYA_VIEWER_ENV != production`. The deployed app has **no write endpoint at all** — not a
   guarded one, an absent one. Tests assert 404 in production for each.
2. **Stored copy renders unescaped**, because the point is to keep the author's `<i>`, `<b>`,
   `<sub>` and inline maths. So it goes through an allowlist sanitiser **on the way in and on the
   way out** — in at save time, out at load time, because the JSON files are meant to be
   hand-edited too and a hand-edit does not go through the save path.
3. **The blocks route carries no text.** A block records `{id, anchor, kind, order}` and nothing
   else; its words go through the copy route under the same id. That split means there is no second
   path into the page that could bypass the sanitiser.
4. **Notes never render in production**, inline or sticky.

The sanitiser (`sanitize_html`) is an allowlist:

| | |
| --- | --- |
| Tags kept | `a b strong i em code sub sup br span small` |
| Attributes kept | `href`, on `<a>` only |
| URL schemes kept | `http https mailto`, plus `#`- and `/`-relative |
| Unknown tags | **unwrapped**, text kept — losing an author's words to a stray tag is worse than losing the tag |
| `script style iframe object embed template noscript` | tag **and contents** dropped — unwrapping would print `alert(1)` as visible page text |
| Unbalanced markup | closed, so a stray `</b>` cannot leak into the rest of the page |

Limits: 20,000 chars per value, 400 keys, 300 notes, 200 blocks, chart heights clamped to 120–1600px.
Keys must match `^[a-z0-9][a-z0-9-]{0,63}$` — they become DOM selectors and filename components.

---

## 4. Architecture

```
viewer_app.py          serve_prelim() reads the stores, passes them to the template
  └─ dev_api           POST /prelim-{copy,blocks,notes,hidden,layout}   ← development only
viewer_copy.py         the store: sanitiser + load/save for all five files
viewer_content/        prelim_{copy,blocks,notes,hidden,layout}.json    ← git-tracked, hand-editable
viewer_templates/
  prelim.html          the page; ed() and slot() macros are the contract
viewer_static/
  prelim.css           page styles + all editing chrome
  js/v2/20-edit.js     edit mode          ← behind {% if editable %}, never ships
  js/v2/21-review.js   notes, blocks, resize, drag persistence  ← ditto
```

**Request path.** `GET /prelim` → `serve_prelim` loads all five stores → template renders copy
into blocks, added blocks into slots, hidden blocks as struck-through-or-nothing, and seeds the
client with `PM_INITIAL_{NOTES,LAYOUT,BLOCKS,HIDDEN}` → client mounts editing chrome if the toolbar
element exists (it does not in production).

**No computation on request.** The page reads a precomputed payload. This toolkit only ever reads
and writes small JSON.

### The five stores

| File | Shape | Notes |
| --- | --- | --- |
| `*_copy.json` | `{key: html}` | the words. Sorted keys, one entry per line, diffable |
| `*_blocks.json` | `[{id, anchor, kind, order}]` | added blocks. `kind ∈ {p, h3, h4}` — it becomes a tag name, so it is an allowlist |
| `*_hidden.json` | `[key]` | deleted **drafted** blocks |
| `*_notes.json` | `[{id, anchor, text, done}]` | sticky notes |
| `*_layout.json` | `{chartId: {height, "annotation:2": {x, y}}}` | chart heights and dragged label positions |

**An empty value in `*_copy.json` deletes its key**, which is how a block reverts to the default
written in the template rather than being pinned to an empty string.

### Why "delete" has three different meanings

This is the design decision most likely to be undone by someone who does not know why:

- An **added** block has a real record in `*_blocks.json` → delete removes it. Hiding it instead
  would leave litter in the file forever.
- A **drafted** block is written in the template → the template is its source and would put it back
  on the next render. So delete records a hidden key: **dropped entirely in production**, rendered
  struck through with a **restore** control in development. A delete with no way back is a trap
  here, because the template is the only copy of those words and there is nowhere else the author
  could go to undo it.
- A **chart** has neither, and gets no delete at all.

### Inline note states

`INLINE_NOTE_RE = @(claude|done)\b(.*?)@`, non-greedy, DOTALL, case-insensitive.

| Stored | Development render | Production render |
| --- | --- | --- |
| `@claude tighten this@` | orange `@claude tighten this` | *(removed)* |
| `@done tighten this@` | green `✓ handled — tighten this` | *(removed)* |

The `<mark>` wrapper is **presentation, not copy**. `pmUnmarkNotes()` in `20-edit.js` converts it
back to `@claude`/`@done` before saving — otherwise the sanitiser would drop the `<mark>`, the
delimiters would go with it, and the note would silently become part of the prose. Any future
client-side handling of notes must preserve that round trip.

An `@` in ordinary prose is safe: `mail me at a@b.com` is untouched, because the pattern requires
the literal `@claude` or `@done`.

---

## 5. The template contract

Two macros. Everything else follows from them.

```jinja
{% call ed('ab-lede') %}Default words, written here in the template.{% endcall %}
{% call ed('ab-head', 'h3') %}A heading{% endcall %}
{{ slot('ab-landscape') }}     {# after every chart, so added blocks have somewhere to go #}
```

- `ed(key, tag='p', cls=None)` renders stored copy if there is any, otherwise the default in the
  `{% call %}` body, and appends `slot(key)`. It handles the hidden branch itself.
- `slot(anchor)` renders blocks the author added under that anchor.
- **Every key must be unique** — two blocks sharing a key overwrite each other on save. A test
  enforces it.
- **Call `slot()` after every chart too**, or a block added under a chart has nowhere to render. A
  test enforces that as well.
- The seed `<script>` declaring `PM_INITIAL_*` must come **before** the client scripts. These are
  classic scripts sharing one global scope, not modules; a `const` in a later `<script>` is not
  visible to an earlier one at evaluation time. Getting this wrong silently loses every note and
  layout on reload — it has already happened once.

---

## 6. The client

Two files, both classic scripts, both behind `{% if editable %}`.

- **`20-edit.js`** — edit mode. `contenteditable` (rich, not plaintext-only: plaintext-only would
  flatten the author's `<i>`/`<sub>` on the first keystroke). Saves **only blocks that changed**,
  compared against a baseline captured on entering edit mode — sending everything would rewrite the
  store with the browser's normalised markup for text nobody touched and bury the real edit in a
  whitespace diff. ⌘S to save, `beforeunload` guard on unsaved work.
- **`21-review.js`** — sticky notes, add/delete blocks, chart resizing, and persistence of Plotly
  `config.edits` drags.

Two client details worth keeping:

- `PM_CHROME = ['pm-block-tools', 'pm-note', 'pm-resize']` — the sibling walk that finds where to
  insert a new block must **skip review chrome**. Without it the second added block lands *above*
  the first, because the walk stops at the toolbar injected between them.
- Blocks are inserted into the DOM directly rather than by reloading. The page has charts and
  typeset maths that would all have to redraw, and the author is mid-thought.

---

## 7. Design tokens

All colour, spacing, radius and shadow come from `tokens.css`, which is the **workspace's design-token
source of truth** — other repos sync from it; never edit a downstream copy.

Tokens this toolkit uses:

```
--lg-bg --lg-surface --lg-card --lg-active
--lg-text --lg-text-2 --lg-text-3
--lg-border --lg-border-h
--lg-gold --lg-gold-soft --lg-highlight --lg-success --lg-warning --lg-danger
--lg-radius-xs --lg-radius-sm --lg-radius-md --lg-shadow-sm --lg-shadow-md --lg-pad
```

Semantic assignments, which should survive a port:

| Element | Token | Why |
| --- | --- | --- |
| Open inline note | `--lg-highlight` (burnt orange `#b8752e` / `#d99a52`) | attention, unhandled |
| Handled inline note | `--lg-success` | done, awaiting review |
| Added block rail | `--lg-gold` | "this wasn't in the draft" |
| Deleted block | 38% opacity + line-through | absent but still legible |
| Editing outline | `--lg-gold-soft` | active edit target |

**One Plotly gotcha:** Plotly parses computed CSS values and does **not** understand `color-mix()`.
Any token handed to a chart must be a literal `rgba()`. That is why `prelim.css` carries
`--pm-axis` and `--pm-axis-rgb` as explicit triples.

Both themes are token-level: `@media (prefers-color-scheme: dark)` plus
`:root[data-theme='dark'|'light']` overrides, so the viewer's theme toggle wins in both directions.

---

## 8. Tests that pin the invariants

`tests/test_viewer_copy.py` (~60 tests) covers the sanitiser and the five stores.
`tests/test_viewer_app.py` covers routing, which is where the safety property lives.

The ones that must not be deleted:

| Test | Pins |
| --- | --- |
| `test_the_copy_save_route_does_not_exist_in_production` | no write endpoint on a public page |
| `test_review_write_routes_do_not_exist_in_production` | same, for notes and layout |
| `test_the_block_route_does_not_exist_in_production` | same, for blocks |
| `test_the_hidden_route_does_not_exist_in_production` | same, for deletions |
| `test_production_renders_no_edit_affordance` | asserts against **rendered production HTML**, not template text |
| `test_a_deleted_drafted_block_is_gone_in_production` | delete actually deletes for the public |
| `test_an_inline_note_never_reaches_the_public_page` | notes are stripped, surrounding words kept |
| `test_a_block_holds_no_text` | no markup path that bypasses the sanitiser |
| `test_every_editable_block_has_a_unique_key` | no silent overwrite on save |
| `test_added_blocks_render_in_production` | added blocks *are* page content and must ship |

**A test that exercises a write route must point `CONTENT_DIR` at `tmp_path`.** The real store is a
git-tracked file holding the author's actual words; one test wrote to it before this was noticed.

Note that `_load_viewer_app()` drops the whole package from `sys.modules` to re-decide the
production/development split at import time — so a `monkeypatch` applied *before* it is thrown away
with the old module object. Patch the module it returns.

---

## 9. Verify it

```bash
source .venv/bin/activate
python -m ruff check . && python -m mypy && python -m pytest -q      # all three must be green
python -m kaya.viewer_app                                            # then open /prelim
```

Browser smoke test, in order — each step must survive a reload:

1. **Edit copy** → change a paragraph → **Save** → reload.
2. **Review tools** → **+ text** → type → **Save** → reload.
3. **delete** on that added block → reload (gone).
4. **delete** on a drafted block → struck through with **restore** → reload → **restore** → reload.
5. Drag a chart's bottom edge; drag a chart label → reload.
6. Type `@claude test@` into a paragraph → **Save** → reload → orange mark appears.
7. `resolve_inline_notes(...)` → reload → mark is green.
8. `KAYA_VIEWER_ENV=production` → reload → no toolbar, no note, no deleted block.

---

## Extraction plan

The destination is a small shared package, per the workspace's integration order
(**package import > generated artifact > thin service > submodule**). Proposed name: `pw_viewer_kit`.

**Do this when the second repo adopts it, not before.** Right now the API would be designed against
exactly one caller. The natural trigger is `garmin`, `evolution_sim`, `climbing_wall`, or
`explainers` wanting the same thing.

**Why a package rather than copy-paste:** the four pieces (Python store, two JS files, CSS) must
stay in step. A change to the sanitiser without the matching change in `pmUnmarkNotes` silently
breaks saving. Copying four files into five repos is how that drifts.

### What is generic vs page-specific

| Generic — goes in the package | Page-specific — stays in the app |
| --- | --- |
| `viewer_copy.py` entire (sanitiser, five stores, notes) | the `prelim.html` template and its keys |
| `20-edit.js`, `21-review.js` | `17-prelim.js`, `18-ability.js`, `19-ability-sim.js` |
| The editing-chrome CSS (`.pm-edit-*`, `.pm-note*`, `.pm-block-tools`, `.pm-tool`, `.pm-resize`, `.pm-deleted`, `.pm-inline-note`) | page layout and chart CSS |
| The `ed()` / `slot()` macros, as an includable Jinja file | which blocks exist |
| The five write routes, as a mountable `APIRouter` | the read route |

### Needs parameterising

1. **Page slug** — `viewer_copy` already takes `page` everywhere; the JS hard-codes
   `/api/prelim-*`. Make the endpoints a config object the seed script sets.
2. **Content directory** — currently `Path(__file__).with_name('viewer_content')`. Becomes a
   required argument, so the package never owns the host app's data.
3. **The dev/prod switch** — the package must not read `KAYA_VIEWER_ENV` itself. The host passes
   `editable: bool`, and the router is mounted or not by the host. Keeping that decision in the
   host is what makes the boundary auditable.
4. **Allowed tags** — a sensible default, overridable, never widened silently.
5. **CSS class prefix** — `pm-` is `/prelim`'s. Either keep it as the package's own namespace
   (simplest, recommended) or make it a variable.

### Suggested API

```python
from pw_viewer_kit import PageStore, editing_router, editing_context

store = PageStore(content_dir=Path(...)/'viewer_content', page='prelim')

# read side
ctx = editing_context(store, editable=not IS_PRODUCTION)   # copy, blocks, hidden, notes, layout

# write side — mounted ONLY when not production
if not IS_PRODUCTION:
    app.include_router(editing_router(store), prefix='/api')
```

Static assets ship inside the package and are mounted by the host, the same way the design tokens
are handled today.

### Migration steps

1. Create the package with `viewer_copy.py` and its tests moved verbatim; they pass unchanged
   except for the import path.
2. Parameterise the endpoints in the two JS files; drive them from the seed script.
3. Replace kaya's copies with the package, keeping `viewer_content/` where it is. Kaya is the
   canary — its ~70 tests are the regression suite.
4. Only then adopt in a second repo, and let *that* adoption tell you what else needs
   parameterising.

### Risks to keep in view

- **The safety boundary must not become the package's decision.** If the package can register its
  own write routes, a host that forgets a flag ships an editable public page. Mounting stays the
  host's explicit act.
- **Sanitiser drift.** One allowlist, one place, tests that travel with it. Never a per-repo copy.
- **Version skew between Python and JS.** They are one contract; ship them from one package version.
- **`tokens.css` stays the source of truth** in `kaya`, synced by `scripts/sync-design-tokens.sh`
  in `system-overview`. The package should consume tokens, never define colours.

---

## Open items

- The two ability figures use **different ceilings** — V6.6 in the static landscape figure, V6.2 in
  the interactive model panel. Deliberate for now; unify when the copy around them settles.
- `/report-change` to `system-overview` has not been filed for this. It is cross-repo relevant:
  `kaya` is the reference viewer-app for `garmin`, `aws_monitor`, `evolution_sim`,
  `project_registry`, `climbing_wall`, and now `explainers`.
- Sticky notes and inline notes are two stores with one purpose. If a third note channel ever
  appears, merge them first.
