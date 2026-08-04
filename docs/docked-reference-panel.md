# The docked reference panel

A portable spec for a collapsible right-hand reference panel that stays in view
while you scroll and is two-way hover-linked to the prose beside it.

This document is **self-contained on purpose** — it carries the markup, the CSS
and the JS, so another repo can implement the pattern without reading the Kaya
codebase. It was extracted from `src/kaya/viewer_static/` in the `kaya` repo
(the "Grading Model v2" tab), which remains the reference implementation.

Last updated 2026-08-04.

---

## 1. What problem it solves

A long technical page asks the reader to hold a vocabulary in their head while
scrolling past the things that use it — 30 symbols against 10 equations, a
column dictionary against a wide table, an ID→name lookup against a chart.
Scrolling back to a definitions table breaks reading; repeating definitions
inline bloats the prose.

Docking the definitions in the right gutter removes the scroll-back. The hover
filter answers the second question — *which of these 30 entries matters right
here* — without the reader having to search the list.

**Use it when:** a page has a fixed reference set (10–60 entries) that many
scattered elements draw on, and the reader is expected to move between them
repeatedly.

**Don't use it when:** the reference set is short enough to inline, or when
each entry is used exactly once (then just define it in place), or on pages
that are scanned rather than read.

---

## 2. Behaviour spec

This is the contract. An implementation that gets these right is the pattern,
regardless of how it is coded.

### The panel

1. **Fixed to the right gutter**, below any sticky nav, full height, stays put
   while the page scrolls.
2. **Collapses to a slim vertical handle** (~30px) with rotated text naming the
   panel ("Symbols"). The handle is always visible — the panel never disappears
   entirely, so its existence is always discoverable.
3. **The handle toggles.** A chevron on the handle points the way it will move
   (`›` when open, `‹` when closed).
4. **State persists** in `localStorage` and is restored on the next visit.
5. **Default open where there is room** — open at ≥1180px viewport width, shut
   below it. Below that breakpoint it behaves as an overlay drawer rather than
   a docked panel.
6. **It pushes content, it does not overlay it** (at ≥1180px). See trap 2.
7. **`Escape` closes it** when the containing view is active.

### The linking

8. **Hovering a referring element filters the panel** to the entries that
   element uses: matched rows keep full contrast and gain a coloured left rule,
   everything else drops to ~28% opacity. Group headers drop to ~40%.
9. **The first matched row scrolls into view** inside the panel, smoothly,
   but only if it is currently out of view. Respect `prefers-reduced-motion`.
10. **Hover only filters when the panel is already open.** Opening a panel on
    hover is jarring and easy to trigger accidentally.
11. **Hovering a panel entry marks the elements that use it** — the reverse
    direction. This is what makes the panel feel like an index rather than a
    sidebar.
12. **Clicking a referring element toggles the panel**: if shut, it opens and
    filters to that element's entries; if already open, it closes. This is the
    way in on a shut panel, and the way out without hunting for the handle.
    Give the element `cursor: pointer`, `role="button"` and a `title`.
13. **Keyboard parity**: referring elements are `tabindex="0"`; focus filters
    the same way hover does, and `Enter`/`Space` toggles the same way click
    does.

### The keys

14. **Linking is by plain string keys** — `data-syms="alpha beta gamma"` on the
    referring element, `data-sym="alpha"` on the panel row. Nothing about the
    wiring is equation-specific; the same code links `data-cols` on a table or
    `data-series` on a chart.

---

## 3. Style spec

- **Panel body ~340px, handle ~30px.** Both live in CSS custom properties
  (`--gloss-body-w`, `--gloss-handle-w`) because the layout reservation in
  trap 2 has to reference the same number.
- The panel sits on the **surface** colour with a **1px left border** and a
  soft directional shadow (`-12px 0 28px -18px rgb(0 0 0 / 0.45)`) so it reads
  as sitting above the page without a heavy edge.
- **`z-index` below the top nav**, so nav stays clickable.
- **A head block** with an uppercase accent-coloured label and one line of
  muted instruction ("Hover an equation to highlight the symbols it uses"). The
  instruction is doing real work — the hover linking is not discoverable
  otherwise.
- **Rows are a table**, key column `white-space: nowrap; width: 1%`, description
  column flexible, unit as a smaller muted block under the description.
- **Group header rows** break the list into sections. They dim rather than hide
  when filtered, so the reader keeps their place.
- **Transitions are short** (140ms for the highlight, 220ms for the slide) and
  the slide uses `cubic-bezier(0.22, 0.61, 0.36, 1)`.
- **Highlight colour is the page accent**, applied as a 2px left rule on the
  key cell plus an 11% background wash. Do not use a border on all four sides —
  it makes the row jump.
- **Referring elements get a matching treatment on hover**: a 7% accent wash
  and a 2px accent left rule, with a negative left margin so the rule sits in
  the gutter and the text does not shift.

---

## 4. Markup

```html
<aside id="glossary" class="glossary" data-open="false"
       aria-label="Parameter definitions">
  <button type="button" class="glossary-handle" id="gloss-toggle"
          aria-expanded="false" aria-controls="glossary-body">
    <span class="glossary-handle-text">Symbols</span>
  </button>
  <div class="glossary-body" id="glossary-body">
    <div class="glossary-head">
      <h4>Parameter definitions</h4>
      <p>Hover an equation to highlight the symbols it uses.</p>
    </div>
    <div class="glossary-scroll">
      <table class="sym-table" id="symbols"></table>
    </div>
  </div>
</aside>
```

Rows are generated from data, one per entry, group headers interleaved:

```html
<tr class="sym-group"><td colspan="2">Ability</td></tr>
<tr class="sym-row" data-sym="beta0">
  <td class="sym">\(\beta_0\)</td>
  <td>baseline ability<span class="unit">grades</span></td>
</tr>
```

A referring element declares the keys it uses:

```html
<div class="eqn" data-syms="beta0 sigma_user eps" tabindex="0">
  $$\text{ability}_u = \beta_0 + \sigma_{\text{user}}\tilde\epsilon_u$$
</div>
```

---

## 5. CSS

Swap the `--lg-*` tokens for the host project's equivalents. `--lg-nav-h` is
the sticky nav height; `--lg-gold` is the page accent.

```css
.glossary {
  position: fixed;
  top: var(--lg-nav-h);
  right: 0;
  bottom: 0;
  z-index: 9;                 /* below the top nav so it stays clickable */
  display: flex;
  align-items: stretch;
  pointer-events: none;       /* only the handle + body catch events */
  transform: translateX(calc(100% - var(--gloss-handle-w)));
  --gloss-handle-w: 30px;
  --gloss-body-w: 340px;
}
/* See trap 1: no transition until JS confirms the resting position is set. */
.glossary.is-animated {
  transition: transform 220ms cubic-bezier(0.22, 0.61, 0.36, 1);
}
.glossary[data-open='true'] { transform: translateX(0); }

.glossary-handle {
  pointer-events: auto;
  flex: 0 0 var(--gloss-handle-w);
  display: flex;
  align-items: center;
  justify-content: center;
  align-self: center;
  height: 160px;
  padding: 0;
  border: 1px solid var(--lg-border);
  border-right: none;
  border-radius: var(--lg-radius-md) 0 0 var(--lg-radius-md);
  background: var(--lg-surface);
  color: var(--lg-text-2);
  cursor: pointer;
  transition: color 140ms ease, background 140ms ease;
}
.glossary-handle:hover { color: var(--lg-gold); background: var(--lg-active); }
.glossary-handle-text {
  writing-mode: vertical-rl;
  text-orientation: mixed;
  transform: rotate(180deg);
  font-size: 0.72rem;
  text-transform: uppercase;
  letter-spacing: 0.12em;
  font-weight: 640;
}
.glossary[data-open='true'] .glossary-handle-text::after { content: ' \203A'; }
.glossary[data-open='false'] .glossary-handle-text::after { content: ' \2039'; }

.glossary-body {
  pointer-events: auto;
  flex: 0 0 var(--gloss-body-w);
  display: flex;
  flex-direction: column;
  min-height: 0;              /* lets .glossary-scroll actually scroll */
  background: var(--lg-surface);
  border-left: 1px solid var(--lg-border);
  box-shadow: -12px 0 28px -18px rgb(0 0 0 / 0.45);
}
.glossary-head { padding: 14px 16px 10px; border-bottom: 1px solid var(--lg-border); }
.glossary-head h4 {
  margin: 0 0 4px;
  font-size: 0.78rem;
  text-transform: uppercase;
  letter-spacing: 0.09em;
  color: var(--lg-gold);
}
.glossary-head p { margin: 0; font-size: 0.76rem; line-height: 1.45; color: var(--lg-text-2); }
.glossary-scroll { overflow-y: auto; overscroll-behavior: contain; padding: 4px 0 24px; min-height: 0; }

.glossary .sym-table { width: 100%; border-collapse: collapse; font-size: 0.8rem; }
.glossary .sym-table td { padding: 7px 12px; vertical-align: top; line-height: 1.45; }
.glossary .sym-table tr.sym-row { border-bottom: 1px solid var(--lg-border); }
.glossary .sym-table td.sym {
  white-space: nowrap;
  width: 1%;
  padding-right: 10px;
  border-left: 2px solid transparent;
}
.glossary .sym-table .unit {
  display: block;
  margin-top: 3px;
  color: var(--lg-text-2);
  font-size: 0.7rem;
  white-space: normal;
}

/* Reserve the gutter so the centred column re-centres beside the panel.
   Below 1180px the panel overlays as a drawer and no space is reserved. */
@media (min-width: 1180px) {
  .pane.gloss-open { padding-right: var(--gloss-body-w, 340px); }
  /* Gated for the same reason as .glossary.is-animated -- see trap 1. */
  .pane.gloss-anim {
    transition: padding-right 220ms cubic-bezier(0.22, 0.61, 0.36, 1);
  }
}
@media (prefers-reduced-motion: reduce) {
  .pane.gloss-anim { transition: none; }
}

/* --- highlight wiring --- */
.glossary.is-filtered .sym-row:not(.sym-hit) { opacity: 0.28; }
.glossary.is-filtered .sym-group { opacity: 0.4; }
.glossary .sym-row.sym-hit td.sym { border-left-color: var(--lg-gold); }
.glossary .sym-row.sym-hit { background: color-mix(in srgb, var(--lg-gold) 11%, transparent); }
.glossary .sym-row, .glossary .sym-group { transition: opacity 140ms ease, background 140ms ease; }

/* Referring elements become hoverable/clickable targets. */
.eqn {
  border-radius: var(--lg-radius-md);
  border-left: 2px solid transparent;
  padding: 2px 0 2px 10px;
  margin-left: -12px;
  transition: background 140ms ease, border-color 140ms ease;
  outline: none;
  cursor: pointer;
}
.eqn:hover, .eqn:focus-visible, .eqn.eqn-active {
  background: color-mix(in srgb, var(--lg-gold) 7%, transparent);
  border-left-color: var(--lg-gold);
}
```

---

## 6. JavaScript

```js
const GLOSS_KEY = 'app.glossary.open';

function setGlossaryOpen(open, persist = true) {
  const panel = document.getElementById('glossary');
  const btn = document.getElementById('gloss-toggle');
  if (!panel) return;
  panel.dataset.open = open ? 'true' : 'false';
  // Reserve the gutter so the centred article re-centres beside the panel
  // instead of running underneath it.
  document.getElementById('pane')?.classList.toggle('gloss-open', open);
  // Anything measuring its own width has to be told the layout moved.
  setTimeout(resizeCharts, 240);
  if (btn) btn.setAttribute('aria-expanded', open ? 'true' : 'false');
  if (persist) {
    try { localStorage.setItem(GLOSS_KEY, open ? '1' : '0'); } catch (e) { /* private mode */ }
  }
}

function highlightSymbols(keys) {
  const panel = document.getElementById('glossary');
  if (!panel) return;
  const rows = panel.querySelectorAll('[data-sym]');
  if (!keys) {
    panel.classList.remove('is-filtered');
    rows.forEach((r) => r.classList.remove('sym-hit'));
    return;
  }
  const want = new Set(keys);
  panel.classList.add('is-filtered');
  let first = null;
  rows.forEach((r) => {
    const hit = want.has(r.dataset.sym);
    r.classList.toggle('sym-hit', hit);
    if (hit && !first && r.classList.contains('sym-row')) first = r;
  });
  const scroller = panel.querySelector('.glossary-scroll');
  if (first && scroller) {
    const fr = first.getBoundingClientRect();
    const sr = scroller.getBoundingClientRect();
    if (fr.top < sr.top || fr.bottom > sr.bottom) {
      const reduce = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
      scroller.scrollTo({
        top: scroller.scrollTop + (fr.top - sr.top) - 12,
        behavior: reduce ? 'auto' : 'smooth',
      });
    }
  }
}

let glossaryBound = false;
function bindGlossary() {
  if (glossaryBound) return;
  const panel = document.getElementById('glossary');
  if (!panel) return;
  glossaryBound = true;

  document.getElementById('gloss-toggle')?.addEventListener('click', () => {
    setGlossaryOpen(panel.dataset.open !== 'true');
  });

  document.querySelectorAll('.eqn').forEach((eq) => {
    const keys = (eq.dataset.syms || '').split(/\s+/).filter(Boolean);
    // Hover filters, but only when the panel is already open -- opening it on
    // hover is jarring.
    const on = () => { if (panel.dataset.open === 'true') highlightSymbols(keys); };
    const off = () => highlightSymbols(null);
    eq.addEventListener('mouseenter', on);
    eq.addEventListener('mouseleave', off);
    eq.addEventListener('focus', on);
    eq.addEventListener('blur', off);

    // Click is a toggle: open onto this element's entries, or shut it again.
    const toggle = () => {
      if (panel.dataset.open === 'true') {
        highlightSymbols(null);
        setGlossaryOpen(false);
        return;
      }
      setGlossaryOpen(true);
      // Wait for the slide, so the scroll-into-view measures final geometry.
      setTimeout(() => highlightSymbols(keys), 220);
    };
    eq.setAttribute('role', 'button');
    eq.setAttribute('title', 'Show these symbols in the reference panel (click again to close it)');
    eq.addEventListener('click', toggle);
    eq.addEventListener('keydown', (ev) => {
      if (ev.key === 'Enter' || ev.key === ' ') { ev.preventDefault(); toggle(); }
    });
  });

  // Reverse direction: hovering a definition marks the elements that use it.
  panel.addEventListener('mouseover', (ev) => {
    const row = ev.target.closest('[data-sym]');
    if (!row) return;
    document.querySelectorAll('.eqn').forEach((eq) => {
      const keys = (eq.dataset.syms || '').split(/\s+/);
      eq.classList.toggle('eqn-active', keys.includes(row.dataset.sym));
    });
  });
  panel.addEventListener('mouseleave', () => {
    document.querySelectorAll('.eqn.eqn-active').forEach((eq) => eq.classList.remove('eqn-active'));
  });

  document.addEventListener('keydown', (ev) => {
    if (ev.key === 'Escape' && panel.dataset.open === 'true') setGlossaryOpen(false);
  });

  // Default open where there is a gutter to open into; remember the choice.
  let stored = null;
  try { stored = localStorage.getItem(GLOSS_KEY); } catch (e) { /* private mode */ }
  const wide = window.matchMedia('(min-width: 1180px)').matches;
  setGlossaryOpen(stored === null ? wide : stored === '1', false);

  // Only now, once the resting position is set, allow the slide to animate.
  // See trap 1.
  const pane = document.getElementById('pane');
  const enableAnim = () => {
    if (panel.classList.contains('is-animated')) return;
    panel.getAnimations().forEach((a) => a.cancel());
    pane?.getAnimations().forEach((a) => a.cancel());
    panel.classList.add('is-animated');
    pane?.classList.add('gloss-anim');
  };
  requestAnimationFrame(() => requestAnimationFrame(enableAnim));
  setTimeout(enableAnim, 120);   // rAF never fires in a background tab
}
```

If the panel content is injected as `innerHTML` after a maths typesetter has
already run (KaTeX auto-render fires on `DOMContentLoaded`), typeset it
explicitly afterwards or the rows show raw `\(...\)` source.

---

## 7. Traps

Each of these cost real debugging time. Check them before assuming a bug is
elsewhere.

### Trap 1 — never start a CSS transition inside a hidden container

If the panel lives inside a tab pane or any container that starts
`display: none`, a transition that begins in that state **sticks in
`playState: "running"` forever and pins the property at its start value**.
Because transitions outrank inline styles in the cascade, `el.style.foo = ...`
cannot override it either. Symptom: the panel silently refuses to open, or a
`padding-right` refuses to apply, with correct and matching CSS.

Fix: declare every transition under a class that JS adds *after* the resting
position is set (`.is-animated`, `.gloss-anim` above), cancelling any
animations that already exist.

The `setTimeout(enableAnim, 120)` backstop is not belt-and-braces:
**`requestAnimationFrame` does not fire at all while a tab is backgrounded**,
so without it the class would never be added for anyone opening the page in a
background tab.

Useful side effect: the panel stops visibly sliding in every time the view is
selected.

**This trap recurs.** It bit again in the same codebase on an unrelated hover
popover whose only transition was `opacity 120ms, visibility 120ms`. For small
elements the cheapest fix is to have **no transition at all** rather than gate
one.

### Trap 2 — a fixed panel must push content, not overlay it

The article column is `width: min(100%, 920px); margin: 0 auto`. A panel fixed
to the right gutter clipped the last ~15px of every line at a 1470px viewport —
subtle enough to look like a font bug.

Reserve the space with `padding-right` on the **pane**, not on the article: a
`margin: 0 auto` column then re-centres inside what is left. Below 1180px there
is no gutter worth reserving, so the panel overlays as a drawer and defaults to
shut.

### Trap 3 — anything that measures its own width must be told

Charts, canvases and any layout that reads `clientWidth` will have measured the
pre-panel width. `setGlossaryOpen` fires a resize hook after the slide
duration. If a redraw is expensive, guard it — only redraw when the width has
actually changed by more than a couple of pixels, or the panel slide will
trigger a full re-render on every animation frame and lock the tab.

### Trap 4 — verifying through browser automation is misleading

A tab driven by automation reports `document.visibilityState: "hidden"` with
its animation clock frozen at `0`, so **no transition ever advances and
everything looks stuck**. Check `document.timeline.currentTime` before
believing a motion bug. Verify resting geometry with transitions stripped
(`classList.remove('is-animated')` plus `getAnimations().forEach(a => a.cancel())`)
rather than waiting on the clock.

---

## 8. Accessibility checklist

- `aria-expanded` and `aria-controls` on the handle, kept in sync.
- `aria-label` on the `<aside>`.
- `Escape` closes.
- Referring elements are `tabindex="0"` with `role="button"`, so the filter and
  the toggle both work from the keyboard.
- Focus filters the same way hover does (`focus`/`blur` alongside
  `mouseenter`/`mouseleave`).
- `prefers-reduced-motion` drops the slide and the scroll-into-view smoothing.
- The panel never becomes the only route to information — it is a convenience
  index over content that also exists in the page.

---

## 9. Sibling pattern — the info dot

A smaller relative, used where a block of guidance would otherwise eat a
screenful: a circled "i" beside a chart or control that reveals a rich popover
(a whole table, in the reference implementation).

- Opens on hover, on keyboard focus, and on click. **Click pins it open**, so
  touch works and a long popover can be scrolled without it vanishing.
- Clicking elsewhere, or `Escape`, closes it. Only one is pinned at a time.
- The popover flips its anchor (`right: -4px` → `left: -4px`) if it would run
  off the left edge; check with `getBoundingClientRect()` after showing.
- Structure it as `.infowrap > (button.infodot + div.infopop)`. Do **not** nest
  the popover inside the button or inside a `<span>` — a `<table>` is not valid
  phrasing content and the nesting will not survive.
- **No transition on the popover** — see trap 1.

---

## 10. Reference implementation

In the `kaya` repo, `src/kaya/viewer_static/`:

| piece | file | anchor |
|---|---|---|
| markup | `index.html` | `<aside id="v2-glossary" class="glossary">` |
| styles | `research.css` | `/* ==== docked symbol glossary ==== */` |
| info dot styles | `research.css` | `/* ---- info dot ---- */` |
| data | `app.js` | `V2_SYMBOLS`, rows are `[key, latex, description, unit]` |
| behaviour | `app.js` | `setV2GlossaryOpen` / `highlightV2Symbols` / `bindV2Glossary` |
| info dot behaviour | `app.js` | `bindInfoDots` |
