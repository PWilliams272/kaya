# Kaya Viewer — Design & Deployment Handoff

Written by the agent working in `aws_flask_site`, for the agent working in `kaya`. The
main site's design system is now locked and is the **source of truth**. This doc tells
you what changed, what to do with it, and what the viewer needs before the site can
reverse-proxy it. Nothing in this repo has been modified except two additions noted
below — the actual integration work is yours to do.

## Update — 2026-07-27: full component reference added, use this as the primary source

Earlier revisions of this doc described things piecemeal (colors, then a follow-up
correction on shape/elevation) — that pattern kept missing pieces. Instead of continuing
to describe the design system in prose, `src/kaya/viewer_static/design-system-reference.html`
is now a complete, self-contained, **open-it-in-a-browser** reference: every token, plus
real working examples of nav + grouped dropdowns + mobile hamburger, underline tabs, the
searchable combobox, pill multiselect, buttons, stat tiles with trend icons, a bar chart
with a highlighted peak bar, a data table, both alert states, cards/badges, and the full
type scale — all built from the actual locked values, not a set of options to compare.

**Treat this file as the primary reference for everything visual.** The color-mapping
table and shape/elevation notes further down are still accurate and useful context for
*why* things are the way they are, but where anything conflicts, the HTML file wins —
it's generated directly from the same CSS as the real site's `tokens.css`/`site.css`.

## Update — 2026-07-27: background/container colors are inverted, and the sidebar needs to go

Both read from your current `app.css`/`index.html` directly, not guessed.

### 1. Background/container inversion — one wrong token mapping

`.card` (and anything else using `--lg-card`) currently renders **grey**, while things
using `--lg-surface` (the sidebar, buttons, tab pills, search box) render **white**. It
should be the reverse: page background grey, cards/containers white — that's how the
reference file and the real site both work. The cause: `--lg-card` was mapped to
`--surface-raised` (a light-grey tone meant for nav-type chrome) instead of `--surface`
(white, meant for content containers). Fix — in `tokens.css`, make `--lg-card` equal
`--lg-surface`'s value in every block:

```css
/* light (:root and [data-theme='light']) */
--lg-card: #ffffff;   /* was #edf0f2 — now matches --lg-surface */

/* dark (@media prefers-color-scheme + [data-theme='dark']) */
--lg-card: #161f2c;   /* was #1c2736 — now matches --lg-surface */
```

No selector changes needed anywhere else — `.card`, `.chart-card`, `.note-card`,
`.sidebar-note` all already reference `var(--lg-card)`, so they'll pick this up
automatically. (There's genuinely no separate "surface-raised" tier used for cards in
the site's system — cards and general surface are the same white.)

### 2. The sidebar shouldn't exist — the site's layout has no persistent side nav

`index.html` currently has `<div class="app-shell"><aside class="sidebar">...</aside>
<main class="main-shell">...</main></div>`, and `app.css` lays that out as a
`grid-template-columns: var(--lg-sidebar-w) 1fr` two-column split. The site's own layout
(and the reference file) is a single-column page with a top bar — no persistent left
rail anywhere on peterwilliams.dev. Concretely:

**In `app.css`:**
```css
/* .app-shell — remove the grid split */
.app-shell {
  display: block;   /* was: display: grid; grid-template-columns: var(--lg-sidebar-w) 1fr; */
}

/* .sidebar — delete this rule entirely once the HTML is removed */

/* .main-shell — stop subtracting sidebar width */
.main-shell {
  width: min(100%, var(--lg-content-max));   /* was: min(calc(100vw - var(--lg-sidebar-w)), var(--lg-content-max)) */
  /* padding/margin/flex rules unchanged */
}
```
Also delete the two `@media (max-width: 1100px)` rules for `.app-shell`/`.sidebar`/
`.main-shell` (lines ~466-481) — they only exist to handle the sidebar collapsing, which
won't apply anymore.

**In `index.html`:** remove the `<aside class="sidebar">…</aside>` block. Its contents
need a new home:
- The `.brand-block` (\"Kaya / Local Viewer / SQLite-backed interactive exploration\")
  is redundant with the `.topbar`'s own `<h1>Kaya Activity Console</h1>` — simplest fix
  is to just drop the duplicate and, if you want the subtitle context kept, add it under
  the topbar's `<h1>` instead.
- The two `.sidebar-note` cards (\"Data Source\", \"Segment Rule\") are about data
  provenance/methodology — the natural home is inside the **Data Overview** tab rather
  than pinned to every tab permanently. If you want that context visible everywhere, an
  info icon/button in the topbar that reveals it in a small popover is a reasonable
  alternative — your call, both are fine, just don't leave it as a persistent sidebar.

Don't build a new nav bar to replace the sidebar — Kaya is a single-view tool with four
tabs, not a multi-page site, so the topbar + tab-bar you already have is the right amount
of navigation chrome. The grouped-dropdown nav pattern in the reference file is for the
main site's multi-page structure and doesn't apply here.

## Update — 2026-07-27: gym pills, search focus, and dropdown hover are grey, should be blue

Same root cause as the background/container issue: a missing token. The site's pills,
focus rings, and hover tints all use `--accent-soft` (a light blue wash) — Kaya's
tokens.css never got an equivalent, so `.combo-pill`, the search box's open/focus state,
and `.combo-option:hover` all fall back to neutral grey tokens (`--lg-active`,
`--lg-border-h`) instead.

**1. Add the missing token**, alongside your existing `--lg-gold`/`--lg-info` etc.:

Light: `--lg-gold-soft: rgba(25, 118, 210, 0.10);`
Dark (both the `@media` block and `[data-theme='dark']`): `--lg-gold-soft: rgba(77, 166, 255, 0.20);`

**2. `.combo-pill`** — currently grey background + neutral border. Match the site's
pill: tinted blue background, blue text, no border, weight 600.
- `background: var(--lg-active)` → `background: var(--lg-gold-soft)`
- `color: var(--lg-text)` → `color: var(--lg-gold)`
- add `font-weight: 600;`
- remove the `border: 1px solid var(--lg-border-h);` line — site pills have no border

**3. `.combo-pill-remove`** — let the × inherit the pill's blue instead of forcing grey;
keep the hover as a neutral dark overlay (reads fine on a light blue chip):
- `color: var(--lg-text-2)` → `color: inherit;`
- in the `:hover` rule: `background: var(--lg-border-h)` → `background: rgba(0, 0, 0, 0.12);`, and drop the `color: var(--lg-text);` hover override entirely

**4. Search box focus/open state** — `.combo-select.is-open .combo-control` currently
uses a neutral border; should glow blue like every other focused input on the site:
- `border-color: var(--lg-border-h)` → `border-color: var(--lg-gold);`
- add `box-shadow: 0 0 0 3px var(--lg-gold-soft);`

**5. Dropdown option hover** — `.combo-option:hover` currently uses the same neutral
`--lg-active` wash as a generic pressed-state; should be the blue tint like every other
hoverable row/link on the site:
- `background: var(--lg-active)` → `background: var(--lg-gold-soft);`

This covers the gym-comparison filter specifically, but the same grey-vs-blue pattern is
worth checking anywhere else `--lg-active`/`--lg-border-h` get used for something that's
actually an interactive/selected/focused state rather than a genuinely neutral one —
`--lg-active` should stay reserved for true neutral press-states, not stand in for the
accent color by default.

## What's already been dropped in this repo (informational only)

- `src/kaya/viewer_static/fonts/ibm-plex-sans-400.woff2` and `-700.woff2` — the real
  font files, copied over so you don't need to re-fetch them.
- `src/kaya/viewer_static/site-tokens-reference.css` — a verbatim copy of the site's
  canonical `app/static/css/tokens.css`. Reference only, **not wired into anything**.

Nothing else was touched. `tokens.css`, `app.css`, `app.js`, and `index.html` are all
exactly as you left them.

## Source of truth

`site-tokens-reference.css` in this repo (copied from `aws_flask_site`'s
`app/static/css/tokens.css`) is canonical. If the site's tokens change later, that file
should get re-copied here by hand — there's no live coupling between the repos, by
design (same reasoning as the reverse-proxy decision: keep this repo fully
self-contained, no submodules, no runtime cross-repo fetches).

## The decisions, and why

- **Primary color**: `#1976d2` / hover `#1565c0` — this is literally the site's
  existing brand blue (already live in `base.html`, `navbar.html`), not a new choice.
- **Secondary/complementary**: copper `#b8752e` (light) / `#d99a52` (dark) — used only
  where something must not read as the primary action (a status badge, a chart's peak
  value). Picked over a "Graphite" alternative specifically for contrast/visibility.
- **Semantic colors** (success/warning/danger) are independent of both accent and
  highlight — never reuse the accent or highlight hue for status meaning.
- **Typography**: IBM Plex Sans, weights 400/700, one family across headings and body
  (no separate display face). Picked over two other traditional sans candidates
  (Source Sans 3, Public Sans) specifically for its slightly more "engineered"
  character, fitting for a data-heavy technical site.
- **Shape**: sharp radii (3/4/6px), flat/bordered elevation — no drop shadows, borders
  do the work. Deliberately on the sharper/flatter side of current design trends,
  matching how GitHub/Linear/Grafana/Stripe's dashboard do it, chosen for internal
  consistency with the flat elevation choice.
- **Light/dark**: defaults to `prefers-color-scheme` (Auto), with `[data-theme="light"]`
  / `[data-theme="dark"]` as manual overrides that should persist (e.g. localStorage).
  Light is the preferred default when no system preference is detected.

## Integration guidance — mapping to your existing tokens.css

I read your current `tokens.css`, `app.css`, and `app.js` before writing this. Findings,
so you don't need to re-derive them:

- **`app.css` has zero hardcoded colors** — every color reference goes through a
  `--lg-*` custom property. That means recoloring is a tokens.css-only change; you
  should not need to touch `app.css` at all for the color/type remap.
- **`app.js` has ~10 hardcoded hex colors** (`#ffc36b`, `#9ad0ff`, `#ff8eb6`, etc.) —
  these are the Plotly chart colorway (series colors for gym-comparison/grade charts),
  not UI chrome. **Leave these alone.** Chart theming is an explicitly separate,
  later piece of work — Plotly renders its own SVG independent of page CSS, so it
  can't be driven by these tokens without a dedicated Plotly template, which hasn't
  been designed yet.
- **Structural tokens are untouched by this handoff**: `--fs-*` (type scale),
  `--lg-nav-h`, `--lg-sidebar-w`, `--lg-content-max`, `--lg-gap`, `--lg-pad`,
  `--lg-radius-*`, `--ease-*`, `--dur-*`. Those are Kaya's own layout/motion system,
  not part of the site's color/type decision — no reason to change them.
- **Two pre-existing issues worth fixing while you're in here:**
  1. `tokens.css` sets `font-family: 'Nicky Sans', system-ui, ...` but there's no
     `@font-face` for Nicky Sans anywhere in the repo — it's a silent fallback to
     `system-ui` on any machine that doesn't happen to have that font installed
     locally. Replacing it with a real embedded IBM Plex Sans (`@font-face` pointing
     at the two `.woff2` files already dropped in `fonts/`) fixes this incidentally.
  2. `index.html` hardcodes `<html data-theme="dark">` as a static default, and
     `app.js`'s `initialize()` only overrides it *from* `localStorage` — meaning the
     viewer currently always opens dark for first-time visitors regardless of their
     OS setting. To match the site's Auto-by-default behavior, remove that hardcoded
     attribute so `prefers-color-scheme` can govern when nothing's saved yet.
     One knock-on fix needed: `updateThemeToggleIcon()` (around line 2223) currently
     checks `dataset.theme === 'light'` to decide the icon, which will show the wrong
     icon on first load if the OS is actually in light mode with no explicit
     attribute set. Fix by checking `window.matchMedia('(prefers-color-scheme: dark)')`
     when `dataset.theme` is unset, rather than only treating `'light'` as light.

- **Suggested `--lg-*` → canonical value mapping** (see `site-tokens-reference.css`
  for the exact hex values, this just maps names):

  | Your token | Maps to |
  |---|---|
  | `--lg-bg` | `--bg` |
  | `--lg-surface` | `--surface` |
  | `--lg-card` | `--surface-raised` |
  | `--lg-border` | `--border` |
  | `--lg-border-h` | derive a slightly stronger neutral from `--border` (no direct equivalent in the site tokens) |
  | `--lg-active` | a low-opacity neutral wash (not accent-colored) — the site tokens don't define this, keep it neutral like your original |
  | `--lg-text` | `--text` |
  | `--lg-text-2` | `--text-muted` |
  | `--lg-text-3` | derive a third, lighter/more-muted tier (site tokens only have two text tiers) |
  | `--lg-gold` (your primary accent) | `--accent` — **this is the one big visible change: gold → blue**. Consider whether the name itself should change too, or stay for compatibility. |
  | `--lg-info` | `--accent-hover` (keeps it distinguishable from `--lg-gold` now that both are blue-family) |
  | `--lg-success` / `--lg-warning` / `--lg-danger` | `--success` / `--warning` / `--danger` directly |

  Restructuring `:root` (dark-first) → light-first with `prefers-color-scheme` +
  `[data-theme]` overrides is recommended for consistency with the site, but is a
  structural change to how values get assigned, not to what `app.css`/`app.js`
  consume — they only ever read the resolved `--lg-*` value, so this is safe to do
  independent of anything else.

  **Heads up**: this is a real, visible re-skin — your current look is a near-black/gold
  dark-first theme; the target is light-first/blue/copper. Confirm with the site owner
  before/while doing this that the full swap (not a partial blend) is actually wanted,
  since you clearly put real design work into the current look.

## Deployment — what this app needs before the site can reverse-proxy it

Earlier discussion settled on **reverse-proxying this app as its own independent
service**, not porting routes into Flask and not a submodule. `viewer_payloads.py` stays
untouched either way — this is purely about how the FastAPI app gets run and reached.

What needs to change here, roughly in priority order:

1. **Production launch command.** Current `uvicorn.run(..., host='127.0.0.1', port=8000,
   reload=True)` in `viewer_app.py` is dev-shaped. Drop `reload=True`. Keep the bind on
   `127.0.0.1` (loopback only) — this app should never be reachable on a public
   interface directly; the site's nginx is the only intended entry point.
2. **CORS.** Currently `allow_origins=['*']` combined with `allow_credentials=True` —
   browsers generally reject that combination anyway, and it needs to be scoped to the
   real site origin(s) before this is exposed publicly.
3. **Remove (or env-gate) the no-cache-everything middleware.** It's explicitly a
   local-dev-loop hack per your own code comments — fine for `uvicorn --reload`, bad
   for production caching of an ~80KB JS bundle and multi-MB JSON payloads.
4. **Systemd unit.** Needs a real service definition so it's supervised (auto-restart,
   starts on boot) — something like:
   ```ini
   [Unit]
   Description=Kaya viewer
   After=network.target

   [Service]
   WorkingDirectory=/path/to/kaya
   ExecStart=/path/to/kaya/.venv/bin/uvicorn kaya.viewer_app:app --host 127.0.0.1 --port 8010
   Restart=on-failure
   User=<tbd — verify on host>
   EnvironmentFile=/path/to/kaya/.env

   [Install]
   WantedBy=multi-user.target
   ```
   Port `8010` is a placeholder — just needs to match whatever the site's nginx config
   points at. Paths and the service user need verifying on the actual EC2 host; don't
   assume this template is complete as-is (per the site's own handoff docs: don't
   invent preview/infra details without verifying against the live host).
5. **No auth needed in this app itself** — confirmed there's none currently, and that's
   fine. The plan is for the site's nginx to gate access via an `auth_request`
   subrequest against a small Flask endpoint that checks the session cookie, before
   proxying through to this service. Nothing for you to build here on the auth side.
6. **Data path — open decision, not made for you:** the viewer currently reads a local
   213MB SQLite file via `KayaDataAccessor` defaults. For a deployed instance, either
   (a) keep it synced on the host via the existing `sync_local_data.py` (S3-driven), or
   (b) run in the existing static-JSON snapshot mode (`?dataMode=static`), which loses
   the live gym/date filter interactivity. Pick based on how live the data needs to be
   day to day.
7. **Root-relative asset paths matter for routing.** Every asset/API call in
   `app.js`/`index.html` uses absolute-rooted paths (`/static/...`, `/api/...`,
   `/viewer-data/...`), not page-relative ones. That works cleanly under a **subdomain**
   (e.g. `kaya.peterwilliams.dev`) but breaks under a path prefix
   (`peterwilliams.dev/kaya/`) unless you add `root_path`/`X-Forwarded-Prefix` support.
   Subdomain is the agreed direction — no code change needed for that to work, just
   confirming so nobody tries a path-prefix mount later and gets confused when assets
   404.

**What's on the site side, not yours** (flagging so you know it's not blocked on you):
DNS record for the subdomain, the nginx server block and `auth_request` endpoint, and
the TLS cert — none of that has been set up yet, and none of it can be done without
access to the live EC2 host, which this session doesn't have. Consider deployment
itself an explicit discovery task when someone does have host access, not something
to assume is ready.

## Update — 2026-07-27: lighter background

The light-mode background and card/surface-raised values got adjusted after seeing them
applied across the real site — the original `--bg`/`--surface-raised` read as too dark
next to a near-white page. `site-tokens-reference.css` in this repo has been re-synced
with the new values; if your `tokens.css` already picked up the old ones (it looks like
it did, based on `--lg-bg`/`--lg-card` in your `:root` and `[data-theme='light']`
blocks), update just these two, light mode only — nothing else changed:

| Token | Old | New |
|---|---|---|
| `--bg` → your `--lg-bg` | `#eef1f4` | `#f6f8f9` |
| `--surface-raised` → your `--lg-card` | `#e4e9ee` | `#edf0f2` |

Dark mode values are untouched. Typography is still an open/tentative decision on the
site side — don't treat IBM Plex Sans as fully locked yet, just the current default.

## Update — 2026-07-27: shape/elevation was missing from this handoff, correcting now

The original version of this doc told you to leave `--lg-radius-*` and elevation alone,
calling them "layout system, not part of the color/type decision." That was wrong —
shape (sharp corners, flat/bordered surfaces, no drop shadows) was a deliberate, explicit
part of the design decision on the site side, just as much as the blue accent was. I read
through `app.css` to see where this actually diverges from what got decided:

- **`.card` uses `var(--lg-radius-lg)` = 10px.** The site's max radius anywhere is 6px.
- **`.ghost-button`, `.theme-toggle-button`, `.status-pill`, `.tab-button` all use
  `border-radius: 999px`** (fully rounded/pill-shaped).
- **`.combo-panel` (the search dropdown) has `box-shadow: 0 12px 24px rgba(0,0,0,0.25)`**
  — a pronounced floating shadow. The site's elevation choice is Flat: borders do the
  work, shadows are minimal-to-none.
- **`.inline-select-control select`, `.combo-control`, `.combo-panel` use
  `var(--lg-radius-md)` = 8px.**

What to actually do:

1. **Tighten the radius scale.** Suggested remap (halves everything, caps at 6px):

   | Token | Old | New |
   |---|---|---|
   | `--lg-radius-xs` | 4px | 3px |
   | `--lg-radius-sm` | 6px | 3px |
   | `--lg-radius-md` | 8px | 4px |
   | `--lg-radius-lg` | 10px | 5px |
   | `--lg-radius-xl` | 12px | 6px |

   Since `.card`, `.combo-control`, `.combo-panel`, etc. all reference these tokens
   rather than hardcoding pixel values, this one change should cascade correctly without
   touching `app.css` itself.

2. **Fix the floating shadow.** Replace `.combo-panel`'s `box-shadow: 0 12px 24px
   rgba(0,0,0,0.25)` with something much more restrained — light mode
   `0 6px 16px rgba(15,23,32,0.10)`, dark mode `0 8px 20px rgba(0,0,0,0.35)` (these are
   the site's own `--shadow-md` values; consider adding a `--lg-shadow-md` token that
   switches with the theme the same way the color tokens do, rather than a single static
   value, since it needs to look right in both modes).

3. **`combo-pill`'s `border-radius: 999px` is correct, leave it.** Pills/chips staying
   fully rounded is consistent with the site's own system (its multi-select pills work
   the same way) — the sharp-corners decision applies to containers/cards/inputs, not
   tag/pill shapes. Don't square those off.

4. **`.tab-button`'s pill-button style is an open question, not a directive.** The
   site's own tab pattern is underline-style — active tab gets a 2px accent-colored
   bottom border, inactive tabs are plain text, no button chrome or background fill.
   Your current pill-shaped tab buttons (border + background, filled on active) are a
   legitimately different, valid pattern — I don't have confirmation from the site owner
   that changing this specific piece is wanted, unlike the radius/shadow items above
   which are unambiguous. Flag it and get an explicit answer before changing tab
   interaction chrome, since it's a real UI pattern change, not a token correction.

5. **`.section-heading`'s uppercase + `letter-spacing: 0.08em` is already exactly
   right** — matches the site's label convention precisely. Nothing to change here,
   just confirming so you don't second-guess it while going through the rest.

## Suggested order of operations

1. Recolor/retype via `tokens.css` following the mapping above; verify visually against
   `site-tokens-reference.css`'s actual values.
2. Fix the `Nicky Sans` fallback and the hardcoded dark-default (both above).
3. Harden for production per the deployment list (drop `reload`, fix CORS, remove
   no-cache middleware).
4. Decide the data-sync approach.
5. Only then is this ready to hand back for the nginx/systemd side of the work.