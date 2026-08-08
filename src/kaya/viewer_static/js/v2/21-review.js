// ---- Review tools: sticky notes, chart resizing, draggable chart labels
//
// DEVELOPMENT ONLY, like 20-edit.js. Loaded behind `{% if editable %}` and
// posting to dev-only routes; see viewer_copy.py for the boundary and why it
// matters on a public, unauthenticated page.
//
// THE THREE THINGS, AND WHY EACH IS HERE.
//
//   NOTES. A sticky note anchored to a block or a chart, saying what to change
//   about it. They persist to viewer_content/prelim_notes.json, which the next
//   session reads -- so "move this label left" lands next to the label instead
//   of in a chat message that has to be re-explained against a page the reader
//   cannot see. Notes never render in production; they are messages about the
//   page, not part of it.
//
//   RESIZE. Drag the bottom edge of any chart to set its height. Persisted to
//   prelim_layout.json and applied in BOTH environments: once a figure has
//   been sized, that is the figure. Only the ability to change it is dev-only.
//
//   LABEL DRAGGING. Plotly can already drag its own annotations and shapes --
//   `config.edits` -- so edit mode turns that on and listens for the relayout
//   event to persist where things landed. This moves the TEXT and the LINES in
//   a figure. It does NOT move individual data points: Plotly has no hook for
//   that, and a hand-rolled one would be a figure editor rather than a nudge.
//   The landscape figure's marks are hard-coded coordinates in 18-ability.js,
//   so ask for those by note and they get edited at the source.

const PM_NOTES_ENDPOINT = '/api/prelim-notes';
const PM_BLOCKS_ENDPOINT = '/api/prelim-blocks';
const PM_LAYOUT_ENDPOINT = '/api/prelim-layout';
const PM_HIDDEN_ENDPOINT = '/api/prelim-hidden';

// Populated from the template; see prelim.html.
const PM_REVIEW = {
  notes: (typeof PM_INITIAL_NOTES !== 'undefined' ? PM_INITIAL_NOTES : []).slice(),
  layout: (typeof PM_INITIAL_LAYOUT !== 'undefined' ? PM_INITIAL_LAYOUT : {}),
  blocks: (typeof PM_INITIAL_BLOCKS !== 'undefined' ? PM_INITIAL_BLOCKS : []).slice(),
  hidden: (typeof PM_INITIAL_HIDDEN !== 'undefined' ? PM_INITIAL_HIDDEN : []).slice(),
  dirty: false,
};

function pmChartEls() {
  return Array.from(document.querySelectorAll('.pm-chart[id]'));
}

// --- persistence --------------------------------------------------------

async function pmPost(url, body, label) {
  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      pmEditSetState(`${label} failed: ${res.status}`, 'bad');
      console.error(`[prelim] ${label} failed`, res.status, await res.text());
      return false;
    }
    return true;
  } catch (err) {
    pmEditSetState(`${label} failed: no response`, 'bad');
    console.error(`[prelim] ${label} failed`, err);
    return false;
  }
}

async function pmSaveNotes() {
  const ok = await pmPost(PM_NOTES_ENDPOINT, { notes: PM_REVIEW.notes }, 'notes');
  if (ok) pmEditSetState(`${PM_REVIEW.notes.length} notes saved`, 'ok');
}

async function pmSaveLayout(chartId, patch) {
  const entry = PM_REVIEW.layout[chartId] || {};
  PM_REVIEW.layout[chartId] = { ...entry, ...patch };
  const ok = await pmPost(PM_LAYOUT_ENDPOINT,
    { layout: { [chartId]: PM_REVIEW.layout[chartId] } }, 'layout');
  if (ok) pmEditSetState(`${chartId} saved`, 'ok');
}

// --- sticky notes -------------------------------------------------------

// Anchors are the element the note is about: a data-copy key, or a chart id.
// Stored rather than a DOM position, so a note stays attached to its subject
// when the page reflows or the copy above it grows.
function pmAnchorOf(el) {
  const block = el.closest('[data-copy]');
  if (block) return block.dataset.copy;
  const chart = el.closest('.pm-chart[id]');
  return chart ? chart.id : null;
}

function pmNoteId() {
  return `n${Date.now().toString(36)}${Math.floor(Math.random() * 1e4).toString(36)}`;
}

function pmRenderNotes() {
  document.querySelectorAll('.pm-note').forEach((n) => n.remove());
  const counts = new Map();
  PM_REVIEW.notes.forEach((note) => {
    const host = document.querySelector(`[data-copy="${note.anchor}"]`)
      || document.getElementById(note.anchor);
    if (!host) return;                    // anchor removed; note kept in file
    const el = document.createElement('div');
    el.className = `pm-note${note.done ? ' pm-note-done' : ''}`;
    el.dataset.noteId = note.id;
    const seen = counts.get(note.anchor) || 0;
    counts.set(note.anchor, seen + 1);
    el.style.marginTop = seen ? '6px' : '';
    el.innerHTML = `
      <textarea class="pm-note-text" rows="2"></textarea>
      <div class="pm-note-actions">
        <button type="button" class="pm-note-btn" data-act="done"
          title="Mark handled">${note.done ? 'reopen' : 'done'}</button>
        <button type="button" class="pm-note-btn" data-act="del"
          title="Delete note">delete</button>
      </div>`;
    // textContent, never innerHTML: a note is plain text, and the server
    // escapes it on the way in for the same reason.
    el.querySelector('.pm-note-text').value = note.text;
    host.insertAdjacentElement('afterend', el);
  });
}

function pmNoteFor(id) {
  return PM_REVIEW.notes.find((n) => n.id === id);
}

function pmAddNote(anchor) {
  PM_REVIEW.notes.push({ id: pmNoteId(), anchor, text: '', done: false });
  pmRenderNotes();
  const last = document.querySelector(`[data-note-id="${PM_REVIEW.notes.at(-1).id}"]`);
  last?.querySelector('.pm-note-text')?.focus();
}

function pmBindNoteEvents() {
  // Delegated, so notes added later need no rebinding.
  document.addEventListener('input', (e) => {
    if (!e.target.classList?.contains('pm-note-text')) return;
    const note = pmNoteFor(e.target.closest('.pm-note').dataset.noteId);
    if (note) { note.text = e.target.value; PM_REVIEW.dirty = true; }
  });
  document.addEventListener('click', (e) => {
    const btn = e.target.closest?.('.pm-note-btn');
    if (btn) {
      const wrap = btn.closest('.pm-note');
      const note = pmNoteFor(wrap.dataset.noteId);
      if (!note) return;
      if (btn.dataset.act === 'done') note.done = !note.done;
      if (btn.dataset.act === 'del') {
        PM_REVIEW.notes = PM_REVIEW.notes.filter((n) => n.id !== note.id);
      }
      pmRenderNotes();
      pmSaveNotes();
      return;
    }
    const tool = e.target.closest?.('.pm-tool');
    if (!tool) return;
    const anchor = tool.dataset.anchor;
    if (tool.dataset.act === 'note') pmAddNote(anchor);
    if (tool.dataset.act === 'add-p') pmAddBlock(anchor, 'p');
    if (tool.dataset.act === 'add-h3') pmAddBlock(anchor, 'h3');
    // The tools sit BEFORE their block, so the block is the next sibling.
    const target = tool.parentElement.nextElementSibling?.dataset?.copy;
    if (tool.dataset.act === 'del-block' && target) pmDeleteBlock(target);
    if (tool.dataset.act === 'hide-block' && target) pmSetHidden(target, true);
    if (tool.dataset.act === 'show-block' && target) pmSetHidden(target, false);
  });
  // Save on blur rather than per keystroke: one file write per note, not one
  // per character.
  document.addEventListener('focusout', (e) => {
    if (e.target.classList?.contains('pm-note-text') && PM_REVIEW.dirty) {
      PM_REVIEW.dirty = false;
      pmSaveNotes();
    }
  });
}

// The per-block toolbar: note, add text, add section, and (for blocks the
// author added) delete. Shown only in review mode, so the page reads normally
// the rest of the time.
function pmMountReviewChrome() {
  const hosts = [...document.querySelectorAll('[data-copy]'), ...pmChartEls()];
  hosts.forEach((host) => {
    const anchor = host.dataset.copy || host.id;
    if (!anchor) return;
    const prev = host.previousElementSibling;
    if (prev?.classList?.contains('pm-block-tools')) return;
    const bar = document.createElement('div');
    bar.className = 'pm-block-tools';
    bar.dataset.for = anchor;
    // Three kinds of block, three delete stories. An ADDED block is deleted
    // outright -- there is a record to remove. A DRAFTED one is hidden, since
    // the template would put it straight back. A chart has neither and gets no
    // delete at all.
    const added = host.dataset.added !== undefined;
    const drafted = !added && host.dataset.copy !== undefined;
    const gone = host.dataset.hidden !== undefined;
    bar.innerHTML = [
      `<button type="button" class="pm-tool" data-act="note" data-anchor="${anchor}"
         title="Leave a note about this">+ note</button>`,
      `<button type="button" class="pm-tool" data-act="add-p" data-anchor="${anchor}"
         title="Add a paragraph below this">+ text</button>`,
      `<button type="button" class="pm-tool" data-act="add-h3" data-anchor="${anchor}"
         title="Add a heading below this">+ section</button>`,
      added
        ? `<button type="button" class="pm-tool pm-tool-del" data-act="del-block"
             data-anchor="${anchor}" title="Delete this added block">delete</button>`
        : '',
      drafted && !gone
        ? `<button type="button" class="pm-tool pm-tool-del" data-act="hide-block"
             data-anchor="${anchor}" title="Delete this from the page">delete</button>`
        : '',
      drafted && gone
        ? `<button type="button" class="pm-tool" data-act="show-block"
             data-anchor="${anchor}" title="Put this back on the page">restore</button>`
        : '',
    ].join('');
    host.insertAdjacentElement('beforebegin', bar);
  });
}

// --- author-added blocks ------------------------------------------------
//
// A new heading or paragraph, inserted after any anchor on the page. Only its
// STRUCTURE is posted here; its words go through the copy route under the same
// id, so a new block is edited and saved exactly like a drafted one.
//
// Inserted into the DOM immediately rather than reloading: the page has charts
// and typeset maths on it that would all have to redraw, and the author is
// mid-thought.

const PM_BLOCK_DEFAULTS = { p: 'New text.', h3: 'New section', h4: 'New subsection' };

function pmBlockId() {
  return `b${Date.now().toString(36)}${Math.floor(Math.random() * 1e4).toString(36)}`;
}

async function pmSaveBlocks() {
  const ok = await pmPost(PM_BLOCKS_ENDPOINT, { blocks: PM_REVIEW.blocks }, 'blocks');
  if (ok) pmEditSetState(`${PM_REVIEW.blocks.length} added blocks saved`, 'ok');
  return ok;
}

// Chrome this walk must step over: review-mode furniture gets injected between
// blocks, so a naive "while the next sibling is one of mine" stops at the first
// toolbar and inserts the second block ABOVE the first.
const PM_CHROME = ['pm-block-tools', 'pm-note', 'pm-resize'];

// Where a new block goes: after the anchor, and after every block already
// anchored there, so repeated adds stack downwards in the order they were made.
function pmInsertPointFor(anchor) {
  const host = document.querySelector(`[data-copy="${anchor}"]`)
    || document.getElementById(anchor);
  if (!host) return null;
  let tail = host;
  let cursor = host.nextElementSibling;
  while (cursor) {
    if (PM_CHROME.some((c) => cursor.classList.contains(c))) {
      cursor = cursor.nextElementSibling;         // furniture: keep looking
    } else if (cursor.dataset?.added === anchor) {
      tail = cursor;                              // one of ours: move past it
      cursor = cursor.nextElementSibling;
    } else {
      break;                                      // someone else's content
    }
  }
  return tail;
}

async function pmAddBlock(anchor, kind) {
  const after = pmInsertPointFor(anchor);
  if (!after) return;
  const order = PM_REVIEW.blocks.filter((b) => b.anchor === anchor).length;
  const block = { id: pmBlockId(), anchor, kind, order };
  PM_REVIEW.blocks.push(block);

  const el = document.createElement(kind);
  el.dataset.copy = block.id;
  el.dataset.added = anchor;
  el.textContent = PM_BLOCK_DEFAULTS[kind] || 'New text.';
  after.insertAdjacentElement('afterend', el);

  if (!await pmSaveBlocks()) return;
  // Persist the placeholder as the block's copy straight away, so a block that
  // is added and never touched still says something after a reload rather
  // than rendering as the template default of an id nobody has copy for.
  await pmPost('/api/prelim-copy',
    { updates: { [block.id]: el.textContent } }, 'copy');
  pmMountReviewChrome();
  if (PM_EDIT.on) {
    el.setAttribute('contenteditable', 'true');
    el.classList.add('pm-editing');
    PM_EDIT.baseline.set(block.id, el.innerHTML);
  }
  el.focus?.();
}

async function pmDeleteBlock(id) {
  PM_REVIEW.blocks = PM_REVIEW.blocks.filter((b) => b.id !== id);
  document.querySelector(`[data-copy="${id}"]`)?.remove();
  document.querySelector(`.pm-block-tools[data-for="${id}"]`)?.remove();
  if (!await pmSaveBlocks()) return;
  // Empty value deletes the key, so the orphaned copy goes with the block.
  await pmPost('/api/prelim-copy', { updates: { [id]: '' } }, 'copy');
}

// Delete/restore for a DRAFTED block. Nothing is removed from the DOM: the
// element stays, struck through, so restoring it is one click and needs no
// reload. Production renders it not at all.
async function pmSetHidden(key, gone) {
  const el = document.querySelector(`[data-copy="${key}"]`);
  if (!el) return;
  PM_REVIEW.hidden = PM_REVIEW.hidden.filter((k) => k !== key);
  if (gone) PM_REVIEW.hidden.push(key);
  el.classList.toggle('pm-deleted', gone);
  if (gone) el.dataset.hidden = '1';
  else delete el.dataset.hidden;
  // Rebuild the bar so delete/restore swaps over.
  document.querySelector(`.pm-block-tools[data-for="${key}"]`)?.remove();
  pmMountReviewChrome();
  const ok = await pmPost(PM_HIDDEN_ENDPOINT, { hidden: PM_REVIEW.hidden }, 'hidden');
  if (ok) pmEditSetState(gone ? 'block deleted' : 'block restored', 'ok');
}

// --- chart resizing -----------------------------------------------------

function pmApplyChartHeights() {
  Object.entries(PM_REVIEW.layout).forEach(([id, spec]) => {
    const el = document.getElementById(id);
    if (el && spec.height) {
      el.style.height = `${spec.height}px`;
      if (window.Plotly && el.data) Plotly.relayout(el, { height: spec.height });
    }
  });
}

// Put dragged labels back where they were left. Applied once, after the
// charts have drawn: a chart that redraws on its own (the model panel, on
// every slider move) rebuilds its annotations from source and drops these
// again, which is the honest limit of dragging a generated figure. The static
// landscape figure -- the one worth nudging -- never redraws, so its positions
// hold.
function pmApplyChartMoves() {
  Object.entries(PM_REVIEW.layout).forEach(([id, spec]) => {
    const el = document.getElementById(id);
    if (!el || !el.data || !spec.moves || !window.Plotly) return;
    const patch = {};
    Object.entries(spec.moves).forEach(([target, coords]) => {
      const [kind, idx] = target.split(':');
      const key = kind === 'annotation' ? 'annotations' : 'shapes';
      Object.entries(coords).forEach(([axis, value]) => {
        patch[`${key}[${idx}].${axis}`] = value;
      });
    });
    if (Object.keys(patch).length) Plotly.relayout(el, patch);
  });
}

function pmMountResizers() {
  pmChartEls().forEach((chart) => {
    if (chart.nextElementSibling?.classList?.contains('pm-resize')) return;
    const grip = document.createElement('div');
    grip.className = 'pm-resize';
    grip.title = 'Drag to resize; double-click to reset';
    chart.insertAdjacentElement('afterend', grip);

    let startY = 0;
    let startH = 0;
    const onMove = (ev) => {
      const h = Math.max(120, Math.min(1600, startH + (ev.clientY - startY)));
      chart.style.height = `${h}px`;
      if (window.Plotly && chart.data) Plotly.relayout(chart, { height: h });
    };
    const onUp = () => {
      document.removeEventListener('mousemove', onMove);
      document.removeEventListener('mouseup', onUp);
      document.body.classList.remove('pm-resizing');
      pmSaveLayout(chart.id, { height: Math.round(chart.getBoundingClientRect().height) });
    };
    grip.addEventListener('mousedown', (ev) => {
      ev.preventDefault();
      startY = ev.clientY;
      startH = chart.getBoundingClientRect().height;
      document.body.classList.add('pm-resizing');
      document.addEventListener('mousemove', onMove);
      document.addEventListener('mouseup', onUp);
    });
    grip.addEventListener('dblclick', () => {
      chart.style.height = '';
      delete PM_REVIEW.layout[chart.id]?.height;
      pmSaveLayout(chart.id, { height: 0 });   // out of range -> dropped
      pmEditSetState(`${chart.id} height reset`, 'ok');
    });
  });
}

// --- draggable chart labels --------------------------------------------

// Plotly fires relayout with keys like `annotations[2].x`; collect them into
// {"annotation:2": {x, y}} so the store holds something a human can read.
function pmMovesFromRelayout(ev) {
  const moves = {};
  Object.entries(ev || {}).forEach(([key, value]) => {
    const m = key.match(/^(annotations|shapes)\[(\d+)\]\.([xy][01]?)$/);
    if (!m || typeof value !== 'number') return;
    const target = `${m[1] === 'annotations' ? 'annotation' : 'shape'}:${m[2]}`;
    moves[target] = { ...(moves[target] || {}), [m[3]]: value };
  });
  return moves;
}

function pmBindChartDragging() {
  pmChartEls().forEach((chart) => {
    if (chart.dataset.pmDragBound) return;
    chart.dataset.pmDragBound = '1';
    chart.on?.('plotly_relayout', (ev) => {
      if (!document.body.classList.contains('pm-review-on')) return;
      const moves = pmMovesFromRelayout(ev);
      if (Object.keys(moves).length) pmSaveLayout(chart.id, { moves });
    });
  });
}

// Plotly's own editing, switched on only in review mode. `edits` is per-plot
// config, so each chart is re-rendered with it rather than patched.
function pmSetChartEditing(on) {
  pmChartEls().forEach((chart) => {
    if (!window.Plotly || !chart.data) return;
    Plotly.react(chart, chart.data, chart.layout, {
      displayModeBar: false,
      responsive: true,
      edits: on ? { annotationPosition: true, shapePosition: true } : {},
    });
  });
  if (on) pmBindChartDragging();
}

// --- wiring -------------------------------------------------------------

function initReviewTools() {
  if (!document.getElementById('pm-edit-bar')) return;   // production
  pmApplyChartHeights();
  pmApplyChartMoves();
  pmRenderNotes();
  pmBindNoteEvents();

  const toggle = document.getElementById('pm-review-toggle');
  if (!toggle) return;
  toggle.addEventListener('click', () => {
    const on = !document.body.classList.contains('pm-review-on');
    document.body.classList.toggle('pm-review-on', on);
    toggle.textContent = on ? 'Hide review tools' : 'Review tools';
    if (on) { pmMountReviewChrome(); pmMountResizers(); }
    pmSetChartEditing(on);
    pmEditSetState(on
      ? 'drag chart labels, resize charts, leave notes'
      : `${PM_REVIEW.notes.length} notes`, '');
  });
}
