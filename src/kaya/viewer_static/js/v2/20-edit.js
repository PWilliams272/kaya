// ---- Edit mode: reword the page in the page, and have it persist
//
// DEVELOPMENT ONLY. The template loads this file behind `{% if editable %}`,
// which is false in production, and the endpoint it posts to lives on the
// dev-only router. kaya.peterwilliams.dev is public and unauthenticated: a
// live page that can rewrite its own text on request would be a defacement
// vector. See viewer_copy.py for the whole boundary.
//
// HOW IT WORKS. Every editable block carries `data-copy="<key>"`. Toggling
// edit mode makes those blocks contenteditable; Save posts the ones that
// actually changed to /api/prelim-copy, which sanitises them and merges them
// into viewer_content/prelim_copy.json. That file is committed like source, so
// an edit made in the browser is still there in the next session -- which is
// the entire point.
//
// WHAT IS NOT EDITABLE. Text that JavaScript writes at render time (the chart
// captions built from the payload, the status strip) has no data-copy hook and
// is skipped: it is generated from numbers, so editing the words in place
// would be overwritten on the next render anyway.

const PM_EDIT_ENDPOINT = '/api/prelim-copy';

const PM_EDIT = {
  on: false,
  // key -> the innerHTML each block had when edit mode was entered, so Save
  // sends only what moved and Cancel can put the rest back exactly.
  baseline: new Map(),
};

// `@claude ...@` notes arrive from the server already wrapped in a <mark> so
// they stand out. That wrapper is presentation, not copy: it has to come back
// off before saving, or the sanitiser would drop the tag, the delimiters would
// go with it, and the note would silently become part of the prose.
function pmUnmarkNotes(html) {
  const box = document.createElement('div');
  box.innerHTML = html;
  box.querySelectorAll('mark.pm-inline-note').forEach((el) => {
    const tag = el.dataset.state === 'done' ? '@done' : '@claude';
    el.replaceWith(document.createTextNode(`${tag} ${el.textContent.trim()}@`));
  });
  return box.innerHTML;
}

function pmEditBlocks() {
  return Array.from(document.querySelectorAll('[data-copy]'));
}

function pmEditSetState(text, tone) {
  const el = document.getElementById('pm-edit-state');
  if (!el) return;
  el.textContent = text;
  el.dataset.tone = tone || '';
}

function pmEditEnter() {
  PM_EDIT.on = true;
  PM_EDIT.baseline.clear();
  pmEditBlocks().forEach((el) => {
    PM_EDIT.baseline.set(el.dataset.copy, el.innerHTML);
    // Rich, not plaintext-only: the copy carries <i>, <b> and <sub> that
    // plaintext-only would flatten on the first keystroke. Anything pasted in
    // that should not survive is stripped server-side by the sanitiser.
    el.setAttribute('contenteditable', 'true');
    el.classList.add('pm-editing');
  });
  document.body.classList.add('pm-edit-on');
  document.getElementById('pm-edit-toggle').textContent = 'Stop editing';
  document.getElementById('pm-edit-save').hidden = false;
  document.getElementById('pm-edit-cancel').hidden = false;
  pmEditSetState(`${PM_EDIT.baseline.size} blocks editable`, '');
}

function pmEditLeave(restore) {
  pmEditBlocks().forEach((el) => {
    if (restore && PM_EDIT.baseline.has(el.dataset.copy)) {
      el.innerHTML = PM_EDIT.baseline.get(el.dataset.copy);
    }
    el.removeAttribute('contenteditable');
    el.classList.remove('pm-editing');
  });
  PM_EDIT.on = false;
  document.body.classList.remove('pm-edit-on');
  document.getElementById('pm-edit-toggle').textContent = 'Edit copy';
  document.getElementById('pm-edit-save').hidden = true;
  document.getElementById('pm-edit-cancel').hidden = true;
}

// Only what actually changed. Sending every block would rewrite the store with
// the browser's normalised markup for text nobody touched, and bury the real
// edit in a diff of whitespace.
function pmEditChanged() {
  const updates = {};
  pmEditBlocks().forEach((el) => {
    const key = el.dataset.copy;
    const now = pmUnmarkNotes(el.innerHTML).trim();
    const was = pmUnmarkNotes(PM_EDIT.baseline.get(key) || '').trim();
    if (now !== was) updates[key] = now;
  });
  return updates;
}

async function pmEditSave() {
  const updates = pmEditChanged();
  const n = Object.keys(updates).length;
  if (!n) { pmEditSetState('nothing changed', ''); return; }
  pmEditSetState(`saving ${n}…`, '');
  try {
    const res = await fetch(PM_EDIT_ENDPOINT, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ updates }),
    });
    if (!res.ok) {
      const detail = await res.text();
      pmEditSetState(`save failed: ${res.status}`, 'bad');
      console.error('[prelim] save failed', res.status, detail);
      return;
    }
    // Re-baseline rather than reloading: the page has charts and KaTeX on it
    // that would all have to redraw, and the saved text is already on screen.
    pmEditBlocks().forEach((el) => {
      PM_EDIT.baseline.set(el.dataset.copy, el.innerHTML);
    });
    pmEditSetState(`saved ${n} block${n === 1 ? '' : 's'}`, 'ok');
  } catch (err) {
    pmEditSetState('save failed: no response', 'bad');
    console.error('[prelim] save failed', err);
  }
}

function initCopyEditor() {
  const bar = document.getElementById('pm-edit-bar');
  if (!bar) return;                       // production: never rendered
  bar.hidden = false;
  pmEditSetState(`${pmEditBlocks().length} editable blocks`, '');

  document.getElementById('pm-edit-toggle').addEventListener('click', () => {
    if (PM_EDIT.on) pmEditLeave(false); else pmEditEnter();
  });
  document.getElementById('pm-edit-save').addEventListener('click', pmEditSave);
  document.getElementById('pm-edit-cancel').addEventListener('click', () => {
    pmEditLeave(true);
    pmEditSetState('reverted', '');
  });

  // Cmd/Ctrl-S saves without leaving the keyboard, which is how anyone
  // actually writing copy will expect it to behave.
  document.addEventListener('keydown', (e) => {
    if (PM_EDIT.on && (e.metaKey || e.ctrlKey) && e.key === 's') {
      e.preventDefault();
      pmEditSave();
    }
  });

  // Losing unsaved wording to a stray reload is the one failure that costs
  // real work, so it gets a guard.
  window.addEventListener('beforeunload', (e) => {
    if (PM_EDIT.on && Object.keys(pmEditChanged()).length) {
      e.preventDefault();
      e.returnValue = '';
    }
  });
}
