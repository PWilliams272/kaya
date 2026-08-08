// ---- The persistent left contents rail
//
// Built FROM THE DOM, not from a hand-written list. Every article page already
// marks its structure with <section class="article-section" id="..."> and
// headings inside it, so the tree is derivable — and derivable means it cannot
// drift. A hand-maintained nested list in four templates would be wrong within
// a week, and wrong quietly: a missing entry looks exactly like a section that
// does not exist.
//
// Two levels: <h2> inside .article-section is a group, <h3> under it is a leaf.
// h4 is deliberately ignored. It exists on these pages (the run log uses it for
// sub-points inside a section) and including it would produce a rail longer
// than some of the sections it indexes.
//
// Collapse behaviour: a group opens when it is the one being read and stays
// open once opened. Auto-collapsing what the reader just left is the behaviour
// everyone hates in a docs sidebar — it moves the thing you were looking at.
//
// The in-page <nav class="toc"> stays in the markup and is hidden by CSS when
// the rail is showing. Below the breakpoint the rail is hidden and the TOC
// comes back, so a narrow window keeps working with no JS involvement.

const PAGENAV_MIN_WIDTH = 1180;   // must match the media query in research.css

let pagenavScrollHandler = null;

/** Park the rail directly under the sticky topbar, whatever height it is.
 *
 * --lg-nav-h is 52px, but .topbar holds an eyebrow above an h1 and renders
 * taller than that. The topbar is sticky at z-index 10 and the rail sits at 9,
 * so guessing low means the topbar covers the rail's first rows as soon as the
 * page scrolls -- and the first row is the "Contents" label. Measured on every
 * build and on resize, because the topbar wraps at narrow widths.
 */
function pagenavSyncTop() {
  const bar = document.querySelector('.topbar');
  if (!bar) return;
  const h = Math.round(bar.getBoundingClientRect().height);
  document.documentElement.style.setProperty('--kaya-topbar-h', `${h}px`);
}

let pagenavResizeBound = false;

function pagenavClear() {
  // Switching tabs rebuilds the rail, so a handler left bound would keep
  // measuring headings in a pane that is now display:none -- every rect zero,
  // and the first entry marked current forever.
  if (pagenavScrollHandler) {
    window.removeEventListener('scroll', pagenavScrollHandler);
    window.removeEventListener('resize', pagenavScrollHandler);
    pagenavScrollHandler = null;
  }
}

/** Read the structure of one tab pane into [{id, title, children:[{id,title}]}]. */
function pagenavTree(pane) {
  const groups = [];
  pane.querySelectorAll('section.article-section').forEach((sec) => {
    if (!sec.id) return;
    const h2 = sec.querySelector(':scope > h2');
    if (!h2) return;
    const children = [];
    sec.querySelectorAll(':scope > h3').forEach((h3, i) => {
      // Subsections rarely carry ids of their own -- they are prose headings.
      // Mint one so the rail can link to it, but only if it is missing, so an
      // id someone deliberately set still wins.
      if (!h3.id) h3.id = `${sec.id}-h${i + 1}`;
      children.push({ id: h3.id, title: h3.textContent.trim() });
    });
    groups.push({ id: sec.id, title: h2.textContent.trim(), children });
  });
  return groups;
}

function pagenavRender(pane, tree) {
  let rail = document.getElementById('page-nav');
  if (!rail) {
    rail = document.createElement('aside');
    rail.id = 'page-nav';
    rail.className = 'page-nav';
    rail.setAttribute('aria-label', 'Contents');
    document.body.appendChild(rail);
  }
  if (!tree.length) {
    rail.hidden = true;
    return null;
  }
  rail.hidden = false;

  const title = pane.querySelector('.masthead h1');
  rail.innerHTML = `
    <div class="page-nav-inner">
      <div class="page-nav-head">
        <div class="toc-label">Contents</div>
        ${title ? `<div class="page-nav-title">${title.textContent.trim()}</div>` : ''}
      </div>
      <nav class="page-nav-list">
        ${tree.map((g) => `
          <div class="page-nav-group" data-group="${g.id}">
            <div class="page-nav-row">
              <a class="page-nav-link" href="#${g.id}" data-target="${g.id}">${g.title}</a>
              ${g.children.length ? `<button class="page-nav-toggle" type="button"
                 aria-expanded="false" aria-controls="pn-sub-${g.id}"
                 aria-label="Show subsections of ${g.title.replace(/"/g, '')}"></button>` : ''}
            </div>
            ${g.children.length ? `<div class="page-nav-sub" id="pn-sub-${g.id}">
              ${g.children.map((c) => `<a class="page-nav-link page-nav-sublink"
                 href="#${c.id}" data-target="${c.id}">${c.title}</a>`).join('')}
            </div>` : ''}
          </div>`).join('')}
      </nav>
    </div>`;
  return rail;
}

function pagenavBind(rail) {
  rail.querySelectorAll('.page-nav-toggle').forEach((btn) => {
    btn.addEventListener('click', (e) => {
      e.preventDefault();
      const group = btn.closest('.page-nav-group');
      const open = group.classList.toggle('open');
      btn.setAttribute('aria-expanded', open ? 'true' : 'false');
    });
  });
  // Anchor navigation is left to the browser -- scroll-margin-top in the CSS
  // keeps a target from landing underneath the sticky topbar, which is the
  // only thing a handler would have been needed for.
  rail.querySelectorAll('.page-nav-link').forEach((a) => {
    a.addEventListener('click', () => {
      const group = a.closest('.page-nav-group');
      if (group && !group.classList.contains('open')) {
        const btn = group.querySelector('.page-nav-toggle');
        if (btn) btn.click();
      }
    });
  });
}

/** Highlight the heading you are actually reading, at both levels.
 *
 * The first version observed sections and subsection headings together and
 * marked the topmost intersecting one. That could never highlight a subsection:
 * a <section> is metres tall and its own <h3> sits inside it, so the section's
 * top is always higher up the page and always won. Only sections ever lit up.
 *
 * What a reader means by "where am I" is the last heading they scrolled PAST,
 * regardless of level -- so that is what this computes. Headings are collected
 * in document order, h2 and h3 interleaved, and the current one is the last
 * whose top has crossed a line just under the topbar.
 *
 * When that heading is a subsection, its parent group is marked too, with a
 * quieter style: two levels of "you are here" rather than one, so the rail
 * still says which section you are in after the section's own heading has
 * scrolled away.
 */
function pagenavSpy(pane, rail) {
  const headings = [];
  pane.querySelectorAll('section.article-section').forEach((sec) => {
    if (!sec.id) return;
    const h2 = sec.querySelector(':scope > h2');
    if (!h2) return;
    // The link points at the section, the position comes from its heading.
    headings.push({ target: sec.id, el: h2, group: sec.id });
    sec.querySelectorAll(':scope > h3').forEach((h3) => {
      if (h3.id) headings.push({ target: h3.id, el: h3, group: sec.id });
    });
  });
  if (!headings.length) return;

  const links = new Map();
  rail.querySelectorAll('.page-nav-link').forEach((a) => {
    links.set(a.dataset.target, a);
  });

  let currentTarget = null;

  const apply = (h) => {
    if (!h || h.target === currentTarget) return;
    currentTarget = h.target;
    links.forEach((a) => a.classList.remove('current', 'parent-current'));

    const a = links.get(h.target);
    if (a) a.classList.add('current');

    // A subsection also lights its parent, quietly. Skipped when the heading
    // IS the group, or the same row would carry both classes.
    if (h.target !== h.group) {
      const parent = links.get(h.group);
      if (parent) parent.classList.add('parent-current');
    }

    const group = rail.querySelector(`.page-nav-group[data-group="${h.group}"]`);
    if (group && !group.classList.contains('open')) {
      group.classList.add('open');
      const btn = group.querySelector('.page-nav-toggle');
      if (btn) btn.setAttribute('aria-expanded', 'true');
    }
    if (a && a.scrollIntoView) {
      // Keep the marked row on screen in a rail taller than the window,
      // without yanking the page: nearest only moves it if it is off-view.
      a.scrollIntoView({ block: 'nearest' });
    }
  };

  const recompute = () => {
    const bar = document.querySelector('.topbar');
    const line = (bar ? bar.getBoundingClientRect().height : 52) + 24;
    let found = headings[0];
    for (const h of headings) {
      if (h.el.getBoundingClientRect().top <= line) found = h;
      else break;                       // document order: the rest are below
    }
    apply(found);
  };

  // rAF-throttled: getBoundingClientRect forces layout, and running that per
  // scroll event on a page with 50 headings is how a smooth page starts to
  // stutter. One measurement per frame is enough to look instant.
  let queued = false;
  const onScroll = () => {
    if (queued) return;
    queued = true;
    requestAnimationFrame(() => { queued = false; recompute(); });
  };

  window.addEventListener('scroll', onScroll, { passive: true });
  window.addEventListener('resize', onScroll);
  pagenavScrollHandler = onScroll;
  recompute();
}

/** Rebuild the rail for the pane that just became active. */
function renderPageNav(tabName) {
  pagenavClear();
  pagenavSyncTop();
  if (!pagenavResizeBound) {
    window.addEventListener('resize', pagenavSyncTop);
    pagenavResizeBound = true;
  }
  const pane = document.getElementById(`tab-${tabName}`);
  const rail = document.getElementById('page-nav');
  if (!pane) {
    if (rail) rail.hidden = true;
    return;
  }
  const tree = pagenavTree(pane);
  const built = pagenavRender(pane, tree);
  document.body.classList.toggle('has-page-nav', Boolean(built));
  if (!built) return;
  pagenavBind(built);
  pagenavSpy(pane, built);
}
