import type { Model } from './model.js';
import { selectedTodo, stats, visibleTodos } from './selectors.js';

/**
 * VIEW — Elm's `view : Model -> Html Msg`, as pure functions from the
 * model to text (terminal) or an HTML string (browser). No state, no events.
 */

export const viewText = (m: Model): string => {
  const s = stats(m);
  const rows = visibleTodos(m).map(
    (t) => `  ${t.id === m.selectedId ? '>' : ' '} [${t.done ? 'x' : ' '}] #${t.id} ${t.title}${t.id === m.editingId ? '  (editing)' : ''}`,
  );
  const detail = selectedTodo(m);
  return [
    `filter: ${m.filter}   ${s.active} active / ${s.done} done / ${s.total} total`,
    ...(rows.length > 0 ? rows : ['  (nothing to show)']),
    ...(detail ? [`  detail: #${detail.id} "${detail.title}" — ${detail.done ? 'done' : 'open'}`] : []),
  ].join('\n');
};

const esc = (s: string): string =>
  s.replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c] as string);

/** Renders the list, footer and detail panel. Actions are declared with data-* attributes. */
export const viewHtml = (m: Model): string => {
  const s = stats(m);
  const items = visibleTodos(m)
    .map((t) =>
      t.id === m.editingId
        ? `<li class="editing"><input class="edit" data-id="${t.id}" value="${esc(t.title)}" autofocus /></li>`
        : `<li class="${t.done ? 'done' : ''} ${t.id === m.selectedId ? 'selected' : ''}">
             <input type="checkbox" data-action="toggle" data-id="${t.id}" ${t.done ? 'checked' : ''} aria-label="toggle" />
             <span class="title" data-action="select" data-id="${t.id}" title="Double-click to edit">${esc(t.title)}</span>
             <button data-action="edit" data-id="${t.id}" aria-label="edit">✎</button>
             <button data-action="remove" data-id="${t.id}" aria-label="delete">✕</button>
           </li>`,
    )
    .join('');
  const filters = (['all', 'active', 'done'] as const)
    .map((f) => `<button data-action="filter" data-filter="${f}" class="${m.filter === f ? 'on' : ''}">${f}</button>`)
    .join('');
  const detail = selectedTodo(m);
  return `
    <ul class="todos">${items || '<li class="empty">Nothing to show</li>'}</ul>
    <footer>
      <span>${s.active} active · ${s.done} done</span>
      <span class="filters">${filters}</span>
      <button data-action="toggle-all" ${s.total === 0 ? 'disabled' : ''}>toggle all</button>
      <button data-action="clear" ${s.done === 0 ? 'disabled' : ''}>clear done</button>
    </footer>
    ${
      detail
        ? `<aside class="detail"><h2>#${detail.id}</h2><p>${esc(detail.title)}</p><p>${detail.done ? 'Done' : 'Open'}</p>
           <button data-action="deselect">close</button></aside>`
        : ''
    }`;
};
