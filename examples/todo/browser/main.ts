/**
 * Browser entry of the Todo CRUDL example.
 *
 *   npm run example:todo      (starts Vite on examples/todo/browser)
 *
 * DOM events → intent streams → (pure app) → model$ → viewHtml → innerHTML.
 * The DOM is touched only here, at the edge.
 */
import {
  animationFrameScheduler,
  auditTime,
  distinctUntilChanged,
  filter,
  fromEvent,
  map,
  merge,
  type Observable,
} from '../../../src/index.js';
import { intents, loadEffect, model$, persistEffect, type StoragePort } from '../src/app.js';
import type { Filter, Todo } from '../src/model.js';
import { viewHtml } from '../src/view.js';

const root = document.querySelector<HTMLElement>('#app')!;
const form = document.querySelector<HTMLFormElement>('#new-todo')!;
const input = form.querySelector<HTMLInputElement>('input')!;

/** Event delegation: a stream of the elements carrying `data-action="<action>"`. */
const action = (type: 'click' | 'change' | 'dblclick', name: string): Observable<HTMLElement> =>
  fromEvent(root, type).pipe(
    map((e) => (e.target as HTMLElement).closest<HTMLElement>(`[data-action="${name}"]`)),
    filter((el): el is HTMLElement => el !== null),
  );
const idOf = (el: HTMLElement): number => Number(el.dataset['id']);

// --- INTENTS -----------------------------------------------------------------
const add$ = fromEvent(form, 'submit').pipe(
  map((e) => {
    e.preventDefault();
    const title = input.value;
    input.value = '';
    return title;
  }),
);
const editKey$ = fromEvent(root, 'keydown').pipe(
  filter((e) => (e.target as HTMLElement).classList.contains('edit')),
);
/**
 * An edit ends exactly once: by Enter, by Escape, or by leaving the field.
 * Re-rendering removes the input, which fires a late `focusout`; marking the
 * input as finished keeps that event from committing a cancelled edit.
 */
const finish = (e: Event): HTMLInputElement | null => {
  const el = e.target as HTMLInputElement;
  if (el.dataset['finished']) return null;
  el.dataset['finished'] = 'true';
  return el;
};
const rename$ = merge(
  editKey$.pipe(filter((e) => e.key === 'Enter')),
  fromEvent(root, 'focusout').pipe(filter((e) => (e.target as HTMLElement).classList.contains('edit'))),
).pipe(
  map(finish),
  filter((el): el is HTMLInputElement => el !== null),
  map((el) => ({ id: idOf(el), title: el.value })),
);
const cancelEdit$ = editKey$.pipe(
  filter((e) => e.key === 'Escape'),
  map(finish),
  filter((el) => el !== null),
  map(() => undefined),
);

const app = intents({
  add$,
  // `null` means Deselect (see `reducers` in ../src/app.ts); the detail
  // panel's close button is what sends it.
  select$: merge(action('click', 'select').pipe(map(idOf)), action('click', 'deselect').pipe(map(() => null))),
  startEdit$: merge(action('click', 'edit'), action('dblclick', 'select')).pipe(map(idOf)),
  cancelEdit$,
  rename$,
  toggle$: action('change', 'toggle').pipe(map(idOf)),
  toggleAll$: action('click', 'toggle-all').pipe(map(() => undefined)),
  remove$: action('click', 'remove').pipe(map(idOf)),
  clearCompleted$: action('click', 'clear').pipe(map(() => undefined)),
  setFilter$: action('click', 'filter').pipe(map((el) => el.dataset['filter'] as Filter)),
});

// --- EFFECTS: localStorage port ---------------------------------------------
const KEY = 'rxjs-fp-todos';
const storage: StoragePort = {
  load: () => {
    try {
      const raw = localStorage.getItem(KEY);
      return raw ? (JSON.parse(raw) as Todo[]) : null;
    } catch {
      return null;
    }
  },
  save: (todos) => {
    try {
      localStorage.setItem(KEY, JSON.stringify(todos));
    } catch {
      /* storage unavailable: the app still works in memory */
    }
  },
};

const model = model$({ ...app, loaded$: loadEffect(storage) });

// --- RENDER ------------------------------------------------------------------
model
  .pipe(
    map(viewHtml),
    distinctUntilChanged(),
    auditTime(0, animationFrameScheduler), // at most one DOM write per frame
  )
  .subscribe((html) => {
    root.innerHTML = html;
    root.querySelector<HTMLInputElement>('input.edit')?.focus();
  });

persistEffect(model, storage).subscribe();
