import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { map } from '../../src/index.js';
import { runMarbles } from '../../src/testing/index.js';
import { intents, loadEffect, memoryStorage, model$, persistEffect } from './src/app.js';
import { initialModel, type Model, type Todo } from './src/model.js';
import { Msg } from './src/msg.js';
import { selectedTodo, stats, visibleTodos } from './src/selectors.js';
import { replay, update } from './src/update.js';

const titles = (m: Model): string => visibleTodos(m).map((t) => `${t.done ? '+' : '-'}${t.title}`).join(',');

/** `selectedId/editingId`, with `-` for null — the two cursors of the Read and Update flows. */
const marks = (m: Model): string => `${m.selectedId ?? '-'}/${m.editingId ?? '-'}`;

describe('update: pure reducers (no streams needed)', () => {
  it('C: adds with increasing ids and ignores blank titles', () => {
    const m = replay(initialModel, [Msg.add('a'), Msg.add('  '), Msg.add(' b ')]);
    expect(m.todos).toEqual([
      { id: 1, title: 'a', done: false },
      { id: 2, title: 'b', done: false },
    ]);
    expect(m.nextId).toBe(3);
  });

  it('R: selects existing todos only', () => {
    const m = replay(initialModel, [Msg.add('a'), Msg.select(1)]);
    expect(selectedTodo(m)?.title).toBe('a');
    expect(update(Msg.select(99))(m)).toBe(m);
  });

  it('R: deselect clears the selection', () => {
    const m = replay(initialModel, [Msg.add('a'), Msg.select(1)]);
    expect(m.selectedId).toBe(1);
    expect(update(Msg.deselect())(m).selectedId).toBeNull();
    // deselecting twice is harmless
    expect(update(Msg.deselect())(update(Msg.deselect())(m)).selectedId).toBeNull();
  });

  it('U: the edit lifecycle sets and clears editingId', () => {
    const m = replay(initialModel, [Msg.add('a'), Msg.add('b')]);
    expect(m.editingId).toBeNull();

    const editing = update(Msg.startEdit(1))(m);
    expect(editing.editingId).toBe(1);

    // cancelling leaves the title alone
    const cancelled = update(Msg.cancelEdit())(editing);
    expect(cancelled.editingId).toBeNull();
    expect(titles(cancelled)).toBe('-a,-b');

    // committing a rename ends the edit
    const renamed = update(Msg.rename(1, 'A'))(editing);
    expect(renamed.editingId).toBeNull();
    expect(titles(renamed)).toBe('-A,-b');

    // starting a new edit while one is open just moves the cursor
    expect(update(Msg.startEdit(2))(editing).editingId).toBe(2);
  });

  it('U: removing the todo under edit clears editingId', () => {
    const editing = replay(initialModel, [Msg.add('a'), Msg.add('b'), Msg.startEdit(2)]);
    expect(update(Msg.remove(2))(editing).editingId).toBeNull();
    // removing a different one leaves the edit open
    expect(update(Msg.remove(1))(editing).editingId).toBe(2);
    // renaming to "" deletes the todo and ends the edit
    expect(update(Msg.rename(2, '   '))(editing).editingId).toBeNull();
  });

  it('U: rename, toggle, toggleAll; renaming to "" deletes', () => {
    const m = replay(initialModel, [Msg.add('a'), Msg.add('b'), Msg.rename(1, 'A'), Msg.toggle(2)]);
    expect(titles(m)).toBe('-A,+b');
    expect(titles(update(Msg.toggleAll())(m))).toBe('+A,+b');
    expect(titles(update(Msg.rename(1, '  '))(m))).toBe('+b');
  });

  it('D: remove and clearCompleted also clear dangling selection', () => {
    const m = replay(initialModel, [Msg.add('a'), Msg.add('b'), Msg.toggle(2), Msg.select(2)]);
    expect(update(Msg.remove(2))(m).selectedId).toBeNull();
    const cleared = update(Msg.clearCompleted())(m);
    expect(titles(cleared)).toBe('-a');
    expect(cleared.selectedId).toBeNull();
  });

  it('L: filters are derived, not stored', () => {
    const m = replay(initialModel, [Msg.add('a'), Msg.add('b'), Msg.toggle(1)]);
    expect(titles(update(Msg.setFilter('active'))(m))).toBe('-b');
    expect(titles(update(Msg.setFilter('done'))(m))).toBe('+a');
    expect(stats(m)).toEqual({ total: 2, active: 1, done: 1 });
  });

  it('never mutates the previous model', () => {
    const before = replay(initialModel, [Msg.add('a')]);
    const snapshot = JSON.stringify(before);
    replay(before, [Msg.toggle(1), Msg.rename(1, 'x'), Msg.remove(1)]);
    expect(JSON.stringify(before)).toBe(snapshot);
  });
});

describe('model$: merged reducer streams folded by scan (marble tests)', () => {
  it('folds intents arriving on different streams in time order', () => {
    runMarbles(({ cold, expectObservable }) => {
      const add$ = cold('-a-b------', { a: 'milk', b: 'eggs' });
      const toggle$ = cold('-----t----', { t: 1 });
      const setFilter$ = cold('-------f--', { f: 'active' as const });
      const remove$ = cold('---------r', { r: 2 });

      const view$ = model$(intents({ add$, toggle$, setFilter$, remove$ })).pipe(map(titles));
      expectObservable(view$).toBe('ab-c-d-e-f', {
        a: '',
        b: '-milk',
        c: '-milk,-eggs',
        d: '+milk,-eggs',
        e: '-eggs',
        f: '',
      });
    });
  });

  it('routes a null on select$ to Deselect and folds the edit lifecycle', () => {
    runMarbles(({ cold, expectObservable }) => {
      const add$ = cold('-a----', { a: 'x' });
      const select$ = cold<number | null>('--s--n', { s: 1, n: null });
      const startEdit$ = cold<number>('---e--', { e: 1 });
      const cancelEdit$ = cold<void>('----c-', { c: undefined });

      const view$ = model$(intents({ add$, select$, startEdit$, cancelEdit$ })).pipe(map(marks));
      expectObservable(view$).toBe('abcdef', {
        a: '-/-', // initial
        b: '-/-', // add: selection untouched
        c: '1/-', // select 1
        d: '1/1', // start editing 1
        e: '1/-', // cancel the edit, keep the selection
        f: '-/-', // null on select$ means Deselect
      });
    });
  });

  it('is shared: two views see one state machine', () => {
    runMarbles(({ cold, expectObservable }) => {
      const model = model$(intents({ add$: cold('-a|', { a: 'x' }) }));
      // when every intent stream has completed, the app completes too
      expectObservable(model.pipe(map((m) => m.todos.length))).toBe('ab|', { a: 0, b: 1 });
      expectObservable(model.pipe(map((m) => m.nextId))).toBe('ab|', { a: 1, b: 2 });
    });
  });
});

describe('the browser edge', () => {
  /**
   * The view declares its actions as `data-action="…"` and the browser entry
   * wires each one to an intent stream. Nothing else connects the two, so a
   * button can be rendered with no stream behind it and look perfectly fine
   * — which is exactly how the detail panel's "close" button stayed dead.
   * Compare the two sets as source text; no DOM needed.
   */
  it('every action the view renders is wired to an intent', () => {
    const dir = dirname(fileURLToPath(import.meta.url));
    const view = readFileSync(join(dir, 'src/view.ts'), 'utf8');
    const main = readFileSync(join(dir, 'browser/main.ts'), 'utf8');

    const rendered = new Set([...view.matchAll(/data-action="([a-z-]+)"/g)].map((m) => m[1]));
    const wired = new Set([...main.matchAll(/action\(\s*'[a-z]+'\s*,\s*'([a-z-]+)'\s*\)/g)].map((m) => m[1]));

    expect(rendered.size).toBeGreaterThan(0);
    expect([...rendered].filter((a) => !wired.has(a))).toEqual([]); // rendered but dead
    expect([...wired].filter((a) => !rendered.has(a))).toEqual([]); // wired but never rendered
  });
});

describe('effects at the edge', () => {
  it('loadEffect hydrates the model from storage', () => {
    const stored: Todo[] = [{ id: 7, title: 'stored', done: true }];
    runMarbles(({ expectObservable }) => {
      const model = model$(intents({ loaded$: loadEffect(memoryStorage(stored)) }));
      expectObservable(model.pipe(map((m) => `${titles(m)}|next=${m.nextId}`))).toBe('(ab|)', { a: '|next=1', b: '+stored|next=8' });
    });
  });

  it('persistEffect saves once, debounced on the injected scheduler', () => {
    const storage = memoryStorage();
    runMarbles(({ cold, scheduler, expectObservable }) => {
      const model = model$(intents({ add$: cold('-a-b', { a: 'one', b: 'two' }) }));
      // changes at frames 1 and 3; one save 5 frames after the last change
      expectObservable(persistEffect(model, storage, 5, scheduler).pipe(map((ts) => ts.length))).toBe('--------x', { x: 2 });
    });
    expect(storage.saved()?.map((t) => t.title)).toEqual(['one', 'two']);
  });
});
