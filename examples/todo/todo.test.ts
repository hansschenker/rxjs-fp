import { describe, expect, it } from 'vitest';
import { map } from '../../src/index.js';
import { runMarbles } from '../../src/testing/index.js';
import { intents, loadEffect, memoryStorage, model$, persistEffect } from './src/app.js';
import { initialModel, type Model, type Todo } from './src/model.js';
import { Msg } from './src/msg.js';
import { selectedTodo, stats, visibleTodos } from './src/selectors.js';
import { replay, update } from './src/update.js';

const titles = (m: Model): string => visibleTodos(m).map((t) => `${t.done ? '+' : '-'}${t.title}`).join(',');

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

  it('is shared: two views see one state machine', () => {
    runMarbles(({ cold, expectObservable }) => {
      const model = model$(intents({ add$: cold('-a|', { a: 'x' }) }));
      // when every intent stream has completed, the app completes too
      expectObservable(model.pipe(map((m) => m.todos.length))).toBe('ab|', { a: 0, b: 1 });
      expectObservable(model.pipe(map((m) => m.nextId))).toBe('ab|', { a: 1, b: 2 });
    });
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
