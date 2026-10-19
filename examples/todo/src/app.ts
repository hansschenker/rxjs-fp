import {
  EMPTY,
  asyncScheduler,
  debounceTime,
  defer,
  distinctUntilChanged,
  of,
  map,
  merge,
  scan,
  shareReplay,
  skip,
  startWith,
  tap,
  type Observable,
  type Scheduler,
} from '../../../src/index.js';
import { initialModel, type Filter, type Model, type Reducer, type Todo } from './model.js';
import { Msg } from './msg.js';
import { update } from './update.js';

/**
 * The Elm loop, written as a stream equation:
 *
 *   intents ──map(update)──▶ reducer streams ──merge──▶ reducer$ ──scan(apply)──▶ model$ ──map(view)──▶ UI
 *      ▲                                                                              │
 *      └──────────────────────────── user events / effects ◀─────────────────────────┘
 *
 * Every intent stream is turned into a stream of *reducer functions*
 * (`Model -> Model`). The reducer streams are merged into one, and `scan`
 * folds them over the initial model. `scan` holds the only state of the app.
 */

/** INTENTS: what the user (or the outside world) wants, one typed stream each. */
export interface Intents {
  readonly add$: Observable<string>; // C
  readonly select$: Observable<number | null>; // R
  readonly startEdit$: Observable<number>; // U
  readonly cancelEdit$: Observable<void>; // U
  readonly rename$: Observable<{ readonly id: number; readonly title: string }>; // U
  readonly toggle$: Observable<number>; // U
  readonly toggleAll$: Observable<void>; // U
  readonly remove$: Observable<number>; // D
  readonly clearCompleted$: Observable<void>; // D
  readonly setFilter$: Observable<Filter>; // L
  /** Results of the load effect come back in as an intent. */
  readonly loaded$: Observable<ReadonlyArray<Todo>>;
}

/** Fill in intents you don't use with EMPTY. */
export const intents = (partial: Partial<Intents>): Intents => ({
  add$: EMPTY,
  select$: EMPTY,
  startEdit$: EMPTY,
  cancelEdit$: EMPTY,
  rename$: EMPTY,
  toggle$: EMPTY,
  toggleAll$: EMPTY,
  remove$: EMPTY,
  clearCompleted$: EMPTY,
  setFilter$: EMPTY,
  loaded$: EMPTY,
  ...partial,
});

/** Each intent stream becomes a stream of reducers; then they are merged. */
export const reducers = (i: Intents): Observable<Reducer<Model>> =>
  merge(
    i.add$.pipe(map((title) => update(Msg.add(title)))),
    i.select$.pipe(map((id) => update(id === null ? Msg.deselect() : Msg.select(id)))),
    i.startEdit$.pipe(map((id) => update(Msg.startEdit(id)))),
    i.cancelEdit$.pipe(map(() => update(Msg.cancelEdit()))),
    i.rename$.pipe(map(({ id, title }) => update(Msg.rename(id, title)))),
    i.toggle$.pipe(map((id) => update(Msg.toggle(id)))),
    i.toggleAll$.pipe(map(() => update(Msg.toggleAll()))),
    i.remove$.pipe(map((id) => update(Msg.remove(id)))),
    i.clearCompleted$.pipe(map(() => update(Msg.clearCompleted()))),
    i.setFilter$.pipe(map((f) => update(Msg.setFilter(f)))),
    i.loaded$.pipe(map((todos) => update(Msg.load(todos)))),
  );

/**
 * MODEL STREAM: fold the merged reducers with `scan`.
 * `startWith` gives subscribers the initial model; `shareReplay` makes
 * the one state machine shared by every view and effect.
 */
export const model$ = (i: Intents, initial: Model = initialModel): Observable<Model> =>
  reducers(i).pipe(
    scan((model: Model, reduce: Reducer<Model>) => reduce(model), initial),
    startWith(initial),
    distinctUntilChanged(),
    shareReplay({ bufferSize: 1, refCount: true }),
  );

// ---------------------------------------------------------------------------
// EFFECTS (Elm: Cmd). They live at the edge and talk to the world through
// a port; the port is injected so tests can pass an in-memory fake.
// ---------------------------------------------------------------------------

export interface StoragePort {
  readonly load: () => ReadonlyArray<Todo> | null;
  readonly save: (todos: ReadonlyArray<Todo>) => void;
}

export const memoryStorage = (initial: ReadonlyArray<Todo> | null = null): StoragePort & { readonly saved: () => ReadonlyArray<Todo> | null } => {
  let stored = initial;
  return {
    load: () => stored,
    save: (todos) => {
      stored = todos;
    },
    saved: () => stored,
  };
};

/** Persists the todo list `ms` after the last change (skips the initial model). */
export const persistEffect = (
  model: Observable<Model>,
  storage: StoragePort,
  ms = 300,
  scheduler: Scheduler = asyncScheduler,
): Observable<ReadonlyArray<Todo>> =>
  model.pipe(
    map((m) => m.todos),
    distinctUntilChanged(),
    skip(1),
    debounceTime(ms, scheduler),
    tap((todos) => storage.save(todos)),
  );

/** Reads the stored list once, lazily at subscription; an empty store yields nothing. */
export const loadEffect = (storage: StoragePort): Observable<ReadonlyArray<Todo>> =>
  defer(() => {
    const stored = storage.load();
    return stored === null ? EMPTY : of(stored);
  });
