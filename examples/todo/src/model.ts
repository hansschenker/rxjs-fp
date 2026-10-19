/**
 * MODEL — the whole application state as one immutable value.
 * (Elm: `type alias Model = { ... }`)
 */

export interface Todo {
  readonly id: number;
  readonly title: string;
  readonly done: boolean;
}

export type Filter = 'all' | 'active' | 'done';

export interface Model {
  readonly todos: ReadonlyArray<Todo>;
  readonly filter: Filter;
  /** CRUDL "Read": the todo currently opened in the detail view. */
  readonly selectedId: number | null;
  /** The todo whose title is being edited inline. */
  readonly editingId: number | null;
  readonly nextId: number;
}

export const initialModel: Model = Object.freeze({
  todos: [],
  filter: 'all',
  selectedId: null,
  editingId: null,
  nextId: 1,
});

/** A reducer is a pure state transition: the unit that `scan` folds over. */
export type Reducer<M> = (model: M) => M;
