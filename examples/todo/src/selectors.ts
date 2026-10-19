import type { Model, Todo } from './model.js';

/** Pure projections of the model (derived state is computed, never stored). */

/** L — List: the todos visible under the current filter. */
export const visibleTodos = (m: Model): ReadonlyArray<Todo> =>
  m.filter === 'all' ? m.todos : m.todos.filter((t) => (m.filter === 'done' ? t.done : !t.done));

/** R — Read: the selected todo, if any. */
export const selectedTodo = (m: Model): Todo | undefined => m.todos.find((t) => t.id === m.selectedId);

export interface Stats {
  readonly total: number;
  readonly active: number;
  readonly done: number;
}

export const stats = (m: Model): Stats => {
  const done = m.todos.filter((t) => t.done).length;
  return { total: m.todos.length, active: m.todos.length - done, done };
};
