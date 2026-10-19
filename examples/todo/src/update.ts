import type { Model, Reducer, Todo } from './model.js';
import type { Msg } from './msg.js';

/**
 * UPDATE — Elm's `update : Msg -> Model -> Model`, curried so that every
 * message becomes a *reducer function* `Model -> Model`.
 *
 * Everything here is pure: no streams, no time, no DOM. This file can be
 * tested with plain function calls.
 */

const identity: Reducer<Model> = (m) => m;

const patchTodo =
  (id: number, f: (t: Todo) => Todo): Reducer<Model> =>
  (m) => ({ ...m, todos: m.todos.map((t) => (t.id === id ? f(t) : t)) });

export const update = (msg: Msg): Reducer<Model> => {
  switch (msg.type) {
    // C — Create
    case 'Add': {
      const title = msg.title.trim();
      if (title === '') return identity;
      return (m) => ({ ...m, todos: [...m.todos, { id: m.nextId, title, done: false }], nextId: m.nextId + 1 });
    }

    // R — Read
    case 'Select':
      return (m) => (m.todos.some((t) => t.id === msg.id) ? { ...m, selectedId: msg.id } : m);
    case 'Deselect':
      return (m) => ({ ...m, selectedId: null });

    // U — Update
    case 'StartEdit':
      return (m) => ({ ...m, editingId: msg.id });
    case 'CancelEdit':
      return (m) => ({ ...m, editingId: null });
    case 'Rename': {
      const title = msg.title.trim();
      // Elm/TodoMVC convention: renaming to "" deletes the todo.
      if (title === '') return (m) => ({ ...update({ type: 'Remove', id: msg.id })(m), editingId: null });
      return (m) => ({ ...patchTodo(msg.id, (t) => ({ ...t, title }))(m), editingId: null });
    }
    case 'Toggle':
      return patchTodo(msg.id, (t) => ({ ...t, done: !t.done }));
    case 'ToggleAll':
      return (m) => {
        const allDone = m.todos.length > 0 && m.todos.every((t) => t.done);
        return { ...m, todos: m.todos.map((t) => ({ ...t, done: !allDone })) };
      };

    // D — Delete
    case 'Remove':
      return (m) => ({
        ...m,
        todos: m.todos.filter((t) => t.id !== msg.id),
        selectedId: m.selectedId === msg.id ? null : m.selectedId,
        editingId: m.editingId === msg.id ? null : m.editingId,
      });
    case 'ClearCompleted':
      return (m) => {
        const todos = m.todos.filter((t) => !t.done);
        const alive = (id: number | null) => (id !== null && todos.some((t) => t.id === id) ? id : null);
        return { ...m, todos, selectedId: alive(m.selectedId), editingId: alive(m.editingId) };
      };

    // L — List
    case 'SetFilter':
      return (m) => ({ ...m, filter: msg.filter });

    // Hydration from storage (an effect's result coming back as a message)
    case 'Load':
      return (m) => ({
        ...m,
        todos: msg.todos.map((t) => ({ ...t })),
        nextId: Math.max(m.nextId, ...msg.todos.map((t) => t.id + 1)),
      });
  }
};

/** Apply a list of messages to a model (handy for tests and replay/time travel). */
export const replay = (model: Model, msgs: ReadonlyArray<Msg>): Model => msgs.reduce((m, msg) => update(msg)(m), model);
