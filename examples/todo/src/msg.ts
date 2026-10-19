import type { Filter } from './model.js';

/**
 * MSG — everything that can happen, as data. (Elm: `type Msg = ...`)
 *
 *   C  Create   Add
 *   R  Read     Select / Deselect
 *   U  Update   Rename, Toggle, ToggleAll, StartEdit, CancelEdit
 *   D  Delete   Remove, ClearCompleted
 *   L  List     SetFilter
 */
export type Msg =
  | { readonly type: 'Add'; readonly title: string }
  | { readonly type: 'Select'; readonly id: number }
  | { readonly type: 'Deselect' }
  | { readonly type: 'StartEdit'; readonly id: number }
  | { readonly type: 'CancelEdit' }
  | { readonly type: 'Rename'; readonly id: number; readonly title: string }
  | { readonly type: 'Toggle'; readonly id: number }
  | { readonly type: 'ToggleAll' }
  | { readonly type: 'Remove'; readonly id: number }
  | { readonly type: 'ClearCompleted' }
  | { readonly type: 'SetFilter'; readonly filter: Filter }
  | { readonly type: 'Load'; readonly todos: ReadonlyArray<{ readonly id: number; readonly title: string; readonly done: boolean }> };

/** Message constructors (keep call sites short and typed). */
export const Msg = {
  add: (title: string): Msg => ({ type: 'Add', title }),
  select: (id: number): Msg => ({ type: 'Select', id }),
  deselect: (): Msg => ({ type: 'Deselect' }),
  startEdit: (id: number): Msg => ({ type: 'StartEdit', id }),
  cancelEdit: (): Msg => ({ type: 'CancelEdit' }),
  rename: (id: number, title: string): Msg => ({ type: 'Rename', id, title }),
  toggle: (id: number): Msg => ({ type: 'Toggle', id }),
  toggleAll: (): Msg => ({ type: 'ToggleAll' }),
  remove: (id: number): Msg => ({ type: 'Remove', id }),
  clearCompleted: (): Msg => ({ type: 'ClearCompleted' }),
  setFilter: (filter: Filter): Msg => ({ type: 'SetFilter', filter }),
  load: (todos: Extract<Msg, { type: 'Load' }>['todos']): Msg => ({ type: 'Load', todos }),
} as const;
