# Todo CRUDL: Elm-style architecture with rxjs-fp

A complete Create / Read / Update / Delete / List app. **State lives in exactly one place: the `scan`.**

```
 DOM events ─▶ intents ──map(update)──▶ reducer streams ──merge──▶ reducer$ ──scan──▶ model$ ──map(view)──▶ DOM
                  ▲                                                                      │
                  └──────────── effects (load / persist through a StoragePort) ◀─────────┘
```

| Elm | Here | File |
|---|---|---|
| `Model` | `Model`: one immutable record | `src/model.ts` |
| `Msg` | `Msg`: a tagged union, plus constructors | `src/msg.ts` |
| `update : Msg -> Model -> Model` | `update(msg)` returns a **reducer** `Model → Model` | `src/update.ts` |
| `view : Model -> Html Msg` | `viewHtml(model)` / `viewText(model)` | `src/view.ts` |
| subscriptions / runtime | intent streams → `merge` → `scan` | `src/app.ts` |
| `Cmd` | `loadEffect` / `persistEffect` through an injected `StoragePort` | `src/app.ts` |

## CRUDL mapping

| Operation | Msg | Intent stream |
|---|---|---|
| **C**reate | `Add` | `add$` |
| **R**ead | `Select` / `Deselect` | `select$` |
| **U**pdate | `Rename`, `Toggle`, `ToggleAll`, `StartEdit`, `CancelEdit` | `rename$`, `toggle$`, … |
| **D**elete | `Remove`, `ClearCompleted` | `remove$`, `clearCompleted$` |
| **L**ist | `SetFilter`, plus the derived `visibleTodos` | `setFilter$` |

## The heart of it

```ts
export const reducers = (i: Intents): Observable<Reducer<Model>> =>
  merge(
    i.add$.pipe(map((title) => update(Msg.add(title)))),
    i.toggle$.pipe(map((id) => update(Msg.toggle(id)))),
    i.remove$.pipe(map((id) => update(Msg.remove(id)))),
    // …
  );

export const model$ = (i: Intents, initial = initialModel) =>
  reducers(i).pipe(
    scan((model, reduce) => reduce(model), initial),
    startWith(initial),
    distinctUntilChanged(),
    shareReplay({ bufferSize: 1, refCount: true }),
  );
```

## Run it

```bash
npm run example:todo        # browser (Vite); state persists in localStorage
npm run example:todo:node   # scripted run in the terminal
npx vitest run examples     # pure reducer tests + marble tests of model$
```

## Why it is easy to test

- **`update` is a pure function.** `replay(initialModel, [Msg.add('a'), Msg.toggle(1)])` returns a model with no streams or DOM involved.
- **`model$` is a function of its intent streams.** A test feeds it cold marble streams and asserts the sequence of rendered states frame by frame.
- **Effects receive their storage and scheduler as arguments.** That makes the debounced save deterministic on virtual time.
