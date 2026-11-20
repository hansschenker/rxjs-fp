# Todo CRUDL: Elm-style MVU architecture with rxjs-fp

A complete Create / Read / Update / Delete / List app that shows how to use **rxjs-fp** in ordinary application code.

**State lives in exactly one place: the `scan`.** Everything else is pure functions or thin edge adapters.

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

## Why this shape is useful with rxjs-fp

- **`update` is pure.** You can call `replay(initialModel, [Msg.add('a'), Msg.toggle(1)])` with no streams or DOM. This is the same discipline that makes Mealy-machine operators easy to test in the library itself.
- **Intents stay cold streams of data.** Mapping them to reducers keeps the reactive part thin.
- **`scan` + `shareLatest` is the only state.** No classes, no `this`, no subjects holding mutable state.
- **Effects are ordinary Observables.** They receive their ports and scheduler as arguments, so virtual-time tests are straightforward.
- **Derived data is computed.** Selectors (`visibleTodos`, `stats`, …) never store results.

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
    shareLatest(),
  );
```

## Day-to-day patterns this demonstrates

| Everyday RxJS need | How it appears here |
|---|---|
| Hold application state | `scan` over a stream of pure reducers |
| Share that state | `shareLatest()` |
| React to user input | cold intent streams from `fromEvent` |
| Debounce a side effect | `persistEffect` uses `debounceTime` + injected scheduler |
| Load once on start | `loadEffect` with `defer` |
| Derived views | pure selectors, never stored |
| Test without DOM | `replay` for the reducer; marble tests for `model$` |
| Keep effects out of the model | `StoragePort` injected into effects |

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

See also the library’s own design notes: pure machines, explicit schedulers, and algebraic policies. This example applies the same ideas at application scale.
