# Architecture

## Layers

```
src/core        types · subscription · observable (sink, subscribeChild) · pipe · machine · errors
src/scheduler   Scheduler interface · async / asap / animationFrame · virtual scheduler
src/creation    of · from · defer · unfold · generate · timer · fromEvent · scheduled · bindCallback …
src/operators   transform · filtering · conditional · flatten (strategies) · combine · time · error
                window (buffer*/window*) · group (groupBy) · inner (internal: duration handles)
src/subject     subjects · share · shareReplay · connectable · connect
src/fp          Option · Either · HKT · typeclasses · lawful Observable instances · do-notation
src/testing     marble parser · runMarbles · record
```

## The execution model

1. `observable(producer)` returns a frozen record `{ subscribe, pipe, [@@observable] }`.
2. `subscribe(observer)`:
   1. creates a `Subscription`, which is a closed flag plus a set of finalizers;
   2. wraps the observer in a **sink** that enforces `next* (error | complete)?`;
   3. if the observer comes from an operator (`subscribeChild`), registers the new subscription with the downstream sink *before* running the producer;
   4. calls `producer(sink)` and registers the returned teardown.
3. Step 2.3 is what makes `take(1)` stop an infinite synchronous `unfold` after one value. The downstream completes, the finalizers run, the upstream sink reports `closed`, and the producer's loop exits.

## Where state lives

| State | Where | Why it is safe |
|---|---|---|
| `closed` flag and finalizer set | `createSubscription` closure | created per subscription and never shared |
| Operator state (`scan` accumulator, `take` count, …) | `let state` in `fromMachine` | one per subscription; `step` itself is pure |
| Essential runtime state (active inners, timers, buffers) | `let` / `Set` inside each operator's producer | one per subscription |
| Shared state | `Subject`, `share`, `shareReplay` | opt-in, part of the documented semantics |

## Flattening as an interpreter

`flatMapWith(strategy)(project)` handles one outer value according to the strategy:

| Strategy | When an outer value arrives |
|---|---|
| `Merge(n)` | runs it if fewer than `n` inners are active, otherwise queues it |
| `Switch` | unsubscribes the active inner, then runs the new one |
| `Exhaust` | runs it only if no inner is active |

The result completes when the outer has completed, no inners are active and the queue is empty.

## Inner Observables with a handle

Duration selectors (`audit`, `debounce`, `throttle`), closing notifiers (`bufferToggle`, `windowToggle`, `bufferWhen`, `windowWhen`) and the like subscribe to inner Observables that may end themselves *while they are still being subscribed* (a synchronous `of(0)` duration). A `Subscription` variable cannot express that, because it is assigned only after `subscribe` returns.

The internal `subscribeInner(input, sink, handlers)` therefore returns a small handle `{ closed, close }`, creates it *before* subscribing, and passes it to the `next` handler. An operator closes "this inner" through the handle, and checks `handle.closed` instead of a possibly stale variable.

## groupBy and the outer subscriber

`groupBy` is the one operator whose source must outlive its downstream: as in RxJS, unsubscribing from the Observable of groups does not stop the source while a group is still subscribed. The source runs in its own connection scope (`subscribeWithin`) with a count of subscribed groups; the scope is closed when the outer subscriber is gone and the count is zero.
