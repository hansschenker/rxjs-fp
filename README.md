# rxjs-fp

**Reactive extensions in pure functional style, for TypeScript.**
No `class`, no `this`, no prototypes. Lawful typeclass instances, checked with property tests. Every type is strict.

```ts
import { fromEvent, map, filter, debounceTime, switchMap, from } from 'rxjs-fp';

const results$ = fromEvent(input, 'input').pipe(
  map((e) => (e.target as HTMLInputElement).value),
  filter((q) => q.length > 2),
  debounceTime(300),
  switchMap((q) => from(fetch(`/search?q=${q}`).then((r) => r.json()))),
);
```

The API looks like RxJS 7, but the architecture underneath is different.

## Why

In his talk *RxJS Evolved* (Reactive 2015), Paul Taylor gave the definition this library is built on:

> An Observable is a function that, when invoked, returns 0 to ∞ values between now and the end of time.

RxJS's implementation has changed shape several times:
- **RxJS 4:** closures everywhere.
- **RxJS 5:** classes and `lift`, for V8's inline caches.
- **RxJS 7:** a hybrid, with `operate()` plus closures inside one generic `OperatorSubscriber` class.

`rxjs-fp` takes this one step further. It keeps the semantics pure and puts the unavoidable mutation in one small, audited place per subscription.

## Design in six rules

1. **An Observable is a frozen record of closures.**
   - `subscribe` means "call the producer". Two subscriptions run it twice, so Observables are cold by default.
   - `pipe` is a closure, not a method that uses `this`.
2. **One mutable cell per subscription, and nowhere else.**
   - A `Subscription` is three closures over a private `closed` flag and a flat set of finalizers. This is RxJS 5's flattened disposable tree, without the class.
3. **Stateful operators are pure Mealy machines.**
   - Each operator is written as `init`, `step(state, value) → {state, emit, done}` and an optional `flush`.
   - One generic shell, `fromMachine`, runs every machine.
   - You can test the logic with no Observable at all: `runMachine(machine, [1, 2, 3])`.
4. **Flattening strategies are data.**
   - `mergeMap`, `concatMap`, `switchMap` and `exhaustMap` are one interpreter, `flatMapWith`, applied to `Merge(n) | Concat | Switch | Exhaust`.
5. **Schedulers are injected, never patched.**
   - A scheduler is `{ now, schedule(work, delay) → cancel }`.
   - Time operators take it as an explicit argument, so tests pass a virtual scheduler and there is no global `TestScheduler.run` patching.
6. **Errors never escape the pipeline.**
   - Exceptions from user functions in operators go to `error`.
   - Exceptions from consumer callbacks are reported on a fresh stack, never re-entered.

The library source must not contain `class`, `this`, `instanceof`, `.prototype` or `new` on custom types. A test checks this on the TypeScript AST (`test/purity.test.ts`).

## Install and scripts

```bash
npm install
npm run check        # typecheck + tests + build
npm test             # vitest (unit, marble, law properties, interop)
npm run build        # dist/ (ESM + .d.ts)
npm run example:todo # Todo CRUDL example in the browser (Vite)
npm run example:todo:node
```

Entry points: `rxjs-fp` (core and operators), `rxjs-fp/fp` (Option, Either, typeclasses, do-notation), `rxjs-fp/testing` (marbles, virtual time).

## What's included

| Area | Functions |
|---|---|
| **Core** | `observable`, `pipe`, `flow`, `identity`, `noop`, `subscribeChild`, `fromMachine`, `runMachine` |
| **Creation** | `of`, `from` (Observable, any Subscribable, Promise, iterable, async iterable, array-like), `defer`, `range`, `unfold`, `generate`, `interval`, `timer`, `iif`, `using`, `scheduled`, `fromEvent` (typed for DOM and Node emitters), `fromEventPattern`, `bindCallback`, `bindNodeCallback`, `animationFrames`, `EMPTY`, `NEVER`, `throwError` |
| **Transformation** | `map`, `as`, `scan`, `reduce`, `pairwise`, `tap`, `toArray`, `count`, `max`, `min`, `defaultIfEmpty`, `materialize`/`dematerialize` (notifications as data), `timestamp`, `timeInterval` |
| **Buffering / windowing** | `buffer`, `bufferCount`, `bufferTime`, `bufferToggle`, `bufferWhen`, `window`, `windowCount`, `windowTime`, `windowToggle`, `windowWhen` |
| **Grouping** | `groupBy` (with `element`, `duration`, `connector`), `partition` |
| **Filtering** | `filter` (with refinements), `filterMap`, `take`, `takeLast`, `skip`, `skipLast`, `takeWhile`, `skipWhile`, `distinct`, `distinctUntilChanged`, `distinctUntilKeyChanged`, `first`, `last`, `single`, `elementAt`, `ignoreElements`, `takeUntil`, `skipUntil` |
| **Conditional** | `every`, `find`, `findIndex`, `isEmpty`, `sequenceEqual`, `throwIfEmpty`, `defaultIfEmpty` |
| **Flattening** | `flatMapWith(strategy)`, `mergeMap`, `concatMap`, `switchMap`, `exhaustMap`, `*All`, `expand`, `mergeScan`, `switchScan` |
| **Combination** | `merge`, `concat`, `combineLatest`, `zip`, `forkJoin` (all tuple-typed), `race`, `onErrorResumeNext`, `withLatestFrom`, `startWith`, `endWith`, `*With` (incl. `raceWith`, `onErrorResumeNextWith`), `combineLatestAll`, `zipAll` |
| **Time** | `delay`, `delayWhen`, `debounceTime`, `debounce`, `throttleTime`, `throttle`, `auditTime`, `audit`, `sampleTime`, `sample`, `timeout` (ms, or `{ each, first, with }`), `subscribeOn`, `observeOn` |
| **Errors** | `catchError`, `retry` (count / delay / reset), `repeat`, `finalize`, `onErrorReturn`; error values `emptyError`, `timeoutError`, `notFoundError`, `sequenceError`, … with `is*` guards |
| **Multicasting** | `createSubject`, `createBehaviorSubject`, `createReplaySubject`, `createAsyncSubject`, `share`, `shareReplay`, `connectable`, `connect` |
| **Conversion** | `firstValueFrom`, `lastValueFrom`, `toAsyncIterable` |
| **Schedulers** | `asyncScheduler`, `asapScheduler`, `queueScheduler` (trampoline), `animationFrameScheduler`, `createVirtualScheduler` |

### Coverage of RxJS 7.8

Every non-deprecated RxJS 7.8 function has an rxjs-fp counterpart. Each operator added to close that gap is checked against RxJS itself: `test/differential.test.ts` feeds the same random marble diagrams to both libraries on virtual time and requires identical notifications at identical frames.

Left out on purpose, because RxJS 7 deprecates them and RxJS 8 removes them: `multicast`, `publish*`, `refCount` (use `share` or `connectable`), `pluck`, `mapTo` (use `map`, `as`), `*MapTo`, `flatMap`, `exhaust`, `combineAll`, `pairs`, `retryWhen`, `repeatWhen`, `timeoutWith` (use `timeout({ with })`), and the class exports.

Known differences:
- **`combineLatest`** (deliberate) completes as soon as one input completes without having emitted, because no combination can ever be produced. RxJS waits for the other inputs to complete. `combineLatestAll` inherits this.
- **`windowToggle`**: an error from `openings` also errors the open windows.
- Time operators take the scheduler as an argument instead of RxJS's positional overloads: `bufferTime(span, scheduler, { creationInterval, maxBufferSize })`, `windowTime(span, scheduler, { creationInterval, maxWindowSize })`.

## Writing an operator

```ts
import { fromMachine, cont, halt, type Operator } from 'rxjs-fp';

/** Emits values until their running sum exceeds `limit`. */
const takeUntilSumExceeds = (limit: number): Operator<number, number> =>
  fromMachine<number, number, number>({
    init: () => 0,
    step: (sum, x) => (sum + x > limit ? halt(sum) : cont(sum + x, [x])),
  });
```

The `step` function is pure, so you can unit- or property-test it with `runMachine` before any stream exists.

## Functional layer (`rxjs-fp/fp`)

```ts
import { MonadMerge, MonadConcat, ObservablePlus, ObservableFilterable, getMergeMonoid,
         Do, bind, let_, orElse, attempt, some, none, left, right } from 'rxjs-fp/fp';
```

These instances are lawful by construction, and `test/laws.test.ts` checks them with fast-check on generated timed streams:

| Instance | Definition | Laws checked |
|---|---|---|
| `MonadMerge` | `chain` = `mergeMap`, `ap` derived from `chain` | identity, associativity, ap/chain consistency. All hold **in time** (same notifications at the same virtual frames, up to the order of simultaneous events) |
| `MonadConcat` | `chain` = `concatMap` | same laws, **up to timing** on error-free streams (edge case below) |
| `ObservablePlus` / `ObservableAlternative` | `alt` = `merge`, `zero` = `EMPTY` | associativity, distributivity, left/right identity |
| `getMergeMonoid`, `getConcatMonoid` | `merge` or `concat`, with `EMPTY` | associativity, identity |
| `ObservableFilterable` | `filterMap` etc. | `filterMap(some) ≡ id`, filter fusion, partition |

Two deliberate choices:
- **Error recovery is not `alt`.** `catchError` is exported as `orElse`.
- **A `combineLatest`-based apply is not the Monad's `ap`.** It is exported as `apLatest` and makes no law claims.

**Edge case: `MonadConcat` associativity and time.** In `chain(chain(m, f), g)`, the `g`-inners of one `f(x)` can overlap the next `f(x)`. In `chain(m, x => chain(f(x), g))` they cannot. Both sides emit the same sequence, but not always at the same moments, and an error can make that difference visible. `MonadMerge` has no such gap.

## Marble testing

```ts
import { runMarbles } from 'rxjs-fp/testing';

runMarbles(({ cold, scheduler, expectObservable }) => {
  expectObservable(cold('ab---c-d----|').pipe(debounceTime(2, scheduler)))
    .toBe('---b-----d--|');
});
```

Syntax: `-` frame, `a` value, `|` complete, `#` error, `(ab)` group, `^` subscription point, `!` unsubscription. The virtual scheduler is passed explicitly to the operator under test.

## Example: Todo CRUDL in Elm style

[`examples/todo`](examples/todo) is a complete Create / Read / Update / Delete / List app:

```
intents ──map(update)──▶ reducer streams ──merge──▶ reducer$ ──scan(apply)──▶ model$ ──map(view)──▶ DOM
```

`update(msg)` returns a pure reducer `Model → Model`. Each intent stream is mapped to reducers, they are merged, and `scan` folds them into the model. It runs in the browser (`npm run example:todo`), in the terminal (`npm run example:todo:node`), and in marble tests.

## Interop with RxJS

- **RxJS reading rxjs-fp:** rxjs-fp Observables expose `'@@observable'`, so `rx.from(ours)` works at runtime. RxJS types its interop key as `Symbol.observable`, so TypeScript needs a cast: `rx.from(ours as unknown as rx.InteropObservable<T>)`.
- **rxjs-fp reading RxJS:** `from(rxObservable)` adopts any object with `subscribe`.

## Background

- Paul Taylor, [*RxJS Evolved*](https://www.youtube.com/watch?v=QhjALubBQPg), Reactive 2015: Observables as functions, schedulers as central dispatchers, `lift`, marble tests.
- This repository replaces an earlier experiment, `rxjs-source-v7-fp`, which removed the `class` keyword from the RxJS 7 sources by using constructor functions and prototypes. `test/regressions.test.ts` covers the issues found there.

## Contributors

- **Hans Schenker**: author
- **Claude Opus 5.5** (Anthropic): co-author, pair-programmed the design, implementation and tests

See [CONTRIBUTORS.md](CONTRIBUTORS.md).

## License

MIT
