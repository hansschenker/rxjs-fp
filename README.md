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

The operators follow RxJS 7.8.2 behavior, with an explicitly functional API and runtime.
Version 0.2 introduces breaking policy signatures for `repeat`, `retry`, `share`,
`shareReplay`, `throttle` / `throttleTime`, and `timeout`. Their implementations
interpret algebraic policy values directly. See [the policy and migration guide](docs/algebraic-policies.md).

## Why

In his talk *RxJS Evolved* (Reactive 2015), Paul Taylor gave the definition this library is built on:

> An Observable is a function that, when invoked, returns 0 to ∞ values between now and the end of time.

RxJS's implementation has changed shape several times:
- **RxJS 4:** closures everywhere.
- **RxJS 5:** classes and `lift`, for V8's inline caches.
- **RxJS 7:** a hybrid, with `operate()` plus closures inside one generic `OperatorSubscriber` class.

`rxjs-fp` takes this one step further. It keeps the semantics pure and puts the unavoidable mutation in one small, audited place per subscription.

## Design in seven rules

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
6. **Error routes are explicit.**
   - Exceptions from operator projections go to `error`; invalid policy numbers throw during construction.
   - Shared reset signals use the unhandled-error route; teardown failures can surface from `unsubscribe()` as `UnsubscriptionError`. The policy guide documents these lifecycle boundaries.
   - Exceptions from consumer callbacks are reported on a fresh stack, never re-entered.

7. **Behaviour is an algebra of named choices.**
   - A sum names alternatives: `immediate | after | on-signal`, or the four `ThrottleMode` values.
   - A readonly product combines independent decisions: `RepeatPolicy = RepeatLimit × RepeatDelay`.
   - Each canonical operator interprets its policy directly, with exhaustive branches. There is no conversion back to a native RxJS config.
   - Functions remain values in that algebra: retry signals receive the error and retry number; timeout fallbacks receive typed diagnostics.
   - Common names such as `throttleTrailing`, `timeoutFirst`, and `retryWith` compose the same canonical implementations.

The plan for reducing rxjs-fp to a small kernel, with every other operator defined as vocabulary on top of it, is in [docs/kernel.md](docs/kernel.md).

Operators can be imported from the root or by family; both give the same functions:

```ts
import { from, of } from 'rxjs-fp/creation';
import { concatMap } from 'rxjs-fp/concurrency';
import { delay, debounceTime } from 'rxjs-fp/time';
```

The families are `rxjs-fp/kernel`, `/creation`, `/values`, `/time`, `/concurrency`, `/combination`, `/errors` and `/multicast`.

A small end-to-end sample in this style is in [examples/users](examples/users): a pipeline of named decisions (`users.ts`), a real-time demo with a fake service (`npm run example:users`), and the same code tested in virtual time (`users.test.ts`). A second one is [examples/search](examples/search): search-as-you-type against a service that is sometimes down and sometimes silent, with a timeout policy, a retry policy and a shared replay, run and tested frame by frame on a virtual clock (`npm run example:search`, `search.test.ts`).

The library source must not contain `class`, `this`, `instanceof`, `.prototype`, `new` on custom types or a `let` at module scope. A test checks this on the TypeScript AST (`test/purity.test.ts`).

## Install and scripts

```bash
npm install
npm run check        # typecheck + tests + build
npm test             # vitest (unit, marble, law properties, interop)
npm run build        # dist/ (ESM + .d.ts)
npm run example:todo # Todo CRUDL example in the browser (Vite)
npm run example:todo:node
npm run example:policies # all six policy families on explicit virtual time
npm run example:search   # search-as-you-type on a virtual clock, frame by frame
```

Entry points: `rxjs-fp` (core and operators), `rxjs-fp/fp` (Option, Either, typeclasses, do-notation), `rxjs-fp/testing` (marbles, virtual time).

## What's included

| Area | Functions |
|---|---|
| **Core** | `observable`, `pipe`, `flow`, `identity`, `noop`, `subscribeChild`, `fromMachine`, `runMachine` |
| **Creation** | `of`, `from` (Observable, any Subscribable, Promise, iterable, async iterable, array-like), `defer`, `range`, `unfold`, `generate(initial, condition, iterate)`, `interval`, `timer`, `iif`, `using`, `scheduled`, `fromEvent` (typed for DOM and Node emitters), `fromEventPattern`, `bindCallback`, `bindNodeCallback`, `animationFrames`, `EMPTY`, `NEVER`, `throwError` |
| **Transformation** | `map`, `as`, `scan`, `reduce`, `pairwise`, `tap`, `toArray`, `count`, `max`, `min`, `defaultIfEmpty`, `materialize`/`dematerialize` (notifications as data), `timestamp`, `timeInterval` |
| **Buffering / windowing** | `buffer`, `bufferCount`, `bufferTime` (+ `Every`, `OrCount`, `EveryOrCount`), `bufferToggle`, `bufferWhen`, `window`, `windowCount`, `windowTime` (+ `Every`, `OrCount`, `EveryOrCount`), `windowToggle`, `windowWhen` |
| **Grouping** | `groupBy`, `groupByUntil` (both with an optional `connector`), `partition` |
| **Filtering** | `filter` (with refinements), `filterMap`, `take`, `takeLast`, `skip`, `skipLast`, `takeWhile`, `skipWhile`, `distinct`, `distinctUntilChanged`, `distinctUntilKeyChanged`, `first`, `last`, `single`, `elementAt`, `ignoreElements`, `takeUntil`, `skipUntil` |
| **Conditional** | `every`, `find`, `findIndex`, `isEmpty`, `sequenceEqual`, `throwIfEmpty`, `defaultIfEmpty` |
| **Flattening** | `flatMapWith(strategy)`, `mergeMap`, `concatMap`, `switchMap`, `exhaustMap`, `*All`, `expand`, `mergeScan`, `switchScan` |
| **Combination** | `merge`, `concat`, `combineLatest`, `zip`, `forkJoin` (all tuple-typed), `race`, `onErrorResumeNext`, `withLatestFrom`, `startWith`, `endWith`, `*With` (incl. `raceWith`, `onErrorResumeNextWith`), `combineLatestAll`, `zipAll` |
| **Time** | `delay`, `emitAfter`, `delayWhen`, `debounceTime`, `debounce`, `throttle(selector, mode)`, `throttleTime(ms, mode, scheduler)` (+ named variants), `auditTime`, `audit`, `sampleTime`, `sample`, `timeout(policy, scheduler)`, `timeoutFirst`, `timeoutFirstAndEach`, `subscribeOn`, `observeOn` |
| **Errors** | `catchError`, `catchTimeout`, `retry(policy, scheduler)`, `repeat(policy, scheduler)`, `retryWith` and `retryConsecutiveWith` (decision functions `times`, `timesWithDelay`, `exponentialBackoff`), `finalize`, `onErrorReturn`; error values `emptyError`, `timeoutError`, `notFoundError`, `sequenceError`, … with `is*` guards |
| **Multicasting** | `createSubject`, `createBehaviorSubject`, `createReplaySubject`, `createAsyncSubject`, `share(policy)`, `shareReplay(policy, scheduler)`, `shareLatest`, `shareWith(connector, WhileSubscribed | Cached)`, `connectable`, `connectableOnce`, `connect`, `publish`, `publishReplay`, `publishBehavior`, `publishLast`, `refCount` |
| **Conversion** | `firstValueFrom`, `firstValueFromOr`, `lastValueFrom`, `lastValueFromOr`, `toAsyncIterable` |
| **Schedulers** | `asyncScheduler`, `asapScheduler`, `queueScheduler` (trampoline), `animationFrameScheduler`, `createVirtualScheduler` |

### Two views of multicasting

- **The theater: `publish`, `publishReplay`, `publishBehavior`, `publishLast`.** Subscribers take their seats, nothing runs until `connect()` raises the curtain, and then everyone seated sees the show from the first value. It is the only way to guarantee that several subscribers see the same values from the very first one. Each is `connectableOnce(source, connector)` with a different Subject behind the curtain, which decides what late arrivals see.
- **Automatic: `share`, `shareReplay`, `shareLatest`.** The show starts when the first subscriber arrives; later arrivals may have missed the beginning.
- **The bridge: `refCount(connectable)`** raises the curtain when the first subscriber arrives and lowers it when the last one leaves.

```ts
const show = publish(source);
show.subscribe(a);
show.subscribe(b);
show.connect(); // a and b both see every value from the first one

const shared = refCount(publish(source)); // connects and disconnects by itself
```

RxJS 8 alpha removed the `publish` family; RxJS 9 beta brings it back. rxjs-fp keeps it, as plain functions of the source like `connectable`, so the result keeps its `connect` method.

### Coverage of RxJS 7.8

Every non-deprecated RxJS 7.8 function has an rxjs-fp counterpart. Each operator added to close that gap is checked against RxJS itself: `test/differential.test.ts` feeds the same random marble diagrams to both libraries on virtual time and requires identical notifications at identical frames.

Left out on purpose, because RxJS 7 deprecates them and RxJS 8 removes them: `multicast` (use `connectable(source, connector)`), `pluck`, `mapTo` (use `map`, `as`), `*MapTo`, `flatMap`, `exhaust`, `combineAll`, `pairs`, `retryWhen`, `repeatWhen`, `timeoutWith` (use `timeout` and `catchTimeout`), and the class exports.

Known differences:
- **`combineLatest`** (deliberate) completes as soon as one input completes without having emitted, because no combination can ever be produced. RxJS waits for the other inputs to complete. `combineLatestAll` inherits this.
- **`sequenceEqual`** (deliberate) answers `false` for sequences of different length. RxJS 7.8.2 answers `true` when the longer side's extra value arrives before the shorter side completes and the longer side completes last (`ab|` against `a|`), because it never checks its own leftover buffer.
- **`from` on an input it cannot convert** (deliberate) returns an Observable that errors with a `TypeError` on subscription. RxJS throws from `from()` at call time. Building a pipeline can therefore never throw; a wrong input is noticed when it is subscribed.
- **`windowToggle`**: an error from `openings` also errors the open windows.
- **`windowTimeEveryOrCount`**: when a value fills one window while others are open, every open window receives the value before the full one closes; RxJS closes the full window before handing the value to the next one. Values, frames and window membership are identical, only that same-frame order differs. (`bufferTimeEveryOrCount` is unaffected: a buffer is only observable when it closes.)
- **Algebraic policy APIs** (rule 7). The six policy families intentionally replace the RxJS config and numeric overloads. Schedulers stay explicit final arguments. The [migration guide](docs/algebraic-policies.md) gives complete examples.

  | RxJS 7.8.2 choice | rxjs-fp 0.2 choice |
  |---|---|
  | `repeat({ count: 3, delay: 100 })` | `repeat({ limit: totalRuns(3), delay: after(100) }, scheduler)` |
  | `retry({ count: 2, resetOnSuccess: true })` | `retry({ limit: additionalRetries(2), delay: immediate(), counter: resetOnNext() })` |
  | `share({ resetOnComplete: false })` | `share({ ...defaultSharePolicy(), onComplete: { kind: 'keep-state' } })` |
  | `shareReplay({ bufferSize: 1, refCount: true })` | `ShareReplayPolicy` with a one-value bound and `'disconnect-and-reset'` |
  | `throttle(selector, { leading: false, trailing: true })` | `throttle(selector, 'trailing-only')` |
  | `timeout({ first, each, with: fallback, meta })` | `TimeoutPolicy` with `'first-then-each'`, `'switch-to'`, and typed `meta` |

  With `'disconnect-and-reset'`, `shareReplay` disconnects an active source when the final subscriber leaves, while retaining the cache after successful completion. Its default `'keep-connected'` mode retains an active connection during idle periods. Named compositions for other operator families (`bufferTimeEvery`, `groupByUntil`, `connectableOnce`, `firstValueFromOr`, and others) remain available.

- **Validated policy numbers.** Count and duration constructors reject invalid numbers. Unlimited bounds use a named alternative, not `Infinity`; the concurrency limit of `Merge`, `mergeMap`, `mergeAll`, `mergeScan` and `expand` is simply omitted, and an explicit `Infinity`, which RxJS accepts, throws here. A repeat count is total runs; a retry count is additional attempts. A timeout gap must be positive; an immediate first deadline uses `deadlineAfter(0)`. Replay bounds retain RxJS's minimum of one value and one millisecond.
- **Timeout diagnostics.** `TimeoutInfo` includes typed `meta` and preserves the last source value. RxJS 7.8.2 can clear `lastValue` during source teardown before creating its diagnostics; rxjs-fp deliberately retains its existing, more informative contract.
- **Cancellation safety.** A cancelled output never starts another source producer. This retains the local runtime's safeguard even in the unusual case of cancellation during repeat/retry teardown.

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
import { attempt, rethrow, Do, bind, let_, some, none, left, right, isRight,
         ObservableFilterable, getMergeMonoid } from 'rxjs-fp/fp';
```

[docs/fp-vocabulary.md](docs/fp-vocabulary.md) explains every name in this layer in RxJS terms.

The functional layer keeps what reads naturally to an RxJS user: `Option` and `Either`, `attempt`/`rethrow` (errors as values), the do-notation (`Do`, `bindTo`, `bind`, `let_`), `chain` and `flatten`, and a few lawful instances. Monad, Applicative and Alternative instances (`MonadMerge`, `MonadConcat`, `ap`, `apLatest`, `alt`, `orElse`) are deliberately left out: an RxJS user writes `mergeMap`, `concatMap`, `combineLatest`, `merge` and `catchError` instead.

The laws are still checked, with fast-check on generated timed streams in `test/laws.test.ts`, and stated on the operators users write:

| Laws for | Laws checked |
|---|---|
| `of` with `mergeMap` | the Monad laws: left identity, right identity, associativity. All hold **in time** (same notifications at the same virtual frames, up to the order of simultaneous events) |
| `of` with `concatMap` | the same laws, **up to timing** on error-free streams (edge case below) |
| `merge` with `EMPTY` | associativity, `map` distributes over `merge`, left/right identity |
| `getMergeMonoid`, `getConcatMonoid` | associativity, identity |
| `ObservableFilterable` | `filterMap(some) ≡ id`, filter fusion, partition |

**Edge case: `concatMap` associativity and time.** Nesting `concatMap` two ways, the inner streams of the second step can overlap the next value's first step in one nesting but not in the other. Both sides emit the same sequence, but not always at the same moments, and an error can make that difference visible. `mergeMap` has no such gap.

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

- **RxJS reading rxjs-fp:** rxjs-fp implements no Observable interop protocol, since none is part of JavaScript. Wrap by hand: `new rx.Observable((s) => ours.subscribe(s))`. Unsubscription crosses that boundary, so `rx.take(1)` stops even a synchronous rxjs-fp source.
- **rxjs-fp reading RxJS:** `from(rxObservable)` adopts any object with `subscribe`.

## Background

- Paul Taylor, [*RxJS Evolved*](https://www.youtube.com/watch?v=QhjALubBQPg), Reactive 2015: Observables as functions, schedulers as central dispatchers, `lift`, marble tests.
- This repository replaces an earlier experiment, `rxjs-source-v7-fp`, which removed the `class` keyword from the RxJS 7 sources by using constructor functions and prototypes. `test/regressions.test.ts` covers the issues found there.

## Contributors

- **Hans Schenker**: author
- **Claude Opus 5.5** (Anthropic): co-author, pair-programmed the design, implementation and tests
- **ChatGPT GPT-6 Astra Pro** (OpenAI): contributor, algebraic operator policies, direct interpreters, differential tests and migration documentation

See [CONTRIBUTORS.md](CONTRIBUTORS.md).

## License

MIT
