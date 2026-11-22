# The kernel and the vocabulary

rxjs-fp is organised in two layers:

- **The kernel** is a small set of operators that are implemented directly, with their own code.
  An operator belongs in the kernel only if no composition of other operators reproduces its
  behaviour exactly: the same values at the same times, the same completion, the same errors and
  the same teardown.
- **The vocabulary** is every other named operator. It has no implementation of its own: its
  definition is a short composition of kernel operators. Users keep the familiar names
  (`delay(300)`, `switchMap(search)`), and each definition shows how the kernel combines.

```ts
// kernel: implemented directly
flatMapWith(strategy)   // Merge(n) | Concat | Switch | Exhaust
timer(ms)

// vocabulary: defined by composition
export const emitAfter = (ms: number) => <A>(value: A) => timer(ms).pipe(as(value));
export const delay = (ms: number) => mergeMap(emitAfter(ms));
```

Laws and tests concentrate on the kernel. A vocabulary operator whose composition has passed its
identity test (below) is correct by construction.

## The kernel

Every operator in rxjs-fp is now either kernel or vocabulary; nothing is left outside the family
modules except infrastructure (schedulers, error constructors, notification helpers, subscription
utilities and the Promise conversions). `src/kernel.ts` lists the implementation boundary and the
helpers needed by vocabulary. Some exported names are specializations of a canonical interpreter.
The algebraic policy API is described in [algebraic-policies.md](algebraic-policies.md).

| Part | What it is |
| --- | --- |
| Primitives | `observable` (the constructor) and `fromMachine` (runs a pure machine) |
| Base operators | Direct implementations, or the base that other operators are built on |
| Policy interpreters | `repeat`, `retry`, `share`, `throttle`, and `timeout`, with named sums and readonly products |
| Specializations | `shareReplay`, `retryWith`, `timeoutFirst` and other public names that select the canonical policies |
| Adapters | Bridges to platform sources: iterables, Promises, async iterables, callbacks, event APIs, animation frames |

When is a machine kernel? Every value operator is a `fromMachine` machine. The kernel holds the
machines other operators are built on (`take`, `filter`, `reduce`, …). A machine that nothing builds
on (`pairwise`, `skipLast`, `single`, `max`, `min`, `bufferCount`) is vocabulary: it is plain data
and pure `step`/`flush` functions handed to the primitive `fromMachine`, with no shell code of its
own.

### Creation

| Kernel | Behaviour | Vocabulary built on it |
| --- | --- | --- |
| `observable` (primitive) | the constructor: a producer function | everything else |
| `from` | adopts any input, through the adapters below | `of`, `EMPTY`, `generate` |
| `defer` | creates the Observable per subscription | `iif` |
| `NEVER` | never emits, never terminates | `never()` |
| `timer` | a value after a delay, optionally repeating | `interval`, `subscribeOn`, `observeOn`, and every `…Time` operator |
| `unfold` | generates values from a seed with a pure function | `range` |
| `using` | ties a resource to the subscription's lifetime | |

Adapters: `fromIterable`, `fromArrayLike`, `fromPromise`, `fromAsyncIterable`, `fromSubscribable`,
`fromEventPattern`, `bindCallback`, `bindNodeCallback`, `animationFrames`, `scheduled`.
`fromEvent` is vocabulary: `fromEventPattern` with add and remove listener calls on the target.

`EMPTY` and `throwError` are vocabulary too: `EMPTY` is `of()`, the sequence of no values, and
`throwError(f)` is `EMPTY.pipe(throwIfEmpty(f))`, the empty sequence with its completion replaced
by an error. `NEVER` keeps its empty producer: no `timer` waits forever (`setTimeout` fires on
`Infinity`), a Promise that never settles keeps every subscriber reachable, and the one exact
composition, a Subject that nobody writes to, keeps an observer-list entry per subscriber where
the producer keeps nothing (rule 4 of the identity test below).

### Value-based

| Kernel | Behaviour | Vocabulary built on it |
| --- | --- | --- |
| `fromMachine` (primitive) | runs a machine: `init`, pure `step` and `flush` | the machines below, and `pairwise`, `skipLast`, `single`, `max`, `min`, `bufferCount` |
| `map` | transforms each value | `as`, `withIndex`, `filterMap` |
| `filter` | keeps values that pass a test | `ignoreElements`, `filterMap`, `partition` |
| `scan` | accumulates, emitting every state | `timeInterval`, `skipUntil` |
| `reduce` | accumulates, emitting the final state | `count`, `toArray` |
| `take` | the first `n` values, completing with the `n`th | `first`, `elementAt`, `find`, `findIndex`, `every`, `isEmpty` |
| `takeLast` | the last `n` values, on completion | `last`, `forkJoin` |
| `takeWhile` | values while a test holds, completing when it fails | `takeUntil`, `skipUntil` |
| `skipWhile` | drops values while a test holds | `skip` |
| `distinct(key?)` | drops values seen before | |
| `distinctUntilChanged` | drops a value equal to the previous one | `distinctUntilKeyChanged` |
| `defaultIfEmpty` | a fallback value if the source completes empty | `first`, `last`, `elementAt` with a default; `find`, `findIndex`, `every`, `isEmpty` |
| `throwIfEmpty` | an error if the source completes empty | `first`, `last`, `elementAt` without a default; `throwError` |
| `materialize`, `dematerialize` | notifications as values, and back | `observeOn` |
| `groupBy`, `groupByUntil` (named core) | one inner Observable per key | |

`first`, `elementAt`, `find`, `findIndex`, `every` and `isEmpty` are one sentence with three choices:
pick the value with `filter` and `take(1)`, then say what an empty stream means with
`defaultIfEmpty` or `throwIfEmpty`. RxJS 7 defines `first`, `last` and `elementAt` the same way.

`take` and `takeWhile` stay separate: `take(n)` completes with its `n`th value, while
`takeWhile((_, i) => i < n)` completes only when the next value arrives and fails the test.

### Time-based

The kernel takes its criterion as an Observable or a function. Each `…Time` operator is the
vocabulary version with a timer, and each buffer operator but one is its window counterpart with
every window collected into an array.

| Kernel | Behaviour | Vocabulary built on it |
| --- | --- | --- |
| `debounce` | emits a value after a quiet period | `debounceTime` |
| `throttle(selector, mode)` | selects leading, trailing, both or suppressed emissions per signal window | `throttleWith`, `throttleTime` and their named variants |
| `audit` | emits the latest value at the end of each window | `auditTime` |
| `sample` | emits the latest value when a notifier fires | `sampleTime` |
| `timeout(policy, scheduler)` | interprets first/gap deadlines and error/fallback outcomes | `timeoutFirst`, `timeoutFirstAndEach`; `catchTimeout` handles a timeout error |
| `window` | chunks of values as Observables, closed by a notifier | `buffer` |
| `windowToggle` | a chunk per opening, closed by its own closing | `windowCount`, `windowTimeEvery`, `windowTimeEveryOrCount`, `bufferToggle` |
| `windowWhen` | back-to-back chunks by a closing factory | `windowTime`, `windowTimeOrCount` |
| `bufferWhen` | back-to-back arrays by a closing factory | |

`windowCount` and the four `windowTime…` are compositions of those three shapes: `windowTime` is
`windowWhen` closed by a timer, `windowTimeEvery` is `windowToggle` opened by an interval, and
`windowCount` is `windowToggle` whose openings and closings are counts of the source's own values.
The one thing a composition cannot take from `pipe` is *order within a value*: a notifier derived
from the source sees each value before the windows do, so a count would fire one value early.
`afterDelivery` (in `src/vocabulary/time.ts`) recovers the order from subscription order with two
`connect` layers — the windows subscribe to the first, the counts to the second, and the second is fed
only after the windows have received the value. `…OrCount` races that count against the timer. The
one difference this leaves is documented in the README: with both an interval and a maximum size, a
value reaches every open window before the window it fills closes, where RxJS closes it between
deliveries.

`bufferWhen` is the exception among the buffers: RxJS 7 opens a new window when the closing
notifier completes, but ignores that completion for a buffer, so `windowWhen` plus `toArray` would
differ from RxJS there. `bufferCount` is not `windowCount` plus `toArray` either: `windowCount`
opens a window on an empty source, so that version would emit `[]` where RxJS emits nothing.

Also vocabulary here: `delay`, `emitAfter`, `delayWhen` (`mergeMap` over a duration), `timestamp`
(`map` with `scheduler.now()`), `timeInterval` (`timestamp` then `scan`), and the two scheduling
operators: `observeOn` is `delay` applied to the notifications instead of the values (`materialize`,
`delay`, `dematerialize`), so completion and errors wait their turn behind the values before them,
and `subscribeOn` is a `timer` flattened into the source (`mergeMap`). The adapter `scheduled` spells
both with kernel operators, since the kernel may not import the vocabulary.

### Concurrency

Concurrency is what happens when a value starts nested work while earlier work is still running.
`flatMapWith(strategy)` takes one of four strategies:

| Vocabulary | Strategy | When new work arrives while work is running |
| --- | --- | --- |
| `mergeMap` | `Merge(n)` | runs it alongside, up to `n` at once; the rest wait |
| `concatMap` | `Concat` | queues it until the running work completes |
| `switchMap` | `Switch` | cancels the running work, starts the new |
| `exhaustMap` | `Exhaust` | ignores the new work until the running work completes |

`scanWith(strategy)` is its twin for accumulators that return Observables (`mergeScan`,
`switchScan`), and `expand` feeds every result back into the projection.

The same four strategies run through the time family. Applied to `timer(ms)`:

| Time behaviour | Strategy over `timer(ms)` |
| --- | --- |
| debounce: each value cancels the previous wait | `Switch` |
| throttle: values are ignored while a wait runs | `Exhaust` |
| delay: every value waits in parallel | `Merge` |
| spacing values out by `ms` | `Concat` |

### Combination

| Kernel | Behaviour | Vocabulary built on it |
| --- | --- | --- |
| `combineLatest` | combines the latest value of each input | `combineLatestWith`, `combineLatestAll`, `forkJoin` |
| `zip` | pairs values by position | `zipWith`, `zipAll` |
| `withLatestFrom` | adds the other inputs' latest values to each source value | |
| `race` | follows whichever input emits first | `raceWith` |
| `sequenceEqual` | compares two sequences value by value | |

Vocabulary on top, together with the concurrency vocabulary: `takeUntil`, `skipUntil`, `forkJoin`,
`startWith`, `endWith`, `onErrorResumeNext`, `onErrorResumeNextWith`, `partition`.

`combineLatest` keeps one deliberate difference from RxJS 7: it completes as soon as an input
completes without a value, since it can never emit after that (RxJS waits for all inputs).

### Errors and effects

| Kernel | Behaviour | Vocabulary built on it |
| --- | --- | --- |
| `catchError` | replaces an error with another Observable | `catchTimeout`, `onErrorReturn`, `onErrorResumeNext` |
| `retry(policy, scheduler)` | restarts after errors according to additional-retry limit, wait and counter choices | `retryWith`, `retryConsecutiveWith`; decision functions `times`, `timesWithDelay`, `exponentialBackoff` |
| `repeat(policy, scheduler)` | restarts after completion according to total-run limit and wait choice | |
| `finalize` | runs a function on teardown | |
| `tap` | runs a side effect per notification | |

The resubscribing operators could be written as recursion: a `catchError` whose handler returns
the retried source. Values and timing would match RxJS, but each retry would add a subscription
layer that lives until the stream ends, so a reconnect loop running for days would keep growing.
The canonical `repeat` and `retry` interpreters stay in the kernel and replace the active source
subscription rather than nesting a new operator layer. Immediate synchronous restarts still use
the JavaScript call stack; a scheduled wait can break that recursion.

### Multicasting

| Kernel | Behaviour | Vocabulary built on it |
| --- | --- | --- |
| the Subjects | `createSubject`, `createBehaviorSubject`, `createReplaySubject`, `createAsyncSubject` | |
| `connectable`, `connectableOnce` | connect on `connect()`: the theater view; `Once` cannot reconnect | `publish`, `publishReplay`, `publishBehavior`, `publishLast` |
| `refCount` | connects a connectable while it has subscribers | |
| `connect(selector)` | multicasts within one subscription | |
| `share(policy)` | connects automatically, with independent reset decisions for error, completion and no subscribers | `shareReplay`, `shareWith`, `shareLatest` |

### How far from 30

The original 0.1 census counted 52 base operators. The 0.2 policy work consolidates retry and
sharing behavior behind canonical interpreters, so that historical count should not be treated as
a current export count. A smaller kernel is still a goal. Further reductions each need an identity test:

- **The window family** — done. `windowCount` and the four `windowTime…` are vocabulary over
  `windowToggle`, `windowWhen` and timers; one base operator and one named core went. The window
  kernel is now `window`, `windowToggle`, `windowWhen` and `bufferWhen`.
- **Trivial sequences** — done. `EMPTY` is `of()` and `throwError` is `EMPTY` with `throwIfEmpty`;
  two base operators went. `NEVER` stays, with the empty producer.
- **Scheduling** — done, and not by moving them: `observeOn` is `materialize`, `delay` and
  `dematerialize`, `subscribeOn` is a `timer` flattened into the source. Two base operators went.
- **Combination.** `withLatestFrom`, `race` and `sequenceEqual` may turn out to be compositions,
  as `takeUntil` and `skipUntil` did.
- **Notifications.** `materialize` and `dematerialize` could count as one primitive pair beside
  `fromMachine`.

A larger reduction needs the postponed step below: time and coordination as pure machines,
where one interpreter replaces most of the native time code. Merely renaming public exports
does not reduce the number of execution rules.

## The vocabulary modules

Each family's vocabulary is an importable module, built only on the kernel. Each family module
also re-exports the kernel operators of its family, so users import by family and never need to
know which operators are kernel:

```ts
import { from } from 'rxjs-fp/creation';
import { concatMap } from 'rxjs-fp/concurrency';
import { delay } from 'rxjs-fp/time';
```

| Entry point | Contents |
| --- | --- |
| `rxjs-fp/kernel` | the kernel operators |
| `rxjs-fp/creation`, `rxjs-fp/values`, `rxjs-fp/time`, `rxjs-fp/concurrency`, `rxjs-fp/combination`, `rxjs-fp/errors`, `rxjs-fp/multicast` | each family's kernel operators and its vocabulary |
| `rxjs-fp` | everything, for convenience and for existing code |

Rules that keep the layering intact:

- **Vocabulary modules import only from the kernel.** A test checks the import statements of every
  vocabulary file, as `test/purity.test.ts` checks the syntax tree.
- **No cost for convenience.** rxjs-fp is marked `sideEffects: false`, so importing from the root
  still bundles only what is used.
- **Vocabulary definitions follow the naming style below.** The library's own source shows the
  style it recommends.

### Status

| Module | Source | State |
| --- | --- | --- |
| `rxjs-fp/kernel` | `src/kernel.ts` | done: re-exports the kernel plus the types and helpers vocabulary needs |
| `rxjs-fp/concurrency` | `src/vocabulary/concurrency.ts` | done: `mergeMap`, `concatMap`, `switchMap`, `exhaustMap`, the four `…All`, `merge`, `concat`, `mergeWith`, `concatWith` |
| `rxjs-fp/time` | `src/vocabulary/time.ts` | done: `delay`, `emitAfter`, `debounceTime`, `throttleTime` and its variants, `throttleTimeWith`, `auditTime`, `sampleTime`, `windowCount`, `windowTime`, `windowTimeEvery`, `windowTimeOrCount`, `windowTimeEveryOrCount`, `observeOn`, `subscribeOn` |
| `rxjs-fp/values` | `src/vocabulary/values.ts` | done: `as`, `withIndex`, `skip`, `ignoreElements`, `count`, `toArray`, `first`, `last`, `elementAt`, `find`, `findIndex`, `every`, `isEmpty`, `distinctUntilKeyChanged` |
| `rxjs-fp/creation` | `src/vocabulary/creation.ts` | done: `of`, `EMPTY`, `throwError`, `range`, `interval`, `iif` |
| `rxjs-fp/combination` | `src/vocabulary/combination.ts` | done: `takeUntil`, `forkJoin`, `startWith`, `endWith`, `onErrorResumeNext`, `combineLatestWith`, `zipWith`, `raceWith`, `onErrorResumeNextWith`, `combineLatestAll`, `zipAll`, `partition` |
| `rxjs-fp/errors` | `src/vocabulary/errors.ts` | done: `retry`, `times`, `timesWithDelay`, `exponentialBackoff`, `catchTimeout`, `onErrorReturn` |
| `rxjs-fp/multicast` | `src/vocabulary/multicast.ts` | done: `share`, `shareReplay`, `shareLatest`, `publish`, `publishReplay`, `publishBehavior`, `publishLast` |
| former outliers | the modules above | done: every operator is now in a family module. Leaf machines (`pairwise`, `skipLast`, `single`, `max`, `min`, `bufferCount`) moved as written; `filterMap`, `skipUntil`, `timeInterval`, `timestamp`, `delayWhen`, `throttle…`, `mergeScan`, `switchScan`, `fromEvent`, `generate`, `empty`, `never` and six buffer operators became compositions |

`test/layering.test.ts` enforces the three rules: vocabulary imports only the kernel, the kernel never
reaches the vocabulary (checked transitively), and the root exports the same functions as the
family modules. Kernel files that used a vocabulary operator internally (`delayWhen`,
`combineLatestAll`, `zipAll`, `startWith`, `endWith`) now call `flatMapWith` directly, and
`scheduled` spells `subscribeOn` and `observeOn` with `timer`, `flatMapWith`, `materialize` and
`dematerialize`.

## The identity test

An operator moves from the kernel to the vocabulary only when its composition passes an identity
test:

1. Write the vocabulary definition as a composition of kernel operators.
2. Run it against RxJS 7 in `test/differential.test.ts` on random marble diagrams: the same
   notifications at the same virtual times. For multicasting, use a scripted scenario with several
   subscribers instead.
3. If the outputs are identical, the operator is vocabulary. If not, it stays in the kernel, or the
   difference is documented as a deliberate one.
4. Identical output is necessary, not sufficient: the composition must also use the same resources.
   A recursive `retry` passes step 2 and still grows by one subscription layer per retry.

A limit of the harness: the rxjs-fp and RxJS test schedulers order a hot and a cold event in the
same frame differently (even a plain `merge` shows it), and rxjs-fp subscribes just after a hot
value at frame 0. Identity tests therefore use cold inputs, or hot inputs that avoid those ties.

A second limit: in run mode, RxJS's `TestScheduler` runs same-frame `asyncScheduler` actions in the
order their timer records were created, not in scheduling order, and throws `Expected a due
immediate or interval` on some ties. The window comparisons that also record opens and closes therefore
use parameters without same-frame ties, and `test/window.test.ts` pins rxjs-fp's own order, which
follows scheduling order.

Results so far:

| Vocabulary | Composition | Result |
| --- | --- | --- |
| `debounceTime(ms)` | `debounce(() => timer(ms))` | identical |
| `debounceTime(ms)` | `switchMap(emitAfter(ms))` | differs: does not flush the pending value on completion, so `debounce` is the better base |
| `throttleTime(ms, mode)` and its variants | `throttle(() => timer(ms), mode)` | identical on the tested diagrams, for all four modes |
| `auditTime(ms)` | `audit(() => timer(ms))` | identical |
| `sampleTime(ms)` | `sample(interval(ms))` | identical. RxJS defines `sampleTime` this way; rxjs-fp's former native version emitted the previous value when a tick and a value fell in the same frame, and the composition fixed that |
| `delay(ms)` | `mergeMap(emitAfter(ms))` | identical on the tested diagrams |
| `skip(n)` | `skipWhile((_, i) => i < n)` | identical |
| `distinct()` | `distinct((x) => x)` | identical |
| `take(n)` | `takeWhile((_, i) => i < n)` | differs: completes later, so both stay in the kernel |
| `first`, `last`, `elementAt` | `filter`, `take(1)` or `takeLast(1)`, then `defaultIfEmpty` or `throwIfEmpty` | identical, including the error names and messages |
| `find`, `findIndex`, `every`, `isEmpty` | `filter` (or `map`), `take(1)`, `defaultIfEmpty` | identical |
| `count`, `toArray` | `reduce`; `toArray` collects into a persistent list, so it stays pure and linear | identical |
| `of`, `range`, `interval`, `iif` | `from`, `unfold`, `timer(ms, ms)`, `defer` | identical, including empty and negative `range` counts |
| `takeUntil(n)` | `merge` of the notifier's first value and the source's end, both as "stop", into the source's values; `takeWhile` until "stop" | identical, including a notifier that fires at subscription (the source is never subscribed). Without the source's end as "stop" it waited for the notifier |
| `forkJoin`, `startWith`, `endWith`, `onErrorResumeNext`, `…With`, `…All`, `partition` | `combineLatest` of `takeLast(1)`, `concat`, `catchError(() => EMPTY)`, `toArray` then `mergeMap` | identical |
| `retryWith`, `retryConsecutiveWith` | `retry` with an algebraic policy and a decision-to-signal function | identical on the tested diagrams |
| `catchTimeout`, `onErrorReturn` | `catchError` with a selected handler | identical on the tested diagrams |
| `retry`, `repeat` | recursion through `catchError` or `concat` | identical output, but grows by one subscription layer per resubscription: canonical interpreters stay in the kernel |
| `shareReplay`, `shareWith`, `shareLatest`, `publish…` | `share` with a policy, or `connectableOnce` with a Subject | identical in the scripted multi-subscriber scenarios |
| `buffer`, `bufferTime…` ×4, `bufferToggle` | the window counterpart, then `mergeMap(toArray())` | identical, including closings that complete empty, never close, or error |
| `bufferWhen` | `windowWhen`, then `mergeMap(toArray())` | differs when the closing notifier completes without emitting: RxJS reopens a window but not a buffer. Stays in the kernel |
| `bufferCount` | `windowCount`, then `mergeMap(toArray())` | differs on an empty source (`[]` instead of nothing). Stays a machine |
| `skipUntil(n)` | the `takeUntil` merge, with the notifier's first value opening a gate (`scan`) instead of stopping | identical |
| `filterMap`, `timeInterval`, `delayWhen`, `timestamp` | `map`/`filter`/`map`; `timestamp` then `scan`; `mergeMap` over the duration; `map` with `scheduler.now()` | identical |
| `fromEvent` | `fromEventPattern` with add/remove listener | identical events, and the listener is removed on unsubscribe, for Node-style and DOM-style targets |
| `generate`, `empty()`, `never()` | `from` or `scheduled` over the states; `EMPTY`; `NEVER` | identical, with and without a scheduler |
| `EMPTY`, `throwError(f)` | `of()`; `EMPTY`, then `throwIfEmpty(f)` | identical: the error arrives before `subscribe` returns, the factory runs once per subscription, and a factory that throws is itself the error |
| `observeOn(s, ms)` | `materialize`, `delay(ms, s)`, `dematerialize` | identical, with and without a delay, including a `take` that cancels the scheduled notifications |
| `subscribeOn(s, ms)` | `timer(ms, s)`, then `mergeMap` into the source | identical, with and without a delay; unsubscribing before the task runs never subscribes the source |

## The naming style for pipelines

Name anything that encodes a decision before the pipeline, so the pipeline reads as a sentence:

```ts
const userIds = [1, 2, 3, 4, 5];

// named projections: what happens to one value
const fetchUser = (id: number): Observable<User> => getUser(id);

// named operators: what happens to the stream
const loadUsersInOrder = concatMap(fetchUser);
const shiftBy = (ms: number) => mergeMap(emitAfter(ms));    // = delay(ms)
const spaceOutBy = (ms: number) => concatMap(emitAfter(ms));

from(userIds).pipe(loadUsersInOrder, shiftBy(500));
```

- **One name per concept.** `emitAfter` is defined once; `shiftBy` and `spaceOutBy` differ only in
  their strategy, and the difference is visible at a glance.
- **Dependencies are arguments.** `loadUsersInOrderWith(fetch)` gives the version for the app and
  a version with a fake `fetch` for the test, without a mocking library.
- **Errors are handled inside the named projection.** There, an error ends only that value's nested
  work, and the pipeline goes on:

  ```ts
  const requestRetries: RetryPolicy = {
    limit: additionalRetries(2), delay: after(1000), counter: keepCount(),
  };
  const fetchUserReliably = (id: number) =>
    fetchUser(id).pipe(retry(requestRetries, scheduler), attempt());   // errors become Either values
  ```

  Handled at the end of the pipeline instead, the first error ends the whole stream.
- **Named pipelines compose.** `flow(loadUsersInOrder, spaceOutBy(500))` is itself an operator, and
  it is how users build their own operators from the kernel.
- **Inline what is self-evident.** `map((user) => user.name)` is clearer inline than behind a name.

## Postponed

- **Time and coordination operators as pure machines.** A Mealy machine has one input and answers
  at once, so it cannot express "wait 300 ms" or "subscribe to this and cancel that". The way
  forward is Elm's design: `step` returns commands as data (`emit`, `schedule`, `subscribe`,
  `cancel`), and timer firings and nested notifications come back as input messages, interpreted
  by one generic shell.
- **The share family and Subjects** hold state shared between subscribers, which is a resource
  rather than per-subscription state, and needs its own treatment.
