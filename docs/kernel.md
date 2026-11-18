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

A first, deliberately generous selection of about 40 operators, to be trimmed towards 30 as the
identity tests show which ones can become vocabulary.

### Creation (10)

| Kernel | Behaviour | Vocabulary built on it |
| --- | --- | --- |
| `observable` | the constructor: a producer function | everything else |
| `from` | adopts any input: array, Promise, iterable, Observable | `of`, `scheduled` |
| `defer` | creates the Observable per subscription | `iif` |
| `EMPTY`, `NEVER`, `throwError` | the three trivial sequences | |
| `timer` | a value after a delay, optionally repeating | `interval` |
| `unfold` | generates values from a seed with a pure function | `generate`, `range` |
| `fromEvent` | adapts DOM or Node events | `fromEventPattern`, `bindCallback` |
| `using` | ties a resource to the subscription's lifetime | |

### Value-based (9)

All of these are machines run by `fromMachine`, the one primitive behind the family.

| Kernel | Behaviour | Vocabulary built on it |
| --- | --- | --- |
| `map` | transforms each value | `as`, `timestamp` |
| `filter` | keeps values that pass a test | `filterMap`, `ignoreElements` |
| `scan` | accumulates, emitting every state | `pairwise`, `count` |
| `reduce` | accumulates, emitting the final state | `toArray`, `max`, `min`, `takeLast` |
| `take` | the first `n` values, completing with the `n`th | `first`, `elementAt` |
| `takeWhile` | values while a test holds, completing when it fails | `find`, `findIndex` |
| `skipWhile` | drops values while a test holds | `skip` |
| `distinct(key?)` | drops values seen before | |
| `distinctUntilChanged` | drops a value equal to the previous one | `distinctUntilKeyChanged` |

`take` and `takeWhile` stay separate: `take(n)` completes with its `n`th value, while
`takeWhile((_, i) => i < n)` completes only when the next value arrives and fails the test.

### Time-based (9)

The kernel takes its criterion as an Observable or a function. Each `…Time` operator is the
vocabulary version with a timer.

| Kernel | Behaviour | Vocabulary built on it |
| --- | --- | --- |
| `debounce` | emits a value after a quiet period | `debounceTime` |
| `throttleWith(strategy)` | lets one value through per window | `throttle`, `throttleTime` and their variants |
| `audit` | emits the latest value at the end of each window | `auditTime` |
| `sample` | emits the latest value when a notifier fires | `sampleTime` |
| `buffer` | collects values into arrays, closed by a notifier | `bufferTime…`, `bufferWhen`, `bufferCount` |
| `window` | like `buffer`, but each chunk is an Observable | `windowTime…`, `windowWhen`, `windowCount` |
| `timeout` | errors when a value takes too long | `timeoutFirst`, `timeoutFirstAndEach` |
| `observeOn`, `subscribeOn` | move notifications or the subscription onto a scheduler | |

### Concurrency (1 operator, 4 strategies)

Concurrency is what happens when a value starts nested work while earlier work is still running.
One kernel operator, `flatMapWith(strategy)`, with four strategies:

| Vocabulary | Strategy | When new work arrives while work is running |
| --- | --- | --- |
| `mergeMap` | `Merge(n)` | runs it alongside, up to `n` at once; the rest wait |
| `concatMap` | `Concat` | queues it until the running work completes |
| `switchMap` | `Switch` | cancels the running work, starts the new |
| `exhaustMap` | `Exhaust` | ignores the new work until the running work completes |

The same four strategies run through the time family. Applied to `timer(ms)`:

| Time behaviour | Strategy over `timer(ms)` |
| --- | --- |
| debounce: each value cancels the previous wait | `Switch` |
| throttle: values are ignored while a wait runs | `Exhaust` |
| delay: every value waits in parallel | `Merge` |
| spacing values out by `ms` | `Concat` |

### Combination (5)

| Kernel | Behaviour | Vocabulary built on it |
| --- | --- | --- |
| `combineLatest` | combines the latest value of each input | `combineLatestWith`, `combineLatestAll` |
| `zip` | pairs values by position | `zipWith`, `zipAll` |
| `withLatestFrom` | adds the other inputs' latest values to each source value | |
| `takeUntil` | completes when a notifier fires | |
| `race` | follows whichever input emits first | `raceWith` |

### Errors and effects (3)

| Kernel | Behaviour | Vocabulary built on it |
| --- | --- | --- |
| `catchError` | replaces an error with another Observable | `retry`, `retryWith`, `retryConsecutiveWith`, `catchTimeout` |
| `finalize` | runs a function on teardown | |
| `tap` | runs a side effect per notification | |

### Multicasting (3)

| Kernel | Behaviour | Vocabulary built on it |
| --- | --- | --- |
| the Subjects | `createSubject`, `createBehaviorSubject`, `createReplaySubject`, `createAsyncSubject` | |
| `connectable` | connects on `connect()`: the theater view | `connectableOnce`, `publish…`, `refCount` |
| `shareWith(connector, lifecycle)` | connects automatically | `share`, `shareReplay`, `shareLatest` |

### Candidates for trimming towards 30

- `EMPTY`, `NEVER` and `throwError` could count as one family of trivial sequences.
- `observeOn` and `subscribeOn` are scheduling tools rather than stream behaviours.
- `withLatestFrom`, `takeUntil` and `race` may turn out to be compositions.
- `using` is rarely needed.

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

## The identity test

An operator moves from the kernel to the vocabulary only when its composition passes an identity
test:

1. Write the vocabulary definition as a composition of kernel operators.
2. Run it against RxJS 7 in `test/differential.test.ts` on random marble diagrams: the same
   notifications at the same virtual times. For multicasting, use a scripted scenario with several
   subscribers instead.
3. If the outputs are identical, the operator is vocabulary. If not, it stays in the kernel, or the
   difference is documented as a deliberate one.

Results so far:

| Vocabulary | Composition | Result |
| --- | --- | --- |
| `debounceTime(ms)` | `debounce(() => timer(ms))` | identical |
| `debounceTime(ms)` | `switchMap(emitAfter(ms))` | differs: does not flush the pending value on completion, so `debounce` is the better base |
| `throttleTime(ms)` | `throttle(() => timer(ms))` | identical |
| `auditTime(ms)` | `audit(() => timer(ms))` | identical |
| `sampleTime(ms)` | `sample(interval(ms))` | RxJS defines `sampleTime` this way; rxjs-fp's own `sampleTime` deviates from RxJS when a tick and a value fall in the same frame |
| `delay(ms)` | `mergeMap(emitAfter(ms))` | identical on the tested diagrams |
| `skip(n)` | `skipWhile((_, i) => i < n)` | identical |
| `distinct()` | `distinct((x) => x)` | identical |
| `take(n)` | `takeWhile((_, i) => i < n)` | differs: completes later, so both stay in the kernel |

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
  const fetchUserReliably = (id: number) =>
    fetchUser(id).pipe(retryWith(timesWithDelay(2, 1000)), attempt());   // errors become Either values
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
