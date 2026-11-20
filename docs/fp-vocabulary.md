# The FP vocabulary of rxjs-fp

rxjs-fp is inspired by [fp-ts](https://gcanti.github.io/fp-ts/), but an RxJS user does not need to
learn fp-ts to use it. Ordinary pipelines are written with the familiar operator names: `map`,
`concatMap`, `merge`, `catchError`. The FP vocabulary lives in one place, `rxjs-fp/fp`, and this guide
explains every name in it, in RxJS terms.

```ts
import { attempt, isRight, Do, bind, let_, fromNullable, matchEither } from 'rxjs-fp/fp';
```

Every example below runs as written: they are checked by `test/fp-vocabulary.test.ts`.

## In one table

| Name | What it means for an RxJS user |
| --- | --- |
| `Option` (`some`, `none`) | a value that may be missing, instead of `null` or `undefined` |
| `Either` (`right`, `left`) | a result: success (`right`) or failure (`left`), instead of a thrown error |
| `attempt()` | try + catch as an operator: values become `right`, an error becomes a final `left` |
| `rethrow()` | the reverse: a `left` becomes an error again |
| `Do`, `bindTo`, `bind`, `let_` | build up a record step by step, instead of nesting `mergeMap` calls |
| `chain`, `flatten`, `compact`, `separate` | short names for `mergeMap`, `mergeAll`, "drop the `none`s", "split the `Either`s" |
| `fromOption`, `fromEither` | turn an `Option` or an `Either` into an Observable |
| `ObservableFunctor`, `ObservableFilterable`, the Monoids | lawful instances, for generic code and for the law tests |

The three that matter most in everyday code are `Option`, `Either` and `attempt()`.

## Option: a value that may be missing

`Option<A>` is either `some(value)` or `none`. It is a TypeScript discriminated union
(`{ _tag: 'Some', value }` or `{ _tag: 'None' }`), so the compiler sees that a value may be missing,
which it cannot see for `null` or `undefined` hidden in a type.

```ts
const email = fromNullable(user.email);                       // some('ada@example.com') or none
matchOption(() => 'no email', (e: string) => `mail to ${e}`)(email);
getOrElse(() => 'unknown')(email);                            // the value, or a fallback
```

The most common use with streams is `filterMap`: map each value to an `Option` and keep only the
`some`s, in one step.

```ts
users$.pipe(filterMap((u: User) => fromNullable(u.email)))    // the emails of the users that have one
```

| Function | Meaning |
| --- | --- |
| `some(a)`, `none` | the two cases |
| `isSome(o)`, `isNone(o)` | which case it is |
| `fromNullable(a)` | `null` or `undefined` become `none` |
| `fromPredicate(p)` | `some(a)` if `p(a)` holds, else `none`: `fromPredicate((age: number) => age >= 18)` |
| `matchOption(onNone, onSome)` | one function per case, like a `switch` |
| `mapOption(f)`, `chainOption(f)` | change the value if there is one |
| `getOrElse(fallback)`, `toNullable`, `toUndefined` | leave `Option` again |

## Either: success or failure, as a value

`Either<E, A>` is either `right(value)`, the success, or `left(error)`, the failure. Many TypeScript
developers write the same thing by hand:

| Hand-written result | `Either` |
| --- | --- |
| `{ ok: true, value }` | `right(value)` |
| `{ ok: false, error }` | `left(error)` |
| `if (r.ok)` | `if (isRight(r))` |

"Right" is the right result; "left" is the other case, which holds the error.

```ts
const parse = (s: string) => tryCatch(() => JSON.parse(s) as unknown, () => 'not JSON');

const describe = matchEither(
  (e: string) => `failed: ${e}`,
  (v: unknown) => `parsed ${JSON.stringify(v)}`,
);

describe(parse('{"a":1}'));   // 'parsed {"a":1}'
describe(parse('{'));         // 'failed: not JSON'
```

`matchEither` takes one function per case, so you never need to touch `.left` or `.right` directly.

| Function | Meaning |
| --- | --- |
| `right(a)`, `left(e)` | the two cases |
| `isRight(x)`, `isLeft(x)` | which case it is |
| `tryCatch(f, onError)` | run `f`; a thrown error becomes a `left` |
| `matchEither(onLeft, onRight)` | one function per case |
| `mapEither(f)` | change a success; a failure passes unchanged |
| `mapLeft(f)` | change a failure; a success passes unchanged |
| `chainEither(f)` | continue with a step that may itself fail |

## attempt and rethrow: errors as values in a stream

In RxJS an error ends a stream. `attempt()` turns it into a value instead: try the source, and catch
its error as data.

```
source:     --1--2--#            (# = error 'boom')
attempt():  --R--R--(L|)         right(1), right(2), then left('boom') and complete
```

It is the familiar RxJS pattern as one operator:

```ts
// by hand
map((value) => ({ ok: true, value })), catchError((error) => of({ ok: false, error }))

// with rxjs-fp
attempt()
```

**Where it is used: inside a projection.** A failing request then ends only its own small stream,
and the outer stream goes on:

```ts
of(1, 4, 5).pipe(
  mergeMap((id) => getUser(id).pipe(attempt())),
  map((r) => (isRight(r) ? `loaded ${r.right.name}` : `failed: ${String(r.left)}`)),
)
// 'loaded Ada', 'failed: user 4 not found', 'loaded Ada', then complete
```

**All settled.** `forkJoin` errors as soon as one request fails, and the other results are lost. With
`attempt()` on each request you get every outcome, like `Promise.allSettled`:

```ts
forkJoin([getUser(1).pipe(attempt()), getUser(4).pipe(attempt())])
// [right(ada), left('user 4 not found')]
```

`rethrow()` is the reverse: it unwraps `right`s and turns the first `left` back into an error, for a
later stage that wants normal RxJS error behaviour.

```ts
const results: Observable<Either<string, number>> = of(right(1), left('boom'), right(3));
results.pipe(rethrow())   // 1, then error 'boom'
```

To handle failures and successes on separate paths, `separate` splits the results into two streams:

```ts
const { left: failures, right: successes } = separate(results);
```

Edge cases:

- **Retry first, then `attempt()`.** In `retryWith(policy), attempt()` the retries run first, and only
  the final failure becomes a value. The other way round there is nothing left to retry.
- **At most one `left` per stream.** An Observable errors at most once, so the `left` is always last.
- **The error type is `unknown`.** RxJS errors carry no type, so `attempt()` gives
  `Either<unknown, A>`. Narrow the `left` where you need a typed error, for example with
  `isTimeoutError`.
- **When to use `catchError` instead.** If a failure should simply disappear, `catchError(() => EMPTY)`
  is simpler; if there is a sensible default, `catchError(() => of(fallback))` keeps the plain type.
  Use `attempt()` when the consumer needs to know *that* something failed.

## Do-notation: build a record step by step

Some pipelines need several results that depend on each other: the user, then that user's orders,
then a count. In plain RxJS each step nests one level deeper, and earlier results have to be carried
along by hand:

```ts
getUser(1).pipe(
  mergeMap((user) => getOrders(user.id).pipe(map((orders) => ({ user, orders })))),
  map(({ user, orders }) => ({ user, orders, count: orders.length })),
)
```

The do-notation keeps it flat. Each step adds one named field to a record, and every later step can
read the fields before it:

```ts
Do.pipe(
  bind('user', () => getUser(1)),
  bind('orders', ({ user }) => getOrders(user.id)),
  let_('count', ({ orders }) => orders.length),
)
// { user, orders, count }
```

| Name | Meaning |
| --- | --- |
| `Do` | the start: a stream of one empty record, `of({})` |
| `bindTo(name)` | start from an existing stream instead: each value becomes `{ [name]: value }` |
| `bind(name, f)` | an effectful step: `f` returns an Observable, its value is added under `name` (via `mergeMap`) |
| `let_(name, f)` | a pure step: `f` returns a plain value, added under `name` (via `map`) |

The field names are checked by the compiler: `bind('user', …)` twice is a type error, and a later
step sees exactly the fields that exist.

```ts
of(1).pipe(bindTo('id'), bind('user', ({ id }) => getUser(id)))   // { id: 1, user }
```

Edge cases:

- **The steps run in sequence.** `bind` is a dependent step: it waits for the record so far. When two
  steps do not depend on each other, combine them with `forkJoin` or `combineLatest` instead, so they
  run at the same time.
- **`bind` runs once per value.** It uses `mergeMap`, so a step that emits two values produces two
  records, and the next step runs for each. With one-value requests this never shows; with streams
  it multiplies:

  ```ts
  Do.pipe(bind('x', () => of(1, 2)), bind('y', () => of('a', 'b')))
  // { x: 1, y: 'a' }, { x: 1, y: 'b' }, { x: 2, y: 'a' }, { x: 2, y: 'b' }
  ```
- **The trailing underscore in `let_`** is there because `let` is a reserved word in JavaScript.

## The remaining helpers

Short names for operations that also exist under their RxJS names:

| Name | RxJS name | Example |
| --- | --- | --- |
| `chain(f)` | `mergeMap(f)`, without the index argument | `of(1, 2).pipe(chain((n) => of(n, n * 10)))` gives 1, 10, 2, 20 |
| `flatten` | `mergeAll()` | `of(of(1), of(2, 3)).pipe(flatten)` gives 1, 2, 3 |
| `compact` | `filterMap((o) => o)`: drop the `none`s | `of(some(1), none, some(3)).pipe(compact)` gives 1, 3 |
| `separate` | two `filterMap`s: failures and successes | see `attempt` above |
| `fromOption(o)` | `some(a)` becomes `of(a)`, `none` becomes `EMPTY` | |
| `fromEither(e)` | `right(a)` becomes `of(a)`, `left(e)` becomes an error | |

`chain` and `flatten` are the building blocks of the do-notation; `compact` and `separate` follow
fp-ts's names for the Compactable typeclass.

## The instances and the laws

For generic code, and for the law tests, `rxjs-fp/fp` has a few lawful instances. They are plain
records of functions, in fp-ts style, not classes:

| Instance | Contents |
| --- | --- |
| `ObservableFunctor` | `map` |
| `ObservableCompactable`, `ObservableFilterable` | `compact`, `separate`, `filter`, `filterMap`, `partition`, `partitionMap` |
| `getMergeMonoid()`, `getConcatMonoid()` | combine two streams with `merge` or `concat`; `EMPTY` is the identity |

A Monoid folds a list of streams into one:

```ts
const M = getMergeMonoid<number>();
[of(1), of(2), EMPTY].reduce(M.concat, M.empty)   // merge(of(1), of(2)): 1, 2
```

`test/laws.test.ts` checks the laws on random timed streams. The Monad laws are stated directly on the
operators users write: `of` with `mergeMap` (they hold in time) and `of` with `concatMap` (they hold
up to timing, on error-free streams).

The `URI`, `Kind` and `URItoKind` types (`src/fp/hkt.ts`) are fp-ts's encoding of higher-kinded types,
which lets a typeclass speak about "any container". You need them only to write such generic code
yourself.

## Left out on purpose

These fp-ts concepts were in rxjs-fp until November 2026 and were removed because they are too far
from what an RxJS user writes. Their laws are still tested, on the operators in the right column.

| Removed | Write instead |
| --- | --- |
| `MonadMerge`, `MonadConcat` (`chain`, `of`, `ap`) | `mergeMap` or `concatMap`, and `of` |
| `ap`, `apLatest` | `combineLatest`, `zip` or `forkJoin` |
| `alt`, `ObservablePlus`, `ObservableAlternative` | `merge`, with `EMPTY` |
| `orElse` | `catchError` |

The one idea from `ap` worth keeping is the difference between dependent and independent steps:
`mergeMap`/`concatMap` (and `bind`) when a step needs the previous result, `combineLatest`/`zip`/
`forkJoin` when the inputs do not need each other.

## From fp-ts to rxjs-fp

For readers who know fp-ts:

| fp-ts | rxjs-fp |
| --- | --- |
| `pipe`, `flow` | `pipe`, `flow`, and `source.pipe(...)` |
| `Option`, `O.some`, `O.none`, `O.fromNullable`, `O.match` | `Option`, `some`, `none`, `fromNullable`, `matchOption` |
| `Either`, `E.right`, `E.left`, `E.tryCatch`, `E.match` | `Either`, `right`, `left`, `tryCatch`, `matchEither` |
| `TaskEither` (typed error channel) | not available: `Observable` errors are `unknown`; `attempt()` bridges to `Either` |
| `Do`, `bind`, `bindTo`, `let` | `Do`, `bind`, `bindTo`, `let_` |
| `Functor`, `Filterable`, `Compactable`, `Monoid` | `ObservableFunctor`, `ObservableFilterable`, `ObservableCompactable`, `getMergeMonoid`, `getConcatMonoid` |
| `Monad`, `Applicative`, `Alternative` | not exported; see "Left out on purpose" |
| `Traversable`, `Foldable`, `Reader`, `IO` | not part of rxjs-fp |
