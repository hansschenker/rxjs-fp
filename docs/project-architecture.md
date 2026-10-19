# Project architecture: reactive extensions without a single `class`

`docs/architecture.md` is the map — which directory holds what, and the execution model in brief.
This document is the **argument**: what object-oriented machinery actually buys you in a library like
RxJS, what replaces each piece of it here, and why the result is not merely class-free on a
technicality but genuinely simpler to reason about and to test.

The short version: a class bundles five separate affordances into one keyword. Taken apart, four of
them are already available in plain functions, and the fifth — encapsulated mutable state — is
exactly the thing a reactive library should be forced to declare explicitly, one subscription at a
time.

> **On the code in this document.** Snippets are quoted from the file named beside them, but about a
> third are abridged for reading: bodies elided as `/* … */`, short multi-line expressions folded
> onto one line, and occasionally a clarifying comment added that is not in the source. Treat them
> as faithful illustrations, not as a diffable copy — the `file:line` references are the
> authoritative pointer, and they were checked against the tree at the commit that added them.

---

## 1. What a `class` gives you, and what replaces it

| A class gives you | Why RxJS needs it | What rxjs-fp uses instead |
|---|---|---|
| **Identity** — a thing you can `instanceof` | `isObservable`, `instanceof Subscriber`, error discrimination | A symbol brand (`OBSERVABLE_BRAND`) and duck-typed guards; errors are tagged by `name` |
| **Encapsulated mutable state** — private fields | A subscription's `closed` flag, an operator's accumulator | One `let` captured by a closure, created *inside* the producer, so it exists once per subscription and is unreachable from outside |
| **Method sharing** — one `prototype`, many instances | `Observable.prototype.pipe`, `Subscriber.prototype.next` | Module-level functions that take their subject as an argument (`pipe(src, f)`), plus a small frozen record of closures per value |
| **Subtype polymorphism** — `MergeMapSubscriber extends Subscriber` | Per-operator behaviour | A tagged union of *data* plus one interpreter function (`FlattenStrategy` + `flatMapWith`) |
| **Open recursion** — `this.method()` resolved late | `lift`, template-method operators, `BehaviorSubject` overriding `_subscribe` | Explicit parameter passing: the behaviour is a function you were handed (`step`, `project`, `scheduler`, `hooks.onSubscribe`) |

Only the second row is irreducible. Streams are stateful; `scan` must remember its accumulator
somewhere. The design question is never "can we eliminate state" but **"how small and how visible
can we make the place it lives?"** This library's answer: one mutable cell per subscription, created
by the producer, named at the top of the function that uses it.

---

## 2. The one definition everything follows from

Paul Taylor's definition, from *RxJS Evolved* (Reactive 2015):

> An Observable is a function that, when invoked, returns 0 to ∞ values between now and the end of time.

Read that literally and the architecture writes itself. If an Observable *is* a function, then
subscribing is *calling* it, and `src/core/observable.ts` is barely more than that:

```ts
export const observable = <A>(producer: Producer<A>): Observable<A> => {
  const subscribe = (o?: Partial<Observer<A>> | ((value: A) => void)): Subscription => { /* … */ };

  let self: Observable<A>;
  const pipe = (...operators: ReadonlyArray<Operator<any, any>>) => pipeArray(operators)(self);
  self = Object.freeze({
    [OBSERVABLE_BRAND]: true as const,
    subscribe,
    pipe: pipe as Observable<A>['pipe'],
    [OBSERVABLE_KEY]: () => self,
  });
  return self;
};
```

Four properties, three of them closures, and the record is frozen (`src/core/observable.ts:168`).
Note the `let self` declared before the object and assigned to it: that is how a method-free record
refers to itself without `this`. The `pipe` closure captures `self` lexically, so `obs.pipe(f)` and
`pipe(obs, f)` are the same computation — and detaching the method (`const p = obs.pipe`) cannot
break it, which is not true of a prototype method that depends on its receiver.

Three consequences fall out for free, each of which RxJS has to arrange deliberately:

1. **Cold by default.** `subscribe` calls `producer(sink)`. Two subscriptions call it twice, with two
   separate sinks and two separate sets of captured `let`s. Sharing is therefore impossible by
   accident; you must ask for it with a Subject or `share`.
2. **No inheritance surface.** There is no `Observable` base class for an operator to extend, so
   there is nothing to override, and no `lift` needed to preserve a subclass through an operator
   chain. An operator is a function `Observable<A> → Observable<B>`; it builds a *new* Observable
   around the old one.
3. **Nothing to construct.** `of(1, 2)` is a function call, not `new Of(...)`. Tree-shaking works on
   function exports in a way it cannot work on a class with many prototype methods.

---

## 3. The execution machinery

Three small pieces carry the whole lifecycle. None of them is a class, and each replaces something
substantial in RxJS.

### 3.1 `Subscription`: three closures over one private cell

```ts
export const createSubscription = (initial?: Teardown): Subscription => {
  let closed = false;
  let finalizers: Set<Teardown> | null = initial ? new Set([initial]) : null;
  /* unsubscribe, add … */
  return Object.freeze({ get closed() { return closed; }, unsubscribe, add });
};
```

This is RxJS 5's "flattened disposable tree" without the tree or the class. Points worth noticing:

- **`closed` is exposed as a getter**, not a copied boolean, so the frozen record still reports live
  state. Freezing the record does not freeze the closure variables behind it — which is precisely
  the encapsulation a class gives you with `#private`, achieved with scope instead.
- **`add` returns its own un-adder** (`src/core/subscription.ts:54-62`). That one return value is
  what keeps a long-lived parent from accumulating dead children: an operator that finishes early
  calls the returned function and removes itself. In RxJS this is `Subscription.prototype.remove`,
  which needs the child's identity to find it; here the closure *is* the handle.
- **Finalizers live in a flat `Set`**, so removal is O(1), and `unsubscribe` is idempotent by the
  `if (closed) return` guard.
- **Errors during teardown are aggregated**, not swallowed: failures from individual finalizers are
  collected and rethrown as a single `UnsubscriptionError`, flattening nested ones
  (`src/core/subscription.ts:36-51`).

### 3.2 The sink: the Observable grammar, enforced in one place

`createSink` (`src/core/observable.ts:90`) wraps a consumer's observer and is the only component
that knows the protocol `next* (error | complete)?`:

- nothing is delivered after a terminal notification or after `unsubscribe` (`stopped || subscription.closed`);
- a terminal notification tears the execution down in a `finally`, so teardown runs even if the
  consumer's handler throws;
- `closed` is again a getter over `stopped || subscription.closed`.

The subtle part is the `internal` flag. For an operator-to-operator sink, exceptions from handlers
are allowed to propagate so the *operator* can route them to `error`. For a consumer-facing sink,
they are caught and handed to `reportUnhandledError`, which rethrows on a fresh task:

```ts
export const reportUnhandledError = (err: unknown): void => {
  setTimeout(() => { throw err; });
};
```

That is design rule 6 — *errors never escape the pipeline* — reduced to five lines. A consumer
callback that throws becomes an uncaught error at the top level, where your monitoring sees it; it
never re-enters the pipeline and never corrupts an in-flight emission.

### 3.3 `subscribeChild` and the `PARENT` symbol

This is the cleverest piece of the core, and it exists to solve a problem that classes solve by
having the subscriber object available before the producer runs.

```ts
const PARENT: unique symbol = Symbol('rxjs-fp/parent');
```

An operator never calls `source.subscribe(...)` directly. It calls
`subscribeChild(source, sink, handlers)`, which builds an observer carrying `[PARENT]: sink`. Inside
`subscribe`, that marker is detected and the new subscription is linked into the downstream scope
**before `producer(sink)` is called** (`src/core/observable.ts:143-148`), with the link removed again
when the child finishes on its own.

Why it matters: consider `unfold` producing values in a synchronous `for(;;)` loop, with `take(1)`
downstream. The ordering is

1. `take(1)`'s subscription is registered with the downstream sink;
2. the upstream producer starts looping;
3. the first value reaches `take`, which emits and calls `sink.complete()`;
4. completion tears down the downstream subscription, which runs the finalizers, which unsubscribes
   the upstream subscription that was registered in step 1;
5. the upstream sink's `closed` getter now returns `true`, so the producer's loop guard
   (`if (sink.closed) return`) fires and the loop exits.

Without step 1 happening before step 2, the infinite loop would never learn that anyone had stopped
listening. RxJS gets this by constructing the `Subscriber` first and passing it down; rxjs-fp gets it
by passing a scope through a symbol-keyed property. `subscribeChild` also wraps every handler so a
throwing user function (a `project`, a `predicate`) is routed to `sink.error`, and its overloads
forbid forwarding upstream values to a sink of a different type without an explicit `next`.

Two sibling functions complete the set, and between them they are the only ways anything in the
library attaches to an upstream:

- **`subscribeWithin(source, scope, observer)`** links against an arbitrary scope rather than a
  downstream sink, for cases where the upstream must outlive the subscriber that started it.
  `share` (§10) and `groupBy` (§7.4) need it.
- **`subscribeInner(input, sink, handlers)`** (`src/operators/inner.ts`) is for *duration* inners —
  the notifier of a `buffer`, the duration selector of a `debounce`, the closing Observable of a
  `windowWhen`. It hands each handler a frozen `Inner` handle with `closed` and `close()`, and the
  reason that handle exists is worth dwelling on: a synchronous duration can end *itself*
  re-entrantly, before `subscribeChild` has returned, so there is no `Subscription` variable to
  unsubscribe yet. The handle closes the logical inner even when the physical subscription is still
  being created, and the trailing `if (closed) sub.unsubscribe()` reconciles the two.

That is the same hazard the `generation` counter solves for `Switch` in §6 — "the thing I would
cancel has not been assigned yet" — met with the same shape of answer. Seeing it twice is what
suggests it is a property of the closure style rather than a one-off: a class instance exists before
its `subscribe` call returns and can carry a flag on itself, whereas a closure's binding does not
exist until the expression completes, so the flag has to be created *before* the subscription and
handed in.

---

## 4. Composition: `pipe` and `flow`

```ts
export const pipeArray =
  (fns: ReadonlyArray<(x: any) => any>) =>
  (input: unknown): any => {
    let acc = input;
    for (const f of fns) acc = f(acc);
    return acc;
  };
```

That loop is the entire implementation behind `pipe(value, ...fns)`, `flow(...fns)` and
`Observable.pipe`. Everything else in `src/core/pipe.ts` is overload signatures: ten for `pipe`,
eight for `flow`, and in `src/core/types.ts` a `PipeMethod<A>` interface with eleven call signatures
whose last one accepts `...rest` so chains longer than nine operators still compile, with the tail degrading to
`Observable<unknown>` — the same ceiling RxJS has, for the same reason.

The deeper point: RxJS's `pipe` is `Observable.prototype.pipe`, which calls `pipeFromArray` and
therefore needs `this`. Because an rxjs-fp operator is just `Observable<A> → Observable<B>`,
composition is ordinary function composition, and `flow(map(f), filter(p))` builds a reusable
operator with no Observable in sight.

---

## 5. Stateful operators as pure Mealy machines

This is the mechanism that makes the no-class rule practical rather than merely survivable, and it
is where the design pays for itself.

A Mealy machine is a transition function plus an output function over an explicit state. Phrase an
operator that way and the state stops being hidden in a subscriber instance:

```ts
export interface Machine<S, A, B> {
  readonly init: () => S;
  readonly step: (state: S, value: A, index: number) => Step<S, B>;
  readonly flush?: (state: S) => Flush<B>;
}
```

`step` returns `{ state, emit, done }` — built with the helpers `cont(state, emit)` and
`halt(state, emit)` — and the optional `flush` turns end-of-stream into a final burst or an error
(`flushWith` / `flushError`). Then exactly one function in the library runs every such machine:

```ts
export const fromMachine =
  <S, A, B>(machine: Machine<S, A, B>): Operator<A, B> =>
  (source) =>
    observable<B>((sink) => {
      let state = machine.init(); // the only mutable cell of this execution
      let index = 0;
      return subscribeChild(source, sink, {
        next: (value) => {
          const r = machine.step(state, value, index++);
          state = r.state;
          for (const out of r.emit) {
            if (sink.closed) return;
            sink.next(out);
          }
          if (r.done) sink.complete();
        },
        complete: () => { /* flush, then complete */ },
      });
    });
```

Three things to take from this:

**The imperative shell is written once.** RxJS 7 has one `OperatorSubscriber` class plus a closure
per operator; rxjs-fp has one `fromMachine` plus a pure `step` per operator. The `let state` is
inside the producer, so it is created per subscription — cold semantics are preserved *by
construction*, not by discipline. Re-reading `fromMachine` once tells you that no machine-based
operator can leak state between subscriptions.

**The emit loop re-checks `sink.closed` on every value.** A machine that emits five values into a
`take(1)` downstream stops after the first. Backpressure-free, but not oblivious.

**`done` is data, so the shell has to latch it.** This is the one place the design needed a second
look, and it makes the strongest case for having a single shell. `step` returns `done: true` meaning
"emit these, then complete", but `sink.complete()` can only run *after* the pending emissions. That
leaves a window in which the machine is logically finished yet still subscribed — and synchronous
re-entrancy can put a value in it. Before the latch existed, this emitted **two** values:

```ts
const s = createSubject<number>();
s.pipe(take(1)).subscribe({ next: (v) => { if (v === 1) s.next(99); } });
s.next(1);   // consumer received 1 AND 99
```

`take`'s `step` says "emit this value and be done" rather than "I am already done, drop it", so the
re-entrant value got its own `step` call, its own `halt`, and its own emission. RxJS avoids the same
hazard in `take` by incrementing and testing its counter *before* forwarding, with a comment naming
it outright: *"we have to do `<=` here, because re-entrant code will increment `seen` twice"*
(`node_modules/rxjs/src/internal/operators/take.ts`).

The fix is a `let halted`, set from `r.done` *before* the emit loop and checked at the top of `next`
— and the point is **where** it goes. Not in `take`; in `fromMachine`. There are eight `halt` call
sites in the library, and seven of them emit a value as they halt: `take`, `first`, `elementAt`,
`every`, `find`, `findIndex`, `isEmpty` (plus `takeWhile`, which emits only when `inclusive`). Every
one of them had the same bug, in the same way, for the same reason — and one latch in the shell
corrected all of them at once — including the four in `src/operators/conditional.ts`, which were
written independently and were repaired without being touched.

The `complete` handler checks the latch too, so a machine that has halted never also runs its
`flush` — which is exactly what `runMachine` already did by returning on `done`, so the two
interpreters now agree where they previously disagreed in silence.

That is the shell earning its keep, and it is the strongest practical argument in this document for
the whole approach. A class-per-operator design would have required the same fix in seven places,
with no structural guarantee that the eighth author would know to apply it. Here a whole class of
re-entrancy bug has exactly one place to be fixed and exactly one place to be audited. (Regression
tests: `test/review-regressions.test.ts`.)

**The logic is testable with no Observable at all.** `runMachine` runs the same machine over an
iterable and returns `{ output, done, error? }`:

```ts
const takeUntilSumExceeds = (limit: number) =>
  fromMachine<number, number, number>({
    init: () => 0,
    step: (sum, x) => (sum + x > limit ? halt(sum) : cont(sum + x, [x])),
  });
```

`step` is a pure function of `(state, value, index)`, so it can be property-tested with fast-check
directly — no scheduler, no virtual time, no subscription. In a class-based design the equivalent
logic lives in a method that needs `this`, a destination subscriber, and a live subscription before
it will run at all. **The cost of not having classes turned out to be a testability win**, which is
the strongest argument in this whole document.

### Where the discipline stops: bounded and unbounded state

A machine's state transition must be pure, so growing a collection means copying it — and that cost
is what decides whether an accumulating operator can be a machine at all. Compare two operators that
do almost the same thing:

```ts
// takeLast(n) — a machine. State is capped at n, so the copy is O(n) per value.
step: (buf, a) => cont(buf.length >= n ? [...buf.slice(1), a] : [...buf, a]),

// toArray — not a machine. State grows without bound.
const acc: A[] = [];                  // essential per-subscription state, local to the shell
next: (a) => { acc.push(a); },
```

Written as a machine, `toArray` would copy an array of length *i* at step *i* — **O(n²)** over a
stream of n values, for an operator whose whole job is to be linear. `distinct` is the same story
with a `Set` that never shrinks. So the line the library draws is not "pure versus impure" but
**bounded versus unbounded**:

| State per subscription | How it is written | Examples |
|---|---|---|
| O(1) | `Machine` | `scan`, `pairwise`, `distinctUntilChanged`, `take`, `first`, `every`, `findIndex` |
| Bounded by a parameter | `Machine`, copying each step | `takeLast(n)`, `bufferCount(size, every)`, `skipLast(n)` |
| Unbounded | producer with a mutable cell | `toArray`, `distinct` |

Both escape hatches are labelled as "essential state" in their own source comments, which is the
right way to take one: the discipline is stepped outside of deliberately and visibly, not quietly
bent, and §13 inventories the cells. The other reasons a machine may be the wrong tool — a second
event source, an incoming error to transform, an Observable to emit, or any effect at all — are
structural rather than about cost, and §17 sorts them.

---

## 6. Flattening: strategies as data, interpreted once

`mergeMap`, `concatMap`, `switchMap` and `exhaustMap` differ in exactly one policy: what to do with a
new outer value while inner Observables are running. In RxJS those are separate operator
implementations over shared `mergeInternals` plumbing. Here the policy is a value:

```ts
export type FlattenStrategy =
  | { readonly _tag: 'Merge'; readonly concurrent: number }
  | { readonly _tag: 'Switch' }
  | { readonly _tag: 'Exhaust' };

export const Merge = (concurrent: number = Number.POSITIVE_INFINITY): FlattenStrategy =>
  Object.freeze({ _tag: 'Merge', concurrent: Math.max(1, concurrent) });
export const Concat: FlattenStrategy = Merge(1);
```

`Concat` is literally `Merge(1)` — the ordering guarantee is a consequence of a concurrency limit of
one, and saying so in code is clearer than any comment. One interpreter, `flatMapWith`, reads the tag
and handles an outer value:

| Strategy | On an outer value |
|---|---|
| `Merge(n)` | run it if `active < n`, otherwise push to `queue` |
| `Switch` | `current?.unsubscribe()`, reset `active`, run the new one |
| `Exhaust` | run it only if `active === 0` |

and completion is one condition in one place:

```ts
const tryComplete = (): void => {
  if (outerDone && active === 0 && queue.length === 0) sink.complete();
};
```

Everything else is derived: `mergeMap`, `concatMap`, `switchMap`, `exhaustMap` are one-line
applications of `flatMapWith`, and `mergeAll`/`concatAll`/`switchAll`/`exhaustAll` are those applied
to the identity projection. Eight operators from one interpreter and three data constructors — and
as §7.1 shows, `merge` and `concat` come out of the same interpreter too.

The one genuinely hard case is `Switch` under re-entrancy, and it is worth reading closely
(`src/operators/flatten.ts:44-70`). A `generation` counter is bumped per inner, and each inner's
handlers consult `isCurrent()` before speaking:

```ts
const gen = ++generation;
const isCurrent = (): boolean => strategy._tag !== 'Switch' || gen === generation;
```

If a synchronous inner emits a value that re-entrantly produces a new outer value, the old inner is
superseded *while it is still being subscribed*; the `if (isCurrent()) current = sub; else sub.unsubscribe()`
at the end of `runInner` catches exactly that window. This is the kind of bug that a class with a
mutable `innerSubscription` field hides and a generation counter makes explicit.

`expand` is the one flattening operator not expressed through `flatMapWith`, because it feeds every
emitted value back into the projection (`emitAndExpand`) and so needs its own recursion.

**The strategy value outlives its first interpreter**, which is the real test of "policy as data".
`mergeScan` and `switchScan` are higher-order `scan`s — the accumulator returns an Observable — and
they are not new operators so much as a second interpreter, `scanWith`, over the *same*
`FlattenStrategy` union:

```ts
const scanWith =
  (strategy: FlattenStrategy) =>
  <A, S>(accumulator: (acc: S, value: A, index: number) => ObservableInput<S>, seed: S): Operator<A, S> =>
  (source) =>
    defer(() => {
      let state = seed;
      return flatMapWith(strategy)((a: A, i: number) => accumulator(state, a, i))(source).pipe(
        tap((s: S) => { state = s; }),
      );
    });
```

`mergeScan` is `scanWith(Merge(n))`, `switchScan` is `scanWith(Switch)`. Note the `defer`: the
accumulator's `let state` has to be created per subscription, and `defer` is what moves it inside
the producer — the §2 cold-by-default rule applied deliberately rather than inherited. Had the
concurrency policy been encoded as four separate operator classes, this reuse would have meant
either duplicating the policy or inventing a shared base class for it.

---

## 7. Operators that own resources

Machines cover operators whose state is a *value*. The rest own something — subscriptions, timers, or
the right to re-subscribe — and are written as explicit producers. They are the honest answer to
"where did the subscriber subclasses go": they became local functions inside a closure.

### 7.1 Combination: tuples, and `merge` derived from flattening

`merge` and `concat` are not separate implementations at all:

```ts
export const merge = <const T extends readonly Observable<unknown>[]>(...sources: T): Observable<ValueOf<T[number]>> =>
  mergeAll<ValueOf<T[number]>>()(from(sources as unknown as Observable<ValueOf<T[number]>>[]));
```

An array of Observables is an `Iterable`, so `from(sources)` is an Observable *of* Observables, and
flattening it with `mergeAll` (or `concatAll`) is exactly `merge` (or `concat`). The flatten
interpreter of §6 therefore powers the combination operators too, and `startWith`/`endWith` reduce
further to `concat(of(...values), source)`.

The genuinely interesting part of this file is the typing. `combineLatest`, `zip` and `forkJoin` must
map a tuple of Observables to an Observable of the tuple of their value types, which is two type
aliases:

```ts
export type ValueOf<O> = O extends Observable<infer A> ? A : never;
export type ValuesOf<T extends readonly Observable<unknown>[]> = { -readonly [K in keyof T]: ValueOf<T[K]> };
```

plus TypeScript 5's `const` type parameter — `<const T extends readonly Observable<unknown>[]>` — which
preserves the tuple literal type at the call site without making callers write `as const`. No classes
are involved in precise variadic typing; mapped and conditional types do all of it.

The multi-source operators share one helper, `runAll`, which subscribes to each source in turn while
re-checking `sink.closed`, and returns a teardown that unsubscribes all of them. Their state is a
pair of parallel arrays (`values`, `has`) plus counters (`ready`, `completed`) — per subscription, in
the producer, exactly as §13 inventories.

### 7.2 Time: one scheduler argument, one cancel handle

Every time-based operator takes the scheduler as a defaulted parameter:

```ts
export const debounceTime =
  <A>(ms: number, scheduler: Scheduler = asyncScheduler): Operator<A, A> => /* … */
```

The pattern inside is always the same: a `let cancel` holding the current scheduled action, replaced
when the window restarts, and cancelled from the producer's teardown. `debounceTime` restarts on each
value; `throttleTime` opens a window and refuses values inside it; `auditTime` emits the pending
value at the end of the window, including on completion; `delay` keeps a `Set` of pending timers so
completion can wait for the last one. Because the scheduler is a parameter and not a global, each of
these is deterministic under a virtual clock — see §8 and §15.

Each of these has a **notifier-driven sibling** where the duration is an Observable rather than a
number — `debounce`, `throttle`, `audit`, `sample`, `delayWhen` — and those are where
`subscribeInner` (§3.3) earns its place: the duration is an inner whose own completion ends the
window, and it may do so synchronously, before the subscription that started it has been assigned.
The shape is otherwise identical, with an `Inner` handle in place of a `cancel` closure, which is a
fair summary of the relationship between the two families: *the same operator with a different
clock*. `timestamp` and `timeInterval` are the degenerate cases — they only read `scheduler.now()`
and never schedule anything.

### 7.3 Resubscription: `catchError`, `retry`, `repeat` as named recursion

This is the subtlest category, because re-subscribing is where a class-based design leans hardest on
having an object to re-run `lift` against. The functional answer is a locally named function that
calls itself:

```ts
const subscribe = (): void => {
  current = subscribeChild(source, sink, {
    next: (a) => { if (cfg.resetOnSuccess) attempt = 0; sink.next(a); },
    error: (err) => {
      if (attempt >= max) { sink.error(err); return; }
      attempt++;
      current?.unsubscribe();
      const d = typeof cfg.delay === 'function' ? cfg.delay(err, attempt) : cfg.delay;
      if (d === undefined) subscribe();
      else cancelDelay = scheduler.schedule(subscribe, d);
    },
  });
};
subscribe();
return () => { cancelDelay?.(); current?.unsubscribe(); };
```

`subscribe` is the recursion; `attempt`, `current` and `cancelDelay` are the per-subscription cells;
and the returned teardown cancels whichever of the two is live. `repeat` is the same shape on
`complete` instead of `error`. Note that the downstream `sink` is *reused* across attempts — only the
upstream subscription is replaced — which is why a retry does not re-deliver anything to the consumer
and why the grammar enforcement of §3.2 is not disturbed.

`catchError` needs one extra trick, because RxJS's handler signature receives `caught` — the
resulting Observable, so that returning it restarts the source with the error handling still in
place. A record of closures can refer to itself the same way `observable` does, via a forward-declared
binding:

```ts
const self: Observable<A | B> = observable<A | B>((sink) => {
  let current: Subscription | null = null;
  current = subscribeChild(source, sink, {
    error: (err) => {
      current?.unsubscribe();
      current = subscribeChild(from(handler(err, self)), sink);
    },
  });
  return () => current?.unsubscribe();
});
return self;
```

`onErrorReturn` is then a one-liner over it: `catchError((err) => [fallback(err)])` — the array is an
iterable, so `from` turns it into a single-value Observable (§9).

`finalize` deserves a note because it is the one operator whose whole body is teardown: it subscribes
through `subscribeChild` and returns `() => { sub.unsubscribe(); f(); }`, so `f` runs on error,
completion *and* unsubscription, which is the documented RxJS behaviour.

### 7.4 Operators that emit Observables

`window*` and `groupBy` are a category of their own: their output values *are* Observables, so they
have to manufacture a multicast source per chunk or per key. This is where §10's Subjects stop being
a user-facing feature and become internal machinery.

`src/operators/window.ts` holds both families, and its header states the relationship exactly:
buffering and windowing cut the source the same way and differ only in delivery — **a buffer is an
array emitted when the chunk closes; a window is an Observable emitted when the chunk opens**, which
then streams the chunk's values live. So `buffer`/`bufferTime`/`bufferToggle`/`bufferWhen` and
`window`/`windowCount`/`windowTime`/`windowToggle`/`windowWhen` are nine operators in one file —
four buffers and five windows — with `subscribeInner` (§3.3) carrying every notifier and
closing-selector.

The asymmetry is the instructive part. `bufferCount` is absent from that file: it lives in
`src/operators/transform.ts` as a Mealy machine (§5), because a count-based buffer needs no notifier
and its state really is just a value. `windowCount` cannot make the same move, because emitting a
window means manufacturing a Subject, and a machine may only return values. The buffer/window
symmetry breaks exactly where the §17 decision procedure says it must.

One detail matters more than it looks: each window is a Subject handed out through
`asObservable()`. That is what makes the emitted window read-only — a consumer holding it cannot
call `next` on the chunk it is observing. The `asObservable` wrapper described in §10 as a
nice-to-have is load-bearing here.

`groupBy` is the most intricate operator in the library, and its hard part is lifetime rather than
logic. The groups live in a `Map<K, Subject<E>>`; a `GroupedObservable` is built by spreading an
`observable` and adding the key, which is the §10 record-spreading trick again:

```ts
const grouped = (key: K, subject: Subject<E>): GroupedObservable<K, E> =>
  Object.freeze({
    ...observable<E>((groupSink) => {
      activeGroups++;
      const inner = subscribeChild(subject, groupSink);
      return () => { inner.unsubscribe(); activeGroups--; maybeDisconnect(); };
    }),
    key,
  });
```

The subtlety is that, as in RxJS, unsubscribing from the *outer* Observable must not stop the source
while some group still has subscribers. Two independent facts therefore have to agree before the
source can be released:

```ts
const maybeDisconnect = (): void => {
  if (outerGone && activeGroups === 0) connection.unsubscribe();
};
```

`outerGone` is set by a finalizer on the outer sink; `activeGroups` is maintained by each group's
own producer and teardown. The source itself is subscribed with `subscribeWithin(source, connection, …)`
rather than against the outer sink, precisely so that its lifetime is the `connection` scope and not
the outer subscriber's. A class-based implementation would hold this in two fields on a subscriber
object; here it is two closure cells and one scope, and the condition that couples them is a single
named function — which is, if anything, easier to audit than fields mutated from two methods.

RxJS's scheduling is a class hierarchy: `Scheduler`, `AsyncScheduler`, `AsapScheduler`,
`AnimationFrameScheduler`, `VirtualTimeScheduler`, each paired with an `Action` subclass
(`AsyncAction`, `VirtualAction`) that models one scheduled unit of work. Here a scheduler is Paul
Taylor's "central dispatcher" reduced to two capabilities:

```ts
export interface Scheduler {
  readonly now: () => number;
  readonly schedule: (work: () => void, delay?: number) => Teardown;
}
```

An `Action` object is unnecessary because `schedule` returns its own cancel function — the same
closure-as-handle idea as `Subscription.add` (§3.1). The real schedulers are frozen literals of
two closures each, with graceful degradation written as plain `if`s rather than subclass overrides:
`asapScheduler` uses `queueMicrotask` for delay 0 and delegates to `asyncScheduler` for any real
delay; `animationFrameScheduler` uses `requestAnimationFrame` where it exists and ~16 ms timeouts
where it does not.

`createQueueScheduler` is the interesting one, because a trampoline is the case where you would most
expect to need a class — RxJS implements it as `QueueScheduler` plus `QueueAction`, with the action
re-entering the scheduler's queue through `this`. Here it is a queue and a flag in a closure:

```ts
const schedule = (work: () => void, delay = 0): Teardown => {
  if (delay > 0) return asyncScheduler.schedule(work, delay);
  const entry = { work, cancelled: false };
  queue.push(entry);
  if (!running) {
    running = true;
    try {
      for (;;) {
        const next = queue.shift();
        if (!next) break;
        if (!next.cancelled) next.work();
      }
    } catch (e) {
      queue.length = 0;   // like RxJS: a failing action drops the rest of the flush
      throw e;
    } finally {
      running = false;
    }
  }
  return () => { entry.cancelled = true; };
};
```

Work scheduled at delay 0 runs synchronously, but work scheduled *while other work is running* is
queued and drained by the outer call, so recursive scheduling never grows the stack. The `running`
flag is the whole trampoline. Note this is the one scheduler whose state is shared across its users:
`queueScheduler` is a module-level singleton over that queue (§13 lists it).

`createVirtualScheduler` is where the design earns its keep (`src/scheduler/scheduler.ts:80`). Time
only moves when you ask:

| Member | Behaviour |
|---|---|
| `now()` | returns `clock`, the virtual time |
| `schedule(work, delay)` | inserts `{ time: clock + delay, seq, work, cancelled }` into a sorted queue; returns a canceller that just sets `cancelled = true` |
| `advanceTo(t)` / `advanceBy(ms)` | runs everything due up to and including `t`, setting `clock` to each action's own time before running it |
| `flush(maxTime, maxActions)` | runs until the queue is empty or `maxTime` is reached |
| `pending()` | number of live actions |

Two details make it trustworthy as a test instrument. **Ties are FIFO**: entries are keyed by
`(time, seq)` and inserted by binary search, so two actions scheduled for the same frame run in
scheduling order — without that, marble tests would be flaky by construction. And **zero-delay loops
are caught**, not hung: both `advanceTo` and `flush` count actions against a `maxActions` budget
(default 1,000,000) and throw a diagnostic naming the clock position rather than spinning forever.

Because the scheduler is an argument, the testing story needs no global patching at all: there is no
`TestScheduler.run()` that swaps `setTimeout` for the duration of a callback, because nothing ever
reached for `setTimeout` except `asyncScheduler` itself. Passing `createVirtualScheduler()` to
`debounceTime(300, scheduler)` *is* the entire mechanism.

---

## 9. Creation and interop: dispatch without `instanceof`

`from` has to accept six different input shapes. RxJS does this partly with `instanceof` checks;
rxjs-fp cannot, and it turns out not to need to — every one of the six is identifiable by a
*capability*, which is a more honest test anyway:

```ts
if (isObservable(input)) return input as Observable<A>;          // OBSERVABLE_BRAND symbol
if (isSubscribable(input)) return fromSubscribable(input);        // typeof .subscribe === 'function'
if (isPromiseLike(input)) return fromPromise(input);              // typeof .then === 'function'
if (isAsyncIterable(input)) return fromAsyncIterable(input);      // Symbol.asyncIterator in x
if (isIterable(input)) return fromIterable(input);                // typeof x[Symbol.iterator] === 'function'
if (typeof input === 'object' && input !== null && 'length' in input) return fromArrayLike(input);
return throwError(() => new TypeError(`rxjs-fp: cannot convert ${String(input)} to an Observable`));
```

Order matters and is deliberate: an rxjs-fp Observable is checked first (identity, cheap), foreign
`Subscribable` before `PromiseLike` (an object could be both), and `ArrayLike` last as the weakest
test. `isObservable` is the only branch that uses a brand, and it uses a symbol created with
`Symbol.for('rxjs-fp/Observable')` — so even two copies of the library in one bundle recognise each
other's Observables, which `instanceof` would not.

Three creation functions are worth singling out.

**`fromSubscribable`** is the interop boundary in the inbound direction, and it has to be generous:
it hands the foreign library an object shaped like an RxJS `Subscriber` — `next`/`error`/`complete`,
a `closed` getter, `add`, `remove`, `unsubscribe` — backed by a private `Map` of teardown → remover.
That is enough for a *synchronous* RxJS source to notice that our downstream has closed and stop
producing, which is the hard half of cross-library unsubscription.

**`fromIterable` and `fromAsyncIterable` respect the iterator protocol's contract.** A `for…of` loop
calls `return()` on the iterator when it breaks early but not when the iterator is exhausted, and
both functions reproduce that with an `exhausted` flag — so a generator's `finally` block runs once,
at the right time. The async version is a recursive `pull()` rather than a loop, with a teardown that
calls `it.return?.()` defensively inside a `try`/`catch` because a throwing `return()` must not break
unsubscription.

**`unfold` is the anamorphism** — the dual of `reduce`, and the creation function that best shows the
style:

```ts
export const unfold = <S, A>(seed: S, f: (s: S) => Option<readonly [A, S]>): Observable<A> =>
  observable((sink) => {
    let s = seed;
    for (;;) {
      if (sink.closed) return;
      const r = f(s);
      if (!isSome(r)) break;
      sink.next(r.value[0]);
      s = r.value[1];
    }
    sink.complete();
  });
```

The generator function `f` is pure and returns `Option<[value, nextSeed]>`; `none` means "the stream
is over". All the impurity is the three-line shell, and the `sink.closed` guard at the top of the loop
is what makes §3.3's `take(1)` story work.

Three further creation modules sit beside `creation.ts` and are worth placing:

- **`scheduled(input, scheduler)`** (`src/creation/scheduled.ts`) is `from` with a clock. Iterables
  and array-likes emit one value per scheduler task; anything already Observable-like is routed
  through `from(input).pipe(subscribeOn(scheduler), observeOn(scheduler))` — a derivation rather
  than a reimplementation, which is only possible because both are ordinary operators.
- **`generate(options)`** is the imperative `for` loop as an Observable (`initialState`, `condition`,
  `iterate`, `resultSelector`), and it is the interesting counterpart to `unfold` below: same
  expressive power, opposite ergonomics. `unfold` is the categorical anamorphism with one function
  returning `Option<[value, nextSeed]>`; `generate` is the loop a JavaScript developer already knows,
  spelled as four fields. Shipping both is a deliberate concession that the honest FP formulation is
  not always the teachable one.
- **`bindCallback` / `bindNodeCallback`** (`src/creation/bind.ts`) wrap callback-style APIs, with a
  `CallbackResult` conditional type that collapses zero arguments to `void`, one to the value itself,
  and several to the tuple — the arity handling done in the type system rather than at runtime.

`fromEvent` shows how a union of two unrelated target shapes is handled without a type hierarchy:
five overloads give precise event types for `HTMLElement`, `Window`, `Document`, a generic DOM-like
target and a Node `EventEmitter`, and the implementation discriminates with `'addEventListener' in target`,
returning the matching removal call as its teardown.

In the outbound direction, `OBSERVABLE_KEY` is the string `'@@observable'`
(`src/core/types.ts:60`). RxJS's `from()` looks that key up and calls it, so `rx.from(ours)` works at
runtime against a plain frozen record — no shared base class, no registration. The two libraries meet
on a *protocol*, which is all a protocol ever needed to be. The one wrinkle is types, not runtime:
RxJS declares its interop key as `Symbol.observable`, so TypeScript needs
`rx.from(ours as unknown as rx.InteropObservable<T>)`.

---

## 10. Subjects and multicasting: the one shared box

A Subject is shared mutable state by definition, so this is the one place the library stops trying to
avoid it and instead *contains* it. The containment is what matters architecturally.

RxJS models the four Subject variants by inheritance: `BehaviorSubject extends Subject` overriding
`_subscribe` to emit the current value, `ReplaySubject extends Subject` overriding it to replay a
buffer, `AsyncSubject extends Subject` overriding `complete`. That is textbook open recursion — the
base class calls a method the subclass redefines. Replace it with a function taking the hook
explicitly and the hierarchy disappears:

```ts
const makeSubject = <A>(hooks: {
  readonly onNext?: (a: A) => void;
  readonly onSubscribe?: (sink: Sink<A>) => void;
}): SubjectCore<A> => { /* observers Set, terminal, disposed */ };
```

Every variant is now `makeSubject` plus a different pair of closures over its own private state:

| Variant | `onNext` | `onSubscribe` |
|---|---|---|
| `createSubject` | — | — |
| `createBehaviorSubject(initial)` | `current = a` | `sink.next(current)` unless terminated |
| `createReplaySubject(size, window, sched)` | append `{ at: scheduler.now(), value }`, then `trim()` | `trim()`, then replay the buffer (re-checking `sink.closed`) |
| `createAsyncSubject` | — (`next` is overridden to only remember `last`) | emit `last` only if the inner subject completed |

`createAsyncSubject` is the one that cannot be expressed as hooks alone, and its solution is
instructive: it composes *two* subjects, an inner one used purely to track the terminal state and an
outer one that does the delivering. Composition where RxJS used an override.

Three invariants in `makeSubject` are worth naming:

- **`check()` throws `objectUnsubscribedError()`** on every operation after `dispose()`, including
  `subscribe` — matching RxJS 7, which throws synchronously there.
- **Terminal state is remembered** as a tagged value, `{ kind: 'E', error } | { kind: 'C' } | null`,
  and a late subscriber is replayed the terminal notification immediately rather than being added to
  the observer set.
- **Delivery iterates a snapshot** — `for (const o of Array.from(observers))` — so an observer that
  subscribes or unsubscribes during delivery cannot corrupt the iteration. This is the re-entrancy
  hazard that a `Set` being mutated mid-notification would otherwise cause.

### `share`: the cold-to-hot boundary, with the ordering that matters

`share` is where several of the earlier mechanisms have to cooperate, and the comments in the source
mark the two orderings that were hardened after review:

```ts
refCount++;
// Registered first, so it also runs when a synchronous source makes
// the downstream close while we are still connecting.
sink.add(() => {
  refCount--;
  if (refCount === 0 && resetOnRefCountZero && !terminated) reset();
});
const s = (subject ??= connector());
subscribeChild(s, sink);
if (connection === null && !terminated && !sink.closed) {
  const conn = createSubscription();
  connection = conn;                 // assigned BEFORE the source runs
  subscribeWithin(source, conn, { /* next/error/complete into s */ });
}
```

First, the ref-count decrement is registered as a finalizer *before* connecting, so a synchronous
source that completes the downstream during connection still decrements correctly. Second,
`connection` is assigned before `subscribeWithin` runs the source, so a re-entrant subscriber arriving
during that synchronous run sees a non-null `connection` and does not open a second one. And
`subscribeWithin` (§3.3) is used rather than a bare `subscribe` so that closing the connection scope
stops even a synchronous source.

`shareReplay` is then not a separate implementation but a configuration:

```ts
share<A>({
  connector: () => createReplaySubject<A>(cfg.bufferSize, cfg.windowTime, cfg.scheduler),
  resetOnError: true,
  resetOnComplete: false,
  resetOnRefCountZero: cfg.refCount ?? false,
})
```

Policy as data again — the same move as `FlattenStrategy`, applied to multicasting.

---

## 11. Errors as tagged values, not subclasses

RxJS ships `EmptyError`, `ArgumentOutOfRangeError`, `ObjectUnsubscribedError`, `TimeoutError` and
`UnsubscriptionError` as classes, and you discriminate them with `instanceof`. Here they are built by
a factory of factories:

```ts
const tagged =
  <N extends string>(name: N, defaultMessage: string) =>
  (message: string = defaultMessage): TaggedError<N> =>
    Object.assign(new Error(message), { name });

const is =
  <N extends string>(name: N) =>
  (e: unknown): e is TaggedError<N> =>
    typeof e === 'object' && e !== null && (e as { name?: unknown }).name === name;
```

`new Error(...)` is a platform value constructor, so it is permitted (§14) and stack traces are
preserved. The guards are `name`-based type predicates, which gives TypeScript the same narrowing
`instanceof` would — and is in one respect strictly better: `instanceof` fails across realms
(iframes, worker boundaries, two copies of a library in one bundle), while a `name` comparison does
not. The same pattern produces `UnsubscriptionError` in `src/core/subscription.ts:14`, with its extra
`errors` array.

---

## 12. The typeclass layer: dictionaries, not interfaces to inherit

`src/fp` is where the absence of classes should hurt most — typeclasses look like the canonical use
for inheritance — and it is where it matters least, because a typeclass was never an inheritance
relation. It is a **dictionary of operations passed explicitly**, and an interface plus an object
literal expresses that exactly.

The only real obstacle is that TypeScript has no higher-kinded types: you cannot write `Functor<F>`
where `F` is "a type constructor". The fp-ts encoding solves it with a URI registry
(`src/fp/hkt.ts`):

```ts
export interface URItoKind<A> {
  readonly Observable: Observable<A>;
  readonly Option: Option<A>;
}
export type URIS = keyof URItoKind<unknown>;
export type Kind<F extends URIS, A> = URItoKind<A>[F];
```

`Kind<'Observable', number>` resolves to `Observable<number>` through the registry, so a typeclass
can be parameterised by a *string* standing in for a type constructor:

```ts
export interface Functor<F extends URIS> {
  readonly URI: F;
  readonly map: <A, B>(fa: Kind<F, A>, f: (a: A) => B) => Kind<F, B>;
}
```

An instance is then an object literal, and new types join the registry by module augmentation —
open-world extension without subclassing. `Monad` is declared as `extends Applicative<F>, Chain<F>`,
which is interface composition, not class inheritance: no implementation is inherited, only the
obligation to supply the members.

### The instances, and two deliberate refusals

| Instance | Definition | Status |
|---|---|---|
| `ObservableFunctor` | `map` | lawful |
| `MonadMerge` | `chain = mergeMap`, `ap` derived from `chain` | lawful, and the laws hold **in time** |
| `MonadConcat` | `chain = concatMap`, `ap` derived from `chain` | lawful **up to timing** (see below) |
| `ObservablePlus` / `ObservableAlternative` | `alt = merge`, `zero = EMPTY` | lawful |
| `ObservableCompactable` / `ObservableFilterable` | `compact`, `separate`, `filter`, `filterMap`, `partition`, `partitionMap` | lawful |
| `getMergeMonoid` / `getConcatMonoid` | `concat = merge` or `concat`, `empty = EMPTY` | lawful |
| `apLatest` | `combineLatest`-based apply | **makes no law claims** |
| `orElse` | `catchError` | **not `alt`** |

The two refusals are the interesting part, because both are mistakes the earlier
`rxjs-source-v7-fp` experiment made:

- **`ap` is derived from `chain`**, via `apFrom(chainF)` — so `Apply` and `Monad` cannot disagree, and
  the ap/chain consistency law holds by construction rather than by luck. A `combineLatest`-based
  apply is genuinely useful for UI state, but it is a *different* operation; it ships as `apLatest`
  with no lawfulness claim attached.
- **Error recovery is not `alt`.** `alt` must be associative with `EMPTY` as its identity; `catchError`
  is neither of those things in the relevant sense, so it is exported under its own name, `orElse`.

There is also a quiet piece of arity hygiene that would be easy to get wrong:

```ts
// Typeclass functions are unary: operators pass an index as a second
// argument, which must not leak into lawful instances.
const _map = <A, B>(fa: Observable<A>, f: (a: A) => B): Observable<B> => map((a: A) => f(a))(fa);
```

`map(f)` calls `f(value, index)`. Handing a user's `f` straight through would make a variadic `f`
observe the index — the classic `['1','2','3'].map(parseInt)` bug — and would break the Functor
composition law. Every instance member therefore wraps its function to drop the extra argument.

**The `MonadConcat` timing edge case**, stated precisely: in `chain(chain(m, f), g)` the `g`-inners
of one `f(x)` can overlap the next `f(x)`; in `chain(m, x => chain(f(x), g))` they cannot. Both sides
emit the same values in the same order, but not always at the same virtual frames, and an error can
make that difference observable. `MonadMerge` has no such gap. The law test for `MonadConcat`
therefore compares notification *sequences* on error-free streams, while `MonadMerge` is checked
frame-for-frame.

`Option` and `Either` are tagged unions (`{ _tag: 'Some', value }` / `{ _tag: 'None' }`), discriminated
by `_tag` rather than by class — the same technique as `FlattenStrategy`, applied to data instead of
policy. The do-notation is plain functions threading a record through `chain`: `Do` is `of({})`,
`bind(name, f)` chains and spreads the result into the scope object, and `let_(name, f)` does the same
with `map` for a pure step. Type-level, each returns
`{ readonly [K in keyof A | N]: K extends keyof A ? A[K] : B }`, so the growing scope record stays
precisely typed — and `Exclude<N, keyof A>` on the name parameter makes shadowing a compile error.

---

## 13. Where the mutation lives

A document that claimed this library is "pure" without qualification would be lying. It is pure in a
precise, checkable sense: **no mutable state is reachable from outside one subscription, except in
three places that exist to be shared and say so in their names** — Subjects, `share`'s connection,
and the `queueScheduler` trampoline. Here is the complete inventory.

| What mutates | Where | Lifetime | Why it is safe |
|---|---|---|---|
| `closed` flag, finalizer `Set` | `createSubscription` closure | one per subscription | created per execution, reachable only through three closures that enforce the lifecycle |
| `stopped` flag | `createSink` closure | one per subscription | same, and only ever moves `false → true` |
| Machine state, `index` | `let` in `fromMachine`'s producer | one per subscription | `step` itself is pure; the cell is written in exactly one statement |
| `active`, `queue`, `current`, `generation`, `outerDone` | `let`/array in each flattening producer | one per subscription | essential runtime bookkeeping; unreachable from any API |
| `values`/`has` arrays, `ready`, `completed` | `let`/`Array` in combination producers | one per subscription | same |
| Timer handles, pending sets | `let`/`Set` in time-operator producers | one per subscription | cancelled by the subscription's finalizers |
| `attempt`, `current`, `cancelDelay` | `let` in `retry`/`repeat` producers | one per subscription | the recursion's own bookkeeping |
| `clock`, `seq`, sorted `queue` | `createVirtualScheduler` closure | one per scheduler instance | a scheduler *is* a clock; tests create one per test |
| Observer set, current/buffered values, terminal | `makeSubject` closure | shared **by design** | sharing is the documented purpose of a Subject |
| `subject`, `connection`, `refCount`, `terminated` | `share`'s operator closure | shared across that operator's subscribers | that is precisely what `share` means |
| Open buffers/windows, chunk timers | `let`/array in each `buffer*`/`window*` producer | one per subscription | chunk bookkeeping; the emitted windows are Subjects exposed only through `asObservable()` |
| `groups` map, `activeGroups`, `outerGone` | `groupBy`'s producer closure | one per subscription, shared between the outer subscriber and every group | the coupling is deliberate and lives in one named function, `maybeDisconnect` |
| Action queue, `running` flag | `createQueueScheduler` closure | **module-global** for the exported `queueScheduler` | a trampoline is a shared run loop by definition; create your own with `createQueueScheduler()` if you need isolation |

Two honest caveats a careful reader will spot:

- **A frozen record of closures over a private `let` is, structurally, an object with private
  fields.** The no-class rule does not abolish encapsulation; it changes how encapsulation is
  obtained (scope instead of prototypes) and what comes with it (no inheritance, no `this`-binding
  hazards, no identity for `instanceof`). The win is in what is *absent*, not in the absence of
  objects as such.
- **Object identity is used as a key.** `makeSubject` keeps a `Set<Sink<A>>`, `distinct` keeps a
  `Set<K>`, `fromSubscribable` and `fromEvent` keep `Map`s of removers, and the virtual scheduler
  mutates `entry.cancelled` in place. Reference equality is load-bearing in those places. That is
  ordinary and fine, but it is not "value semantics all the way down".

---

## 14. How the rule is enforced

The claim "no classes" is worth nothing as a convention and quite a lot as a test.
`test/purity.test.ts` parses every `.ts` file under `src/` and `examples/` with the TypeScript
compiler API and walks the AST — so comments and string literals cannot trip it, and it cannot be
defeated by formatting:

| Rejected node | What it catches |
|---|---|
| `isClassDeclaration` / `isClassExpression` | any `class`, declared or inline |
| `ThisKeyword` / `ThisType` | any `this`, including `this`-typed parameters |
| `BinaryExpression` with `InstanceOfKeyword` | `x instanceof Y` |
| `PropertyAccessExpression` named `prototype` | `.prototype` fiddling |
| `NewExpression` whose callee is not allow-listed | `new` on a custom type |

The allow-list is the point of calibration:

```ts
const ALLOWED_NEW = new Set(['Error', 'TypeError', 'Promise', 'Set', 'Map', 'Array', 'WeakMap', 'WeakSet']);
```

Every `new` in `src/` today is one of those eight. The distinction being drawn is not "no mutable
containers" — `Set` and `Map` are used freely — but **no user-defined types with identity and
inheritance**. `new Set()` creates a data structure; `new MergeMapSubscriber()` creates a participant
in a type hierarchy. The first is a container, the second is an architecture.

The test runs in `npm run check` and in CI on Node 20 and 22, so the invariant is enforced on every
commit rather than remembered.

---

## 15. Testing: marbles on virtual time

Because no operator reaches for a global clock (§8), the test harness needs no patching — it only
needs to hand out a virtual scheduler and read notifications back with their frame numbers.

`parseMarbles` is a **pure function** from a diagram to timed events, which is why it can be tested
on its own and reused for both sides of an assertion:

```ts
export type RecordedEvent<A = unknown> =
  | { readonly frame: number; readonly kind: 'N'; readonly value: A }
  | { readonly frame: number; readonly kind: 'E'; readonly error: unknown }
  | { readonly frame: number; readonly kind: 'C' };
```

One character is one frame is one virtual millisecond: `-` advances, a letter emits (looked up in the
`values` record, or taken literally), `|` completes, `#` errors, `(ab)` groups events at the frame of
the `(`, `^` marks a subscription point and `!` an unsubscription. Groups are handled with a
`groupFrame` sentinel rather than recursion, and the three malformed cases — nested, unmatched and
unclosed groups — throw with the offending diagram in the message.

The pieces built on it mirror the cold/hot distinction exactly:

- **`cold(marbles)`** schedules the parsed events *inside the producer*, so each subscription gets its
  own set of scheduled actions and its own teardown that cancels them. Cold means re-run per
  subscriber, and here that is literally what the code does.
- **`hot(marbles)`** schedules them once against a `createSubject`, offset by the `^` frame, and hands
  out `subject.asObservable()`. Shared by construction.
- **`record(source, scheduler, …)`** subscribes and pushes `{ frame: scheduler.now(), kind, … }` for
  every notification — the frame comes from the virtual clock, so it is exact rather than measured.

`runMarbles` ties it together with one ordering trick worth noticing: `expectObservable(...).toBe(...)`
does not compare anything when you call it. It pushes a *closure* onto a `checks` array. The body of
your test therefore declares all its expectations while virtual time is still at zero; then
`runMarbles` calls `scheduler.flush(maxFrames ?? 1000)`, and only afterwards runs the checks and
throws a single `Error` describing every mismatch with both diagrams rendered. Comparison uses a
structural `deepEqual` that compares `Error`s by `name` and `message` — consistent with §11, where
errors are tagged values rather than class instances.

### Differential testing against RxJS

Marble tests check behaviour against what the author *expected*. `test/differential.ts` checks it
against what RxJS 7.8 actually does, which is a materially stronger claim and the natural one for a
library whose whole premise is "same API, different architecture underneath".

The harness runs the same marble inputs through both implementations on their own virtual clocks —
rxjs-fp via `runMarbles`/`record`, RxJS via its `TestScheduler` — then normalises both recordings to
`{ frame, kind, value | error }` and requires them to be equal. Errors are compared as
`name:message`, which is exactly the right granularity given §11: rxjs-fp's errors are tagged values
and RxJS's are class instances, so they can never be structurally identical, but a `TimeoutError`
carrying the same message is the same observable behaviour. `test/differential.test.ts` applies it
across the conditional and mathematical operators, the duration-selector family, `buffer`/`window`,
and the higher-order operators — the four areas where "the same operator with a different clock" is
most likely to diverge by a frame.

Alongside these, `test/laws.test.ts` checks the §12 instances with fast-check over generated timed
streams, at a run count controlled by the `LAW_RUNS` environment variable (CI sets `1000`).
`test/machine.test.ts` exercises machines through `runMachine` with no streams at all, and
`test/purity.test.ts` is the AST guard of §14 — which scans `src/` and `examples/` but deliberately
**not** `test/`, since test helpers are free to use whatever JavaScript they like.

---

## 16. What it costs

| Cost | Reality |
|---|---|
| **Allocation per subscription** | Every subscription allocates a frozen record plus several closures, where RxJS allocates one `Subscriber` whose methods live on a shared prototype. For streams at ordinary rates this is noise; for very hot synchronous inner loops it is measurable. |
| **No shared prototype methods** | RxJS 5 moved to classes partly for V8's inline caches. Closure-per-instance call sites are less friendly to that optimisation. This is the deliberate trade: clarity and testability over the last few percent. |
| **Stack depth** | Synchronous chains nest producer calls; deep pipes over synchronous sources produce deep stacks, same as RxJS but without class names to orient you in a trace. |
| **Debuggability** | A stack frame reads `next` or an anonymous arrow rather than `MergeMapSubscriber.prototype._next`. The upside is that `runMachine` lets you debug operator *logic* without a stack at all. |
| **Type-inference ceiling** | `pipe` is typed by overloads, so the tenth operator onward degrades to `unknown` — exactly RxJS's limitation, from the same cause. |
| **No `instanceof` for users** | Consumers must use the exported guards (`isObservable`, `isEmptyError`, …) instead of `instanceof`. Across realms this is an improvement; as an API habit it is a difference to document. |
| **Hooks are less discoverable than overrides** | `makeSubject({ onNext, onSubscribe })` is more compact than three subclasses, but a reader looking for "what does ReplaySubject change?" has to read two closures rather than one class body. |

---

## 17. Adding an operator: the decision procedure

The architecture answers "how do I write this operator?" with a five-way test. In order:

1. **Stateless?** Write a plain closure over the source — one `observable(sink => subscribeChild(...))`,
   or build it from existing operators. (`map`, `filter`, `tap`.)
2. **Stateful, but the state is a value and the logic is a pure function of it?** Write a `Machine`
   and hand it to `fromMachine`. Test `step` with `runMachine` before any stream exists.
   (`scan`, `pairwise`, `bufferCount`, `take`, `distinct`.)
3. **Does it own resources — subscriptions, timers, re-subscription?** Write a producer and use
   `subscribeChild` for every upstream, never `source.subscribe`. If it involves time, take
   `scheduler: Scheduler = asyncScheduler` as an explicit parameter and never touch a global.
   (`switchMap`, `debounceTime`, `retry`, `combineLatest`.)
4. **Is a notifier, duration or closing selector involved?** Subscribe it with `subscribeInner`, not
   `subscribeChild`, and drive the operator off the `Inner` handle it returns — a synchronous
   duration can end before your subscription variable is assigned. (`debounce`, `audit`, `buffer`,
   `windowWhen`.)
5. **Does it emit Observables?** You need a Subject per chunk or per key, handed out through
   `asObservable()` so consumers cannot push into it, and you need to decide explicitly what keeps
   the source alive — see `groupBy`'s `maybeDisconnect` for the two-condition version.
   (`window*`, `groupBy`.)

Then the invariants, each with the place that enforces it:

| Invariant | Enforced by |
|---|---|
| No `class`, `this`, `instanceof`, `.prototype`, custom `new` | `test/purity.test.ts` (AST) |
| Operators link teardown and route exceptions | use `subscribeChild`, never `source.subscribe` |
| Stateful logic stays pure | express it as a `Machine`; `step` takes and returns state |
| Time is injectable | `Scheduler` parameter; no global patching |
| New typeclass instances stay lawful | a property test in `test/laws.test.ts` |
| Imports resolve under NodeNext | `.js` extensions on every relative import |

---

## 18. RxJS 7 → rxjs-fp translation table

| RxJS 7.8 | rxjs-fp | File |
|---|---|---|
| `class Observable<T>` with `subscribe`/`pipe`/`lift` | `observable(producer)` → frozen record of closures | `src/core/observable.ts` |
| `class Subscriber<T> extends Subscription` | `createSink(observer, subscription, internal)` | `src/core/observable.ts:90` |
| `class Subscription` with parent/child tree | `createSubscription()` → three closures over one cell, flat finalizer `Set` | `src/core/subscription.ts` |
| `Observable.prototype.pipe` + `pipeFromArray` | `pipeArray` + overloads; `pipe`/`flow` | `src/core/pipe.ts` |
| `operate()` + `OperatorSubscriber` | `fromMachine(machine)` + pure `step`/`flush` | `src/core/machine.ts` |
| `mergeMap`/`concatMap`/`switchMap`/`exhaustMap` + `mergeInternals` | `flatMapWith(strategy)` over `Merge(n) \| Switch \| Exhaust` | `src/operators/flatten.ts` |
| `merge`/`concat` as their own implementations | `mergeAll()(from(sources))` / `concatAll()(from(sources))` | `src/operators/combine.ts:9` |
| Variadic overloads for `combineLatest`/`zip` tuples | `ValuesOf<T>` mapped type + `const` type parameters | `src/core/types.ts:85` |
| `class AsyncScheduler` / `AsyncAction` / `VirtualTimeScheduler` / `VirtualAction` | `{ now, schedule(work, delay) → cancel }` records | `src/scheduler/scheduler.ts` |
| `class QueueScheduler` / `QueueAction` (trampoline) | `createQueueScheduler()` → a queue and a `running` flag in a closure | `src/scheduler/scheduler.ts:153` |
| `class GroupedObservable` + `groupBy`'s internal subscriber | `Map<K, Subject>` + a spread `observable` carrying `key` | `src/operators/group.ts` |
| `buffer*`/`window*` subscriber classes | one producer per operator, chunks held as Subjects behind `asObservable()` | `src/operators/window.ts` |
| Inner-subscription bookkeeping inside operator classes | `subscribeInner` → a frozen `Inner` handle closable before assignment | `src/operators/inner.ts` |
| `generate` / `bindCallback` / `bindNodeCallback` / `scheduled` | plain functions; `scheduled` derives from `subscribeOn` + `observeOn` | `src/creation/scheduled.ts`, `bind.ts` |
| `class Subject extends Observable implements Observer` | `createSubject()` → record of closures over an observer `Set` | `src/subject/subject.ts` |
| `BehaviorSubject`/`ReplaySubject`/`AsyncSubject` overriding `_subscribe` | `makeSubject({ onNext, onSubscribe })` hooks | `src/subject/subject.ts:35` |
| `class ConnectableObservable` + `multicast`/`refCount` | `share(config)`; `shareReplay` = `share` with a replay connector | `src/subject/subject.ts:237` |
| `class EmptyError extends Error` et al. | `tagged(name, msg)` + `name`-based guards | `src/core/errors.ts` |
| `instanceof Observable` | `isObservable` via a `Symbol.for` brand | `src/core/types.ts:63` |
| `instanceof` dispatch in `from` | capability checks (`then`, `Symbol.iterator`, `subscribe`, …) | `src/creation/creation.ts:165` |
| Typeclass-ish inheritance | dictionary records + URI/`Kind` registry | `src/fp/hkt.ts`, `src/fp/typeclasses.ts` |
| `TestScheduler.run()` with global patching | `runMarbles` + an explicitly passed virtual scheduler | `src/testing/marbles.ts` |
