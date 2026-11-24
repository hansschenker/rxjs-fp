# Algebraic operator policies

## Intuition

An Observable describes values moving over time. Applying an operator describes another
dataflow; subscribing starts its execution. A policy says how that operator connects,
waits, forwards, cancels or shares. User functions still own the meaning of the values.

Version **0.2.0** replaces the canonical `repeat`, `retry`, `share`, `shareReplay`,
`throttle` / `throttleTime`, and `timeout` interfaces with algebraic policies. These are
intentional breaking changes. The implementation uses rxjs-fp's existing functional
runtime; RxJS **7.8.2** is an exact development dependency used as a behavioral reference.

The notation reads from decisions to stream transformation:

```text
repeat      :: RepeatPolicy → Scheduler → (A* → A*)
retry       :: RetryPolicy → Scheduler → (A* → A*)
share       :: SharePolicy A → (A* → A*)
shareReplay :: ShareReplayPolicy → Scheduler → (A* → A*)
throttle    :: (A → Signal) → ThrottleMode → (A* → A*)
throttleTime:: Number → ThrottleMode → Scheduler → (A* → A*)
timeout     :: TimeoutPolicy A B M → Scheduler → (A* → (A | B)*)
```

Here `A*` means an Observable of `A`, including its timing and terminal notifications.
The arrows describe partial application conceptually: the actual TypeScript functions
take ordinary argument lists and return an `Operator<A, B>`. Supplying a policy constructs
the operator; supplying its source constructs an Observable; subscription runs it.

A sum names alternatives. A product combines independent choices. Records remain useful
as explicit readonly products, without optional fields that obscure the chosen behavior:

| Former RxJS config | Algebraic policy |
| --- | --- |
| `RepeatConfig` | `RepeatLimit × RepeatDelay` |
| `RetryConfig` | `RetryLimit × RetryDelay × RetryCounter` |
| `ShareConfig<A>` | `Connector<A> × ResetOnError × ResetOnComplete × ResetOnNoSubscribers` |
| `ShareReplayConfig` | `ReplayBounds × IdlePolicy` |
| `ThrottleConfig` | `leading-only + trailing-only + leading-and-trailing + suppress-values` |
| `TimeoutConfig<A, B, M>` | `TimeoutTiming × TimeoutOutcome<A, B, M> × M` |

`+` means choose one alternative; `×` means supply each independent decision. The types
expose the available decisions. The execution rules below specify the effects of those
decisions; a TypeScript signature alone cannot prove timing or cancellation correctness.

## Specification

### Shared vocabulary and numeric domains

The shared constructors and types are exported from `rxjs-fp` and `rxjs-fp/kernel`.
Policy-specific types are also exported from their `/errors`, `/time`, or `/multicast`
family entry point. The root and family entries export the same function objects.

| Constructor | Accepted values or meaning |
| --- | --- |
| `natural(n)` | Nonnegative safe integer |
| `positiveInteger(n)` | Positive safe integer |
| `milliseconds(n)` | Finite nonnegative milliseconds; fractions are allowed |
| `positiveMilliseconds(n)` | Finite positive milliseconds |
| `epochMilliseconds(n)` | Integer timestamp within the JavaScript Date range |
| `limited(value)` / `unlimited()` | A finite bound or explicitly no bound |
| `immediate()` | Restart synchronously, without creating a timer |
| `after(ms)` | Wait for a timer signal on the supplied scheduler |

Invalid domain inputs throw an `ArgumentOutOfRangeError` at policy construction. Infinity
is represented by an alternative, not a numeric payload. `after(0)` is a scheduled wait,
distinct from `immediate()`; whether that scheduler runs synchronously is the scheduler's
decision. Replay count and age bounds are normalized to at least one, matching RxJS 7.8.2.

`Signal` is `ObservableInput<unknown>`: an Observable, subscribable, Promise, iterable,
async iterable or array-like input accepted by this runtime. It is the signal's events
that control the operator; its next-value contents are ignored.

Policies are readonly data. Constructors freeze the records they create, and interpreters
capture their discriminants, numeric payloads and function references at operator
construction. Mutating a supplied policy record afterwards does not reconfigure that
operator. User-owned metadata, callback closures and injected schedulers are not deep-cloned.

### Repeat: total runs and the next connection

```text
RepeatLimit = unlimited + total-runs(NonNegativeInt)
RepeatDelay = immediate + after(Milliseconds) + on-signal(Number → Signal)
RepeatPolicy = RepeatLimit × RepeatDelay
```

Source values pass through unchanged. Completion may start another source subscription;
an error ends the output. `totalRuns(3)` includes the initial connection, so it permits
at most three runs. `totalRuns(0)` completes without connecting to the source.

The signal factory receives the number of completed runs, starting at one. It is evaluated
only when another run is allowed. The first signal value closes that signal and starts the
next run. Signal completion without a value completes the output; a signal error errors it;
a silent signal leaves it waiting. Cancellation closes the current source or wait.

```ts
import { after, repeat, totalRuns, type RepeatPolicy } from 'rxjs-fp';

const repeatThreeTimes: RepeatPolicy = {
  limit: totalRuns(3),
  delay: after(100),
};

const repeated$ = source$.pipe(repeat(repeatThreeTimes, scheduler));
```

The default is unlimited runs with an immediate restart. Immediate restarts remain
synchronous and recursive; an unbounded synchronously completing source needs a terminating
or scheduled policy. Source teardown finishes before the next synchronous run begins.

### Retry: additional attempts and the counter rule

```text
RetryLimit = unlimited + additional-retries(NonNegativeInt)
RetryDelay = immediate + after(Milliseconds) + on-signal((Unknown, Number) → Signal)
RetryCounter = keep-count + reset-on-next
RetryPolicy = RetryLimit × RetryDelay × RetryCounter
```

Source values pass through unchanged. An error may start another subscription; completion
ends the output. `additionalRetries(2)` permits the initial attempt and two more.
`additionalRetries(0)` uses the source without any restart handling.

`resetOnNext()` resets the counter on every source value. With that choice, a finite retry
limit bounds errors since the most recent value; it does not bound the lifetime number of
attempts. The signal factory receives the triggering error and one-based retry number.
Its next, completion, error and cancellation rules match repeat's restart signal.

```ts
import {
  additionalRetries, immediate, resetOnNext, retry, type RetryPolicy,
} from 'rxjs-fp';

const reconnect: RetryPolicy = {
  limit: additionalRetries(2),
  delay: immediate(),
  counter: resetOnNext(),
};

const connected$ = source$.pipe(retry(reconnect, scheduler));
```

The default is unlimited additional retries, immediate restart and `keepCount()`.
`retryWith` and `retryConsecutiveWith` remain compositions over this interpreter.
Their pure function type is now named **`RetryDecisionPolicy`**; **`RetryPolicy`** names
the algebraic product. `times`, `timesWithDelay` and `exponentialBackoff` still produce
decision functions. There is one canonical retry engine.

### Share: three independent reset decisions

```text
ResetPolicy Args = keep-state + reset-now + reset-on-next(Args → Signal)
SharePolicy A = (() → Subject A)
              × ResetPolicy [Unknown]
              × ResetPolicy []
              × ResetPolicy []
```

The product fields are `connector`, `onError`, `onComplete`, and `onNoSubscribers`.
The connector creates the Subject for each shared session. The first subscriber connects
the source; simultaneous subscribers share that one connection. Different applications
of the operator have independent sessions.

| Alternative | Effect |
| --- | --- |
| `keep-state` | Keep the Subject and session state; with no subscribers, keep an active source connected |
| `reset-now` | Discard session state immediately; also disconnect an active source on the zero-subscriber path |
| `reset-on-next` | Wait for the factory's signal to emit, then perform the corresponding reset |

A terminal reset enables a future subscriber to start a new session. Current subscribers
still receive the source's error or completion. A new subscriber cancels a pending
zero-subscriber reset while the source is active. It does not cancel a terminal reset.

For share, a reset signal that completes empty **does not reset**. This differs from
repeat/retry's empty restart signal. A reset signal's error notification is reported through
the runtime's unhandled-error route, matching the reference behavior; the original terminal
notification still reaches the shared observers.

A factory that throws before returning its signal has different lifecycle consequences.
On the error/completion path, that exception is reported as unhandled and can interrupt
delivery of the original terminal notification. On the zero-subscriber path, it contributes
to an `UnsubscriptionError` from unsubscribe. These cases are covered by differential tests.

```ts
import { defaultSharePolicy, share, timer, type SharePolicy } from 'rxjs-fp';

const briefIdleGrace: SharePolicy<number> = {
  ...defaultSharePolicy<number>(),
  onNoSubscribers: {
    kind: 'reset-on-next',
    signal: () => timer(500, undefined, scheduler),
  },
};

const shared$ = source$.pipe(share(briefIdleGrace));
```

The default uses a plain Subject and immediate reset for all three events.
`shareWith` and `shareLatest` select policies for this same interpreter.

### ShareReplay: retained values and idle behavior

```text
ReplayBounds = Limit PositiveInt × Limit PositiveMilliseconds
IdlePolicy = keep-connected + disconnect-and-reset
ShareReplayPolicy = ReplayBounds × IdlePolicy
```

The fields are `replay: { maxValues, maxAge }` and `onNoSubscribers`. Count and age limits
apply together. The scheduler is the age clock; replay itself is synchronous.

```ts
import {
  limited, positiveInteger, shareReplay, unlimited, type ShareReplayPolicy,
} from 'rxjs-fp';

const latestWhileObserved: ShareReplayPolicy = {
  replay: {
    maxValues: limited(positiveInteger(1)),
    maxAge: unlimited(),
  },
  onNoSubscribers: 'disconnect-and-reset',
};

const state$ = source$.pipe(shareReplay(latestWhileObserved, scheduler));
```

`keep-connected` keeps an active connection when the final subscriber leaves.
`disconnect-and-reset` closes it and discards its replay buffer. In **both** modes, a
successful completion retains the completed cache, and a source error resets the session.
Expired cached values disappear, but expiry does not reconnect an already completed source.
This includes the RxJS `refCount: true` combination that the older two-choice lifecycle API
could not fully express.

The default has unlimited count, unlimited age and `keep-connected`. `shareReplay` translates
its own algebraic choices into the canonical `share` policy and a replay Subject; it never
constructs an RxJS config or calls native RxJS.

### Throttle: four emission choices

```text
ThrottleMode = leading-only + trailing-only + leading-and-trailing + suppress-values
```

The duration selector supplies a signal for each throttle window. Time enters through that
signal or through the timer used by `throttleTime`.

| Mode | When a value opens a window | While the window is active |
| --- | --- | --- |
| `leading-only` | Forward it immediately | Suppress later values |
| `trailing-only` | Retain it | Retain the latest value for the window's signal |
| `leading-and-trailing` | Forward it immediately | Retain any later latest value for the signal |
| `suppress-values` | Start the duration without forwarding | Suppress values, while still running duration selection |

A duration's first value ends its window and, for a trailing mode, forwards a retained
value. A duration that completes empty closes the window without a trailing emission.
Errors propagate. Source completion waits for an active trailing value where required;
downstream cancellation closes both the source and its duration. One isolated leading value
is not duplicated as a trailing value.

```ts
source$.pipe(throttle(durationForValue, 'leading-and-trailing'));
source$.pipe(throttleTime(100, 'trailing-only', scheduler));
```

The default is `leading-only`. Existing `Leading`, `Trailing`, `LeadingAndTrailing`, and
the named helper operators select these same modes. The scheduler is now the **third**
argument of `throttleTime`, after the mode.

### Timeout: the deadline and its outcome

```text
Deadline = after(Milliseconds) + at(EpochMilliseconds)
TimeoutTiming = first-only(Deadline)
              + each-gap(PositiveMilliseconds)
              + first-then-each(Deadline, PositiveMilliseconds)
TimeoutOutcome A B M = raise-timeout-error
                     + switch-to(TimeoutInfo A M → ObservableInput B)
TimeoutPolicy A B M = TimeoutTiming × TimeoutOutcome A B M × M
```

The policy fields are `timing`, `outcome`, and `meta`. Timing is mandatory:

- `first-only` guards the first source value, then stops timing.
- `each-gap` guards both the initial wait and every gap after a value.
- `first-then-each` uses its own first deadline, then a positive gap duration.

`deadlineAfter(ms)` is relative to subscription. `deadlineAt(date)` captures the Date's
timestamp when constructing the deadline; mutating the Date afterwards has no effect.
Absolute deadlines and the scheduler must use the same clock origin.

When a deadline wins, timeout closes the source before raising `TimeoutError` or invoking
the fallback factory. The fallback is a new dataflow and is not subject to the original
timeout again. Cancelling downstream cancels whichever dataflow is active and any timer.

```ts
import {
  deadlineAfter, of, positiveMilliseconds, timeout, type TimeoutSwitchPolicy,
} from 'rxjs-fp';

const useCache: TimeoutSwitchPolicy<number, string, { readonly request: string }> = {
  timing: {
    kind: 'first-then-each',
    deadline: deadlineAfter(1000),
    duration: positiveMilliseconds(250),
  },
  outcome: {
    kind: 'switch-to',
    fallback: (info) => of(`cached ${info.meta.request}; received ${info.seen}`),
  },
  meta: { request: 'prices' },
};

const result$ = source$.pipe(timeout(useCache, scheduler)); // Observable<number | string>
```

`TimeoutErrorPolicy<A, M>` preserves the source value type. `TimeoutSwitchPolicy<A, B, M>`
and a general `TimeoutPolicy<A, B, M>` produce `A | B`. Metadata stays typed in the fallback.
`defaultTimeoutErrorPolicy(timing)` selects the error outcome and `meta: null`. Its value type
parameter defaults to `unknown`: inline in a `pipe` it is inferred from the source, but a policy
kept in a constant must name it — `defaultTimeoutErrorPolicy<Hit[]>(timing)` — or `timeout` widens
the stream to `Observable<unknown>`.
`timeoutFirst` and `timeoutFirstAndEach` remain compositions selecting those policies.

`TimeoutInfo` contains `seen`, `lastValue`, and `meta`. rxjs-fp retains its established
contract that `lastValue` is the last emitted source value. RxJS 7.8.2 can clear that field
during source teardown before constructing the info. That diagnostic difference is deliberate.

### Migration checklist

| Previous call or type | Replacement |
| --- | --- |
| `repeat(n)` | `repeat({ limit: totalRuns(n), delay: immediate() })` |
| `retry(n)` | `retry({ limit: additionalRetries(n), delay: immediate(), counter: keepCount() })` |
| Function-valued `RetryPolicy` | `RetryDecisionPolicy` |
| `shareReplay(n)` | A `ShareReplayPolicy` with `limited(positiveInteger(n))`, unlimited age, and `'keep-connected'` |
| `throttleTime(ms, scheduler)` | `throttleTime(ms, 'leading-only', scheduler)` |
| `timeout(ms, scheduler)` | `timeout(defaultTimeoutErrorPolicy({ kind: 'each-gap', duration: positiveMilliseconds(ms) }), scheduler)` |
| Any of the six native RxJS config objects | The corresponding named policy above |

No `toRepeatConfig` or other native-config adapter is necessary. The canonical names are
the policy implementations. Schedulers remain explicit final arguments because they are
execution dependencies already used throughout rxjs-fp, and all examples can run on virtual time.

## Verification

Run the repository's required gate and the deterministic example:

```sh
npm ci
npm run check
npm run example:policies
```

`npm run check` runs strict TypeScript checking, Vitest, and the declaration/ESM build.
The CI property-test setting is `LAW_RUNS=1000 npm test`.

| Source | Verification |
| --- | --- |
| `src/policies/core.ts` | `test/policies-types.test.ts`: numeric domains, frozen constructors, accepted/rejected signatures and type inference |
| `src/operators/error.ts` | `test/policies-repeat-retry.test.ts`: property comparisons, counts, notifier outcomes, counter resets, synchronous teardown, cancellation and snapshots |
| `src/subject/subject.ts` | `test/policies-sharing.test.ts`: reset products, delayed resets and races, replay bounds, completed caches and reentrant delivery |
| `src/operators/time.ts` and `src/vocabulary/time.ts` | `test/policies-time.test.ts`: all throttle modes, timeout timing/outcomes, metadata, teardown and fallback cancellation |
| Root, kernel and family exports | `test/layering.test.ts`: one implementation per exported function and no reverse dependencies |
| All library and example sources | `test/purity.test.ts`: no class, this, instanceof, prototype or custom construction |

The existing operator, example, differential and typeclass-law suites remain part of the
gate. Differential tests run actual RxJS 7.8.2 beside rxjs-fp; they compare notifications,
frames, subscription behavior and relevant cancellation ordering. Passing those tests is
evidence for their covered cases, not a claim that every possible stream and scheduler has
been proven equivalent.

The runtime deliberately keeps its closed-sink safeguard: cancellation during repeat/retry
teardown does not execute another source initializer. It also preserves the meaningful
timeout diagnostics described above. The existing documented differences in other operators
remain in the [README](../README.md#coverage-of-rxjs-78).

Reference implementations: the RxJS 7.8.2 sources for
[repeat](https://github.com/ReactiveX/rxjs/blob/7.8.2/src/internal/operators/repeat.ts),
[retry](https://github.com/ReactiveX/rxjs/blob/7.8.2/src/internal/operators/retry.ts),
[share](https://github.com/ReactiveX/rxjs/blob/7.8.2/src/internal/operators/share.ts),
[shareReplay](https://github.com/ReactiveX/rxjs/blob/7.8.2/src/internal/operators/shareReplay.ts),
[throttle](https://github.com/ReactiveX/rxjs/blob/7.8.2/src/internal/operators/throttle.ts), and
[timeout](https://github.com/ReactiveX/rxjs/blob/7.8.2/src/internal/operators/timeout.ts).
