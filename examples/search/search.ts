/**
 * Search-as-you-type against a lookup service that is slow at times, down at
 * times and silent for some queries: wait for the typing to settle, ask once
 * per distinct query, give every request a deadline and one more try, keep a
 * failed search as a value instead of ending the stream, and share the latest
 * answer with every part of the UI.
 *
 * Written the recommended way (docs/kernel.md, "The naming style"): every
 * decision is a named value defined before the pipeline, and the service and
 * the scheduler are arguments, so the same code runs in the app and in a test
 * with a fake service and virtual time.
 */
import { after, isTimeoutError, limited, positiveInteger, unlimited, type Observable, type PositiveMilliseconds, type Scheduler } from 'rxjs-fp';
import { debounceTime, deadlineAfter, defaultTimeoutErrorPolicy, timeout } from 'rxjs-fp/time';
import { distinctUntilChanged, filter, map } from 'rxjs-fp/values';
import { switchMap } from 'rxjs-fp/concurrency';
import { additionalRetries, keepCount, retry, type RetryPolicy } from 'rxjs-fp/errors';
import { shareReplay, type ShareReplayPolicy } from 'rxjs-fp/multicast';
import { attempt, isLeft, type Either } from 'rxjs-fp/fp';

export interface Hit {
  readonly id: number;
  readonly title: string;
}

/** The one dependency: how to look a query up. The app passes HTTP, a test passes a fake. */
export type Lookup = (query: string) => Observable<readonly Hit[]>;

/** What one search produced: its hits, or the failure that ended it, as a value. */
export interface Outcome {
  readonly query: string;
  readonly result: Either<unknown, readonly Hit[]>;
}

// --- named decisions -----------------------------------------------------------

/** A query is what the user has typed once the keys have been quiet for 300 ms. */
export const settledTyping = (scheduler: Scheduler) => debounceTime<string>(300, scheduler);

/** No surrounding whitespace, nothing empty, and not the query that was just asked. */
export const cleanQueries = (typed: Observable<string>): Observable<string> =>
  typed.pipe(
    map((text) => text.trim()),
    filter((text) => text.length > 0),
    distinctUntilChanged(),
  );

/** An answer that has not started within 400 ms is a failure. */
export const answerDeadline = defaultTimeoutErrorPolicy<readonly Hit[]>({ kind: 'first-only', deadline: deadlineAfter(400) });

/** One more try, 50 ms after a failure. */
export const oneMoreTry: RetryPolicy = Object.freeze({
  limit: additionalRetries(1),
  delay: after(50),
  counter: keepCount(),
});

/** Every consumer sees the latest answer; when the last one leaves, forget it and disconnect. */
export const latestAnswer: ShareReplayPolicy = Object.freeze({
  replay: Object.freeze({ maxValues: limited(positiveInteger(1)), maxAge: unlimited<PositiveMilliseconds>() }),
  onNoSubscribers: 'disconnect-and-reset',
});

/** One query: the deadline, one retry, and then a failure becomes a value instead of ending the search. */
export const searchOnce =
  (lookup: Lookup, scheduler: Scheduler) =>
  (query: string): Observable<Outcome> =>
    lookup(query).pipe(
      timeout(answerDeadline, scheduler),
      retry(oneMoreTry, scheduler),
      attempt(),
      map((result) => ({ query, result })),
    );

const hasMessage = (x: unknown): x is { readonly message: string } =>
  typeof x === 'object' && x !== null && typeof (x as { readonly message?: unknown }).message === 'string';

const failureText = (failure: unknown): string =>
  isTimeoutError(failure) ? 'no answer within 400 ms' : hasMessage(failure) ? failure.message : String(failure);

/** One line per outcome, for a log or a status bar. */
export const describeOutcome = ({ query, result }: Outcome): string =>
  isLeft(result)
    ? `"${query}": ${failureText(result.left)}`
    : `"${query}": ${result.right.length === 0 ? 'no hits' : result.right.map((hit) => hit.title).join(' | ')}`;

// --- the pipeline ----------------------------------------------------------------

/** Keystrokes in, outcomes out: one request per settled distinct query, the newest query wins, answers shared. */
export const searchResults =
  (lookup: Lookup, scheduler: Scheduler) =>
  (keystrokes: Observable<string>): Observable<Outcome> =>
    keystrokes.pipe(
      settledTyping(scheduler),
      cleanQueries,
      switchMap(searchOnce(lookup, scheduler)),
      shareReplay(latestAnswer, scheduler),
    );
