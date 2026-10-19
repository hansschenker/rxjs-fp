import { observable, subscribeChild } from '../core/observable.js';
import type { Observable } from '../core/types.js';
import type { Scheduler } from '../scheduler/scheduler.js';
import { createAsyncSubject } from '../subject/subject.js';
import { observeOn, subscribeOn } from '../operators/time.js';

/** One callback result: nothing, a single value, or a tuple for several. */
export type CallbackResult<R extends readonly unknown[]> = R extends readonly [] ? void : R extends readonly [infer X] ? X : R;

/**
 * @internal Shared core: the wrapped function is called at most once, on the
 * first subscription; its result is cached in an AsyncSubject and replayed
 * to every subscriber (RxJS semantics).
 */
const bindInternal =
  <Args extends readonly unknown[], R>(
    call: (args: Args, resolve: (value: R) => void, reject: (err: unknown) => void) => void,
    scheduler?: Scheduler,
  ) =>
  (...args: Args): Observable<R> => {
    const subject = createAsyncSubject<R>();
    let called = false;
    const bound = observable<R>((sink) => {
      const sub = subscribeChild(subject, sink);
      if (!called) {
        called = true;
        let settled = false;
        const settle = (f: () => void): void => {
          if (settled) return;
          settled = true;
          f();
        };
        try {
          call(
            args,
            (value) =>
              settle(() => {
                subject.next(value);
                subject.complete();
              }),
            (err) => settle(() => subject.error(err)),
          );
        } catch (e) {
          settle(() => subject.error(e));
        }
      }
      return sub;
    });
    return scheduler ? bound.pipe(subscribeOn<R>(scheduler), observeOn<R>(scheduler)) : bound;
  };

/**
 * Turns a callback-last function into a function that returns an
 * Observable. The callback's arguments are emitted (one value, or an array
 * for several), then the Observable completes.
 *
 * As in RxJS, the function runs once, on the first subscription, and its
 * result is replayed to later subscribers of the same returned Observable.
 */
export const bindCallback =
  <Args extends readonly unknown[], R extends readonly unknown[]>(
    fn: (...args: [...Args, (...results: R) => void]) => void,
    scheduler?: Scheduler,
  ) =>
  (...args: Args): Observable<CallbackResult<R>> =>
    bindInternal<Args, CallbackResult<R>>((a, resolve) => {
      fn(...a, (...results: R) => resolve((results.length <= 1 ? results[0] : results) as CallbackResult<R>));
    }, scheduler)(...args);

/**
 * Like `bindCallback`, for Node-style callbacks `(err, ...results)`: a
 * truthy `err` becomes an error notification.
 */
export const bindNodeCallback =
  <Args extends readonly unknown[], R extends readonly unknown[]>(
    fn: (...args: [...Args, (err: any, ...results: R) => void]) => void,
    scheduler?: Scheduler,
  ) =>
  (...args: Args): Observable<CallbackResult<R>> =>
    bindInternal<Args, CallbackResult<R>>((a, resolve, reject) => {
      fn(...a, (err: unknown, ...results: R) => {
        if (err) reject(err);
        else resolve((results.length <= 1 ? results[0] : results) as CallbackResult<R>);
      });
    }, scheduler)(...args);
