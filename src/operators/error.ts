import { observable, subscribeChild } from '../core/observable.js';
import type { Observable, Operator, Subscription, Teardown } from '../core/types.js';
import { from, timer, type ObservableInput } from '../creation/creation.js';
import { asyncScheduler, type Scheduler } from '../scheduler/scheduler.js';

/**
 * On error, switches to the Observable returned by `handler`. `caught` is
 * the source itself, so `handler` can resubscribe.
 */
export const catchError =
  <A, B>(handler: (err: unknown, caught: Observable<A>) => ObservableInput<B>): Operator<A, A | B> =>
  (source) => {
    const self: Observable<A | B> = observable<A | B>((sink) => {
      let current: Subscription | null = null;
      current = subscribeChild(source, sink, {
        error: (err) => {
          current?.unsubscribe();
          current = subscribeChild(from(handler(err, source)), sink);
        },
      });
      return () => current?.unsubscribe();
    });
    return self;
  };

export interface RetryConfig {
  /** Maximum number of resubscriptions. Default: infinite. */
  readonly count?: number;
  /** Delay before resubscribing: ms, or a function of (error, attempt). */
  readonly delay?: number | ((error: unknown, attempt: number) => number);
  /** Reset the attempt counter after a successful value. Default: false. */
  readonly resetOnSuccess?: boolean;
  readonly scheduler?: Scheduler;
}

/** Resubscribes to the source when it errors, up to `count` times. */
export const retry =
  <A>(config: number | RetryConfig = {}): Operator<A, A> =>
  (source) =>
    observable<A>((sink) => {
      const cfg: RetryConfig = typeof config === 'number' ? { count: config } : config;
      const max = cfg.count ?? Number.POSITIVE_INFINITY;
      const scheduler = cfg.scheduler ?? asyncScheduler;
      let attempt = 0;
      let current: Subscription | null = null;
      let cancelDelay: Teardown | null = null;

      const subscribe = (): void => {
        current = subscribeChild(source, sink, {
          next: (a) => {
            if (cfg.resetOnSuccess) attempt = 0;
            sink.next(a);
          },
          error: (err) => {
            if (attempt >= max) {
              sink.error(err);
              return;
            }
            attempt++;
            current?.unsubscribe();
            const d = typeof cfg.delay === 'function' ? cfg.delay(err, attempt) : cfg.delay;
            if (d === undefined) subscribe();
            else cancelDelay = scheduler.schedule(subscribe, d);
          },
        });
      };
      subscribe();
      return () => {
        cancelDelay?.();
        current?.unsubscribe();
      };
    });

/** Resubscribes on completion, `count` times in total (default: forever). */
export const repeat =
  <A>(count: number = Number.POSITIVE_INFINITY, delayMs?: number, scheduler: Scheduler = asyncScheduler): Operator<A, A> =>
  (source) =>
    observable<A>((sink) => {
      let done = 0;
      let current: Subscription | null = null;
      let cancel: Teardown | null = null;
      const subscribe = (): void => {
        current = subscribeChild(source, sink, {
          complete: () => {
            done++;
            if (done >= count) {
              sink.complete();
              return;
            }
            current?.unsubscribe();
            if (delayMs === undefined) subscribe();
            else cancel = timer(delayMs, undefined, scheduler).subscribe({ complete: subscribe }).unsubscribe;
          },
        });
      };
      if (count <= 0) sink.complete();
      else subscribe();
      return () => {
        cancel?.();
        current?.unsubscribe();
      };
    });

/** Runs `f` when the execution ends for any reason (error, complete, unsubscribe). */
export const finalize =
  <A>(f: () => void): Operator<A, A> =>
  (source) =>
    observable<A>((sink) => {
      const sub = subscribeChild(source, sink);
      return () => {
        sub.unsubscribe();
        f();
      };
    });

/** Errors become a fallback value, then complete. */
export const onErrorReturn = <A, B>(fallback: (err: unknown) => B): Operator<A, A | B> =>
  catchError<A, B>((err) => [fallback(err)]);
