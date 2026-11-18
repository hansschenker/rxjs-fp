/**
 * Creation vocabulary: `import { of } from 'rxjs-fp/creation'`.
 *
 *   of(...values)        from(values)
 *   range(start, count)  unfold: emit n, continue with n + 1, until start + count
 *   interval(ms)         timer(ms, ms)
 *   iif(test, a, b)      defer(() => test() ? a : b)
 *
 * Built only on the kernel (checked by test/layering.test.ts), and each
 * definition confirmed against RxJS 7 by test/differential.test.ts.
 */
import { asyncScheduler, defer, from, none, some, timer, unfold } from '../kernel.js';
import type { Observable, ObservableInput, Scheduler } from '../kernel.js';

// The family's kernel members, so the family is importable from one place
export { observable, from, defer, EMPTY, NEVER, throwError, timer, unfold, fromEvent, using } from '../kernel.js';

/** Emits the given values synchronously, then completes. */
export const of = <T extends readonly unknown[]>(...values: T): Observable<T[number]> => from<T[number]>(values);

/** Emits `count` consecutive integers starting at `start`. */
export const range = (start: number, count: number): Observable<number> =>
  unfold(start, (n) => (n < start + count ? some([n, n + 1] as const) : none));

/** Emits 0, 1, 2, … every `period` ms. */
export const interval = (period: number, scheduler: Scheduler = asyncScheduler): Observable<number> =>
  timer(period, period, scheduler);

/** Chooses between two Observables at subscription time. */
export const iif = <A, B>(condition: () => boolean, onTrue: ObservableInput<A>, onFalse: ObservableInput<B>): Observable<A | B> =>
  defer<A | B>(() => (condition() ? onTrue : onFalse));
