/**
 * A simple user test of rxjs-fp: load users one after another, retry each
 * failed request, keep going when one user cannot be loaded, and shift the
 * results by 500 ms.
 *
 * Written the recommended way (docs/kernel.md, "The naming style"):
 * every decision is a named function defined before the pipeline, and the
 * service and the scheduler are arguments, so the same code runs in the app
 * and in a test with a fake service and virtual time.
 */
import type { Observable, Scheduler } from 'rxjs-fp';
import { from } from 'rxjs-fp/creation';
import { concatMap } from 'rxjs-fp/concurrency';
import { delay } from 'rxjs-fp/time';
import { map } from 'rxjs-fp/values';
import { retryWith, timesWithDelay } from 'rxjs-fp/errors';
import { attempt, isRight, type Either } from 'rxjs-fp/fp';

export interface User {
  readonly id: number;
  readonly name: string;
}

/** The one dependency: how to fetch a user. The app passes HTTP, a test passes a fake. */
export type GetUser = (id: number) => Observable<User>;

// --- named decisions -----------------------------------------------------------

/** One user: two retries 100 ms apart, then the error becomes a value instead of ending the stream. */
export const fetchUserReliably =
  (getUser: GetUser, scheduler: Scheduler) =>
  (id: number): Observable<Either<unknown, User>> =>
    getUser(id).pipe(retryWith(timesWithDelay(2, 100), scheduler), attempt());

/** All users, one after another, in the order of their ids. */
export const loadUsersInOrder = (getUser: GetUser, scheduler: Scheduler) => concatMap(fetchUserReliably(getUser, scheduler));

/** Every result arrives `ms` later. */
export const shiftBy = (ms: number, scheduler: Scheduler) => delay<Either<unknown, User>>(ms, scheduler);

/** One line per user. */
export const describeResult = map((result: Either<unknown, User>) =>
  isRight(result) ? `loaded ${result.right.name}` : `failed: ${String(result.left)}`,
);

// --- the pipeline ----------------------------------------------------------------

export const userReport =
  (getUser: GetUser, scheduler: Scheduler) =>
  (userIds: readonly number[]): Observable<string> =>
    from(userIds).pipe(loadUsersInOrder(getUser, scheduler), shiftBy(500, scheduler), describeResult);
