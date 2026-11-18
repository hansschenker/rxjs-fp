/**
 * Runs the user report in real time with a fake service.
 *
 *   npm run example:users
 *
 * The fake answers after 200 ms; user 4 does not exist, so its request fails
 * three times (the first try and two retries) before the report moves on.
 */
import { asyncScheduler, type Observable } from 'rxjs-fp';
import { defer, throwError, timer } from 'rxjs-fp/creation';
import { concatMap } from 'rxjs-fp/concurrency';
import { as } from 'rxjs-fp/values';
import { userReport, type GetUser, type User } from './users.js';

const names: Readonly<Record<number, string>> = { 1: 'Ada', 2: 'Grace', 3: 'Barbara', 5: 'Margaret' };

const start = Date.now();
const at = (): string => `${String(Date.now() - start).padStart(5)} ms`;

/** After 200 ms: the user, or a "not found" error. Logs every request it receives. */
const fakeGetUser: GetUser = (id) =>
  defer((): Observable<User> => {
    console.log(`${at()}  request user ${id}`);
    const name = names[id];
    return name === undefined
      ? timer(200).pipe(concatMap(() => throwError(() => `user ${id} not found`)))
      : timer(200).pipe(as({ id, name }));
  });

userReport(fakeGetUser, asyncScheduler)([1, 2, 3, 4, 5]).subscribe({
  next: (line) => console.log(`${at()}  ${line}`),
  complete: () => console.log(`${at()}  done`),
});
