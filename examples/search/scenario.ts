/**
 * The scripted world the demo and the test share: a lookup service that
 * answers after 120 ms, is down between 1700 and 1900 on the clock, and never
 * answers "slow"; and a user who types in bursts. Nothing here keeps state:
 * what the service does is a function of the query and the clock.
 */
import type { Observable, Scheduler } from 'rxjs-fp';
import { defer, throwError, timer } from 'rxjs-fp/creation';
import { concatMap } from 'rxjs-fp/concurrency';
import { as } from 'rxjs-fp/values';
import type { Subject } from 'rxjs-fp/multicast';
import type { Hit, Lookup } from './search.js';

const catalogue: Readonly<Record<string, readonly Hit[]>> = Object.freeze({
  rxjs: [{ id: 1, title: 'RxJS 7 in depth' }, { id: 2, title: 'rxjs-fp: no class' }],
  rx: [{ id: 1, title: 'RxJS 7 in depth' }, { id: 2, title: 'rxjs-fp: no class' }, { id: 3, title: 'Rx.NET' }],
});

/** What the service knows about a query. */
export const hitsFor = (query: string): readonly Hit[] => catalogue[query] ?? [];

/** The service is down from 1700 up to 1900 on the clock: a request started then fails after 120 ms. */
export const outage = Object.freeze({ from: 1700, until: 1900 });

const answersAfter = 120;

/** The fake service. Calls `onRequest` for every request it receives. */
export const fakeLookup =
  (scheduler: Scheduler, onRequest: (query: string) => void): Lookup =>
  (query) =>
    defer((): Observable<readonly Hit[]> => {
      onRequest(query);
      const now = scheduler.now();
      if (now >= outage.from && now < outage.until) {
        return timer(answersAfter, undefined, scheduler).pipe(concatMap(() => throwError(() => new Error('503 service unavailable'))));
      }
      if (query === 'slow') return timer(60_000, undefined, scheduler).pipe(as<readonly Hit[]>([]));
      return timer(answersAfter, undefined, scheduler).pipe(as(hitsFor(query)));
    });

/** What the user types, and when. */
export const typing: ReadonlyArray<readonly [at: number, text: string]> = [
  [0, 'r'], [100, 'rx'], [200, 'rxj'], [260, 'rxjs'], // one burst: one request, for "rxjs"
  [1500, 'rx'], // asked during the outage: fails once, the retry succeeds
  [3000, 'slow'], // never answered: two deadlines pass, then the failure is a value
  [6000, 'rxjs'], // asked again, fetched again
];

/** Feeds the script into `keystrokes` on the scheduler and closes the search at 7000. */
export const typeTheScript = (keystrokes: Subject<string>, scheduler: Scheduler): void => {
  for (const [at, text] of typing) scheduler.schedule(() => keystrokes.next(text), at);
  scheduler.schedule(() => keystrokes.complete(), 7000);
};
