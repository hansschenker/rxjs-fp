import { describe, expect, it } from 'vitest';
import type { Observable } from 'rxjs-fp';
import { defer, throwError, timer } from 'rxjs-fp/creation';
import { concatMap } from 'rxjs-fp/concurrency';
import { as } from 'rxjs-fp/values';
import { createVirtualScheduler, record, type VirtualScheduler } from 'rxjs-fp/testing';
import { left, right } from 'rxjs-fp/fp';
import { fetchUserReliably, userReport, type GetUser, type User } from './users.js';

/**
 * The same code as the demo, in virtual time: no waiting, exact times.
 * The fake service is an argument, so no mocking library is needed.
 */
const names: Readonly<Record<number, string>> = { 1: 'Ada', 2: 'Grace', 3: 'Barbara', 5: 'Margaret' };

/** After 200 ms: the user, or a "not found" error. Writes every request into `requests`. */
const fakeService = (scheduler: VirtualScheduler, requests: string[]): GetUser => (id) =>
  defer((): Observable<User> => {
    requests.push(`${scheduler.now()}: user ${id}`);
    const name = names[id];
    return name === undefined
      ? timer(200, undefined, scheduler).pipe(concatMap(() => throwError(() => `user ${id} not found`)))
      : timer(200, undefined, scheduler).pipe(as({ id, name }));
  });

describe('the user report, in virtual time', () => {
  it('loads users in order, retries user 4 twice, reports its failure and goes on', () => {
    const scheduler = createVirtualScheduler();
    const requests: string[] = [];
    const out = record(userReport(fakeService(scheduler, requests), scheduler)([1, 2, 3, 4, 5]), scheduler);
    scheduler.flush();

    expect(requests).toEqual([
      '0: user 1',
      '200: user 2',
      '400: user 3',
      '600: user 4', // fails at 800
      '900: user 4', // first retry, 100 ms later; fails at 1100
      '1200: user 4', // second retry; fails at 1400, then the error becomes a value
      '1400: user 5',
    ]);
    expect(out).toEqual([
      { frame: 700, kind: 'N', value: 'loaded Ada' }, // loaded at 200, shifted by 500
      { frame: 900, kind: 'N', value: 'loaded Grace' },
      { frame: 1100, kind: 'N', value: 'loaded Barbara' },
      { frame: 1900, kind: 'N', value: 'failed: user 4 not found' },
      { frame: 2100, kind: 'N', value: 'loaded Margaret' },
      { frame: 2100, kind: 'C' },
    ]);
  });
});

describe('one named decision, tested on its own', () => {
  it('fetchUserReliably: a user that exists is a right value', () => {
    const scheduler = createVirtualScheduler();
    const out = record(fetchUserReliably(fakeService(scheduler, []), scheduler)(1), scheduler);
    scheduler.flush();
    expect(out).toEqual([
      { frame: 200, kind: 'N', value: right({ id: 1, name: 'Ada' }) },
      { frame: 200, kind: 'C' },
    ]);
  });

  it('fetchUserReliably: three failed tries become one left value, not an error', () => {
    const scheduler = createVirtualScheduler();
    const requests: string[] = [];
    const out = record(fetchUserReliably(fakeService(scheduler, requests), scheduler)(4), scheduler);
    scheduler.flush();
    expect(requests).toEqual(['0: user 4', '300: user 4', '600: user 4']);
    expect(out).toEqual([
      { frame: 800, kind: 'N', value: left('user 4 not found') },
      { frame: 800, kind: 'C' },
    ]);
  });
});
