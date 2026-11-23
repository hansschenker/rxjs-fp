import { describe, expect, it } from 'vitest';
import { createSubject } from 'rxjs-fp/multicast';
import { map, scan } from 'rxjs-fp/values';
import { createVirtualScheduler, record, type VirtualScheduler } from 'rxjs-fp/testing';
import { isLeft, right } from 'rxjs-fp/fp';
import { fakeLookup, hitsFor, typeTheScript } from './scenario.js';
import { describeOutcome, searchOnce, searchResults } from './search.js';

/**
 * The same code as the demo, in virtual time: no waiting, exact frames.
 * The fake service is an argument, so no mocking library is needed.
 */
const serviceLog = (scheduler: VirtualScheduler, requests: string[]) =>
  fakeLookup(scheduler, (query) => requests.push(`${scheduler.now()}: "${query}"`));

describe('search-as-you-type, in virtual time', () => {
  it('one request per settled query, a retry through the outage, two deadlines for the silent one, answers shared', () => {
    const clock = createVirtualScheduler();
    const requests: string[] = [];
    const keystrokes = createSubject<string>();
    const results = searchResults(serviceLog(clock, requests), clock)(keystrokes);

    const lines = record(results.pipe(map(describeOutcome)), clock);
    const answered = record(results.pipe(scan((n, { result }) => (isLeft(result) ? n : n + 1), 0)), clock);
    typeTheScript(keystrokes, clock);
    clock.flush();

    expect(requests).toEqual([
      '560: "rxjs"', // four keystrokes, quiet from 260, settled at 560
      '1800: "rx"', // inside the outage: fails at 1920
      '1970: "rx"', // the one retry, 50 ms later; answered at 2090
      '3300: "slow"', // the deadline passes at 3700
      '3750: "slow"', // the retry; the deadline passes at 4150, then the failure is a value
      '6300: "rxjs"', // the same query again is a new request: nobody asked to keep old answers
    ]);
    expect(lines).toEqual([
      { frame: 680, kind: 'N', value: '"rxjs": RxJS 7 in depth | rxjs-fp: no class' },
      { frame: 2090, kind: 'N', value: '"rx": RxJS 7 in depth | rxjs-fp: no class | Rx.NET' },
      { frame: 4150, kind: 'N', value: '"slow": no answer within 400 ms' },
      { frame: 6420, kind: 'N', value: '"rxjs": RxJS 7 in depth | rxjs-fp: no class' },
      { frame: 7000, kind: 'C' },
    ]);
    // the second consumer saw every answer without causing a single extra request
    expect(answered).toEqual([
      { frame: 680, kind: 'N', value: 1 },
      { frame: 2090, kind: 'N', value: 2 },
      { frame: 4150, kind: 'N', value: 2 },
      { frame: 6420, kind: 'N', value: 3 },
      { frame: 7000, kind: 'C' },
    ]);
  });
});

describe('one named decision, tested on its own', () => {
  it('searchOnce: a request during the outage fails once and the retry answers', () => {
    const clock = createVirtualScheduler(1800);
    const requests: string[] = [];
    const out = record(searchOnce(serviceLog(clock, requests), clock)('rx'), clock);
    clock.flush();
    expect(requests).toEqual(['1800: "rx"', '1970: "rx"']);
    expect(out).toEqual([
      { frame: 2090, kind: 'N', value: { query: 'rx', result: right(hitsFor('rx')) } },
      { frame: 2090, kind: 'C' },
    ]);
  });

  it('searchOnce: a query that is never answered becomes one failure value after two deadlines', () => {
    const clock = createVirtualScheduler();
    const requests: string[] = [];
    const out = record(searchOnce(serviceLog(clock, requests), clock)('slow').pipe(map(describeOutcome)), clock);
    clock.flush();
    expect(requests).toEqual(['0: "slow"', '450: "slow"']);
    expect(out).toEqual([
      { frame: 850, kind: 'N', value: '"slow": no answer within 400 ms' },
      { frame: 850, kind: 'C' },
    ]);
  });
});
