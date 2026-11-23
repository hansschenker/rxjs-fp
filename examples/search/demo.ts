/**
 * Runs the search on a virtual clock and prints every request and every
 * answer with the frame it happened at:
 *
 *   npm run example:search
 *
 * The pipeline and the fake service take the scheduler as an argument. Pass
 * `asyncScheduler` to both instead and they run in real time (then there is
 * nothing to flush: the timers fire on their own).
 */
import { createSubject } from 'rxjs-fp/multicast';
import { scan } from 'rxjs-fp/values';
import { isLeft } from 'rxjs-fp/fp';
import { createVirtualScheduler } from 'rxjs-fp/testing';
import { describeOutcome, searchResults } from './search.js';
import { fakeLookup, typeTheScript } from './scenario.js';

const clock = createVirtualScheduler();
const log = (line: string): void => console.log(`t=${String(clock.now()).padStart(5)}  ${line}`);

const keystrokes = createSubject<string>();
const results = searchResults(fakeLookup(clock, (query) => log(`request "${query}"`)), clock)(keystrokes);

// two consumers of the shared answers: the result list and a counter
results.subscribe({ next: (outcome) => log(describeOutcome(outcome)), complete: () => log('search closed') });
results.pipe(scan((n, { result }) => (isLeft(result) ? n : n + 1), 0)).subscribe((n) => log(`  answered searches so far: ${n}`));

typeTheScript(keystrokes, clock);
clock.flush();
