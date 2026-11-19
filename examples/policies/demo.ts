/** All six algebraic policies, executed on an explicit virtual clock. */
import {
  additionalRetries,
  after,
  createSubject,
  deadlineAfter,
  defaultSharePolicy,
  defer,
  keepCount,
  limited,
  map,
  of,
  positiveInteger,
  repeat,
  retry,
  share,
  shareReplay,
  take,
  tap,
  throttleTime,
  throwError,
  timeout,
  timer,
  totalRuns,
  unlimited,
  type Observable,
  type RepeatPolicy,
  type RetryPolicy,
  type SharePolicy,
  type ShareReplayPolicy,
  type TimeoutSwitchPolicy,
} from 'rxjs-fp';
import { createVirtualScheduler, type VirtualScheduler } from 'rxjs-fp/testing';

const logStream = <A>(label: string, source: Observable<A>, clock: VirtualScheduler) =>
  source.subscribe({
    next: (value) => console.log(`  t=${clock.now()} ${label}:`, value),
    error: (error) => console.log(`  t=${clock.now()} ${label} error:`, error),
    complete: () => console.log(`  t=${clock.now()} ${label} complete`),
  });

const run = (label: string, scenario: (clock: VirtualScheduler) => void): void => {
  console.log(`\n${label}`);
  const clock = createVirtualScheduler();
  scenario(clock);
  clock.flush();
  console.log(`  pending scheduled work: ${clock.pending()}`);
};

run('repeat: three total runs, 10 ms between runs', (clock) => {
  const policy: RepeatPolicy = { limit: totalRuns(3), delay: after(10) };
  const source = timer(5, undefined, clock).pipe(map(() => 'package'));
  logStream('repeated', source.pipe(repeat(policy, clock)), clock);
});

run('retry: two additional attempts, 10 ms between attempts', (clock) => {
  let attempts = 0;
  const source = defer(() => {
    attempts++;
    console.log(`  t=${clock.now()} connecting attempt ${attempts}`);
    return attempts < 3 ? throwError(() => 'temporary failure') : of('recovered');
  });
  const policy: RetryPolicy = {
    limit: additionalRetries(2),
    delay: after(10),
    counter: keepCount(),
  };
  logStream('retried', source.pipe(retry(policy, clock)), clock);
});

run('share: concurrent observers use one source connection', (clock) => {
  const policy: SharePolicy<number> = {
    ...defaultSharePolicy<number>(),
    connector: createSubject<number>,
  };
  const shared = timer(0, 5, clock).pipe(
    take(3),
    tap((value) => console.log(`  t=${clock.now()} source produced ${value}`)),
    share(policy),
  );
  logStream('first', shared, clock);
  clock.schedule(() => { logStream('second', shared, clock); }, 3);
});

run('shareReplay: retain the completed cache for a late observer', (clock) => {
  const policy: ShareReplayPolicy = {
    replay: { maxValues: limited(positiveInteger(1)), maxAge: unlimited() },
    onNoSubscribers: 'disconnect-and-reset',
  };
  const shared = of('first value', 'latest value').pipe(shareReplay(policy, clock));
  logStream('first', shared, clock);
  clock.schedule(() => { logStream('late', shared, clock); }, 10);
});

run('throttle: leading and trailing values in 3 ms windows', (clock) => {
  const source = timer(0, 1, clock).pipe(take(6));
  logStream('throttled', source.pipe(throttleTime(3, 'leading-and-trailing', clock)), clock);
});

run('timeout: cancel the source at 4 ms and select a typed fallback', (clock) => {
  const policy: TimeoutSwitchPolicy<string, string, { readonly key: string }> = {
    timing: { kind: 'first-only', deadline: deadlineAfter(4) },
    outcome: {
      kind: 'switch-to',
      fallback: (info) => of(`cached ${info.meta.key}; received ${info.seen} values`),
    },
    meta: { key: 'package' },
  };
  const source = timer(8, undefined, clock).pipe(map(() => 'fresh package'));
  logStream('timed', source.pipe(timeout(policy, clock)), clock);
});
