import { describe, expect, it, vi } from 'vitest';
import { of, filterMap } from '../src/index.js';
import { MonadMerge, ObservablePlus, some } from '../src/fp/index.js';
import { collect } from './helpers.js';

/**
 * Regression tests for the bugs found in the earlier rxjs-source-v7-fp
 * experiment. Each one is a law or contract that must not break again.
 */
describe('regressions from rxjs-source-v7-fp', () => {
  it('filterMap routes exceptions from f to error (was: reported as unhandled, stream continued)', () => {
    vi.useFakeTimers();
    try {
      const out = collect(
        of(1, 2, 3).pipe(
          filterMap((x) => {
            if (x === 2) throw new Error('boom');
            return some(x);
          }),
        ),
      );
      expect(out).toEqual([1, '#boom']);
      expect(() => vi.runAllTimers()).not.toThrow();
    } finally {
      vi.useRealTimers();
    }
  });

  it('alt(zero, fa) ≡ fa (was: alt = catchError but zero = EMPTY, giving [|])', () => {
    expect(collect(ObservablePlus.alt(ObservablePlus.zero<number>(), () => of(1, 2)))).toEqual([1, 2, '|']);
  });

  it('ap agrees with chain (was: ap via combineLatest → [100, 200])', () => {
    const fab = of((x: number) => x * 10, (x: number) => x * 100);
    const fa = of(1, 2);
    const viaAp = collect(MonadMerge.ap(fab, fa));
    const viaChain = collect(MonadMerge.chain(fab, (f) => MonadMerge.map(fa, f)));
    expect(viaAp).toEqual([10, 20, 100, 200, '|']);
    expect(viaAp).toEqual(viaChain);
  });

  it('the root entry point does not leak fp names (was: export * from fp in rxjs root)', async () => {
    const root = await import('../src/index.js');
    for (const name of ['URI', 'left', 'right', 'chain', 'Do', 'bind', 'some', 'none']) {
      expect(Object.keys(root)).not.toContain(name);
    }
  });
});
