import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { filter, map, type Observable } from '../src/index.js';
import {
  MonadConcat,
  MonadMerge,
  ObservableFilterable as F,
  ObservablePlus as P,
  getConcatMonoid,
  getMergeMonoid,
  none,
  some,
} from '../src/fp/index.js';
import type { VirtualScheduler } from '../src/testing/index.js';
import { bySimultaneity, completingSpec, fromSpec, run, timedSpec, type TimedSpec } from './helpers.js';

/**
 * Property-based law tests on virtual time. Streams are generated as timed
 * specs (values at random delays, ending in complete / error / never), so
 * the laws are checked including timing and termination, not just values.
 *
 * Merge-based laws compare frames as multisets (`bySimultaneity`): merge
 * gives no ordering guarantee between simultaneous events.
 */

const runs = { numRuns: Number(process.env['LAW_RUNS'] ?? 200) };
const intFn = fc.func(fc.integer({ min: -50, max: 50 }));
const specFn = (arb: fc.Arbitrary<TimedSpec>) => fc.func(arb);

type Build = (s: VirtualScheduler) => Observable<number>;
const same = (a: Build, b: Build): void => {
  expect(run(a)).toEqual(run(b));
};
/** Same notifications in the same order, ignoring *when* they happen. */
const sameSequence = (a: Build, b: Build): void => {
  const strip = (b: Build) => run(b).map(({ frame: _frame, ...rest }) => rest);
  expect(strip(a)).toEqual(strip(b));
};
const sameUpToSimultaneity = (a: Build, b: Build): void => {
  expect(bySimultaneity(run(a))).toEqual(bySimultaneity(run(b)));
};

describe('Functor laws', () => {
  it('identity: map(id) ≡ id', () => {
    fc.assert(
      fc.property(timedSpec(), (spec) => same((s) => fromSpec(s)(spec).pipe(map((x) => x)), (s) => fromSpec(s)(spec))),
      runs,
    );
  });

  it('composition: map(g ∘ f) ≡ map(f) ; map(g)', () => {
    fc.assert(
      fc.property(timedSpec(), intFn, intFn, (spec, f, g) =>
        same(
          (s) => fromSpec(s)(spec).pipe(map((x) => g(f(x)))),
          (s) => fromSpec(s)(spec).pipe(map((x: number) => f(x)), map((x: number) => g(x))),
        ),
      ),
      runs,
    );
  });
});

/**
 * MonadMerge is lawful *in time*: both sides of every law produce the same
 * notifications at the same virtual frames (up to simultaneity).
 *
 * MonadConcat is lawful *up to timing*, for error-free streams: both sides
 * produce the same notification sequence, but associativity can change WHEN
 * things happen. Example: in chain(chain(m, f), g) the g-inners of f(x₁)
 * overlap f(x₂), while in chain(m, x => chain(f(x), g)) they cannot.
 * (Errors make that timing difference observable, so they are excluded.)
 */
const errorFree = (n: number) => timedSpec(n).filter((s) => s.end !== 'error');

const monadSuites = [
  { name: 'MonadMerge', M: MonadMerge, eq: sameUpToSimultaneity, spec: timedSpec },
  { name: 'MonadConcat', M: MonadConcat, eq: sameSequence, spec: errorFree },
] as const;

for (const { name, M, eq, spec: arb } of monadSuites) {
  describe(`${name} laws`, () => {
    it('left identity: chain(of(a), f) ≡ f(a)', () => {
      fc.assert(
        fc.property(fc.integer(), specFn(arb(3)), (a, f) =>
          eq(
            (s) => M.chain(M.of(a), (x: number) => fromSpec(s)(f(x))),
            (s) => fromSpec(s)(f(a)),
          ),
        ),
        runs,
      );
    });

    it('right identity: chain(m, of) ≡ m', () => {
      fc.assert(
        fc.property(arb(5), (spec) => eq((s) => M.chain(fromSpec(s)(spec), M.of), (s) => fromSpec(s)(spec))),
        runs,
      );
    });

    it('associativity: chain(chain(m, f), g) ≡ chain(m, x => chain(f(x), g))', () => {
      fc.assert(
        fc.property(arb(3), specFn(arb(3)), specFn(arb(2)), (spec, f, g) =>
          eq(
            (s) => M.chain(M.chain(fromSpec(s)(spec), (x: number) => fromSpec(s)(f(x))), (y: number) => fromSpec(s)(g(y))),
            (s) => M.chain(fromSpec(s)(spec), (x: number) => M.chain(fromSpec(s)(f(x)), (y: number) => fromSpec(s)(g(y)))),
          ),
        ),
        runs,
      );
    });

    it('consistency: ap ≡ ap derived from chain', () => {
      fc.assert(
        fc.property(arb(3), arb(3), intFn, (fs, xs, f) =>
          eq(
            (s) => M.ap(M.map(fromSpec(s)(fs), (k: number) => (x: number) => f(x) + k), fromSpec(s)(xs)),
            (s) => M.chain(M.map(fromSpec(s)(fs), (k: number) => (x: number) => f(x) + k), (h) => M.map(fromSpec(s)(xs), h)),
          ),
        ),
        runs,
      );
    });
  });
}

describe('Alt / Plus laws (alt = merge, zero = EMPTY)', () => {
  it('associativity', () => {
    fc.assert(
      fc.property(timedSpec(), timedSpec(), timedSpec(), (a, b, c) =>
        sameUpToSimultaneity(
          (s) => P.alt(P.alt(fromSpec(s)(a), () => fromSpec(s)(b)), () => fromSpec(s)(c)),
          (s) => P.alt(fromSpec(s)(a), () => P.alt(fromSpec(s)(b), () => fromSpec(s)(c))),
        ),
      ),
      runs,
    );
  });

  it('distributivity: map(alt(x, y), f) ≡ alt(map(x, f), map(y, f))', () => {
    fc.assert(
      fc.property(timedSpec(), timedSpec(), intFn, (a, b, f) =>
        sameUpToSimultaneity(
          (s) => P.map(P.alt(fromSpec(s)(a), () => fromSpec(s)(b)), f),
          (s) => P.alt(P.map(fromSpec(s)(a), f), () => P.map(fromSpec(s)(b), f)),
        ),
      ),
      runs,
    );
  });

  it('left and right identity: alt(zero, fa) ≡ fa ≡ alt(fa, zero)', () => {
    fc.assert(
      fc.property(timedSpec(), (a) => {
        same((s) => P.alt(P.zero<number>(), () => fromSpec(s)(a)), (s) => fromSpec(s)(a));
        same((s) => P.alt(fromSpec(s)(a), () => P.zero<number>()), (s) => fromSpec(s)(a));
      }),
      runs,
    );
  });
});

describe('Monoid laws', () => {
  for (const [name, M, eq] of [
    ['merge', getMergeMonoid<number>(), sameUpToSimultaneity],
    ['concat', getConcatMonoid<number>(), same],
  ] as const) {
    it(`${name}: associativity and identity`, () => {
      fc.assert(
        fc.property(completingSpec(), completingSpec(), completingSpec(), (a, b, c) => {
          eq(
            (s) => M.concat(M.concat(fromSpec(s)(a), fromSpec(s)(b)), fromSpec(s)(c)),
            (s) => M.concat(fromSpec(s)(a), M.concat(fromSpec(s)(b), fromSpec(s)(c))),
          );
          same((s) => M.concat(M.empty, fromSpec(s)(a)), (s) => fromSpec(s)(a));
          same((s) => M.concat(fromSpec(s)(a), M.empty), (s) => fromSpec(s)(a));
        }),
        runs,
      );
    });
  }
});

describe('Filterable laws', () => {
  const pred = fc.func(fc.boolean());
  it('filterMap(some) ≡ id', () => {
    fc.assert(fc.property(timedSpec(), (a) => same((s) => F.filterMap(fromSpec(s)(a), some), (s) => fromSpec(s)(a))), runs);
  });

  it('filterMap(_ => none) ≡ EMPTY values (terminal preserved)', () => {
    fc.assert(
      fc.property(timedSpec(), (a) => {
        expect(run((s) => F.filterMap(fromSpec(s)(a), () => none)).every((e) => e.kind !== 'N')).toBe(true);
      }),
      runs,
    );
  });

  it('filter fusion: filter(p) ; filter(q) ≡ filter(x => p(x) && q(x))', () => {
    fc.assert(
      fc.property(timedSpec(), pred, pred, (a, p, q) =>
        same(
          (s) => fromSpec(s)(a).pipe(filter((x: number) => p(x)), filter((x: number) => q(x))),
          (s) => fromSpec(s)(a).pipe(filter((x: number) => p(x) && q(x))),
        ),
      ),
      runs,
    );
  });

  it('partition splits into complementary halves', () => {
    fc.assert(
      fc.property(completingSpec(), pred, (a, p) => {
        const parts = (s: VirtualScheduler) => F.partition(fromSpec(s)(a), p);
        const values = (o: Build) => run(o).flatMap((e) => (e.kind === 'N' ? [e.value] : []));
        const all = values((s) => fromSpec(s)(a));
        expect([...values((s) => parts(s).left), ...values((s) => parts(s).right)].sort()).toEqual([...all].sort());
      }),
      runs,
    );
  });
});
