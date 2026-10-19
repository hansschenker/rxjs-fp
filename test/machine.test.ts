import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { cont, flushWith, fromMachine, halt, runMachine, type Machine, of, take, scan } from '../src/index.js';
import { collect } from './helpers.js';

/** Operators as pure Mealy machines can be tested without any Observable. */
describe('Mealy machines', () => {
  const runningMax: Machine<number, number, number> = {
    init: () => Number.NEGATIVE_INFINITY,
    step: (m, x) => (x > m ? cont(x, [x]) : cont(m)),
  };

  it('runMachine evaluates a machine as a pure function', () => {
    expect(runMachine(runningMax, [1, 3, 2, 5, 4]).output).toEqual([1, 3, 5]);
  });

  it('fromMachine turns it into an operator with identical behaviour', () => {
    expect(collect(of(1, 3, 2, 5, 4).pipe(fromMachine(runningMax)))).toEqual([1, 3, 5, '|']);
  });

  it('each subscription gets its own state (cold)', () => {
    const op = of(1, 2).pipe(fromMachine(runningMax));
    expect(collect(op)).toEqual(collect(op));
  });

  it('halt completes early; flush emits on completion', () => {
    const firstEven: Machine<null, number, number> = { init: () => null, step: (s, x) => (x % 2 === 0 ? halt(s, [x]) : cont(s)) };
    expect(collect(of(1, 3, 4, 6).pipe(fromMachine(firstEven)))).toEqual([4, '|']);
    const sum: Machine<number, number, number> = { init: () => 0, step: (s, x) => cont(s + x), flush: (s) => flushWith([s]) };
    expect(runMachine(sum, [1, 2, 3]).output).toEqual([6]);
  });

  it('property: take(n) emits exactly min(n, length) values', () => {
    fc.assert(
      fc.property(fc.array(fc.integer()), fc.nat(10), (xs, n) => {
        const out = collect(of(...xs).pipe(take(n)));
        expect(out.slice(0, -1)).toEqual(xs.slice(0, n));
        expect(out.at(-1)).toBe('|');
      }),
    );
  });

  it('property: scan agrees with Array.reduce prefixes', () => {
    fc.assert(
      fc.property(fc.array(fc.integer({ min: -100, max: 100 })), (xs) => {
        const prefixes = xs.map((_, i) => xs.slice(0, i + 1).reduce((a, b) => a + b, 0));
        expect(collect(of(...xs).pipe(scan((a: number, b: number) => a + b, 0)))).toEqual([...prefixes, '|']);
      }),
    );
  });
});
