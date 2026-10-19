import { describe, expectTypeOf, it } from 'vitest';
import {
  combineLatest,
  filter,
  forkJoin,
  map,
  merge,
  of,
  pipe,
  startWith,
  withLatestFrom,
  zip,
  type Observable,
  type Operator,
} from '../src/index.js';
import { Do, bind, let_, type Option } from '../src/fp/index.js';

/** Compile-time checks: these tests pass if the file type-checks. */
describe('static types', () => {
  it('of infers widened element types', () => {
    expectTypeOf(of(1, 2)).toEqualTypeOf<Observable<number>>();
    expectTypeOf(of(1, 'a')).toEqualTypeOf<Observable<number | string>>();
  });

  it('pipe threads types through operators', () => {
    const o = of(1, 2).pipe(
      map((x) => `${x}`),
      map((s) => s.length > 0),
    );
    expectTypeOf(o).toEqualTypeOf<Observable<boolean>>();
    const f: Operator<number, string> = map((x: number) => x.toFixed(1));
    expectTypeOf(pipe(of(1), f)).toEqualTypeOf<Observable<string>>();
  });

  it('filter narrows with refinements', () => {
    const o = of<(string | number)[]>(1, 'a').pipe(filter((x): x is string => typeof x === 'string'));
    expectTypeOf(o).toEqualTypeOf<Observable<string>>();
  });

  it('combinators produce tuple and union types', () => {
    const a = of(1);
    const b = of('x');
    const c = of(true);
    expectTypeOf(combineLatest([a, b, c] as const)).toEqualTypeOf<Observable<[number, string, boolean]>>();
    expectTypeOf(zip([a, b] as const)).toEqualTypeOf<Observable<[number, string]>>();
    expectTypeOf(forkJoin([a, c] as const)).toEqualTypeOf<Observable<[number, boolean]>>();
    expectTypeOf(merge(a, b)).toEqualTypeOf<Observable<number | string>>();
    expectTypeOf(a.pipe(withLatestFrom(b, c))).toEqualTypeOf<Observable<[number, string, boolean]>>();
    expectTypeOf(a.pipe(startWith(null))).toEqualTypeOf<Observable<number | null>>();
  });

  it('do-notation builds typed records', () => {
    const r = Do.pipe(
      bind('a', () => of(1)),
      bind('b', ({ a }) => of(`${a}`)),
      let_('c', ({ a, b }) => a + b.length),
    );
    expectTypeOf(r).toEqualTypeOf<Observable<{ readonly a: number; readonly b: string; readonly c: number }>>();
  });

  it('Option is a tagged union', () => {
    expectTypeOf<Option<number>>().toEqualTypeOf<{ readonly _tag: 'None' } | { readonly _tag: 'Some'; readonly value: number }>();
  });
});
