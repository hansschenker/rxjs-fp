import { describe, expect, it } from 'vitest';
import { EMPTY, filterMap, forkJoin, map, merge, mergeMap, of, throwError, type Observable } from '../src/index.js';
import {
  attempt,
  bind,
  bindTo,
  chain,
  compact,
  Do,
  flatten,
  fromEither,
  fromNullable,
  fromOption,
  fromPredicate,
  getMergeMonoid,
  getOrElse,
  isRight,
  left,
  let_,
  mapEither,
  matchEither,
  matchOption,
  none,
  rethrow,
  right,
  separate,
  some,
  tryCatch,
  type Either,
} from '../src/fp/index.js';
import { collect } from './helpers.js';

/**
 * Every example in docs/fp-vocabulary.md, run as written, so the guide
 * cannot drift from the code.
 */

interface User {
  readonly id: number;
  readonly name: string;
  readonly email?: string;
}
interface Order {
  readonly id: string;
}

const users: ReadonlyArray<User> = [
  { id: 1, name: 'Ada', email: 'ada@example.com' },
  { id: 2, name: 'Grace' },
];
const getUser = (id: number): Observable<User> => (id === 4 ? throwError(() => 'user 4 not found') : of(users[0] as User));
const getOrders = (userId: number): Observable<Order[]> => of([{ id: `${userId}-a` }, { id: `${userId}-b` }]);

describe('docs/fp-vocabulary.md, Option', () => {
  it('fromNullable, matchOption, getOrElse', () => {
    const email = fromNullable(users[1]?.email);
    expect(matchOption(() => 'no email', (e: string) => `mail to ${e}`)(email)).toBe('no email');
    expect(getOrElse(() => 'unknown')(fromNullable(users[0]?.email))).toBe('ada@example.com');
  });

  it('filterMap with fromNullable keeps the users that have an email', () => {
    expect(collect(of(...users).pipe(filterMap((u: User) => fromNullable(u.email))))).toEqual(['ada@example.com', '|']);
  });

  it('fromPredicate', () => {
    const adult = fromPredicate((age: number) => age >= 18);
    expect([adult(20), adult(12)]).toEqual([some(20), none]);
  });
});

describe('docs/fp-vocabulary.md, Either', () => {
  it('tryCatch and matchEither', () => {
    const parse = (s: string) => tryCatch(() => JSON.parse(s) as unknown, () => 'not JSON');
    const describe = matchEither(
      (e: string) => `failed: ${e}`,
      (v: unknown) => `parsed ${JSON.stringify(v)}`,
    );
    expect(describe(parse('{"a":1}'))).toBe('parsed {"a":1}');
    expect(describe(parse('{'))).toBe('failed: not JSON');
  });

  it('mapEither changes only a right', () => {
    expect(mapEither((n: number) => n * 2)(right(21))).toEqual(right(42));
    expect(mapEither((n: number) => n * 2)(left('e'))).toEqual(left('e'));
  });
});

describe('docs/fp-vocabulary.md, attempt and rethrow', () => {
  it('attempt: values become right, an error becomes a final left and the stream completes', () => {
    expect(collect(merge(of(1, 2), throwError(() => 'boom')).pipe(attempt()))).toEqual([right(1), right(2), left('boom'), '|']);
  });

  it('attempt inside the projection keeps the outer stream going', () => {
    const out = collect(
      of(1, 4, 5).pipe(
        mergeMap((id) => getUser(id).pipe(attempt())),
        map((r) => (isRight(r) ? `loaded ${r.right.name}` : `failed: ${String(r.left)}`)),
      ),
    );
    expect(out).toEqual(['loaded Ada', 'failed: user 4 not found', 'loaded Ada', '|']);
  });

  it('all settled: forkJoin with attempt on each request', () => {
    const out = collect(forkJoin([getUser(1).pipe(attempt()), getUser(4).pipe(attempt())]));
    expect(out).toEqual([[right(users[0]), left('user 4 not found')], '|']);
  });

  it('rethrow turns a left back into an error', () => {
    const results: Observable<Either<string, number>> = of(right(1), left('boom'), right(3));
    expect(collect(results.pipe(rethrow()))).toEqual([1, '#boom']);
  });

  it('separate splits results into failures and successes', () => {
    const results = of(right(1), left('a'), right(2));
    const { left: failures, right: successes } = separate(results);
    expect(collect(failures)).toEqual(['a', '|']);
    expect(collect(successes)).toEqual([1, 2, '|']);
  });
});

describe('docs/fp-vocabulary.md, do-notation', () => {
  it('Do, bind, let_ build up a record step by step', () => {
    const summary$ = Do.pipe(
      bind('user', () => getUser(1)),
      bind('orders', ({ user }) => getOrders(user.id)),
      let_('count', ({ orders }) => orders.length),
    );
    expect(collect(summary$)).toEqual([
      { user: users[0], orders: [{ id: '1-a' }, { id: '1-b' }], count: 2 },
      '|',
    ]);
  });

  it('the same pipeline with plain RxJS operators', () => {
    const summary$ = getUser(1).pipe(
      mergeMap((user) => getOrders(user.id).pipe(map((orders) => ({ user, orders })))),
      map(({ user, orders }) => ({ user, orders, count: orders.length })),
    );
    expect(collect(summary$)).toEqual([{ user: users[0], orders: [{ id: '1-a' }, { id: '1-b' }], count: 2 }, '|']);
  });

  it('bindTo starts from an existing stream', () => {
    expect(collect(of(1).pipe(bindTo('id'), bind('user', ({ id }) => getUser(id))))).toEqual([{ id: 1, user: users[0] }, '|']);
  });

  it('edge case: bind runs once per value, so a step with two values doubles the records', () => {
    const out = collect(Do.pipe(bind('x', () => of(1, 2)), bind('y', () => of('a', 'b'))));
    expect(out).toEqual([{ x: 1, y: 'a' }, { x: 1, y: 'b' }, { x: 2, y: 'a' }, { x: 2, y: 'b' }, '|']);
  });
});

describe('docs/fp-vocabulary.md, the remaining helpers', () => {
  it('chain = mergeMap, flatten = mergeAll, compact = filterMap(identity)', () => {
    expect(collect(of(1, 2).pipe(chain((n) => of(n, n * 10))))).toEqual([1, 10, 2, 20, '|']);
    expect(collect(of(of(1), of(2, 3)).pipe(flatten))).toEqual([1, 2, 3, '|']);
    expect(collect(of(some(1), none, some(3)).pipe(compact))).toEqual([1, 3, '|']);
  });

  it('fromOption and fromEither', () => {
    expect(collect(fromOption(some(1)))).toEqual([1, '|']);
    expect(collect(fromOption(none))).toEqual(['|']);
    expect(collect(fromEither(left('e')))).toEqual(['#e']);
  });

  it('getMergeMonoid folds a list of streams into one', () => {
    const M = getMergeMonoid<number>();
    expect(collect([of(1), of(2), EMPTY].reduce(M.concat, M.empty))).toEqual([1, 2, '|']);
  });
});
