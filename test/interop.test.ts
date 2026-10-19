import { describe, expect, it } from 'vitest';
import * as rx from 'rxjs';
import { from, map, of, toArray } from '../src/index.js';
import { firstValueFrom, lastValueFrom, toAsyncIterable } from '../src/index.js';
import { collect } from './helpers.js';

describe('interop with RxJS 7', () => {
  it('RxJS consumes rxjs-fp Observables via @@observable', async () => {
    // rxjs types its interop key as `Symbol.observable`; at runtime it falls back
    // to '@@observable', which rxjs-fp implements. Hence the cast.
    const ours = of(1, 2, 3) as unknown as rx.InteropObservable<number>;
    const values = await rx.firstValueFrom(rx.from(ours).pipe(rx.map((x) => x * 2), rx.toArray()));
    expect(values).toEqual([2, 4, 6]);
  });

  it('rxjs-fp consumes RxJS Observables via subscribe', () => {
    const theirs = rx.of(1, 2, 3);
    expect(collect(from<number>(theirs).pipe(map((x) => x + 1)))).toEqual([2, 3, 4, '|']);
  });

  it('unsubscribing propagates across the boundary', () => {
    let torn = false;
    const theirs = new rx.Observable<number>(() => () => {
      torn = true;
    });
    from(theirs).subscribe().unsubscribe();
    expect(torn).toBe(true);
  });
});

describe('conversion', () => {
  it('firstValueFrom / lastValueFrom', async () => {
    await expect(firstValueFrom(of(1, 2, 3))).resolves.toBe(1);
    await expect(lastValueFrom(of(1, 2, 3))).resolves.toBe(3);
    await expect(firstValueFrom(of<number[]>(), { defaultValue: 0 })).resolves.toBe(0);
    await expect(lastValueFrom(of())).rejects.toMatchObject({ name: 'EmptyError' });
  });

  it('from(Promise) and from(async iterable)', async () => {
    await expect(lastValueFrom(from(Promise.resolve(5)))).resolves.toBe(5);
    async function* gen() {
      yield 1;
      yield 2;
    }
    await expect(lastValueFrom(from(gen()).pipe(toArray()))).resolves.toEqual([1, 2]);
  });

  it('toAsyncIterable', async () => {
    const out: number[] = [];
    for await (const x of toAsyncIterable(of(1, 2, 3))) out.push(x);
    expect(out).toEqual([1, 2, 3]);
  });
});
