import { describe, expect, it } from 'vitest';
import * as rx from 'rxjs';
import { from, map, of, throwError, toArray } from '../src/index.js';
import { firstValueFrom, firstValueFromOr, lastValueFrom, lastValueFromOr, toAsyncIterable } from '../src/index.js';
import { collect } from './helpers.js';

describe('interop with RxJS 7', () => {
  it('RxJS reads rxjs-fp Observables through a hand-written wrapper: rxjs-fp implements no interop protocol', async () => {
    expect(() => rx.from(of(1, 2, 3) as unknown as rx.ObservableInput<number>)).toThrow(TypeError);
    const bridged = new rx.Observable<number>((s) => {
      const sub = of(1, 2, 3).subscribe(s);
      return () => sub.unsubscribe();
    });
    const values = await rx.firstValueFrom(bridged.pipe(rx.map((x) => x * 2), rx.toArray()));
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

  it('from reports an input it cannot convert on subscription, where RxJS throws at call time (README)', () => {
    expect(() => rx.from(42 as unknown as rx.ObservableInput<number>)).toThrow(TypeError);
    const ours = from(42 as never);
    expect(collect(ours)).toEqual(['#rxjs-fp: cannot convert 42 to an Observable']);
  });
});

describe('conversion', () => {
  it('firstValueFrom / lastValueFrom', async () => {
    await expect(firstValueFrom(of(1, 2, 3))).resolves.toBe(1);
    await expect(lastValueFrom(of(1, 2, 3))).resolves.toBe(3);
    await expect(firstValueFromOr(of<number[]>(), 0)).resolves.toBe(0);
    await expect(firstValueFromOr(of(1, 2), 0)).resolves.toBe(1);
    await expect(lastValueFromOr(of<number[]>(), 0)).resolves.toBe(0);
    await expect(lastValueFromOr(of(1, 2), 0)).resolves.toBe(2);
    await expect(firstValueFrom(of())).rejects.toMatchObject({ name: 'EmptyError' });
    await expect(firstValueFromOr(throwError(() => 'boom'), 0)).rejects.toBe('boom');
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
