import { emptyError } from './core/errors.js';
import type { Observable, Subscription } from './core/types.js';

const NO_DEFAULT: unique symbol = Symbol('rxjs-fp/no-default');

/** Resolves with the first value and unsubscribes. Rejects with `EmptyError` if none (and no default). */
export function firstValueFrom<A>(source: Observable<A>): Promise<A>;
export function firstValueFrom<A, D>(source: Observable<A>, config: { readonly defaultValue: D }): Promise<A | D>;
export function firstValueFrom<A, D>(source: Observable<A>, config?: { readonly defaultValue: D }): Promise<A | D> {
  const def: D | typeof NO_DEFAULT = config ? config.defaultValue : NO_DEFAULT;
  return new Promise<A | D>((resolve, reject) => {
    let done = false;
    let sub: Subscription | undefined;
    sub = source.subscribe({
      next: (a) => {
        if (done) return;
        done = true;
        resolve(a);
        sub?.unsubscribe(); // undefined if the value arrived synchronously; handled below
      },
      error: reject,
      complete: () => {
        if (done) return;
        if (def === NO_DEFAULT) reject(emptyError());
        else resolve(def);
      },
    });
    if (done) sub.unsubscribe();
  });
}

/** Resolves with the last value on completion. Rejects with `EmptyError` if none (and no default). */
export function lastValueFrom<A>(source: Observable<A>): Promise<A>;
export function lastValueFrom<A, D>(source: Observable<A>, config: { readonly defaultValue: D }): Promise<A | D>;
export function lastValueFrom<A, D>(source: Observable<A>, config?: { readonly defaultValue: D }): Promise<A | D> {
  const def: D | typeof NO_DEFAULT = config ? config.defaultValue : NO_DEFAULT;
  return new Promise<A | D>((resolve, reject) => {
    let has = false;
    let last: A | undefined;
    source.subscribe({
      next: (a) => {
        has = true;
        last = a;
      },
      error: reject,
      complete: () => {
        if (has) resolve(last as A);
        else if (def === NO_DEFAULT) reject(emptyError());
        else resolve(def);
      },
    });
  });
}

/** Consumes the source as an async iterable (buffers values that arrive between pulls). */
export const toAsyncIterable = <A>(source: Observable<A>): AsyncIterable<A> => ({
  [Symbol.asyncIterator]: () => {
    const buffer: A[] = [];
    const waiting: Array<{ resolve: (r: IteratorResult<A>) => void; reject: (e: unknown) => void }> = [];
    let finished = false;
    let failure: { readonly error: unknown } | null = null;
    const sub = source.subscribe({
      next: (a) => {
        const w = waiting.shift();
        if (w) w.resolve({ value: a, done: false });
        else buffer.push(a);
      },
      error: (e) => {
        failure = { error: e };
        waiting.splice(0).forEach((w) => w.reject(e));
      },
      complete: () => {
        finished = true;
        waiting.splice(0).forEach((w) => w.resolve({ value: undefined, done: true }));
      },
    });
    return {
      next: () => {
        if (buffer.length > 0) return Promise.resolve({ value: buffer.shift() as A, done: false });
        if (failure) return Promise.reject(failure.error);
        if (finished) return Promise.resolve({ value: undefined, done: true });
        return new Promise<IteratorResult<A>>((resolve, reject) => waiting.push({ resolve, reject }));
      },
      return: () => {
        sub.unsubscribe();
        return Promise.resolve({ value: undefined, done: true });
      },
    };
  },
});
