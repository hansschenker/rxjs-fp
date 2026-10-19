/** Option<A>: a value that may be absent, as a tagged union of plain records. */

export interface None {
  readonly _tag: 'None';
}
export interface Some<A> {
  readonly _tag: 'Some';
  readonly value: A;
}
export type Option<A> = None | Some<A>;

export const none: Option<never> = Object.freeze({ _tag: 'None' });
export const some = <A>(value: A): Option<A> => ({ _tag: 'Some', value });

export const isNone = <A>(o: Option<A>): o is None => o._tag === 'None';
export const isSome = <A>(o: Option<A>): o is Some<A> => o._tag === 'Some';

export const fromNullable = <A>(a: A): Option<NonNullable<A>> => (a == null ? none : some(a as NonNullable<A>));

export function fromPredicate<A, B extends A>(refinement: (a: A) => a is B): (a: A) => Option<B>;
export function fromPredicate<A>(predicate: (a: A) => boolean): (a: A) => Option<A>;
export function fromPredicate<A>(predicate: (a: A) => boolean) {
  return (a: A): Option<A> => (predicate(a) ? some(a) : none);
}

export const matchOption =
  <A, B>(onNone: () => B, onSome: (a: A) => B) =>
  (o: Option<A>): B =>
    isNone(o) ? onNone() : onSome(o.value);

export const mapOption =
  <A, B>(f: (a: A) => B) =>
  (o: Option<A>): Option<B> =>
    isNone(o) ? none : some(f(o.value));

export const chainOption =
  <A, B>(f: (a: A) => Option<B>) =>
  (o: Option<A>): Option<B> =>
    isNone(o) ? none : f(o.value);

export const getOrElse =
  <A>(onNone: () => A) =>
  (o: Option<A>): A =>
    isNone(o) ? onNone() : o.value;

export const toNullable = <A>(o: Option<A>): A | null => (isNone(o) ? null : o.value);
export const toUndefined = <A>(o: Option<A>): A | undefined => (isNone(o) ? undefined : o.value);
