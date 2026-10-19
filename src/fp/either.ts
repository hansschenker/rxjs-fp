/** Either<E, A>: a value that is a failure `E` or a success `A`. */

export interface Left<E> {
  readonly _tag: 'Left';
  readonly left: E;
}
export interface Right<A> {
  readonly _tag: 'Right';
  readonly right: A;
}
export type Either<E, A> = Left<E> | Right<A>;

export const left = <E, A = never>(e: E): Either<E, A> => ({ _tag: 'Left', left: e });
export const right = <A, E = never>(a: A): Either<E, A> => ({ _tag: 'Right', right: a });

export const isLeft = <E, A>(x: Either<E, A>): x is Left<E> => x._tag === 'Left';
export const isRight = <E, A>(x: Either<E, A>): x is Right<A> => x._tag === 'Right';

export const matchEither =
  <E, A, B>(onLeft: (e: E) => B, onRight: (a: A) => B) =>
  (x: Either<E, A>): B =>
    isLeft(x) ? onLeft(x.left) : onRight(x.right);

export const mapEither =
  <A, B>(f: (a: A) => B) =>
  <E>(x: Either<E, A>): Either<E, B> =>
    isLeft(x) ? x : right(f(x.right));

export const mapLeft =
  <E, G>(f: (e: E) => G) =>
  <A>(x: Either<E, A>): Either<G, A> =>
    isLeft(x) ? left(f(x.left)) : x;

export const chainEither =
  <E, A, B>(f: (a: A) => Either<E, B>) =>
  (x: Either<E, A>): Either<E, B> =>
    isLeft(x) ? x : f(x.right);

/** Runs a throwing computation and captures the outcome as a value. */
export const tryCatch = <E, A>(f: () => A, onError: (e: unknown) => E): Either<E, A> => {
  try {
    return right(f());
  } catch (e) {
    return left(onError(e));
  }
};
