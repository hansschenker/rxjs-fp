import type { Kind, URIS } from './hkt.js';
import type { Option } from './option.js';
import type { Either } from './either.js';

/**
 * Typeclass interfaces (data-first, fp-ts 2 style). Every instance shipped
 * by rxjs-fp is accompanied by property tests for its laws (see test/laws).
 */

export interface Semigroup<A> {
  /** Associative: concat(concat(x, y), z) ≡ concat(x, concat(y, z)) */
  readonly concat: (x: A, y: A) => A;
}

export interface Monoid<A> extends Semigroup<A> {
  /** Identity: concat(empty, x) ≡ x ≡ concat(x, empty) */
  readonly empty: A;
}

export interface Functor<F extends URIS> {
  readonly URI: F;
  /** map(fa, id) ≡ fa;  map(fa, g∘f) ≡ map(map(fa, f), g) */
  readonly map: <A, B>(fa: Kind<F, A>, f: (a: A) => B) => Kind<F, B>;
}

export interface Apply<F extends URIS> extends Functor<F> {
  readonly ap: <A, B>(fab: Kind<F, (a: A) => B>, fa: Kind<F, A>) => Kind<F, B>;
}

export interface Applicative<F extends URIS> extends Apply<F> {
  readonly of: <A>(a: A) => Kind<F, A>;
}

export interface Chain<F extends URIS> extends Apply<F> {
  /** chain(chain(fa, f), g) ≡ chain(fa, a => chain(f(a), g)) */
  readonly chain: <A, B>(fa: Kind<F, A>, f: (a: A) => Kind<F, B>) => Kind<F, B>;
}

/**
 * Monad laws: left identity chain(of(a), f) ≡ f(a), right identity
 * chain(fa, of) ≡ fa, plus Chain associativity. Consistency: `ap` must
 * equal the `ap` derived from `chain`.
 */
export interface Monad<F extends URIS> extends Applicative<F>, Chain<F> {}

export interface Alt<F extends URIS> extends Functor<F> {
  /** associative; map distributes: map(alt(x, y), f) ≡ alt(map(x, f), map(y, f)) */
  readonly alt: <A>(fx: Kind<F, A>, fy: () => Kind<F, A>) => Kind<F, A>;
}

export interface Plus<F extends URIS> extends Alt<F> {
  /** alt(zero, fa) ≡ fa ≡ alt(fa, zero);  map(zero, f) ≡ zero */
  readonly zero: <A>() => Kind<F, A>;
}

export interface Alternative<F extends URIS> extends Applicative<F>, Plus<F> {}

export interface Separated<L, R> {
  readonly left: L;
  readonly right: R;
}

export interface Compactable<F extends URIS> {
  readonly URI: F;
  readonly compact: <A>(fa: Kind<F, Option<A>>) => Kind<F, A>;
  readonly separate: <E, A>(fa: Kind<F, Either<E, A>>) => Separated<Kind<F, E>, Kind<F, A>>;
}

export interface Filterable<F extends URIS> extends Functor<F>, Compactable<F> {
  readonly filter: <A>(fa: Kind<F, A>, predicate: (a: A) => boolean) => Kind<F, A>;
  readonly filterMap: <A, B>(fa: Kind<F, A>, f: (a: A) => Option<B>) => Kind<F, B>;
  readonly partition: <A>(fa: Kind<F, A>, predicate: (a: A) => boolean) => Separated<Kind<F, A>, Kind<F, A>>;
  readonly partitionMap: <A, B, C>(fa: Kind<F, A>, f: (a: A) => Either<B, C>) => Separated<Kind<F, B>, Kind<F, C>>;
}
