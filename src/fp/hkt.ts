import type { Observable } from '../core/types.js';
import type { Option } from './option.js';
import type { Either } from './either.js';

/**
 * Lightweight higher-kinded types via a URI registry (the fp-ts encoding).
 * Extend `URItoKind` with module augmentation to register more types.
 */
export interface URItoKind<A> {
  readonly Observable: Observable<A>;
  readonly Option: Option<A>;
}

export interface URItoKind2<E, A> {
  readonly Either: Either<E, A>;
}

export type URIS = keyof URItoKind<unknown>;
export type URIS2 = keyof URItoKind2<unknown, unknown>;
export type Kind<F extends URIS, A> = URItoKind<A>[F];
export type Kind2<F extends URIS2, E, A> = URItoKind2<E, A>[F];
