import { cont, flushError, flushWith, fromMachine } from '../core/machine.js';
import { emptyError } from '../core/errors.js';
import type { Operator } from '../core/types.js';

/**
 * The conditional kernel: `throwIfEmpty`, a pure Mealy machine whose
 * decision lives in `flush`. The rest of the family (`every`, `find`,
 * `isEmpty`, `sequenceEqual`, …) is vocabulary.
 */

/** Errors with `factory()` (default: `EmptyError`) if the source completes without emitting. */
export const throwIfEmpty = <A>(factory: () => unknown = emptyError): Operator<A, A> =>
  fromMachine<boolean, A, A>({
    init: () => false,
    step: (_seen, a) => cont(true, [a]),
    flush: (seen) => (seen ? flushWith([]) : flushError(factory())),
  });
