import type { Subscription, Teardown } from './types.js';

const noop: Teardown = () => {};

/**
 * An error raised when one or more finalizers throw during `unsubscribe`.
 * Built with the platform `Error` (a value), never subclassed.
 */
export interface UnsubscriptionError extends Error {
  readonly name: 'UnsubscriptionError';
  readonly errors: readonly unknown[];
}

export const unsubscriptionError = (errors: readonly unknown[]): UnsubscriptionError =>
  Object.assign(new Error(`${errors.length} error(s) occurred during unsubscription`), {
    name: 'UnsubscriptionError' as const,
    errors,
  });

export const isUnsubscriptionError = (e: unknown): e is UnsubscriptionError =>
  typeof e === 'object' && e !== null && (e as { name?: unknown }).name === 'UnsubscriptionError';

/**
 * Creates a Subscription: a frozen record of three closures sharing one
 * private, mutable cell (`closed` + the finalizer set). That cell is the
 * only "dirty" state of an execution's lifecycle; nothing else can touch it.
 *
 * Finalizers live in a flat `Set` (RxJS 5's "flattened disposable tree"),
 * so removal is O(1) and short-lived children never leak into long-lived
 * parents.
 */
export const createSubscription = (initial?: Teardown): Subscription => {
  let closed = false;
  let finalizers: Set<Teardown> | null = initial ? new Set([initial]) : null;

  const unsubscribe = (): void => {
    if (closed) return;
    closed = true;
    const toRun = finalizers;
    finalizers = null;
    if (!toRun) return;
    const errors: unknown[] = [];
    for (const f of toRun) {
      try {
        f();
      } catch (e) {
        if (isUnsubscriptionError(e)) errors.push(...e.errors);
        else errors.push(e);
      }
    }
    if (errors.length > 0) throw unsubscriptionError(errors);
  };

  const add = (finalizer: Teardown): Teardown => {
    if (closed) {
      finalizer();
      return noop;
    }
    (finalizers ??= new Set()).add(finalizer);
    return () => {
      finalizers?.delete(finalizer);
    };
  };

  return Object.freeze({
    get closed() {
      return closed;
    },
    unsubscribe,
    add,
  });
};

/** A subscription that is already closed. */
export const EMPTY_SUBSCRIPTION: Subscription = (() => {
  const s = createSubscription();
  s.unsubscribe();
  return s;
})();

/** Duck-typed check for anything that can be unsubscribed. */
export const isSubscription = (x: unknown): x is Subscription =>
  typeof x === 'object' &&
  x !== null &&
  typeof (x as Subscription).unsubscribe === 'function' &&
  typeof (x as Subscription).add === 'function';
