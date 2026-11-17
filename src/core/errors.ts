/**
 * Named error values. They are plain `Error` objects with a `name` tag
 * (built with the platform constructor, never subclassed), so they keep
 * stack traces and can be told apart with the type guards below.
 */

export interface TaggedError<N extends string> extends Error {
  readonly name: N;
}

const tagged =
  <N extends string>(name: N, defaultMessage: string) =>
  (message: string = defaultMessage): TaggedError<N> =>
    Object.assign(new Error(message), { name });

const is =
  <N extends string>(name: N) =>
  (e: unknown): e is TaggedError<N> =>
    typeof e === 'object' && e !== null && (e as { name?: unknown }).name === name;

export type EmptyError = TaggedError<'EmptyError'>;
export const emptyError = tagged('EmptyError', 'no elements in sequence');
export const isEmptyError = is('EmptyError');

/** What a timeout knew when it fired. `TimeoutError` carries it as `info`. */
export interface TimeoutInfo<A = unknown> {
  /** Number of values seen before the timeout. */
  readonly seen: number;
  /** The last value seen, or `null` if none. */
  readonly lastValue: A | null;
}

export interface TimeoutError extends TaggedError<'TimeoutError'> {
  readonly info: TimeoutInfo;
}
export const timeoutError = (
  info: TimeoutInfo = { seen: 0, lastValue: null },
  message: string = 'Timeout has occurred',
): TimeoutError => Object.assign(new Error(message), { name: 'TimeoutError' as const, info });
export const isTimeoutError = (e: unknown): e is TimeoutError => is('TimeoutError')(e);

export type ArgumentOutOfRangeError = TaggedError<'ArgumentOutOfRangeError'>;
export const argumentOutOfRangeError = tagged('ArgumentOutOfRangeError', 'argument out of range');
export const isArgumentOutOfRangeError = is('ArgumentOutOfRangeError');

export type ObjectUnsubscribedError = TaggedError<'ObjectUnsubscribedError'>;
export const objectUnsubscribedError = tagged('ObjectUnsubscribedError', 'object unsubscribed');
export const isObjectUnsubscribedError = is('ObjectUnsubscribedError');

export type NotFoundError = TaggedError<'NotFoundError'>;
export const notFoundError = tagged('NotFoundError', 'No matching values');
export const isNotFoundError = is('NotFoundError');

export type SequenceError = TaggedError<'SequenceError'>;
export const sequenceError = tagged('SequenceError', 'Too many matching values');
export const isSequenceError = is('SequenceError');
