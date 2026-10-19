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

export type TimeoutError = TaggedError<'TimeoutError'>;
export const timeoutError = tagged('TimeoutError', 'Timeout has occurred');
export const isTimeoutError = is('TimeoutError');

export type ArgumentOutOfRangeError = TaggedError<'ArgumentOutOfRangeError'>;
export const argumentOutOfRangeError = tagged('ArgumentOutOfRangeError', 'argument out of range');
export const isArgumentOutOfRangeError = is('ArgumentOutOfRangeError');

export type ObjectUnsubscribedError = TaggedError<'ObjectUnsubscribedError'>;
export const objectUnsubscribedError = tagged('ObjectUnsubscribedError', 'object unsubscribed');
export const isObjectUnsubscribedError = is('ObjectUnsubscribedError');
