import { argumentOutOfRangeError } from '../core/errors.js';
import type { ObservableInput } from '../creation/creation.js';

/** Validated domains used by the algebraic operator policies. */
declare const naturalBrand: unique symbol;
declare const positiveIntegerBrand: unique symbol;
declare const millisecondsBrand: unique symbol;
declare const positiveMillisecondsBrand: unique symbol;
declare const epochBrand: unique symbol;

export type NonNegativeInt = number & { readonly [naturalBrand]: true };
export type PositiveInt = NonNegativeInt & { readonly [positiveIntegerBrand]: true };
export type Milliseconds = number & { readonly [millisecondsBrand]: true };
export type PositiveMilliseconds = Milliseconds & { readonly [positiveMillisecondsBrand]: true };
export type EpochMilliseconds = number & { readonly [epochBrand]: true };
export type Signal = ObservableInput<unknown>;

export type Limit<N> =
  | Readonly<{ kind: 'unlimited' }>
  | Readonly<{ kind: 'limited'; value: N }>;

export type ImmediateDelay = Readonly<{ kind: 'immediate' }>;
export type AfterDelay = Readonly<{ kind: 'after'; duration: Milliseconds }>;

export const natural = (value: number): NonNegativeInt => {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw argumentOutOfRangeError('Expected a nonnegative safe integer');
  }
  return value as NonNegativeInt;
};

export const positiveInteger = (value: number): PositiveInt => {
  natural(value);
  if (value === 0) throw argumentOutOfRangeError('Expected a positive safe integer');
  return value as PositiveInt;
};

export const milliseconds = (value: number): Milliseconds => {
  if (!Number.isFinite(value) || value < 0) {
    throw argumentOutOfRangeError('Expected finite nonnegative milliseconds');
  }
  return value as Milliseconds;
};

export const positiveMilliseconds = (value: number): PositiveMilliseconds => {
  milliseconds(value);
  if (value === 0) throw argumentOutOfRangeError('Expected positive milliseconds');
  return value as PositiveMilliseconds;
};

export const epochMilliseconds = (value: number): EpochMilliseconds => {
  if (!Number.isSafeInteger(value) || Math.abs(value) > 8_640_000_000_000_000) {
    throw argumentOutOfRangeError('Expected a valid integer Date timestamp');
  }
  return value as EpochMilliseconds;
};

export const unlimited = <N>(): Limit<N> => Object.freeze({ kind: 'unlimited' });
export const limited = <N>(value: N): Limit<N> => Object.freeze({ kind: 'limited', value });
export const immediate = (): ImmediateDelay => Object.freeze({ kind: 'immediate' });
export const after = (duration: number): AfterDelay => Object.freeze({ kind: 'after', duration: milliseconds(duration) });

/** Adding a policy alternative requires defining its execution rule. */
export const assertNever = (_value: never): never => {
  throw new TypeError('Unsupported policy alternative');
};
