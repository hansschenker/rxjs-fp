import { observable } from '../core/observable.js';
import { cont, flushError, flushWith, fromMachine } from '../core/machine.js';
import { emptyError } from '../core/errors.js';
import type { Observable, Operator } from '../core/types.js';
import type { ObservableInput } from '../creation/creation.js';
import { subscribeInner } from './inner.js';

/**
 * Conditional, boolean and mathematical operators.
 *
 * Almost all of them are pure Mealy machines: the decision logic lives in
 * `step` and `flush`, and can be tested with `runMachine` alone.
 */

/** Errors with `factory()` (default: `EmptyError`) if the source completes without emitting. */
export const throwIfEmpty = <A>(factory: () => unknown = emptyError): Operator<A, A> =>
  fromMachine<boolean, A, A>({
    init: () => false,
    step: (_seen, a) => cont(true, [a]),
    flush: (seen) => (seen ? flushWith([]) : flushError(factory())),
  });

/**
 * Emits `true` if the source and `compareTo` emit equal sequences
 * (by `comparator`, default `===`), `false` as soon as they differ.
 * Both are subscribed at once; values that arrive early are buffered.
 */
export const sequenceEqual =
  <A>(compareTo: ObservableInput<A>, comparator: (a: A, b: A) => boolean = (a, b) => a === b): Operator<A, boolean> =>
  (source: Observable<A>) =>
    observable<boolean>((sink) => {
      interface Side {
        readonly buffer: A[];
        complete: boolean;
      }
      const left: Side = { buffer: [], complete: false };
      const right: Side = { buffer: [], complete: false };
      const emit = (equal: boolean): void => {
        sink.next(equal);
        sink.complete();
      };
      // `self` received a value: compare it with the oldest unmatched one of `other`
      const onValue = (self: Side, other: Side, a: A, aIsLeft: boolean): void => {
        if (other.buffer.length === 0) {
          if (other.complete) emit(false);
          else self.buffer.push(a);
          return;
        }
        const b = other.buffer.shift() as A;
        if (!(aIsLeft ? comparator(a, b) : comparator(b, a))) emit(false);
      };
      const onComplete = (self: Side, other: Side): void => {
        self.complete = true;
        // like RxJS, decide only once both sides are done
        if (other.complete) emit(other.buffer.length === 0);
      };
      const l = subscribeInner(source, sink, {
        next: (a) => onValue(left, right, a, true),
        complete: () => onComplete(left, right),
      });
      const r = subscribeInner(compareTo, sink, {
        next: (b) => onValue(right, left, b, false),
        complete: () => onComplete(right, left),
      });
      return () => {
        l.close();
        r.close();
      };
    });
