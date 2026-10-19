import { observable, subscribeChild } from './observable.js';
import type { Operator } from './types.js';

/**
 * Stateful operators as pure Mealy machines.
 *
 * A machine describes *what* an operator does as pure functions over an
 * explicit state value. The single generic shell `fromMachine` is the only
 * place that holds a mutable cell (`let state`) — one per subscription, so
 * cold semantics are preserved by construction.
 *
 *   step : (S, A) -> (S, B[], done?)      on every upstream value
 *   flush: S      -> B[] | error          on upstream completion
 */
export interface Step<S, B> {
  readonly state: S;
  readonly emit: ReadonlyArray<B>;
  readonly done: boolean;
}

export type Flush<B> = { readonly emit: ReadonlyArray<B> } | { readonly error: unknown };

export interface Machine<S, A, B> {
  readonly init: () => S;
  readonly step: (state: S, value: A, index: number) => Step<S, B>;
  readonly flush?: (state: S) => Flush<B>;
}

const NONE: ReadonlyArray<never> = Object.freeze([]);

/** Continue with a new state, emitting `emit`. */
export const cont = <S, B = never>(state: S, emit: ReadonlyArray<B> = NONE): Step<S, B> => ({ state, emit, done: false });

/** Emit `emit` and complete. */
export const halt = <S, B = never>(state: S, emit: ReadonlyArray<B> = NONE): Step<S, B> => ({ state, emit, done: true });

/** Flush result helpers. */
export const flushWith = <B>(emit: ReadonlyArray<B>): Flush<B> => ({ emit });
export const flushError = (error: unknown): Flush<never> => ({ error });

/** The one imperative shell that drives every machine. */
export const fromMachine =
  <S, A, B>(machine: Machine<S, A, B>): Operator<A, B> =>
  (source) =>
    observable<B>((sink) => {
      let state = machine.init(); // the only mutable cell of this execution
      let index = 0;
      // `halt` means the machine is finished *now*, but `sink.complete()` can
      // only run after the pending emissions. Latching it first closes the
      // window in which a re-entrant upstream value (a consumer pushing into
      // the source from inside `next`) would get its own `step` and slip past
      // the halt — `take(1)` emitting twice. Same rule as `runMachine`, which
      // returns on `done` and never flushes afterwards.
      let halted = false;
      return subscribeChild(source, sink, {
        next: (value) => {
          if (halted) return;
          const r = machine.step(state, value, index++);
          state = r.state;
          if (r.done) halted = true;
          for (const out of r.emit) {
            if (sink.closed) return;
            sink.next(out);
          }
          if (r.done) sink.complete();
        },
        complete: () => {
          if (halted) return;
          if (machine.flush) {
            const f = machine.flush(state);
            if ('error' in f) {
              sink.error(f.error);
              return;
            }
            for (const out of f.emit) sink.next(out);
          }
          sink.complete();
        },
      });
    });

/**
 * Runs a machine over a finite list of inputs, without any Observable.
 * Useful for unit- and property-testing operator logic as pure functions.
 */
export const runMachine = <S, A, B>(
  machine: Machine<S, A, B>,
  inputs: Iterable<A>,
): { readonly output: ReadonlyArray<B>; readonly done: boolean; readonly error?: unknown } => {
  let state = machine.init();
  let index = 0;
  const output: B[] = [];
  for (const a of inputs) {
    const r = machine.step(state, a, index++);
    state = r.state;
    output.push(...r.emit);
    if (r.done) return { output, done: true };
  }
  if (machine.flush) {
    const f = machine.flush(state);
    if ('error' in f) return { output, done: true, error: f.error };
    output.push(...f.emit);
  }
  return { output, done: true };
};
