/**
 * rxjs-fp — reactive extensions in pure functional style.
 *
 * No classes, no `this`, no prototypes. Observables are records of
 * closures; stateful operators are pure Mealy machines; flattening
 * strategies are data; schedulers are passed explicitly.
 */

// core
export type {
  Observable,
  Observer,
  Operator,
  Producer,
  Sink,
  Subscribable,
  Subscription,
  Teardown,
  ValueOf,
  ValuesOf,
} from './core/types.js';
export { OBSERVABLE_KEY } from './core/types.js';
export { observable, isObservable, subscribeChild } from './core/observable.js';
export type { ChildHandlers } from './core/observable.js';
export { createSubscription, EMPTY_SUBSCRIPTION, isSubscription, isUnsubscriptionError } from './core/subscription.js';
export type { UnsubscriptionError } from './core/subscription.js';
export { pipe, flow, identity } from './core/pipe.js';
export { fromMachine, runMachine, cont, halt, flushWith, flushError } from './core/machine.js';
export type { Machine, Step, Flush } from './core/machine.js';
export * from './core/errors.js';

// scheduling
export {
  asyncScheduler,
  asapScheduler,
  animationFrameScheduler,
  createVirtualScheduler,
} from './scheduler/scheduler.js';
export type { Scheduler, VirtualScheduler } from './scheduler/scheduler.js';

// creation
export * from './creation/creation.js';

// operators
export * from './operators/transform.js';
export * from './operators/filtering.js';
export * from './operators/flatten.js';
export * from './operators/combine.js';
export * from './operators/time.js';
export * from './operators/error.js';

// multicasting
export * from './subject/subject.js';

// conversion
export * from './conversion.js';
