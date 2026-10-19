import { subscribeChild } from '../core/observable.js';
import type { Sink, Subscription } from '../core/types.js';
import { from, type ObservableInput } from '../creation/creation.js';

/**
 * @internal A handle on an inner (duration, closing or notifier) Observable.
 *
 * `closed` turns true when the inner completes or is closed by the
 * operator. Closing works even while the inner is still being subscribed
 * (a synchronous duration that ends itself re-entrantly), which a plain
 * `Subscription` variable cannot express because it is not assigned yet.
 */
export interface Inner {
  readonly closed: boolean;
  readonly close: () => void;
}

export interface InnerHandlers<A> {
  /** Receives the inner's own handle, usable before `subscribeInner` has returned. */
  readonly next?: (value: A, inner: Inner) => void;
  readonly complete?: () => void;
  /** Default: forward to the sink. */
  readonly error?: (err: unknown) => void;
}

/**
 * @internal Subscribes an inner Observable on behalf of `sink`, linked to it
 * (closing the sink closes the inner). Exceptions in handlers go to
 * `sink.error`. Nothing is delivered after `close()`.
 */
export const subscribeInner = <A, B>(input: ObservableInput<A>, sink: Sink<B>, handlers: InnerHandlers<A>): Inner => {
  let closed = false;
  let sub: Subscription | null = null;
  const close = (): void => {
    if (closed) return;
    closed = true;
    sub?.unsubscribe();
  };
  const inner: Inner = Object.freeze({
    get closed() {
      return closed;
    },
    close,
  });
  sub = subscribeChild(from(input), sink, {
    next: (a) => {
      if (!closed) handlers.next?.(a, inner);
    },
    error: (e) => {
      if (closed) return;
      closed = true;
      if (handlers.error) handlers.error(e);
      else sink.error(e);
    },
    complete: () => {
      if (closed) return;
      closed = true;
      handlers.complete?.();
    },
  });
  if (closed) sub.unsubscribe();
  return inner;
};
