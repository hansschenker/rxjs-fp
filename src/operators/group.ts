import { observable, subscribeChild, subscribeWithin } from '../core/observable.js';
import { createSubscription } from '../core/subscription.js';
import type { Observable, Operator, Subscription } from '../core/types.js';
import { from, type ObservableInput } from '../creation/creation.js';
import { createSubject, type Subject } from '../subject/subject.js';

/** An Observable of the values that share one key. */
export interface GroupedObservable<K, A> extends Observable<A> {
  readonly key: K;
}

export interface GroupByOptions<A, K, E> {
  /** Maps each value before it enters its group. */
  readonly element?: (value: A) => E;
  /** A group completes (and is forgotten) when `duration(group)` emits or ends; a later value with the same key opens a new group. */
  readonly duration?: (group: GroupedObservable<K, E>) => ObservableInput<unknown>;
  /** The Subject behind each group. Default: `createSubject`. */
  readonly connector?: () => Subject<E>;
}

/**
 * Splits the source into one `GroupedObservable` per key.
 *
 * Base case: the first value with a new key emits a new group; that value
 * and all later values with the same key flow into it. Completion and
 * errors of the source reach every open group and the outer Observable.
 *
 * Edge case, as in RxJS: unsubscribing from the outer Observable does not
 * stop the source while some group still has subscribers. The source
 * subscription lives in its own scope and is closed only when the outer
 * subscriber is gone *and* no group is subscribed.
 */
export function groupBy<A, K>(keySelector: (value: A) => K): Operator<A, GroupedObservable<K, A>>;
export function groupBy<A, K, E = A>(
  keySelector: (value: A) => K,
  options: GroupByOptions<A, K, E> & { readonly element: (value: A) => E },
): Operator<A, GroupedObservable<K, E>>;
export function groupBy<A, K>(keySelector: (value: A) => K, options: GroupByOptions<A, K, A>): Operator<A, GroupedObservable<K, A>>;
export function groupBy<A, K, E>(keySelector: (value: A) => K, options: GroupByOptions<A, K, E> = {}): Operator<A, GroupedObservable<K, E>> {
  const connector = options.connector ?? createSubject;
  const element = options.element ?? ((a: A) => a as unknown as E);
  return (source) =>
    observable<GroupedObservable<K, E>>((sink) => {
      const groups = new Map<K, Subject<E>>();
      const connection = createSubscription();
      let activeGroups = 0;
      let outerGone = false;

      const maybeDisconnect = (): void => {
        if (outerGone && activeGroups === 0) connection.unsubscribe();
      };
      const notify = (f: (o: { error: (e: unknown) => void; complete: () => void }) => void): void => {
        const open = [...groups.values()];
        groups.clear();
        open.forEach(f);
        f(sink);
        connection.unsubscribe();
      };
      const fail = (e: unknown): void => notify((o) => o.error(e));

      const grouped = (key: K, subject: Subject<E>): GroupedObservable<K, E> =>
        Object.freeze({
          ...observable<E>((groupSink) => {
            activeGroups++;
            const inner = subscribeChild(subject, groupSink);
            return () => {
              inner.unsubscribe();
              activeGroups--;
              maybeDisconnect();
            };
          }),
          key,
        });

      const watchDuration = (key: K, subject: Subject<E>, group: GroupedObservable<K, E>): void => {
        let ended = false;
        let sub: Subscription | null = null;
        const end = (f: () => void): void => {
          if (ended) return;
          ended = true;
          if (groups.get(key) === subject) groups.delete(key);
          sub?.unsubscribe();
          f();
        };
        sub = subscribeWithin(from((options.duration as NonNullable<typeof options.duration>)(group)), connection, {
          next: () => end(() => subject.complete()),
          error: (e) => end(() => subject.error(e)),
          complete: () => end(() => subject.complete()),
        });
        if (ended) sub.unsubscribe();
      };

      sink.add(() => {
        outerGone = true;
        maybeDisconnect();
      });

      subscribeWithin(source, connection, {
        next: (a) => {
          try {
            const key = keySelector(a);
            let subject = groups.get(key);
            if (subject === undefined) {
              subject = connector();
              groups.set(key, subject);
              const group = grouped(key, subject);
              sink.next(group);
              if (options.duration) watchDuration(key, subject, group);
            }
            subject.next(element(a));
          } catch (e) {
            fail(e);
          }
        },
        error: fail,
        complete: () => notify((o) => o.complete()),
      });
    });
}
