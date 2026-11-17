import { observable, subscribeChild, subscribeWithin } from '../core/observable.js';
import { createSubscription } from '../core/subscription.js';
import type { Observable, Operator, Subscription } from '../core/types.js';
import { from, type ObservableInput } from '../creation/creation.js';
import { createSubject, type Subject } from '../subject/subject.js';

/** An Observable of the values that share one key. */
export interface GroupedObservable<K, A> extends Observable<A> {
  readonly key: K;
}

/**
 * Shared by `groupBy` and `groupByUntil`. Splits the source into one
 * `GroupedObservable` per key.
 *
 * Base case: the first value with a new key emits a new group; that value
 * and all later values with the same key flow into it. Completion and
 * errors of the source reach every open group and the outer Observable.
 *
 * With a `duration`, a group completes (and is forgotten) when
 * `duration(group)` emits or ends; a later value with the same key opens a
 * new group. `connector` creates the Subject behind each group.
 *
 * Edge case, as in RxJS: unsubscribing from the outer Observable does not
 * stop the source while some group still has subscribers. The source
 * subscription lives in its own scope and is closed only when the outer
 * subscriber is gone *and* no group is subscribed.
 */
function groupByCore<A, K>(
  keySelector: (value: A) => K,
  duration: ((group: GroupedObservable<K, A>) => ObservableInput<unknown>) | undefined,
  connector: () => Subject<A>,
): Operator<A, GroupedObservable<K, A>> {
  return (source) =>
    observable<GroupedObservable<K, A>>((sink) => {
      const groups = new Map<K, Subject<A>>();
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

      const grouped = (key: K, subject: Subject<A>): GroupedObservable<K, A> =>
        Object.freeze({
          ...observable<A>((groupSink) => {
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

      const watchDuration = (key: K, subject: Subject<A>, group: GroupedObservable<K, A>): void => {
        let ended = false;
        let sub: Subscription | null = null;
        const end = (f: () => void): void => {
          if (ended) return;
          ended = true;
          if (groups.get(key) === subject) groups.delete(key);
          sub?.unsubscribe();
          f();
        };
        sub = subscribeWithin(from((duration as NonNullable<typeof duration>)(group)), connection, {
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
              if (duration) watchDuration(key, subject, group);
            }
            subject.next(a);
          } catch (e) {
            fail(e);
          }
        },
        error: fail,
        complete: () => notify((o) => o.complete()),
      });
    });
}

/**
 * Splits the source into one `GroupedObservable` per key, as `groupByCore`
 * describes. `connector` creates the Subject behind each group; pass
 * `() => createReplaySubject()` to let late subscribers of a group see its
 * earlier values. To map the values of a group, use `map` on the group.
 */
export const groupBy = <A, K>(
  keySelector: (value: A) => K,
  connector: () => Subject<A> = createSubject,
): Operator<A, GroupedObservable<K, A>> => groupByCore(keySelector, undefined, connector);

/**
 * Like `groupBy`, but each group completes when `duration(group)` emits or
 * ends, and is forgotten: a later value with the same key opens a new group.
 */
export const groupByUntil = <A, K>(
  keySelector: (value: A) => K,
  duration: (group: GroupedObservable<K, A>) => ObservableInput<unknown>,
  connector: () => Subject<A> = createSubject,
): Operator<A, GroupedObservable<K, A>> => groupByCore(keySelector, duration, connector);
