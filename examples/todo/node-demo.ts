/**
 * Terminal demo of the Todo CRUDL app.
 *
 *   npx tsx examples/todo/node-demo.ts
 *
 * User actions are simulated by pushing into Subjects (the "event
 * sources"); everything between the subjects and the console is pure.
 */
import { createSubject, createVirtualScheduler } from '../../src/index.js';
import { intents, loadEffect, memoryStorage, model$, persistEffect } from './src/app.js';
import type { Filter, Todo } from './src/model.js';
import { viewText } from './src/view.js';

const add$ = createSubject<string>();
const select$ = createSubject<number | null>();
const rename$ = createSubject<{ id: number; title: string }>();
const toggle$ = createSubject<number>();
const remove$ = createSubject<number>();
const clearCompleted$ = createSubject<void>();
const setFilter$ = createSubject<Filter>();

const storage = memoryStorage([{ id: 1, title: 'Read Paul Taylor: RxJS Evolved', done: true }] as Todo[]);
const scheduler = createVirtualScheduler(); // so the debounced save is deterministic here

const model = model$(
  intents({ add$, select$, rename$, toggle$, remove$, clearCompleted$, setFilter$, loaded$: loadEffect(storage) }),
);

let step = 0;
const sub = model.subscribe((m) => console.log(`\n── ${step++} ──\n${viewText(m)}`));
const saving = persistEffect(model, storage, 300, scheduler).subscribe();

add$.next('Write chapter: LINQ → Rx.NET'); //          C
add$.next('Port operators to Mealy machines'); //      C
add$.next('   '); //                                  C (ignored: empty title)
toggle$.next(2); //                                    U
rename$.next({ id: 3, title: 'Port stateful operators to Mealy machines' }); // U
select$.next(3); //                                    R
setFilter$.next('active'); //                          L
setFilter$.next('all'); //                             L
clearCompleted$.next(); //                             D
remove$.next(3); //                                    D

scheduler.advanceBy(300);
console.log('\nsaved to storage:', storage.saved());

sub.unsubscribe();
saving.unsubscribe();
