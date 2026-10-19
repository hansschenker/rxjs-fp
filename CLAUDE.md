# rxjs-fp — notes for Claude

- Pure FP RxJS in TypeScript. In `src/` and `examples/`: no `class`, `this`, `instanceof`, `.prototype`, or `new` on custom types. `test/purity.test.ts` enforces this on the AST.
- Mutable state only inside the per-subscription shell (`let` in an `observable(sink => …)` producer). Stateful operators go through `fromMachine` with pure `step`/`flush`.
- Operators subscribe upstream with `subscribeChild(source, sink, handlers)` — never `source.subscribe(...)` inside an operator (it links teardown and routes exceptions to `error`).
- Time-based code takes a `Scheduler` argument; never patch globals.
- Typeclass instances must stay lawful: add a property test in `test/laws.test.ts` for any new instance.
- `npm run check` must pass (typecheck, vitest, build). Imports use `.js` extensions (NodeNext).
- Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
