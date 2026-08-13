# Subworkflows & Iteration

Guideline for parallel and sequential iteration patterns, and the distinction between subworkflows and independent child workflows. Companion to `signals-timers.md`, `running-workflows.md`.

## Core distinction: subworkflow vs independent workflow

Two genuinely different concepts; the distinction is the answer to the O(n²) resume problem:

- **Subworkflow** = an inline closure reading the parent's local variables. Essentially it is just a scope function that prefixes all Step and Await-IDs in it, so that the same Step definition can be executed multiple times independently (in a loop for example). It has no independent existence; its wait state (suspensions on signals, timers) lives in the parent → **the parent re-runs on each of its events**. Replay of other elements is cheap (cache + decision-record lookups), but total work is O(events × elements). Fine for modest n or when strict data coupling to parent is required.
  
- **Independent workflow** = started with explicit args, no shared scope, its own instance/log/cursor/history. Awaiting N of these does **not** re-run the parent per event. Documented escape for large n or latency-sensitive per-element flows. Each child is independently queryable, retryable, observable.

- **Child workflow** = an independent workflow that is also coupled to the parent lifecycle (e.g., cancelled when the parent completes). Falls between pure independence and subworkflow coupling. Not yet designed; placeholder for future API.

```scala
// Subworkflow constructs (capture parent scope; element gets stable sub-id):
Workflow.foreach(id, items)(a => ...)          // sequential; each completes before next
Workflow.map(id, items)(a => b): Seq[B]         // sequential, collect results
Workflow.fold(id, items)(z)((acc, a) => ...)    // sequential accumulate
Workflow.mapParallel(id, items)(a => b): Seq[B] // all active at once; parent wakes per event; parent does not suspend until all are complete or suspended.

// Independent-workflow fan-out (no parent re-run per event):
val children = items.map(i => childWf.createAndRunAsync(key(i), i))
val results  = Wait.all("fan-in", children.map(_.completion))
```

Each subworkflow element needs a **stable element key** (index by default; a `keyBy` function when collection may reorder). This ties into step cache-key semantics — the key is part of the await/step cache identity.

**`whenAllReady` (batching parent wake until all elements ready) is REJECTED**: delaying element effects to the slowest element and possible deadlock when one element's post-await step unblocks another. Use the independent-workflow pattern instead.

## Rationale

- O(n²) solution: subworkflows inherently re-run the parent; for many independent awaits, independent workflows give each one its own wake-up path and no parent cascade.
- Closure capture is the defining characteristic: subworkflows are for when you need parent scope; independent workflows are for when you don't and can afford the decoupling.
- **Scoped vs concurrent**: subworkflows can execute sequentially (parent doesn't re-run until completion) or in parallel (parent re-runs as each completes). Independent workflows are always "parallel" in the sense that each gets its own execution context — parent parallelism is just `Wait.all(completions)`.

## Open questions and TODOs

1. **Iteration with plain loops** — **TODO**: Design `Workflow.scoped(id: String)(body)` function that suspends and re-runs the body after every Step, even without explicit await. Enables plain loop + await patterns within a scope. Ties into subworkflow execution model. What is the relationship between `scoped` and `foreach`/`map`?

2. **Child workflows (coupled independence)** — **TODO**: Design child workflows as independent workflows that inherit parent lifecycle semantics (e.g., cancellation, parent-close policies). How do they differ from independent workflows created via `createAndRunAsync`? What versioning/coupling semantics apply?

3. **Subworkflow element key derivation** — **TODO**: Finalize element key scheme. Default = index in collection at execution time. When collections are reordered between replays, should `keyBy(fn)` derive the key from the element value, or does the user maintain stable ids? How does this interact with step cache-key semantics?

4. **Loop semantics under invalidation** — **TODO**: If the collection in `forEach`/`map`/`fold` changes (fewer/more elements, reordered), what happens to the state of subworkflows for elements no longer in the collection? Are they abandoned, or kept in a "stale" state? Can you "resume" them if the element reappears?
