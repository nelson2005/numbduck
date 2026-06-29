# numbduck subsystem: "Examples as usage contracts"

Scope: `examples/_common.py`, `examples/haversine.py`, `examples/fraud_score.py`,
`examples/online_scoring.py`, `examples/run_irr.py` (+ the module it launches,
`examples/irr.py`). These files are not just demos — they are the de-facto
reference for how a correct caller wires numba JIT code to DuckDB's C API
through numbduck. This note describes how each actually works, the
invariants a correct user must uphold, the C/JIT/Python boundaries, and the
load-bearing/fragile assumptions a downstream defect review should scrutinize.

All paths are absolute. Line cites are `file:line`.

---

## 0. The shared spine (the contract every example signs)

Every example is built from the same primitives, all imported from numbox and
numbduck:

- **Connection-pointer extraction** — `extract_connection_ptr(conn)`
  (`/home/erik/projects/numbduck/numbduck/pybridge.py:10`). This reaches into
  the pybind11 instance layout of `DuckDBPyConnection` by **hardcoded byte
  offsets**: `id(conn)+16` → `DuckDBPyConnection*`, then `+32` → `Connection*`
  (`pybridge.py:59`, `pybridge.py:62`). It self-validates by running
  `SELECT 1` through the C API before returning (`pybridge.py:65-72`).
  **This is the single most fragile assumption shared by all four examples:**
  the offsets are "Validated on duckdb 1.3.2 / Linux x86-64 / libstdc++"
  (`pybridge.py:31-34`) and are explicitly documented as breakable across
  duckdb major releases. Every example's correctness rests on this.

- **Pointer typing convention** — numbduck represents *all* C pointers as
  numba `intp`, never `voidptr`. Callbacks therefore use `nb_types.intp`
  signatures (e.g. `haversine.py:116`), and any `carray()` over a DuckDB
  buffer must first bridge `intp → void*` via numbox's `_cast_int_to_void_p`
  intrinsic (e.g. `haversine.py:99`). This is a hard contract: get it wrong
  and numba either fails to compile or builds a `carray` over garbage.

- **Handle allocation** — DuckDB out-params (result, prepared statement,
  chunk) are backed by numpy `int64` buffers from
  `/home/erik/projects/numbduck/numbduck/duckdb_utils.py`. Note the sizes:
  `create_duckdb_result` allocates **6** int64 slots (`duckdb_utils.py:35-37`)
  — the in-memory `duckdb_result` struct — while statement/chunk/connection
  allocate **1** (`duckdb_utils.py:23-25`, `:29-31`). The online-scoring loop's
  `result_buf = numpy.zeros(6, ...)` (`online_scoring.py:109`) must match that 6.

- **String passing** — SQL text and function names cross into C as
  `get_unicode_data_p(...)` (numbox), returning a raw `char*` into the Python
  `str`'s buffer (e.g. `haversine.py:126`, `online_scoring.py:154-155`).

- **Handle destruction via buffer** — you never pass a handle directly to a
  `destroy_*`; you box it: `buf = numpy.array([handle_p], dtype=numpy.intp);
  destroy(buf.ctypes.data)` (e.g. `haversine.py:136-137`,
  `haversine.py:131-132`). The destroy takes a *pointer to the handle*.

- **Cross-checking** — `_common.assert_results_match`
  (`/home/erik/projects/numbduck/examples/_common.py:63`) enforces that the
  JIT variant produced the same answer as the reference variant(s). This is
  the examples' guard against "fast because it's wrong" (`_common.py:65`). It
  uses **exact `!=`** comparison (`_common.py:73`); callers must pre-round
  floats (online_scoring does, `online_scoring.py:185-187`).

`_common.py` is deliberately minimal (header warns it must stay <~80 lines,
`_common.py:3-5`); it owns only env printing (`:18`), median timing with an
env-overridable repeat count (`:28-42`), table formatting (`:45`), and the
cross-check (`:63`). `time_median` does **no warmup** — each example warms up
itself before timing (`haversine.py:158-162`, `online_scoring.py:176-178`).

---

## 1. `haversine.py` — scalar UDF, throughput axis

**Canonical pattern: register a JIT chunk-callback scalar function.**

Control flow of registration, `register_jit_udf` (`haversine.py:121-137`):

1. `extract_connection_ptr(conn)` → raw `Connection*` (`:124`).
2. `duckdb_create_scalar_function()` → `func_p` handle (`:125`).
3. **Set the name on the function** via `duckdb_scalar_function_set_name(func_p,
   get_unicode_data_p("hv_jit"))` (`:126`). **Invariant:** the SQL-visible
   name lives on the function object; without this call the function cannot be
   referenced from SQL (CLAUDE.md key-pattern #7: for a function *set*, each
   member function needs its own `set_name` — the set name alone is
   insufficient; these examples are the single-function degenerate case).
4. Create one `DOUBLE` logical type, add it as a parameter 4× (`:128-129`),
   set the same as return type (`:130`), then **destroy the logical type**
   (`:131-132`) *before* registering. **Assumption:** `add_parameter` /
   `set_return_type` deep-copy the logical type into the function, so the
   source handle is safe to free immediately. Matches the DuckDB C API
   contract; flagged here because the free precedes registration.
5. Bind the native code: `duckdb_scalar_function_set_function(func_p,
   _haversine_chunk_cb.address)` (`:133`) — passes the raw cfunc entry-point
   address.
6. `duckdb_register_scalar_function(conn_ptr, func_p)`; assert `DuckDBSuccess`
   (`:134-135`).
7. **Destroy the function handle** (`:136-137`) — DuckDB has copied it into the
   catalog, so the builder handle is freed.

The callback boundary (`haversine.py:87-118`):

- `_haversine_chunk_impl` is an `@njit` function (`:87`) — it *can* call
  `math.*` and `ducklib.*`.
- `_haversine_chunk_cb` is a thin `@cfunc(void(intp,intp,intp))` wrapper
  (`:116-118`) that just forwards. **Invariant (CLAUDE.md #1):** a `@cfunc`
  body cannot contain `import`/closures, so all real work lives in a
  module-level `@njit` impl and the `@cfunc` is a trampoline.
- Inside the impl: read chunk size (`:89`), get each input vector by index
  (`:90-93`), get each vector's raw data pointer (`:94-98`), `carray` each as
  `float64[n]` (`:99-103`), then a plain scalar loop writing into the output
  carray (`:105-113`). No Python crossings, no allocations — that is the whole
  performance thesis (`:217-219`).

Result reading: the UDF writes into the output vector; the *caller* reads the
query result through ordinary `conn.execute(sql).fetchone()` (`:165-168`). So
for scalar UDFs the "result reading" invariant is delegated to DuckDB — the
contract the user upholds is only "write `n` values into the output carray."

Three SQL variants are derived by string-replacing the function name
(`:154-155`); `hv_py`/`hv_arrow` are stock duckdb `create_function`
registrations (`:185-188`). Python is run only for `n <= PY_MAX_N` (10K,
`:57`, `:156`) to stay under the 30s budget (`:26-28`).

**Risks to flag (not audited):**
- No validity/NULL handling in the chunk loop — assumes all inputs non-NULL.
  Safe only because `setup_data` generates dense `random()` columns
  (`:140-145`). A NULL input would be read as an uninitialized/garbage double.
- `info` (function-info pointer) is accepted and ignored (`:88`) — fine here,
  but means no access to bind data or error reporting from the callback.
- The cfunc object `_haversine_chunk_cb` is a **module global** (`:117`); this
  keeps it alive for the connection's life. If it were ever local/GC-able, the
  `.address` handed to DuckDB would dangle.

---

## 2. `fraud_score.py` — scalar UDF, branchy-logic axis

Structurally identical to haversine (same register/callback/cross-check
skeleton). The differences are the contract-relevant parts:

- **Heterogeneous column types.** Parameters are `DOUBLE, TINYINT×4, INTEGER`
  (`fraud_score.py:159-165`), so three distinct logical types are created
  (`:156-158`) and the chunk callback `carray`s each vector at the **matching
  numpy dtype**: `float64`, `int8` (TINYINT), `int32` (INTEGER)
  (`:118-124`). **Invariant:** the dtype used in `carray` must equal the
  DuckDB physical type of that column; a mismatch silently reinterprets bytes.
  This is the load-bearing per-example contract and the most likely place a
  user error would hide.
- All three logical types are destroyed in a loop after `set_return_type` but
  before register (`:166-168`) — same copy-then-free assumption as haversine.
- Output is `INTEGER`, callback writes `int32` (`:124`, `:144`); the Arrow
  variant must explicitly `pc.cast(total, "int32")` (`:99`) so the cross-check
  (`:208`) compares equal integers.

This example also encodes a **design contract**: a printed DECISION gate
(`:261-263`) saying if the largest-tier Arrow/JIT ratio is `< 2x` the example
should be dropped because Arrow is the right tool. That makes the example a
falsifiable hypothesis test, not just a benchmark — relevant if a reviewer is
deciding whether the example still "earns its place."

Same NULL-handling caveat as haversine: dense generated data, no validity
checks (`:176-188`, `:125-144`).

---

## 3. `online_scoring.py` — prepared-statement loop, latency + GIL-free axis

This is the richest contract because the JIT loop does **execute + fetch +
read + destroy** entirely inside `@njit(nogil=True)`, with no Python per
iteration.

Setup, `score_jit` (`online_scoring.py:150-162`):
1. Extract `Connection*` (`:152`).
2. Allocate a 1-slot statement buffer (`:153`) and `duckdb_prepare(conn_ptr,
   sql_ptr, stmt.ctypes.data)` with a `$1`-parameterised SQL (`:154-155`);
   assert success (`:156`).
3. Run the `@njit` core, passing `int(stmt[0])` — the **statement handle as an
   int** (`:160`).
4. After the loop, `duckdb_destroy_prepare(stmt.ctypes.data)` (`:161`).

The JIT core `_score_jit_loop` (`online_scoring.py:106-147`):
- Pre-allocates a **6-slot result buffer** and a 1-slot chunk buffer once
  (`:109-110`), takes their addresses as `intp` (`:111-112`). The result
  struct is written into `result_buf` by each execute.
- Per event (`:114`):
  - `monotonic_ns()` from **numbox's clock** (`:115`) — bound from libc /
    kernel32 *inside* the JIT, not `time.monotonic_ns`, because the latter
    would require the GIL (`:14-15`, `:313-316`). Different clock apparatus
    than the Python variant's `time.monotonic_ns` (`:96`) — a measurement
    asymmetry, not a correctness bug, but worth noting for any latency claim.
  - `duckdb_bind_int64(stmt_p, 1, ids[i])` (`:117`) — bind param index 1
    (1-based), then `duckdb_execute_prepared(stmt_p, result_p)` (`:118`)
    writing the result struct into `result_buf`.
  - Build a 6-tuple snapshot of the result struct slots (`:119-122`) and call
    `duckdb_fetch_chunk(result_tup)` (`:123`). **Invariant (CLAUDE.md #5):**
    fetch is `duckdb_fetch_chunk(tuple(result))`, *by value*, not a
    nonexistent `duckdb_result_get_chunk`.
  - Read the 4 output vectors (`:125-132`), `carray` each as `float64[1]`
    (`:133-136`), compute the dot product into `scores_out[i]` (`:137-140`).
  - **Destruction order:** stash `chunk_p` into `chunk_buf[0]` then
    `duckdb_destroy_data_chunk(chunk_pp)` (`:142-143`), then
    `duckdb_destroy_result(result_p)` (`:144`). **Invariant:** every iteration
    destroys both the chunk and the result; the reused `result_buf` is
    overwritten by the next `execute_prepared`. Forgetting either is a per-event
    leak (the example is a leak contract by construction).

**Risks to flag (not audited):**
- **Exactly-one-row assumption.** The loop reads `carray(..., (1,), ...)` and
  index `[0]` with no check on chunk size or `chunk_p != 0`
  (`:123-136`). It is correct *only because* `setup_features` builds `id =
  range(k)` (`online_scoring.py:74-79`) and events draw `ids` from
  `[0, k_features)` (`:84`), guaranteeing exactly one matching row per probe.
  A miss would make `duckdb_fetch_chunk` return a null/empty chunk and the
  subsequent `duckdb_data_chunk_get_vector(0, ...)` dereference garbage. This
  is the example's most dangerous implicit contract.
- **No validity check** on the fetched values — fine given the dense feature
  table, but a NULL feature would be read as raw bytes.
- The result struct is captured as a value-tuple snapshot (`:119-122`) taken
  *after* execute and *before* destroy — relies on `duckdb_fetch_chunk`
  consuming the struct by value, consistent with numbduck's by-value fetch
  binding.

Parallel scaling (`run_scaling_block`, `:192-225`) is the headline axis:
each worker thread builds **its own connection** via `conn_factory`
(`:203-209`) against a shared on-disk db (`:266-278`), because the JIT loop is
`nogil=True` and so scales with cores while the Python loop is GIL-bound
(`:311-318`). **Invariant:** one connection per thread — connections are not
shared across the worker pool. The on-disk temp db is created, seeded, and
removed in a `try/finally` (`:267-281`); note the file is created then removed
so `duckdb.connect` can recreate it (`:267-269`).

---

## 4. `run_irr.py` + `irr.py` — UDAF, and the module-identity contract

`run_irr.py` is a 19-line launcher whose *entire reason to exist* is a numba
contract: `irr.py` defines `IRRStateType` at module level
(`irr.py:60-62`); running `irr.py` as `__main__` would set
`IRRStateType.__module__ == "__main__"`, giving the structref class a fresh
identity each process and breaking numba's **warm-cache type inference**
(`run_irr.py:3-8`, `irr.py:24-28`). So the launcher imports `irr` to pin
`__module__ == "irr"` and calls `main()` (`run_irr.py:14`). `irr.py` itself
hard-refuses to run as `__main__` (`irr.py:33-39`). **Invariant:** any
structref-typed state used across the C-callback boundary must live in a
stably-named importable module, never an entry-point script.

Although the prompt scopes the read to `run_irr.py`, the contract it protects
only makes sense alongside `irr.py`, so the UDAF lifecycle is summarized here:

- DuckDB aggregate lifecycle: `state_size → init → update(per chunk) →
  combine(parallel merge) → finalize → destroy` (`irr.py:99-101`). Each stage
  is an `@njit` impl + `@cfunc` trampoline pair (e.g. `:104-111`, `:114-126`).
- **State bridging (the memory contract):** `init` builds an `IRRState`
  structref, calls numbox `export_meminfo(s)` to get a raw refcounted pointer,
  and stores it into the DuckDB-provided state slot (`irr.py:115-121`).
  `update`/`combine`/`finalize` reconstruct the structref with
  `borrow_structref(irr_state_type, slot[0])` (`:156`, `:178-179`, `:201`) —
  **borrow = incref + raw deref, does not consume the export's ref.** `destroy`
  calls `release_meminfo(slot[0])` to drop the ref `export_meminfo` took
  (`:215-220`). **Invariant:** exactly one `export_meminfo` (init) paired with
  exactly one `release_meminfo` (destroy); borrows in between must not release.
  `main` verifies this with NRT alloc/free stats and exits non-zero on
  imbalance (`:265-267`, `:372-377`) — the example is an explicit leak test.
- **NULL handling done right here** (unlike the scalar examples): `update`
  fetches each column's validity mask via `duckdb_vector_get_validity` and
  skips rows where any input is NULL (`irr.py:140-153`). This is the reference
  for how a careful caller *should* handle validity — the scalar examples omit
  it only because their data is dense.
- **Per-group-constant contract:** `investment`/`target_npv` are captured from
  the first non-NULL row and subsequent values ignored (`:159-162`,
  `:182-185`); callers must pass identical values per GROUP BY key
  (`irr.py:18-22`). A documented input contract the user must uphold.
- Registration mirrors the scalar path but with the aggregate API:
  `set_functions(state_size, init, update, combine, finalize)` (`:244-251`)
  plus a separate `set_destructor` (`:252`), register, then destroy the
  builder handle (`:257-258`). Result is read by ordinary
  `conn.execute(...).fetchone()/fetchall()` (`:285`, `:326-331`).

---

## Cross-cutting invariants a correct user must uphold

1. **Handle destruction is mandatory and paired.** create↔destroy for logical
   types (`haversine.py:127/131-132`), scalar/aggregate function builders
   (`:125/136-137`, `irr.py:233/257-258`), prepared statements
   (`online_scoring.py:153/161`), and — per iteration — chunks and results
   (`online_scoring.py:142-144`). UDAF state is the meminfo
   export/release pair (`irr.py:115-121` ↔ `:215-220`).
2. **Name must be set on the function object** before register
   (`haversine.py:126`, `fraud_score.py:155`, `irr.py:235`); for function
   *sets*, on every member (CLAUDE.md #7).
3. **`carray` dtype must equal the DuckDB column physical type** and pointers
   must be bridged `intp→void*` via `_cast_int_to_void_p`
   (`fraud_score.py:118-124`).
4. **Result reading:** scalar UDFs write into the output vector and the caller
   reads via `conn.execute().fetchone()`; the JIT loop reads chunks directly
   and must `duckdb_fetch_chunk(tuple(result))` then destroy chunk+result each
   time (`online_scoring.py:123,142-144`).
5. **Callbacks: module-level `@njit` impl + thin `@cfunc(intp...)` trampoline**,
   kept alive as module globals for the connection's lifetime.
6. **Cross-check every fast path** against a reference variant with pre-rounded
   floats (`_common.py:63`, `online_scoring.py:185-187`).

## Fragile assumptions (surface for defect review)

- **Hardcoded pybind11/`DuckDBPyConnection` offsets** in
  `extract_connection_ptr` (`pybridge.py:59,62`) — version/platform/ABI
  bound; underpins every example.
- **`online_scoring` assumes exactly one matching row** with no chunk-size /
  null-chunk / validity guard (`online_scoring.py:123-136`); correct only by
  construction of the feature/event data.
- **Scalar UDFs omit validity handling** (`haversine.py:105-113`,
  `fraud_score.py:125-144`) — silently wrong on NULL input.
- **Logical types freed before register** assumes DuckDB deep-copies them on
  `add_parameter`/`set_return_type` (`haversine.py:131-132`,
  `fraud_score.py:166-168`).
- **structref `__module__` stability** is required for warm-cache type
  inference; the whole `run_irr.py` launcher exists to protect it
  (`run_irr.py:3-8`).
- **Latency comparison uses two different clocks** (numbox JIT clock vs
  `time.monotonic_ns`, `online_scoring.py:115` vs `:96`) — apparatus
  asymmetry, not a correctness bug.
- **`_common.assert_results_match` uses exact `!=`** (`_common.py:73`) — any
  float path that does not pre-round will spuriously fail or, worse, mask
  intended tolerance.
