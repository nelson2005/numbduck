# numbduck subsystem: "Examples as usage contracts"

Scope: the six example scripts under `examples/`. These are the *canonical
usage patterns* for numbduck — they demonstrate, and implicitly define the
correctness contract for, every public surface: connection-pointer extraction,
scalar-function-set registration, aggregate-function (UDAF) lifecycle,
prepared-statement execution inside a JIT loop, chunk/vector reading, and
handle destruction. Each also cross-checks its JIT variant against a Python /
Arrow baseline, so they double as differential tests.

All examples share one architecture: Python does *setup* (connect, create
tables, register functions, allocate buffers), then a numba `@njit`/`@cfunc`
kernel does the *hot* work by calling the `ducklib` proxy wrappers directly on
raw `intp` pointers with **no Python crossings** inside the loop. The boundary
crossings are the load-bearing, fragile part.

---

## Shared infrastructure

### `_common.py` — benchmark harness only, no DuckDB knowledge

- `print_env()` (`_common.py:18`) — env banner every example prints first.
- `time_median(fn, repeats=3)` (`_common.py:28`) — median wall time; `repeats`
  overridable via `NUMBDUCK_BENCH_REPEATS`; **no auto-warmup, caller must warm
  up** (`_common.py:31`). Raises if `repeats < 1` (`_common.py:35`).
- `format_table(...)` (`_common.py:45`) — pure stdout formatting.
- `assert_results_match(*results, label)` (`_common.py:63`) — the differential
  guard ("your fast variant is fast because it's wrong"). **Uses exact `!=`
  equality** (`_common.py:73`); callers must round floats *before* calling
  (haversine passes raw `count(*)` ints; online_scoring rounds to 6 dp at
  `online_scoring.py:185-187`; fraud passes int sums). Returns silently with
  `< 2` args (`_common.py:69`) — a single-arg call is a no-op, not an error.

This file is deliberately capped (~80 lines, `_common.py:3-5`); it holds no
DuckDB-specific logic.

### The two boundary-bridging idioms (used by every JIT example)

1. **Connection pointer**: `extract_connection_ptr(conn)`
   (`pybridge.py:10`) reads the raw `Connection*` out of the pybind11
   `DuckDBPyConnection` instance via hard-coded ctypes offsets
   (`id(conn)+16` → C++ obj, then `+32` → `Connection*`, `pybridge.py:59-62`)
   and validates it with a `SELECT 1` through the C API
   (`pybridge.py:65-72`). **Offsets are duckdb-version/ABI specific**
   (documented `pybridge.py:31-34`, validated only on 1.3.2 / Linux x86-64 /
   libstdc++). Every JIT example calls this at registration time.

2. **`intp` ↔ `voidptr` cast**: numbduck represents *all* pointers as `intp`.
   `carray()` inside `@njit` requires a `voidptr`, so every example wraps a raw
   data pointer with `_cast_int_to_void_p(...)` (from `numbox.utils.lowlevel`)
   before `carray`. This appears in every kernel, e.g.
   `haversine.py:99-103`, `fraud_score.py:118-124`, `online_scoring.py:133-136`,
   `irr.py:120,136-139,155,196-197`.

3. **Names into the C API**: strings are passed as raw UTF pointers via
   `get_unicode_data_p("...")` (numbox), e.g. function names
   (`haversine.py:126`, `fraud_score.py:155`, `irr.py:234-235`) and SQL text
   (`online_scoring.py:155`).

4. **Buffer allocation**: `duckdb_utils.create_duckdb_*()` return zeroed numpy
   `int64` arrays sized to the C struct. Note `create_duckdb_result()` returns
   **6 int64 slots = 48 bytes** (`duckdb_utils.py:36-39`) — this exact size is
   assumed by the JIT result-reading code (see online_scoring below).

### The `@cfunc` / `@njit` split (why every callback is two functions)

DuckDB C-API callbacks must be plain C function pointers, so they are `@cfunc`.
But `@cfunc` bodies cannot use module imports and must use `nb_types.intp`
(not `voidptr`) for pointer args. So every example uses the pattern:
module-level `@njit` `_..._impl` doing the real work + a thin `@cfunc`
`_..._cb` wrapper that just forwards. Examples:
`haversine.py:87-118`, `fraud_score.py:102-149`, and all five IRR callbacks
(`irr.py:104-225`). The C API receives `_..._cb.address`.

---

## Example 1 — `haversine.py`: scalar UDF, throughput axis

Canonical pattern: **register a JIT scalar function that reads whole chunks**.

Control flow:
- `main()` (`haversine.py:179`) opens a conn, registers three UDFs: Python
  (`create_function`), Arrow (`type="arrow"`), and the JIT one via
  `register_jit_udf` (`haversine.py:121`).
- `register_jit_udf` is the contract to study (`haversine.py:121-137`):
  1. `extract_connection_ptr(conn)` → raw ptr.
  2. `duckdb_create_scalar_function()` → `func_p`.
  3. `duckdb_scalar_function_set_name(func_p, get_unicode_data_p("hv_jit"))`
     — **name MUST be set on the function object**, not just via SQL.
  4. Create one `DOUBLE` logical type, add it as a parameter **4 times**
     (`haversine.py:128-129`), set it as return type.
  5. **Destroy the logical type exactly once** via the numpy-buffer idiom
     (`haversine.py:131-132`) — the type is reused for all 4 params but freed
     once. Destroy-by-buffer: `numpy.array([ptr], intp)` then
     `destroy(buf.ctypes.data)`.
  6. `duckdb_scalar_function_set_function(func_p, _haversine_chunk_cb.address)`.
  7. `duckdb_register_scalar_function(conn_ptr, func_p)`; **assert rc ==
     DuckDBSuccess** (`haversine.py:135`).
  8. `duckdb_destroy_scalar_function(...)` — the registered function is
     copied into the connection, so the local handle is freed immediately
     (`haversine.py:136-137`).

The kernel (`_haversine_chunk_impl`, `haversine.py:87-113`): reads
`duckdb_data_chunk_get_size(chunk)` for `n`, then for each of the 4 input
columns `duckdb_data_chunk_get_vector(chunk, col)` →
`duckdb_vector_get_data(vec)` → `carray(_cast_int_to_void_p(d), (n,),
float64)`. Output vector = `output` arg directly (index-free) → its data ptr →
`a_out`. Writes results in a tight LLVM-fused loop. **Invariant: the kernel
assumes all inputs are non-NULL** — it never checks validity (contrast IRR).
Safe here because inputs are generated columns.

Cross-check + timing in `run_one` (`haversine.py:148`): warm up each SQL once
(`haversine.py:159-162`), assert Arrow/JIT/(Python) match (`haversine.py:164-171`),
then `time_median`. Python variant is gated to `n <= PY_MAX_N` (10K,
`haversine.py:57,156`) because per-row interpreter round-trips blow the 30s
budget.

## Example 2 — `fraud_score.py`: scalar UDF, branchy/mixed-type axis

Same registration contract as haversine (`register_jit_udf`,
`fraud_score.py:152-173`) but demonstrates **heterogeneous parameter types**:
one `DOUBLE`, four `TINYINT`, one `INTEGER` (`fraud_score.py:156-165`). Three
distinct logical types are created and each destroyed once via a loop over the
tuple `(dbl_p, tin_p, int_p)` (`fraud_score.py:166-168`) — again, types are
reused across params but freed once each.

The kernel (`_fraud_chunk_impl`, `fraud_score.py:102-144`) reads six vectors at
the **correct per-column numpy dtype** — `float64` for amount, `int8` for the
TINYINT columns, `int32` for recent_txns and the output
(`fraud_score.py:118-124`). **Load-bearing invariant: the `carray` dtype must
exactly match the DuckDB logical type declared at registration**, or the raw
memory is misread. TINYINT→int8, INTEGER→int32; getting this wrong silently
corrupts. Output is `int32` matching the `INTEGER` return type.

This example carries a design gate (`fraud_score.py:258-263`): if largest-tier
Arrow/JIT ratio `< 2x`, the example is meant to be dropped. It also, like
haversine, assumes no NULLs.

## Example 3 — `online_scoring.py`: prepared-statement execution *inside* a JIT loop

This is the most intricate contract: a `@njit(nogil=True)` loop
(`_score_jit_loop`, `online_scoring.py:106-147`) that executes a prepared
statement per event and reads/destroys the result **entirely in JIT with no
GIL**.

Setup (`score_jit`, `online_scoring.py:150-162`):
- `create_duckdb_prepared_statement()` → 1-slot buffer.
- `duckdb_prepare(conn_ptr, get_unicode_data_p(sql), stmt.ctypes.data)`;
  assert success. SQL uses `$1` positional param (`online_scoring.py:154`).
- Calls the JIT loop with `int(stmt[0])` (the statement handle).
- After the loop, `duckdb_destroy_prepare(stmt.ctypes.data)`
  (`online_scoring.py:161`).

The per-iteration contract inside the loop (`online_scoring.py:114-147`), which
a correct user MUST replicate in order:
1. Two persistent scratch buffers, allocated **once outside the loop**:
   `result_buf = zeros(6, int64)` (the 48-byte `duckdb_result`) and
   `chunk_buf = zeros(1, int64)` (`online_scoring.py:109-112`). Their addresses
   are taken once (`result_p`, `chunk_pp`).
2. `duckdb_bind_int64(stmt_p, uint64(1), ids[i])` — bind param 1
   (**1-indexed**, `online_scoring.py:117`).
3. `duckdb_execute_prepared(stmt_p, result_p)` — writes the result struct into
   `result_buf`.
4. **Rebuild the result as a 6-tuple** from `result_buf[0..5]`
   (`online_scoring.py:119-122`) and pass it *by value* to
   `duckdb_fetch_chunk(result_tup)` (`online_scoring.py:123`). This is the
   documented "`duckdb_fetch_chunk(tuple(result))`" idiom — the binding is
   `_call_lib_func_byval` over the 6-field result struct
   (`ducklib.py:943-946`). The 6-slot result buffer size is thus load-bearing.
5. Read the 4 output vectors → data ptrs → `carray(..., (1,), float64)` (one
   row per lookup) and compute the dot product (`online_scoring.py:125-140`).
6. **Destruction order (critical)**: store `chunk_p` into `chunk_buf[0]`
   (`online_scoring.py:142`), then `duckdb_destroy_data_chunk(chunk_pp)` then
   `duckdb_destroy_result(result_p)` (`online_scoring.py:143-144`). Both destroy
   functions take a *pointer to the handle*, hence the scratch buffers.

Invariants / fragile assumptions:
- **Every iteration must destroy both the chunk and the result**, or leak. The
  loop does exactly one execute → fetch one chunk → destroy pair per event; it
  assumes the query returns exactly one chunk with exactly one row (a `WHERE id
  = $1` point lookup). A multi-chunk or empty result would break the read
  (`carray(..., (1,), ...)` would read row 0 of a possibly-empty vector — no
  size check).
- **`monotonic_ns` is numbox's JIT-bound clock** (`online_scoring.py:50,115,146`)
  — POSIX `clock_gettime` / Windows `QueryPerformanceCounter` — chosen so the
  timing calls stay inside the nogil region (can't call `time.monotonic_ns` in
  JIT). This is the whole point of the GIL-free axis.
- Parallel scaling (`run_scaling_block`, `online_scoring.py:192-225`) opens **one
  connection per worker thread** on a shared **on-disk** temp db
  (`online_scoring.py:266-278`). The temp file is created then `os.remove`'d so
  `duckdb.connect` can create it (`online_scoring.py:267-269`); cleaned up in a
  `finally` (`online_scoring.py:279-281`). In-memory DBs are per-connection, so
  a file is required to share the `features` table across workers.
- Correctness proven by `assert_results_match` on the rounded score sums
  (`online_scoring.py:184-188`).

## Examples 4/5 — `run_irr.py` + `irr.py`: UDAF over structref state

`run_irr.py` is a **launcher whose only job is module identity**
(`run_irr.py:1-18`): running `irr.py` as `__main__` would give `IRRStateType`
`__module__ == "__main__"`, a fresh class identity per process, which breaks
numba warm-cache type inference (`No conversion from
numba.IRRStateType(...)`). `irr.py` even hard-`exit(1)`s if run directly
(`irr.py:33-39`). **Invariant: numba structref types used across the C-API
boundary need a stable module identity.**

State design (`irr.py:55-73`): `make_vector(float64)` for growable cashflow /
period vectors; a structref `IRRState` with those two vectors plus
`investment`, `target_npv` scalars and an `initialized` flag, built via
numbox `make_structref` and `@structref.register`.

The UDAF lifecycle — DuckDB calls in order **state_size → init → update(per
chunk) → combine(parallel merge) → finalize → destroy** (`irr.py:98-100`).
Each callback is the `@njit impl` + `@cfunc cb` pair, and each bridges the raw
state slot to a structref:
- `_irr_state_size` (`irr.py:104-111`): returns `uint64(8)` — the state slot is
  **just an 8-byte pointer**; the real state lives in an NRT-managed structref
  whose meminfo pointer is stored in the slot.
- `_irr_init` (`irr.py:114-127`): builds an `IRRState`, `export_meminfo(s)`
  → raw ptr, stores it into `slot[0]` (the DuckDB-provided state pointer).
  `export_meminfo` **incref's and hands ownership to DuckDB's slot**.
- `_irr_update` (`irr.py:129-167`): per chunk, reads 4 input vectors *and their
  validity masks* (`duckdb_vector_get_validity`, `irr.py:140-143`). **This is the
  only example that honors NULLs**: rows where any of the four columns is NULL
  are skipped (`irr.py:146-153`, guarded by `val != 0` because a fully-valid
  vector may have a NULL validity pointer). For each valid row it
  `borrow_structref(irr_state_type, slot[0])` (incref + raw deref) and
  `vector_push`es the values; investment/target_npv captured only from the
  first non-NULL row (`irr.py:159-162`) — the documented per-group-constant
  contract (`irr.py:19-23`).
- `_irr_combine` (`irr.py:170-190`): merges `count` source states into targets
  via `vector_extend`; propagates investment/target_npv/initialized if the
  target is empty.
- `_irr_finalize` (`irr.py:193-211`): writes into the **result vector at
  `offset + i`** (`irr.py:197,204,206`) — the `offset` arg is load-bearing;
  finalize may be called for a sub-range of groups. Empty state → `NaN`
  (`irr.py:203-204`). Otherwise runs `irr_bisect` (`irr.py:78-94`, 100-iteration
  bisection on the discount rate).
- `_irr_destroy` (`irr.py:214-225`): `release_meminfo(slot[0])` for each of
  `count` states — **the decref that balances `export_meminfo`**. This is the
  UDAF's memory-correctness contract.

Registration (`register_irr`, `irr.py:230-258`): create aggregate function,
set name, add 4 DOUBLE params + DOUBLE return, then
`duckdb_aggregate_function_set_functions(func_p, size, init, update, combine,
finalize)` (`irr.py:244-251`) and a **separate**
`duckdb_aggregate_function_set_destructor(func_p, destroy)` (`irr.py:252`) —
the destructor has a different signature `(states, count)` and is registered
apart from the five core callbacks. Then register, assert, destroy the local
handle.

Correctness harness in `main()` (`irr.py:261-379`): wraps the whole run in NRT
allocation-stats capture (`irr.py:265-267,372-377`) and **fails with exit 1 if
`alloc != free`** — i.e. the example is a live leak test for the
export/borrow/release meminfo protocol. Three analytic checks: uniform
cashflows, multi-group `GROUP BY` (exercises combine), and a sparse single
period at t=12 that "catches the exponent = i+1 trap" (`irr.py:348-365`).

---

## Cross-cutting invariants a correct user must uphold

1. **Set the name on the function object** (`duckdb_scalar_function_set_name` /
   `duckdb_aggregate_function_set_name`), every time — not just in SQL. Per
   CLAUDE.md, for scalar-function *sets* each member function needs its own
   name set.
2. **Assert the registration/prepare rc == `DuckDBSuccess`** (all examples do).
3. **Destroy every handle you create** via the numpy-buffer idiom
   (`numpy.array([ptr], intp)` → `destroy(buf.ctypes.data)`): logical types
   (once per distinct type, even if reused for N params), the scalar/aggregate
   function object (immediately after register — DuckDB copies it), prepared
   statements, and per-iteration results + chunks.
4. **`carray` dtype must match the declared DuckDB logical type exactly**
   (TINYINT→int8, INTEGER→int32, DOUBLE→float64). Mismatch → silent corruption.
5. **Chunk loop**: `n = duckdb_data_chunk_get_size(chunk)`; index vectors by
   position; the output vector is passed directly (scalar UDF) or is the result
   vector written at `offset+i` (UDAF finalize).
6. **Destruction order per prepared-execute iteration**: fetch chunk → read →
   destroy chunk → destroy result (online_scoring). Destroy functions take a
   pointer-to-handle, so keep a scratch slot.
7. **NULL handling is opt-in**: only IRR reads validity masks; the scalar
   examples assume dense non-NULL inputs. A user porting these to nullable
   columns must add `duckdb_vector_get_validity` / `duckdb_validity_row_is_valid`
   checks (guarded by `validity_ptr != 0`, since a fully-valid vector reports a
   NULL mask).
8. **Structref state across the C boundary** requires: a stable module identity
   (hence `run_irr.py`), `export_meminfo` at init, `borrow_structref` in
   update/combine/finalize, and `release_meminfo` in destroy — balanced, or NRT
   leaks (the example asserts this).

## Fragile assumptions worth flagging for the defect review

- **`extract_connection_ptr` hard-codes pybind11/`DuckDBPyConnection` offsets
  (+16, +32)** validated only on duckdb 1.3.2 / Linux x86-64 / libstdc++
  (`pybridge.py:31-34,59-62`). The pin is `duckdb>=1.3.2,<1.6`; nothing in these
  offsets is re-checked at runtime beyond a `SELECT 1` that could pass on a
  *wrong-but-plausible* pointer. Highest-risk assumption in the subsystem.
- **online_scoring assumes one chunk / one row per lookup** with no size or
  null check (`online_scoring.py:123-140`); a query returning 0 rows would read
  past a possibly-empty vector.
- **Result buffer is exactly 6 int64 slots** (`duckdb_utils.py:39`) and is
  reconstructed field-by-field into a 6-tuple for `duckdb_fetch_chunk`
  (`online_scoring.py:119-123`); if the `duckdb_result` struct layout/size
  changes across the supported duckdb range this silently mis-reads.
- **Logical-type double-free avoidance**: types are reused for multiple params
  but destroyed once; correct only because DuckDB copies the type on
  `add_parameter`/`set_return_type`. Relied on but not asserted.
- **`assert_results_match` uses exact equality** (`_common.py:73`); float
  variants must pre-round. haversine/fraud dodge this by comparing integer
  aggregates; online_scoring rounds to 6 dp. A future float-returning example
  that forgets to round would get spurious mismatches.
- **Timing has no warmup guarantee from `time_median`** (`_common.py:31`); each
  example warms up manually before measuring — an easy contract to break when
  copying the pattern.
- **`@njit(nogil=True)` loop touches DuckDB state without the GIL**
  (`online_scoring.py:106`); correctness depends on each worker using its own
  connection/statement (it does), but there is no guard preventing a user from
  sharing a statement across threads.
