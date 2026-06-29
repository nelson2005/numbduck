# numbduck — Overall architecture & the three UDF patterns

Scope: how numbduck adapts the DuckDB C API for numba `@njit`, the role of numbox,
and the three user-facing patterns (scalar chunk callback, JIT query loop,
aggregate/UDAF). Cites are `file:line`. This note feeds a defect review; load-bearing
mechanics and risky spots are flagged inline and collected at the end.

All `numbduck/*` cites are in `/home/erik/projects/numbduck`; numbox cites are in
`/home/erik/projects/numbox`.

---

## 1. The big picture

numbduck is a **thin binding layer**, not a framework. It does exactly one structural
thing: it makes every DuckDB C API function callable from inside numba `@njit` code at
native speed, with no Python/ctypes crossing per call. Everything else (UDFs, UDAFs,
query loops) is *application code built on top of those bindings* and lives in
`test/test_ducklib.py` and `examples/`, not in the library.

The package has only four source modules (`numbduck/`):

- `utils.py` — finds and loads `libduckdb` with `RTLD_GLOBAL` so its symbols are
  visible to LLVM's JIT linker.
- `ducklib.py` — the binding table: ~230 DuckDB C functions, each registered in
  numbox's global `signatures` dict and wrapped with `@cres` + `_call_lib_func`.
- `duckdb_utils.py` — trivial `@njit` allocators that return numpy buffers sized to
  hold DuckDB out-params (database/connection/result/chunk/vector handles).
- `pybridge.py` — extracts the raw C `Connection*` out of a Python `duckdb` connection
  object so JIT code and the stock Python API can share one connection.

The whole adaptation rests on **numbox's bindings toolkit** (`numbox/core/bindings/`)
plus a few numbox utilities (`cres`, `_cast_int_to_void_p`, `get_unicode_data_p`, the
meminfo bridge, `make_structref`). numbduck adds no new lowering machinery of its own —
it only declares signatures and reuses numbox's intrinsics.

---

## 2. The binding mechanism (C ↔ JIT boundary)

### 2.1 Library load and symbol visibility

`load_duckdb()` (`numbduck/utils.py:113`) locates the shared library
(`find_duckdb_shared_lib`, `utils.py:94` — handles duckdb 1.3.x in-package `.so` vs
duckdb 1.4+ `_duckdb.*` in site-packages) and loads it via numbox's `load_lib_path`
(`numbox/core/bindings/utils.py:167`), which on POSIX uses `CDLL(path, mode=RTLD_GLOBAL)`
(`utils.py:180`). The module runs this at import time: `duckdb_lib = load_duckdb()`
(`numbduck/ducklib.py:11`).

`RTLD_GLOBAL` is **load-bearing**: it pushes `duckdb_*` symbols into the process's global
symbol table so that LLVM's `ll.address_of_symbol(func_name)`
(`numbox/core/bindings/call.py:72`) can resolve them when the intrinsic is compiled. The
binding wrappers do **not** hold the `CDLL` handle or go through ctypes at call time — they
bake the resolved function address straight into JIT IR. Invariant: the lib must be loaded
(globally) before any `@cres` wrapper is compiled/typed.

On macOS the duckdb wheel deliberately strips C API symbols (`_has_capi_symbols` checks for
`duckdb_open`, `utils.py:29`); `load_duckdb` then falls back to a standalone
`libduckdb.dylib` (env var, cache, Homebrew paths, or a prompted download, `utils.py:50`).

### 2.2 Signature table + `@cres` wrapper

Each binding is two pieces:

1. A signature registered into numbox's shared `signatures` dict, e.g.
   `signatures["duckdb_vector_get_data"] = intp(intp)` (`ducklib.py:235`). **Every pointer
   is typed `intp`** — handles, vectors, validity masks, out-param buffer addresses, even
   `char*`. There is no `voidptr` in the public binding surface.
2. A `@cres`-decorated wrapper whose body just forwards to `_call_lib_func`:

   ```python
   @cres(signatures.get("duckdb_vector_get_data"))
   def duckdb_vector_get_data(duckdb_vector_p):
       return _call_lib_func("duckdb_vector_get_data", (duckdb_vector_p,))
   ```
   (`ducklib.py:1200`)

`cres` (`numbox/utils/highlevel.py:26`) is `njit(sig)` plus a wrap: it compiles the
function **eagerly at import** for the one given signature and returns a
`CompileResultWAP` (a `FunctionType` proxy) rather than a `CPUDispatcher`. The practical
effect is that the wrapper is a first-class JIT function value that can be called from
other `@njit` code and inlined — so a chunk-callback loop that calls
`duckdb_vector_get_data` ends up as a direct native call to the DuckDB symbol with zero
Python on the path.

`_call_lib_func` (`numbox/core/bindings/call.py:21`) is an `@intrinsic` that, at compile
time: resolves the symbol address, reads the registered signature, classifies the return
and each argument as scalar / struct≤16B / struct>16B, and emits an ABI-correct LLVM call
(`call.py:118-197`). It handles SysV-x86-64, AAPCS64, and Windows-x64 calling conventions,
including the `{i32,i32,i64}`→`{i64,i64}` eightbyte repack that works around llvmlite
dropping fields for non-canonical 16-byte INT/INT structs (`call.py:212-257`), and
`byval`+`optnone`+`noinline` for >16B by-value struct args on SysV
(`call.py:170-189`). The DuckDB struct numba types are declared at `ducklib.py:67-74`
(e.g. `duckdb_result_ty = UniTuple(intp, 6)`, `duckdb_decimal_ty` 24B, `duckdb_varint_ty`
24B).

Two sibling helpers cover by-pointer struct passing:
- `_call_lib_func_byval` (`call.py:280`) passes an arg by pointer on all platforms; used
  where the C signature is `func(T*)` but the caller holds `T` by value, e.g.
  `duckdb_fetch_chunk(result)` (`ducklib.py:942`) and `duckdb_result_return_type`
  (`ducklib.py:1128`), which take the 48-byte `duckdb_result` by pointer.

Version-conditional bindings use `@cres_if_available` (`highlevel.py:41`), which stubs the
wrapper to raise `NotImplementedError` if the symbol is absent from the loaded lib (e.g.
`duckdb_create_varint`, `ducklib.py:725`; `duckdb_scalar_function_set_init`,
`ducklib.py:1284`) — this is how one binding table spans duckdb 1.3–1.5.

### 2.3 Error-handling contract

Bindings mirror the C protocol exactly: they return state codes
(`DuckDBSuccess=0`/`DuckDBError=1`, `ducklib.py:18-19`) and raw pointers, and **never raise**.
Callers do C-style return-code checks (`assert rc == ducklib.DuckDBSuccess`). This is a
deliberate invariant (CLAUDE.md "Error Handling").

### 2.4 Out-params via numpy buffers

DuckDB out-params (`duckdb_database*`, `duckdb_result*`, etc.) are modeled as numpy
buffers whose `.ctypes.data` address (Python side) or `.ctypes.data` inside `@njit` is
passed as an `intp`. `duckdb_utils.py` provides `@njit` allocators:
`create_duckdb_result()` returns `numpy.zeros(6, int64)` (a 48-byte `duckdb_result`,
`duckdb_utils.py:35`), others return a 1-element buffer for a single handle. Destroy
functions take a pointer-to-handle: the idiom is
`buf = numpy.array([handle_p], dtype=numpy.intp); destroy_func(buf.ctypes.data)`
(e.g. `examples/haversine.py:131-132`, `136-137`).

---

## 3. Connection sharing: `extract_connection_ptr` (the fragile bridge)

To register UDFs/UDAFs and to drive a prepared statement from JIT, numbduck needs the raw
`duckdb_connection` (a C `Connection*`) that lives inside the Python `duckdb` connection.
`pybridge.extract_connection_ptr` (`numbduck/pybridge.py:10`) reads it with **hardcoded
pointer arithmetic on the pybind11 instance layout**:

- `id(conn) + 16` → `DuckDBPyConnection*` (the C++ object) (`pybridge.py:59`)
- `DuckDBPyConnection* + 32` → `Connection*` (`pybridge.py:62`), where +32 skips a
  16-byte `weak_ptr` + 16-byte `shared_ptr<DuckDB>` (`pybridge.py:21-29`)

It then validates by running `SELECT 1` through the C API before returning
(`pybridge.py:64-72`). This is explicitly "validated on duckdb 1.3.2 / Linux x86-64 /
libstdc++" and called out as an implementation detail that may break on duckdb upgrades
(`pybridge.py:30-34`). **This is the single most fragile assumption in the project** and
the reason `duckdb` is pinned `>=1.3.2,<1.6` (CLAUDE.md). A silent layout change would
return a wrong pointer; the `SELECT 1` probe is the only guard.

---

## 4. The three UDF patterns

All three share one C↔JIT scaffolding rule, because DuckDB calls **C function pointers**
and `@cfunc` bodies cannot use `import` or rich numba features:

- Real work lives in a module-level `@njit` *impl*; a thin `@cfunc` *wrapper* just forwards
  to it (e.g. `examples/haversine.py:87-118`).
- `@cfunc` signatures use `nb_types.intp` for every pointer (matching the binding
  convention), never `voidptr`.
- Inside `@njit`, `carray()` needs a `voidptr`, so the `intp` data pointer is bridged with
  numbox's `_cast_int_to_void_p` intrinsic before `carray` (`haversine.py:99-103`).
- The DuckDB-visible entry point is `_cb.address` (the `@cfunc`'s C address), handed to
  DuckDB during registration (`haversine.py:133`).
- Registration is done from **Python** code (not JIT): create function object, set
  name/params/return type, hand over callback addresses, register, then destroy the
  builder object.

### 4.1 Pattern A — Scalar UDF as a chunk callback (`haversine.py`, `fraud_score.py`)

**Control flow:** `register_jit_udf` (`haversine.py:121`) builds a scalar function via the
C API: `duckdb_create_scalar_function()` → `set_name` →
`add_parameter`×4 (DOUBLE) → `set_return_type` → `set_function(func_p, cb.address)` →
`duckdb_register_scalar_function(conn_ptr, func_p)` → destroy the builder object
(`haversine.py:125-137`). Thereafter SQL like
`... WHERE hv_jit(lat,lon,...) < 50` (`haversine.py:152-155`) makes DuckDB invoke the
callback **once per data chunk** (~2048 rows), not per row.

**Data flow inside the callback** (`_haversine_chunk_impl`, `haversine.py:88-113`):
`duckdb_data_chunk_get_size(chunk)` for `n`; `duckdb_data_chunk_get_vector(chunk, i)` then
`duckdb_vector_get_data(vec)` for each input column's raw buffer; `carray` views each as a
length-`n` float64 array; one tight LLVM loop computes results into the output vector's
buffer. **No Python crossing per chunk, no intermediate arrays** — that is the entire
performance thesis (`README.md:16-19`, `examples/README.md:11-15`; measured ~400× vs a
per-row Python scalar UDF, ~100× vs PyArrow at 1M rows).

`fraud_score.py` is the same shape with branchy integer logic; it exists to show numbduck
beats Arrow even in Arrow's wheelhouse (~16× at 10K, ~1750× at 1M; `examples/README.md:25`).

Note: this pattern reads vector data directly and does **not** consult validity masks — it
assumes non-null inputs (fine for the synthetic benchmark data; a correctness gap for real
nullable columns).

### 4.2 Pattern B — JIT query loop driving a prepared statement (`online_scoring.py`)

Here there is no UDF; instead an entire per-event loop runs inside one
`@njit(nogil=True)` function that calls the DuckDB C API directly (`_score_jit_loop`,
`online_scoring.py:106-147`).

**Setup (Python):** `extract_connection_ptr` → `duckdb_prepare(conn, sql, stmt_buf)` once
(`online_scoring.py:152-156`), then the raw `stmt[0]` int is passed into the JIT loop
(`:160`).

**Per iteration (JIT):** `duckdb_bind_int64(stmt, 1, id)` → `duckdb_execute_prepared(stmt,
result_p)` → build the 6-tuple `duckdb_result` value from `result_buf` and call
`duckdb_fetch_chunk(result_tup)` (by-value-by-pointer) → `get_vector`/`get_data`/`carray`
to read the 4 feature columns → compute the dot product → set `chunk_buf[0]=chunk_p` and
`duckdb_destroy_data_chunk(chunk_pp)` + `duckdb_destroy_result(result_p)` to release
per-iteration resources (`online_scoring.py:114-144`). Timing uses
`numbox.utils.clock.monotonic_ns` bound *inside* the loop so no `time.*` Python call
intrudes.

**Why it matters:** with `nogil=True` and zero Python per iteration, the loop scales across
threads (~2.4× on 8 threads) while the pure-Python `conn.execute` loop plateaus under the
GIL; per-event latency ~2.2× lower (`README.md:21-24`, `online_scoring.py:28-38`). The
invariant the example relies on: each worker thread uses its **own** connection
(`online_scoring.py:202-209, 275-277`).

### 4.3 Pattern C — Aggregate (UDAF) with structref state (`irr.py`, design doc `test/test_ducklib.md`)

This is the most intricate pattern: DuckDB's aggregate API gives each group a fixed-size
opaque byte buffer (`state_size`), and numbduck stores a **numba structref** there by
smuggling its NRT `MemInfo*` through that raw `void*`.

**State:** an `IRRState` structref built with numbox `make_structref`
(`irr.py:55-73`), holding two `numbox` vectors plus scalars. `state_size = 8`
(`irr.py:104-106`) — exactly one `intp` pointer.

**Six lifecycle callbacks** (`irr.py`), each a `@njit` impl + `@cfunc` wrapper:

| Stage | impl | what it does | bridge call |
|-------|------|--------------|-------------|
| state_size | `:104` | returns 8 | — |
| init | `:114` | alloc `IRRState`, store `MemInfo*` int in the slot | `export_meminfo(s)` |
| update | `:129` | per row: reconstruct structref from slot, `vector_push` (validity-checked) | `borrow_structref` |
| combine | `:170` | merge partials (parallel) | `borrow_structref` ×2 |
| finalize | `:193` | reconstruct, run `irr_bisect`, write output vector | `borrow_structref` |
| destroy | `:214` | per group: `release_meminfo(slot[0])` | `release_meminfo` |

**Registration** (`register_irr`, `irr.py:230`): `duckdb_create_aggregate_function()` →
name/params/return type → `duckdb_aggregate_function_set_functions(func_p, state_size_cb,
init_cb, update_cb, combine_cb, finalize_cb)` (5 callbacks; `irr.py:244-251`) →
`duckdb_aggregate_function_set_destructor(func_p, destroy_cb)` (separate; `:252`) →
`duckdb_register_aggregate_function` → destroy builder. SQL
`SELECT irr(...) FROM t GROUP BY ...` then drives the whole lifecycle.

**The NRT↔DuckDB ownership bridge** (numbox `numbox/utils/meminfo.py`, design rationale in
`test/test_ducklib.md`):

- `export_meminfo(s)` (`meminfo.py:143`) extracts the `MemInfo*` via `structref_meminfo`
  and `_incref_meminfo` (+1) so the object survives past the init callback's scope; returns
  it as `intp`.
- `borrow_structref(type, p)` (`meminfo.py:131`) does `_incref_meminfo(p)` then
  `_deref_structref_raw_ptr` to rebuild a live structref; the local's scope-exit decref
  balances the incref → net zero on the external owner.
- `release_meminfo(p)` (`meminfo.py:155`) calls `NRT_MemInfo_release` directly
  (`_release_meminfo`, `meminfo.py:90`); at refcount 0 NRT runs the structref destructor,
  freeing nested allocations (the vectors).

**The subtle correctness mechanism — `removerefctpass`** (`test/test_ducklib.md:101-163`):
because every callback's signature is e.g. `void(intp,intp)` with **no NRT-tracked types**,
numba's `removerefctpass` strips *all* `NRT_incref`/`NRT_decref` pairs from the callback
body. The stripping is symmetric (every incref's matching scope-exit decref also goes), so
the net refcount change at each stage is exactly intended: init leaves the allocation at
refcount 1; update/combine/finalize are net-zero; destroy's `NRT_MemInfo_release` is **not**
in the pass's accepted-fn allowlist, so its mere presence disables the pass for that
function and the −1 survives. This is why `release_meminfo` must use a direct
`NRT_MemInfo_release` call and not `context.nrt.decref()`. The array-state variant differs
(an inline `numpy.zeros`/`NRT_MemInfo_alloc*` disables the pass, so the incref must be done
via the inlining `_incref_meminfo` intrinsic, not the separate-compilation-unit
`export_meminfo` wrapper) — `test_ducklib.md:143-159`.

`irr.main` asserts NRT alloc==free at the end (`irr.py:372-377`) as a leak gate.

---

## 5. Invariants & boundaries (summary)

- **Pointer width:** everything is `intp`; the design assumes a 64-bit host (state_size=8,
  6×int64 result buffer, MemInfo pointer in 8 bytes). 32-bit is unsupported in practice.
- **Symbol resolution:** `duckdb_lib` must be loaded `RTLD_GLOBAL` before any binding is
  compiled; the wrappers resolve symbols by name through LLVM, not through the `CDLL` handle.
- **C error protocol only:** bindings never raise; callers must check return codes.
- **`@cfunc` thin-wrapper rule:** real logic in `@njit`, `intp` (not `voidptr`) signatures,
  `_cast_int_to_void_p` before `carray`.
- **One connection per thread** for the JIT query-loop pattern.
- **Builder objects destroyed after registration**, but logical types are destroyed
  *immediately* after `add_parameter`/`set_return_type` (e.g. `haversine.py:131-132`,
  `irr.py:241-242`) — this assumes DuckDB copies the logical type into the function object.

---

## 6. Risky / fragile spots to scrutinize in the defect review

(Flagged, not audited.)

1. **`extract_connection_ptr` hardcoded offsets** (`pybridge.py:59,62`): byte offsets
   `+16`/`+32` over a pybind11 / `DuckDBPyConnection` layout validated only on duckdb 1.3.2
   / Linux / libstdc++. Any duckdb (or pybind11/libc++) change silently yields a wrong
   `Connection*`; only `SELECT 1` guards it. The `<1.6` pin is the mitigation.

2. **`@cfunc` swallows exceptions:** all UDF/UDAF callbacks are `@cfunc`. If the inner
   `@njit` impl raises (e.g. bad type id, out-of-range index, validity logic bug), numba's
   `@cfunc` returns a zero/default and does not unwind to DuckDB — silent wrong results and
   potential NRT ref leaks across the raising call. Worth checking each callback for
   raising paths.

3. **Scalar UDF ignores validity masks** (`haversine.py`, `fraud_score.py`): reads vector
   data directly with no null check. Correct only for non-nullable inputs; the UDAF path
   *does* check validity (`irr.py:146-153`), highlighting the asymmetry.

4. **`removerefctpass` dependency** (`test/test_ducklib.md:101-163`): the entire
   structref-via-raw-pointer UDAF correctness depends on numba's refcount pass being
   all-or-nothing and symmetric. A future numba change that strips increfs but not decrefs
   (or narrows the accepted-fn bailout) would break it. Cross-version stability is argued
   but not guaranteed.

5. **Logical-type lifetime assumption:** destroying `dbl_type_p` right after
   `add_parameter`/`set_return_type` (`haversine.py:132`, `irr.py:242`) assumes DuckDB deep-
   copies the type. If any binding path retains the handle instead, this is a use-after-free.

6. **`finalize` output indexing:** `_irr_finalize_impl` carrays the output buffer as
   `(offset+count,)` and writes at `offset+iu` (`irr.py:194-206`); relies on DuckDB's output
   vector being large enough and on the `offset`/`count` contract. Off-by-one or chunked
   finalize would write OOB.

7. **Per-iteration resource release in the JIT loop** (`online_scoring.py:142-144`):
   correctness of `duckdb_destroy_data_chunk`/`duckdb_destroy_result` depends on the
   `_p`/`_pp` (pointer vs pointer-to-pointer) discipline being exactly right; a mismatch
   leaks or double-frees silently inside a `nogil` loop.

8. **Doc/code drift:** CLAUDE.md "Follow-ups" references hand-rolled bind intrinsics at
   `ducklib.py:1525-1640`, but the current file is 1432 lines and
   `duckdb_bind_hugeint`/`uhugeint`/`interval`/`decimal` are already plain `_call_lib_func`
   wrappers (`ducklib.py:1410-1431`). The migration appears already landed; the follow-up
   note is stale. Verify no dead/duplicated lowering remains.
