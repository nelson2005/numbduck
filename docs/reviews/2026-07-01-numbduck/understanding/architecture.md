# numbduck — Overall architecture & the three UDF patterns

Review target: `review/numbduck-2026-06-29` @proxy tree. File:line cites are to the
current tree. This note feeds a downstream defect review; risky/load-bearing details
are flagged inline and collected at the end.


## 1. What numbduck is

numbduck adapts the **DuckDB C API** (`libduckdb`) so it can be called from inside
numba `@njit` / `@cfunc` code with no Python round-trips. It is a thin binding layer
built entirely on the **numbox** bindings toolkit; numbduck itself contains almost no
codegen — it registers type signatures and generates one wrapper per C function
(`README.md:1-6`, `CLAUDE.md` "Architecture").

Three files do the work:

- `numbduck/utils.py` — finds and `ctypes.CDLL(..., RTLD_GLOBAL)`-loads `libduckdb`.
- `numbduck/ducklib.py` — 68 KB of signature registrations + one wrapper per C func.
- `numbduck/duckdb_utils.py` — numpy-buffer allocators for DuckDB out-param structs.

Two support modules bridge the Python/JIT boundary:

- `numbduck/configurations.py` — reads `NUMBDUCK_JIT_OPTIONS` (default `{"cache": True}`).
- `numbduck/pybridge.py` — extracts the raw `Connection*` from a Python duckdb conn.


## 2. Library loading & symbol resolution (the C boundary)

`load_duckdb()` (`utils.py:113-130`) runs at `ducklib` import time (`ducklib.py:12`).
It locates the shared lib (`find_duckdb_shared_lib`, `utils.py:94-110` — handles both
duckdb 1.3.x package-internal `duckdb*.so` and 1.4+ `_duckdb*.so` in site-packages) and
loads it via numbox's `load_lib_path` → `ctypes.CDLL` with `RTLD_GLOBAL`. `RTLD_GLOBAL`
is what makes the DuckDB symbols visible to LLVM's JIT linker: at call time
`_call_lib_func` emits an **extern declaration by name** and lets llvmlite resolve it via
`dlsym(RTLD_DEFAULT)` (numbox `CLAUDE.md` "LLVM symbol resolution"; `call.py:181`).

macOS wrinkle: the duckdb Python wheel strips C API symbols on macOS (`_has_capi_symbols`
presence-checks `duckdb_open`, `utils.py:29-30`). If absent, `load_duckdb` falls back to a
cached/standalone `libduckdb.dylib`, or interactively downloads one
(`utils.py:33-91,116-130`). On non-Darwin a missing C API is a hard error (`utils.py:124-128`).

`_call_lib_func`'s only symbol check is a **presence assertion**:
`ll.address_of_symbol(func_name)` (`call.py:72-74`); the resulting address is never baked
into IR (that would break `cache=True` across ASLR). Actual binding is by extern name at
JIT-link time.


## 3. The signature/proxy/call spine (the JIT boundary)

Every binding is three coordinated pieces, all resolved at `ducklib` import:

**(a) Signature registration.** `ducklib.py:78-273` fills numbox's global `signatures`
dict, e.g. `signatures["duckdb_open"] = duckdb_state_ty(intp, intp)` (`ducklib.py:218`).
All signatures are registered (78-273) *before* any wrapper is defined (276+), so the
`signatures.get(name)` lookups in decorators always see a populated entry.

**(b) Wrapper + `@proxy`.** Each C function gets a wrapper like (`ducklib.py:1093-1096`):

```python
@proxy(signatures.get("duckdb_open"), jit_options=jit_options)
def duckdb_open(path_p, duckdb_database_pp):
    return _call_lib_func("duckdb_open", (path_p, duckdb_database_pp))
```

`@proxy` (`numbox .../proxy/proxy.py:40-133`) eagerly `njit(sig, **jit_options)`-compiles
the body, then generates an `@intrinsic` + an `inline='always'` `@njit` wrapper so the
call statically links cheaply into any caller. It also registers a **process-stable LLVM
alias** for the body's cfunc wrapper via `ll.add_symbol` (`proxy.py:20-37,79-80`) — without
this, `cache=True` caches built in different processes reference numba's process-local
`v<uid>` wrapper name and abort on load with `Symbol not found: cfunc...`. This alias
machinery is load-bearing precisely because the default `jit_options` is `{"cache": True}`.

**(c) ABI lowering in `_call_lib_func`.** The wrapper body calls numbox's `_call_lib_func`
(`numbox .../bindings/call.py:21-200`), an `@intrinsic` that classifies each arg and the
return as scalar / ≤16-byte struct / >16-byte struct and lowers per host ABI
(SysV x86-64, AAPCS64, Win x64): scalars direct; small structs by-value in registers (with
an `{i64,i64}` eightbyte repack for INT/INT layouts like `duckdb_interval`
`{i32,i32,i64}`); large structs by pointer (`byval` + `optnone`/`noinline` on SysV);
returns direct or via `sret`. Two hand-rolled wrappers use the sibling
`_call_lib_func_byval` (`call.py:280-301`) which always passes an arg **by pointer**.

**Version gating.** Symbols that only exist in newer duckdb use
`@proxy_if_available(duckdb_lib, sig, ...)` (`proxy.py:136-164`): if
`libduckdb` lacks the symbol the wrapper becomes a stub raising `NotImplementedError`, and
`.as_func` is *not* exposed. Used at `ducklib.py:726` (`duckdb_create_varint`),
`1051` (`duckdb_get_varint`), `1285` (`duckdb_scalar_function_set_init`). Signatures are
still registered unconditionally.


## 4. Pointer discipline — the single most pervasive invariant

**Every DuckDB handle and every C pointer is typed `intp`** in numbduck (a signed
machine-word int), never `voidptr`. Consequences that ripple through all three UDF
patterns and the test/example code:

- `@cfunc` callback signatures must use `nb_types.intp` for pointer params, not `voidptr`
  (`CLAUDE.md` "Key patterns" #2; e.g. `haversine.py:116`).
- `carray()` inside `@njit` *requires* `voidptr`, so callers bridge with numbox's
  `_cast_int_to_void_p` intrinsic on every buffer view (`haversine.py:99-103`,
  `online_scoring.py:133-136`, `irr.py:120,136-139`).
- Out-params are numpy `int64` buffers passed as `buf.ctypes.data` (an int/intp)
  (`duckdb_utils.py`, `pybridge.py:67`).

Related struct typedefs live at `ducklib.py:68-75`: `duckdb_result_ty = UniTuple(intp, 6)`
(a 48-byte struct), plus hugeint/uhugeint/interval/decimal/blob/varint tuple types.


## 5. Out-param buffers (duckdb_utils.py)

DuckDB out-params (`duckdb_database`, `duckdb_connection`, `duckdb_prepared_statement`,
`duckdb_data_chunk`, `duckdb_value`, `duckdb_vector`) are single-`int64` numpy buffers
(`duckdb_utils.py:7-51`). `duckdb_result` is **6 int64 = 48 bytes**
(`create_duckdb_result`, `duckdb_utils.py:36-39`) — this hard-codes the C
`duckdb_result` struct size. The buffer's `.ctypes.data` is the `T*` / `T**` passed to
the C call. Destroy funcs take `T**`, so the "destroy via buffer" idiom writes the handle
into a buffer and passes the buffer address
(`CLAUDE.md` "Key patterns" #6; `online_scoring.py:142-144`, `haversine.py:131-132`).

Note the by-value/by-pointer subtlety for `duckdb_result`: C `duckdb_fetch_chunk`,
`duckdb_result_return_type`, `duckdb_result_statement_type` take `duckdb_result` **by
value**. Because it is a >16-byte (48 B) aggregate, the SysV/Win ABI passes such a struct
in memory via a hidden pointer — which is exactly what `_call_lib_func_byval` emits
(alloca+store+pass-pointer). So these three wrappers use `_call_lib_func_byval` on the
6-tuple, not `_call_lib_func` (`ducklib.py:943-946,1129-1138`; caller builds the 6-tuple
from the result buffer, `online_scoring.py:119-123`).


## 6. Connection-pointer extraction (pybridge.py)

To drive an existing Python `duckdb.connect()` connection from JIT code, numbduck must
recover the raw `Connection*`. `extract_connection_ptr` (`pybridge.py:10-74`) walks the
**pybind11 instance layout** with hard-coded offsets:

1. `id(conn) + 16` → `DuckDBPyConnection*` (`pybridge.py:59`).
2. `DuckDBPyConnection* + 32` → `Connection*` (`pybridge.py:62`).

It then validates by running `SELECT 1` through the C API (`pybridge.py:64-72`). The
docstring is explicit that these offsets are "validated on duckdb 1.3.2 / Linux x86-64 /
libstdc++" and "may change with major duckdb releases" (`pybridge.py:31-34`). This is the
most fragile external contract in the project (duckdb pin is `>=1.3.2,<1.6`, spanning
1.4/1.5 where the layout could shift).


## 7. The three UDF patterns

All three register with DuckDB from **Python setup code** and do the hot work in JIT.
DuckDB invokes C function pointers (`@cfunc.address`). Because `@cfunc` bodies cannot use
`import` or complex numba features, the universal shape is a module-level `@njit` impl +
a thin `@cfunc` wrapper that just forwards (`test_ducklib.md:54-75`; e.g.
`haversine.py:87-118`).

### 7a. Scalar chunk callback (throughput) — `examples/haversine.py`

One `@cfunc(void(intp, intp, intp))` = `(function_info, input_chunk, output_vector)`
(`haversine.py:116-118`). Inside the `@njit` impl (`haversine.py:87-113`): read chunk size
`duckdb_data_chunk_get_size`; for each input column get its vector
(`duckdb_data_chunk_get_vector`) then its raw data pointer (`duckdb_vector_get_data`);
`carray`-view every input and the output as `float64`; run one fused loop writing directly
into the output vector's buffer. No per-row Python; LLVM fuses the math. Registration
(`haversine.py:121-137`): `duckdb_create_scalar_function` → set name / add N params / set
return type (via `duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE)`) → `set_function(cb.address)`
→ `duckdb_register_scalar_function`. The logical type is destroyed right after via the
buffer idiom; the scalar-function handle is destroyed after registration.

### 7b. JIT query-loop (latency / GIL-free) — `examples/online_scoring.py`

Prepare once in Python (`duckdb_prepare`, `online_scoring.py:154-156`), then run a single
`@njit(nogil=True)` loop (`online_scoring.py:106-147`) that per iteration: `duckdb_bind_int64`
→ `duckdb_execute_prepared` → build the 6-tuple result → `duckdb_fetch_chunk` → get vectors
→ `carray` the values → compute → **destroy chunk and result each iteration** via the
buffer idiom (`online_scoring.py:142-144`). Timing is taken with numbox's JIT-callable
`monotonic_ns` so the whole loop stays in native code with the GIL released, which is what
gives parallel scaling. Note the loop `carray`s each result vector as size `(1,)`
unconditionally, assuming the `WHERE id = ?` lookup matches exactly one row.

### 7c. Aggregate / UDAF — `examples/irr.py`, `test/test_ducklib.py` (Welford)

The richest pattern; design doc is `test/test_ducklib.md`. Per-group aggregate state is a
**numba structref** whose `MemInfo*` is stashed in DuckDB's opaque state slot. numbduck
sets `state_size = 8` so the slot holds exactly one `intp` pointer (`test_ducklib.md:17`,
`irr.py:104-107`). Six lifecycle callbacks (`test_ducklib.md:15-51`), each `@cfunc`→`@njit`:

- **state_size** → returns 8 (`irr.py:104-111`).
- **init** → allocate the structref, `export_meminfo(s)` (incref + return `MemInfo*`),
  write the pointer into the slot (`irr.py:114-127`).
- **update** → per row, `borrow_structref(type, slot[0])` to reconstruct the structref,
  mutate it (`irr.py:129-167`). Includes NULL handling via
  `duckdb_vector_get_validity` + `duckdb_validity_row_is_valid` (`irr.py:140-153`).
- **combine** → reconstruct source+target structrefs, merge partials (parallel aggregation)
  (`irr.py:170-190`).
- **finalize** → reconstruct, compute the result, write into the output vector at
  `offset+i` (`irr.py:193-211`).
- **destroy** → `release_meminfo(slot[0])`; when refcount hits 0 NRT runs the structref's
  destructor, freeing nested allocations (`irr.py:214-225`).

Registration mirrors the scalar path but uses
`duckdb_aggregate_function_set_functions(func_p, state_size, init, update, combine, finalize)`
(6 args, matching the signature at `ducklib.py:1351-1354`) plus a separate
`_set_destructor` (`irr.py:244-252`).

**The NRT↔DuckDB bridge and why it works** (`test_ducklib.md:78-163`): the three bridge
intrinsics (`export_meminfo`, `borrow_structref`, `release_meminfo`, from
`numbox.utils.meminfo`) depend on numba's `removerefctpass` being **all-or-nothing**.
Callbacks have signatures with no NRT-tracked types (`void(intp, intp)`), so numba strips
*all* `NRT_incref`/`NRT_decref` pairs symmetrically — leaving the intended net refcount
change at each stage. `release_meminfo` deliberately calls `NRT_MemInfo_release` (not
`context.nrt.decref`) because any `NRT_`-prefixed non-accepted symbol makes `_legalize`
bail out and *skip* the pass for that whole function, so the decref survives
(`test_ducklib.md:101-127`). The doc itself flags this as version-fragile
(`test_ducklib.md:161-163`).


## 8. Error-handling contract

Bindings mirror the DuckDB C protocol exactly: they **return state codes**
(`DuckDBSuccess = 0` / `DuckDBError = 1`, `ducklib.py:19-20`) and pointers, and never
raise into C. Callers do C-style return-code checks (`CLAUDE.md` "Error Handling";
`online_scoring.py:156` asserts `rc == DuckDBSuccess`). A JIT loop that ignores `rc` after
a failing call proceeds on garbage/empty results — the contract puts that burden on the
caller.


## 9. Load-bearing details & fragile assumptions (for the defect pass)

1. **pybind11 offsets in `pybridge.py` (`+16`, `+32`)** are hard-coded and version/
   platform/stdlib-specific; validated only on duckdb 1.3.2 / Linux x86-64 / libstdc++
   (`pybridge.py:31-34,59,62`). The `SELECT 1` validation catches a *dead* pointer but not
   a plausibly-wrong-but-survivable one. Highest-risk external contract; pin allows 1.4/1.5.
2. **`duckdb_result` = 48 bytes / 6×int64** is hard-coded in both the buffer allocator
   (`duckdb_utils.py:36-39`) and `duckdb_result_ty` (`ducklib.py:68`). If DuckDB grows the
   result struct, buffers under-allocate → memory corruption.
3. **`_call_lib_func_byval` vs `_call_lib_func` for by-value `duckdb_result`**
   (`ducklib.py:943-946,1129-1138`) relies on the SysV/Win "large struct by value = pass in
   memory via pointer" ABI equivalence. Correct today but subtle; a wrong choice silently
   corrupts args.
4. **Symbol resolution assumes exactly one `libduckdb` wins `dlsym(RTLD_DEFAULT)`.** Two
   loaded copies (system vs wheel vs standalone), or the macOS shared-cache-wins hazard,
   could bind calls to the wrong library. `_call_lib_func` only presence-checks
   (`call.py:72-74`).
5. **`cache=True` default (`configurations.py:11`) hard-depends on the `@proxy`
   process-stable cfunc alias** (`proxy.py:20-37,79-80`); a regression there resurfaces as
   `Symbol not found: cfunc...` on cache load across processes. Malformed
   `NUMBDUCK_JIT_OPTIONS` raises `ValueError` at import (`configurations.py:15-16`).
6. **UDAF refcount correctness rides on numba `removerefctpass` staying all-or-nothing**
   and on `NRT_MemInfo_release` disabling the pass — an internal numba invariant, not a
   public API (`test_ducklib.md:101-163`).
7. **UDAF `state_size = 8`** assumes a single `intp` fits and that DuckDB always calls
   `init` before `update`/`combine`/`finalize` for a slot (`irr.py:104-107`,
   `test_ducklib.md:17`); a slot never seen by `init` would deref an uninitialized pointer.
8. **Query-loop reads assume fixed result shape** — e.g. `online_scoring.py:133-136`
   `carray`s each vector as `(1,)` regardless of actual chunk size; a zero-row match reads
   uninitialized/OOB memory. Pattern-level assumption, not a binding bug, but easy to
   replicate wrongly.
9. **Everything-is-`intp` (signed pointers).** Bit-patterns round-trip fine, but any
   signed comparison / arithmetic on a high-half address would misbehave; mixing with
   `uint64` index params (`ducklib.py` bind signatures) is a place to watch.
10. **`@proxy_if_available` stubs raise only at call time**, and never expose `.as_func`;
    any caller passing `.as_func` to a function-type arg must `hasattr`-guard
    (`proxy.py:145-163`).
