# numbduck subsystem understanding — Test architecture & coverage shape

Scope: `test/test_ducklib.py` (3549 lines) and `test/test_init.py` (6 lines), as they
stand at HEAD (branch `review/numbduck-2026-06-29`). This is a *map*, not an audit.
Cites are `file:line`. Feeds a downstream defect review, so load-bearing mechanisms
and risk-shaped spots are flagged inline.

---

## 1. What these files are

- `test/test_init.py:1-6` — a smoke import test: `import numbduck; assert numbduck`.
  Nothing else.
- `test/test_ducklib.py` — a single flat module of ~180 top-level `test_*` functions
  plus ~40 module-level `@njit`/`@cfunc` callback definitions and helper `aux_*`
  functions. **No `conftest.py`, no fixtures, no parametrization, no classes.** Config
  lives entirely in `pyproject.toml:24-27` (only a `benchmark` marker, "deselected in
  CI"). Verified: `find` returned no conftest/pytest.ini/setup.cfg/tox.ini.

The file is organized by hand-written banner comments into sections (see §3). Every
test is a straight-line procedure that allocates handles, calls `ducklib` wrappers,
asserts, and manually tears down. There is no shared setup/teardown machinery, so a
mid-test assertion failure leaks the DB/connection/result for that test (no
`try/finally`); this affects test isolation, not library correctness.

---

## 2. The core mechanisms the tests exercise

### 2.1 Handle-out-parameter pattern (the spine of the suite)
DuckDB C API writes opaque handles into caller-provided `T*` out-params. Tests model
each handle as a **1-element numpy buffer**:

- `numbduck/duckdb_utils.py:7-51` — `allocate_buffer(sz)` returns `numpy.zeros(sz,
  dtype=int64)` under `@njit(**jit_options)`; `create_duckdb_database/connection/
  prepared_statement/data_chunk/value/vector` all allocate size **1**;
  `create_duckdb_result` allocates size **6** (`duckdb_utils.py:36-39`), i.e. the
  DuckDB `duckdb_result` struct is treated as 6×int64 = 48 bytes.
- A handle is passed as `buf.ctypes.data` (Python) or `array_data_p(buf)` (JIT) — the
  address of the buffer, i.e. a pointer-to-handle. Example: `aux_open_database`
  (`test_ducklib.py:28-33`).
- The dereferenced handle is read as `buf[0]`. Destroy functions zero the slot; the
  suite asserts this as its liveness check, e.g. `test_disconnect`
  (`test_ducklib.py:98`), `test_prepare_and_destroy` (`test_ducklib.py:287`),
  `test_duckdb_destroy_data_chunk` (`test_ducklib.py:208`).

### 2.2 Pass-a-struct-by-value via a Python tuple
Functions that take/return the whole `duckdb_result` **by value** are called by passing
`tuple(out_result)` — a 6-tuple of int64 — which routes through
`_call_lib_func_byval` (arg passed by pointer regardless of platform, per CLAUDE.md).
Load-bearing and easy to get wrong. Sites:
- `duckdb_fetch_chunk(tuple(result))` — `test_ducklib.py:175, 214-217, 256, 1971,
  2029, 2173, 2346, 2446(via tuple), 3446, 3528`.
- `duckdb_result_return_type(result_tup)` — `test_ducklib.py:977`.
- `duckdb_result_statement_type(result_tup)` — `test_ducklib.py:987`.
- In JIT the tuple is built by hand: `(result[0], ... result[5])`
  (`test_ducklib.py:1091-1092`), then `duckdb_fetch_chunk(result_tup)`
  (`test_ducklib.py:1093`).

### 2.3 Reading result column data across the C boundary
Two readers, one per side of the JIT boundary:
- **Python side:** `(ctypes.c_int32 * N).from_address(data_p)` / `.from_address(data_p +
  offset)` on the raw vector data pointer. Pervasive; helper `aux_read_column_data`
  (`test_ducklib.py:261-264`). Struct fields read by manual byte offsets, e.g. hugeint
  lower/upper at `data_p` / `data_p+8` (`test_ducklib.py:844-845`), interval
  months/days/micros at `+0/+4/+8` (`test_ducklib.py:880-882`).
- **JIT side:** `carray(_cast_int_to_void_p(data_p), (n,), dtype=...)`. The
  `_cast_int_to_void_p` intrinsic (from numbox) bridges numbduck's `intp` pointer
  convention to the `voidptr` that `carray` demands. Example `test_ducklib.py:1097-
  1112`, and every UDF callback.

### 2.4 Inline string / blob layout assumption
`aux_read_inline_string` (`test_ducklib.py:267-272`) decodes DuckDB `string_t` as
`uint32 length` at `data_p` followed by inline chars at `data_p+4`. BLOB read uses the
same layout (`test_ducklib.py:778-782`). **This only matches the short-string inline
representation (len ≤ 12).** All tested strings ("hello", "hello world"[:5],
"42", 4-byte blob) are short; the long-string *pointer* layout (len > 12, data stored
out-of-line) is never exercised.

### 2.5 JIT boundary (`@njit` calling ducklib directly)
Section "JIT Tests" (`test_ducklib.py:993-1160`) proves the wrappers are callable
inside `@njit`: `jit_open_close` (`1013-1020`), `jit_connect_query_disconnect`
(`1029-1046`), `jit_prepare_bind_execute` (`1061-1131`). SQL strings enter JIT via
`get_unicode_data_p(...)` (numbox); comment at `994-995` notes this is NRT-safe only
with numbox ≥ 0.5.6. Bind args are explicitly cast (`numpy.uint64(1)`,
`numpy.int32(99)`, `test_ducklib.py:1081-1084`).

### 2.6 C callback boundary (`@cfunc` over `@njit` impl)
The UDF/UDAF machinery follows a fixed two-layer idiom (CLAUDE.md "Key patterns"):
module-level `@njit def _x_impl(...)` + thin `@cfunc(...) def _x_cb(...)` wrapper whose
`.address` is handed to DuckDB. **All `@cfunc` signatures use `nb_types.intp` for every
pointer** (never `voidptr`) — e.g. `test_ducklib.py:1936, 2211, 2221, 2238, 2274-2275,
2293-2294`. State-size callbacks return `nb_types.uint64` and the impl returns
`numpy.uint64(8/16)` (`test_ducklib.py:2206-2213, 3039-3045, 3204-3211`). Callback
addresses are registered via `duckdb_scalar_function_set_function` /
`duckdb_aggregate_function_set_functions` / `_set_destructor`
(`test_ducklib.py:1958, 2328-2332, 3422-3431`).

### 2.7 structref ↔ raw MemInfo bridge (the most intricate area)
`test_ducklib.py:2794-3549`. UDAF state slots hold a numba NRT MemInfo pointer;
lifecycle is bridged with numbox primitives imported at `test_ducklib.py:10-14`:
`export_meminfo` (+1 incref), `borrow_structref` (net-zero borrow),
`release_meminfo` (-1 decref → dtor at 0), `_incref_meminfo` (inlining intrinsic),
`structref_meminfo`. A long design comment (`2794-2851`) documents the
`removerefctpass` interaction (numba strips incref/decref when args/return are plain
`intp`, so an in-JIT refcount==2 observation is impossible; the stripping is symmetric
so liveness is preserved). The dead `_refcount_of_meminfo` intrinsic is preserved
commented-out as a cautionary example (`2836-2851`).

Verification tooling:
- `_read_refcount(meminfo_intp)` reads MemInfo's first field (`size_t refct`) via
  ctypes (`test_ducklib.py:2903-2909`).
- Leak checks use `rtsys.get_allocation_stats()` and assert `alloc == free`
  (`test_ducklib.py:3384-3389, 3462-3466, 3544-3548`).

### 2.8 Python-connection bridge (`pybridge`)
`numbduck/pybridge.py:10-74` `extract_connection_ptr(conn)` reads the raw
`Connection*` out of a live `duckdb.DuckDBPyConnection` by walking **hardcoded
pybind11 offsets** (`id(conn)+16` → C++ object, then `+32` → `Connection*`,
`pybridge.py:59-62`), then validates with a `SELECT 1` through the C API
(`pybridge.py:64-72`). Three tests depend on this:
`test_hybrid_jit_udf_on_python_connection` (`test_ducklib.py:2562`),
`test_jit_udf_vs_python_udf` (`test_ducklib.py:2619`), `test_udf_benchmark`
(`test_ducklib.py:2685`). Offsets documented valid only on duckdb 1.3.2 / Linux
x86-64 / libstdc++ (`pybridge.py:31-34`).

### 2.9 Version gating
Two symbols are probed at collection time via `hasattr(ducklib.duckdb_lib, ...)`:
- `test_create_get_varint` skips unless `duckdb_create_varint`/`duckdb_get_varint`
  exist (`test_ducklib.py:1341-1344`).
- `test_scalar_function_set_init` skips unless `duckdb_scalar_function_set_init`
  exists — "v1.5+ only" (`test_ducklib.py:2372-2375`).
This is the visible seam of the `duckdb>=1.3.2,<1.6` pin; most other wrappers are
assumed present across the whole range with no gate.

---

## 3. Coverage map (by section)

| Lines | Section | Shape of assertions |
|---|---|---|
| 36-53 | open/close DB | rc==Success/Error, handle!=0; invalid path |
| 56-99 | connect/disconnect | rc, handle!=0, null-after-disconnect; null-DB error |
| 102-236 | query + chunk/vector | col/row counts (2/3), validity `[1,1,0]`, chunk exhaustion, destroy |
| 238-411 | prepared statements | prepare/destroy, nparams, execute, bind-all-types, invalid index, unbound-param error |
| 413-474 | error messages | prepare_error / result_error non-null on failure, null on success |
| 477-623 | bind bool/float/date/timestamp | happy-path round-trip + one invalid-index each |
| 626-906 | scalar & struct binds | int8/16, uint8/16/32/64, time, timestamptz, varchar_length, blob, negatives, hugeint, uhugeint, interval, decimal |
| 909-990 | result metadata | column_name/type/logical_type, rows_changed, error_type, return_type, statement_type |
| 993-1160 | JIT calls | open/close, connect+query+disconnect, prepare+bind+execute — all assert Success only |
| 1163-1367 | value create/get | dates, times, timestamps (ms/ns/s/tz), blob, hugeint(+neg), uhugeint(+large), interval(+zero), decimal, uuid, varint(gated), bit |
| 1370-1508 | scalar/string values | bool/int/uint/float/double create+get; varchar, varchar_length, value_to_string |
| 1511-1525 | null values | create_null_value, is_null_value true/false |
| 1528-1681 | container values | list(+empty), map, struct, array, list-from-column-logical-type |
| 1684-1724 | value type/destroy, size guard | get_value_type (double-free note), destroy_value; `test_struct_size_guard` |
| 1727-1919 | logical types | integer/varchar, decimal_type, alias set/get, list/array/map/struct/union/enum type |
| 1924-2198 | scalar UDFs | add_one round-trip, extra_info, set_error→query fail, overload set (int/double) |
| 2203-2531 | aggregate UDFs | my_sum round-trip, set_init (gated), aggregate overload set (int32/int64) |
| 2538-2791 | hybrid/py-connection | jit_isqrt on Python conn, jit vs py UDF equality, benchmark (also asserts equality) |
| 2794-3549 | meminfo bridges & UDAFs | refcount ladder, array meminfo ladder, nested-heap dtor cascade, welford numba-only, structref stddev UDAF, array variance UDAF (all with leak asserts on the last few) |

---

## 4. Invariants the suite pins

- **C-style error protocol** (never exceptions): `rc == ducklib.DuckDBSuccess` /
  `DuckDBError` everywhere. Matches CLAUDE.md "Error Handling".
- **Handle liveness via slot zeroing**: destroy → `buf[0] == 0`.
- **Value round-trip**: bound/created value equals the value read back from the result
  vector or getter, within tolerance for floats (`< 1e-10`, `< 1e-6`,
  `test_ducklib.py:372, 534`).
- **Validity bitmap**: `[1,1,0]` for the fixture's `j_col=[4,6,NULL]`
  (`test_ducklib.py:230`).
- **Refcount ladder collapses to 1** after export+local-drop and after borrow
  (`test_ducklib.py:2960, 2968, 3008, 3015`).
- **No leak**: `alloc == free` across N repeated cycles / an end-to-end UDAF query
  (`test_ducklib.py:3386-3389, 3465, 3547`).
- **ABI size guard math**: `test_struct_size_guard` (`1706-1724`) asserts the ≤16-byte
  threshold arithmetic — but see §5, it is pure Python and does not touch ducklib.

---

## 5. Thin / fragile spots (flagged for the defect review — not audited)

1. **Leak checking is almost absent outside the bridge tests.** Only 4 tests assert
   `alloc==free` (`3386, 3465, 3547`) or the refcount ladder. The ~150 value / logical-
   type / bind tests create DuckDB handles (`duckdb_value`, logical types, function
   handles) and destroy them by hand, but nothing asserts the destroy actually ran or
   that the wrapper didn't leak — a wrapper that silently dropped a handle would pass.

2. **NULL handling in UDF/UDAF update paths is untested by design.** Explicit NOTE at
   `test_ducklib.py:3227-3228`: welford update "skips NULL validity checks — test data
   has no NULLs." Every UDF callback (`_add_one_impl`, `_arr_update_impl`,
   `_welford_update_impl`, etc.) reads `duckdb_vector_get_data` and never calls
   `duckdb_vector_get_validity`. Correctness is only shown for dense, NULL-free input.

3. **Struct-by-value ABI is only smoke-tested.** hugeint/uhugeint/interval/decimal are
   each exercised with a single happy-path value (and one zero/edge, e.g. interval
   `(0,0,0)` at `1310`). The interval eightbyte repack (`_build_packed_interval`, per
   CLAUDE.md) is exercised only by `(1,2,3000000)` and `(0,0,0)`. Decimal is only the
   width≤18 → INT64 physical path (`test_ducklib.py:900-903, 1319-1327`); the INT128
   (width > 18) path is never hit. varint (24B) is version-gated and lightly tested.

4. **`test_struct_size_guard` does not test the guard.** `test_ducklib.py:1706-1724`
   only computes `sum(t.bitwidth ...)/8` on numba type tuples and asserts the numbers —
   it never invokes `_call_lib_func_struct_in/out` or any ducklib code path. It is a
   documentation-grade arithmetic check, not a behavioral guard test.

5. **Windows/sret ABI path is never exercised.** The whole suite runs SysV x86-64
   (benchmark docstring confirms WSL2/Linux, `test_ducklib.py:2693`). The
   `_call_lib_func_struct_in/out` Windows branches and any sret handling are untested
   here.

6. **`pybridge` offsets are a hard dependency of 3 tests.** `extract_connection_ptr`
   hardcodes `+16`/`+32` (`pybridge.py:59-62`) valid only on duckdb 1.3.2/Linux/
   libstdc++; the pin allows up to `<1.6`. The internal `SELECT 1` validation
   (`pybridge.py:64-72`) catches a *grossly* wrong pointer but would not catch a
   plausible-but-wrong offset that still lands on readable memory.

7. **JIT error paths are untested.** All JIT tests (`993-1160`) assert only Success;
   there is no invalid-SQL / failed-bind / null-handle case inside `@njit`.

8. **Double-free hazard encoded only as a comment.** `test_get_value_type`
   (`test_ducklib.py:1687-1694`) notes `duckdb_get_value_type` returns the *same* handle
   as the value for scalar types and warns "do NOT destroy both" — the safety is a human
   comment, not an assertion or wrapper guard.

9. **Single-chunk / small-vector only.** The query fixture `aux_query_1`
   (`test_ducklib.py:107-129`) inserts exactly 3 rows (`i_col=[3,5,7]`,
   `j_col=[4,6,"NULL"]`, `test_ducklib.py:102-103`). Chunk exhaustion is validated only
   for a 3-row single chunk (`test_ducklib.py:212-219`). No multi-chunk result, no
   vector larger than the DuckDB standard 2048 size, no multi-`fetch_chunk` loop.

10. **Short-string layout only.** See §2.4 — the long (>12 char, out-of-line) `string_t`
    representation is never read back.

11. **Manual per-test teardown, no isolation guarantees.** Cleanup is imperative
    (`aux_close_db`, `duckdb_destroy_*`) with no `try/finally`; an early assert leaks the
    DB/connection/result for that test. Benign for library correctness, but means a
    failing test can perturb the process for later tests (esp. the NRT stats-based leak
    tests that assume a clean allocator baseline).

12. **Heavy reliance on numbox internals pinned to `numbox>=0.5.13`.** The suite imports
    non-trivially private numbox symbols (`_incref_meminfo`, `structref_meminfo`,
    `borrow_structref`, `export_meminfo`, `release_meminfo`, `get_unicode_data_p`,
    `_cast_int_to_void_p`, `array_data_p`, `_cast_int_to_void_p`) at
    `test_ducklib.py:10-25`. A numbox API/semantic drift would break the boundary these
    tests are meant to validate — the tests assume, rather than verify, those
    primitives' refcount contract (only §2.7's ladder tests probe it directly).

---

## 6. Load-bearing details worth carrying into the defect review

- The **6-slot int64 result buffer** (`duckdb_utils.py:36-39`) and its **`tuple(result)`
  by-value pass** (`_call_lib_func_byval`) is the single most reused non-trivial ABI
  contract; `fetch_chunk`, `result_return_type`, `result_statement_type` all ride it.
- **`intp` everywhere for pointers** — ducklib wrappers, `@cfunc` signatures, and the
  `_cast_int_to_void_p` bridge to `carray`. Any wrapper that expected `voidptr` would be
  a mismatch the suite would surface only in UDF paths.
- **State-size callbacks must return `uint64`**; init callbacks write a MemInfo pointer
  into an 8-byte (structref) or 16-byte (array: `[meminfo_p, data_p]`) slot; destructor
  callbacks `release_meminfo(slot[0])`. The array variant's 16-byte layout is defined at
  `test_ducklib.py:3031-3035`.
- **`jit_options`** flows `configurations.py:19` → `duckdb_utils.py` decorators
  (`@njit(**jit_options)`), default `{"cache": True}`; the buffer allocators are cached
  njit functions. (ducklib wrappers use `@proxy(..., jit_options=jit_options)` per the
  task brief; not defined in the test file.)
