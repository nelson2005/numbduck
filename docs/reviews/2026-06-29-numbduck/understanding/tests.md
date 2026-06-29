# numbduck subsystem: Test architecture & coverage shape

Scope: `test/test_ducklib.py` (3617 lines, the integration suite) and
`test/test_init.py` (5 lines, import smoke test). This note maps the suite's
structure and explains how the test machinery actually works across the
Python / ctypes / numba-JIT / DuckDB-C boundaries. It flags load-bearing and
risky areas for a downstream defect review; it does **not** audit them.

All `file:line` cites are into `test/test_ducklib.py` unless noted.

---

## 1. Top-level shape

- **One flat module, no classes, no fixtures, no parametrization.** Every test
  is a bare `def test_*()`. Shared setup is done through hand-written `aux_*`
  helper functions, not pytest fixtures. Consequence: there is **no teardown
  safety net** — cleanup (disconnect/close/destroy/free) is inlined at the
  bottom of each test body, so any assertion failure mid-test skips cleanup and
  leaks DuckDB handles / NRT allocations (see §7).
- Databases are almost always `:memory:` and independent, so leaked handles
  rarely cross-contaminate later tests — but the leak-accounting tests (§6) are
  the exception and are sensitive to process-global state.
- `test_init.py` is a pure import smoke test: `assert numbduck` (test_init.py:4-5).
  No real coverage; exists to catch import-time breakage.

### Module-level data and the two query fixtures
- `i_col=[3,5,7]`, `j_col=[4,6,"NULL"]`, `arr_ty = ctypes.c_int32 * 3`
  (lines 102-104) — the canonical 2-column `integers` table, with `j_col[2]`
  deliberately NULL to exercise validity.
- `aux_query_1()` (107-129): open → connect → CREATE → INSERT → `SELECT *`,
  returns `(out_result, db, conn)`. The backbone for all result-metadata tests.
- `aux_get_data_vector()` (172-182): runs `aux_query_1`, fetches the first
  chunk, reads column 0, asserts it equals `i_col`. Backbone for chunk/vector
  tests.

---

## 2. Section-by-section map

The file is organized by `# --- ... ---` banner comments. In order:

| Lines | Section | What it exercises / asserts |
|-------|---------|------------------------------|
| 28-99 | DB lifecycle | open/close, invalid-path → `DuckDBError` (46-53), connect, connect-null-db → error (81-85), disconnect nulls the handle (88-99) |
| 102-235 | Query + chunk/vector | column_count/row_count (153-160), destroy_result, fetch_chunk, fetch exhaustion → null second chunk (212-219), validity readback `j_val==[1,1,0]` (222-235) |
| 238-410 | Prepared statements | prepare/destroy (nulls handle), invalid SQL, nparams, execute, `test_bind_all_types` (338-385), invalid param idx → error, unbound params → error |
| 413-474 | Error messages | `duckdb_prepare_error` / `duckdb_result_error` return non-null+nonempty on failure, null on success |
| 477-623 | Extra bind types | boolean/float/date/timestamp + an `*_invalid_param_index` error variant for each |
| 626-830 | Scalar bind types | int8/16, uint8/16/32/64, time, timestamp_tz, varchar_length (truncation to 5), blob (binary roundtrip), negative int8/16, `bind_parameter_index` (name "1" → idx 1) |
| 833-906 | Struct bind types | hugeint, uhugeint, interval, decimal — the **≤16B by-value ABI** path (load-bearing, §5) |
| 909-990 | Result metadata | column_name, column_type, column_logical_type (+destroy), rows_changed==3, result_error_type==0, result_return_type==3, result_statement_type==1 |
| 993-1160 | JIT (lifecycle + prepared) | run open/connect/query/disconnect and full prepare-bind-execute-readback **inside `@njit`** (§4) |
| 1163-1367 | Value interface | create/get round-trips for date/time/timestamp(*ms/ns/s/tz), blob, hugeint(+neg), uhugeint(+large), interval(+zero), decimal, uuid, varint (skipif), bit |
| 1370-1466 | Scalar value create/get | bool, int8-64, uint8-64, float, double |
| 1469-1508 | String values | varchar, varchar_length (embedded-NUL truncation), value_to_string("42") |
| 1511-1525 | Null values | create_null_value / is_null_value true & false |
| 1528-1681 | Container values | list, map, struct, array; `test_list_value_from_column_logical_type` derives child type from a real query result |
| 1684-1700 | Value type/destroy | `test_get_value_type` documents a **shared-handle double-free hazard** (1692-1694) |
| 1703-1724 | Struct size guard | `test_struct_size_guard` — pure-Python arithmetic on `bitwidth`, does **not** touch ducklib codegen (§5) |
| 1727-1919 | Logical types | create_logical_type, decimal_type (width/scale/internal), alias get/set, list/array/map/struct/union/enum constructors + introspection |
| 1924-2197 | Scalar UDFs | round-trip add_one (43), extra_info magic round-trip, set_error → query fails, overloaded int/double function set |
| 2203-2355 | Aggregate UDF | int32 sum via 5-callback ABI → 6 |
| 2362-2420 | scalar set_init | skipif `duckdb_scalar_function_set_init` absent (v1.5+) |
| 2427-2531 | Aggregate set overloads | int32 + int64 input variants in one set |
| 2538-2664 | Hybrid JIT-on-Python-conn | `extract_connection_ptr` + `_isqrt`, and JIT-vs-Python-UDF equality over 100 rows |
| 2667-2791 | Benchmark | `@pytest.mark.benchmark`; times Python/Arrow/JIT `x*x`, asserts result equality |
| 2794-2919 | MemInfo bridge intrinsics | `_incref_meminfo`, `export_meminfo`, `_deref_structref_raw_ptr`, `borrow_structref`, `_release_meminfo`/`release_meminfo`; a commented-out `_refcount_of_meminfo` preserved as a cautionary example (2882-2919) |
| 2922-2996 | structref/heap state types | WelfordState structref + `_read_refcount` ctypes helper + heap-owning `_HeapState` |
| 2999-3085 | Refcount ladders | structref ladder (1→1→0) and array ladder; assert refcount via Python between separate `@njit` calls |
| 3088-3373 | UDAF callbacks | array-backed variance + Welford stddev callback families |
| 3376-3617 | End-to-end UDAF + leak | numba-only Welford check, nested-heap dtor cascade (100 cycles), structref stddev UDAF, array variance UDAF — last two assert `alloc_delta == free_delta` |

---

## 3. The repeated ctypes / pointer idioms (load-bearing, fragile)

These patterns recur in nearly every test and are the most defect-prone surface.

### 3a. Handle buffers
DuckDB out-params are modeled as 1-element numpy arrays whose `.ctypes.data`
address is passed as a `**` pointer-to-handle. Allocated by
`duckdb_utils.create_duckdb_database/connection/result/...`. Handle reads use
`buf[0]`; destroys store the handle then pass the buffer address, e.g.
`aux_destroy_value` (1165-1170) and the inline `numpy.array([p], dtype=intp);
destroy(buf.ctypes.data)` idiom used everywhere for logical-type / function /
chunk destruction.

### 3b. String / void* argument plumbing
Two distinct conventions appear and are easy to confuse:
- `q_p = get_unicode_data_p(text)` — numbox helper, safe inside `@njit`
  (comment 994-995); used for SQL text and UDF names.
- `b = ctypes.c_char_p(b"..."); p = ctypes.c_void_p.from_buffer(b).value`
  (e.g. 37-38, 353-354, 824-825, 1763-1764) — **the `c_char_p` local `b` must
  stay alive for the duration of the call.** It does here because the call is
  immediate, but this is an implicit-lifetime assumption, not enforced.

### 3c. Result readback
- Python side: `(ctypes.c_int32 * N).from_address(data_p)` /
  `arr_ty.from_address(...)` — reads the raw flat vector. Assumes the DuckDB
  flat-vector physical layout (and for strings, the inline `{uint32 len; char
  data[]}` `string_t`, `aux_read_inline_string` 267-272).
- JIT side: `carray(_cast_int_to_void_p(data_p), (n,), dtype=...)` — `intp` must
  be bridged to `voidptr` via numbox's `_cast_int_to_void_p` (CLAUDE.md key
  pattern #3).

### 3d. Single-chunk assumption
Every result test fetches **exactly one** chunk via
`duckdb_fetch_chunk(tuple(result))` and reads it. The only multi-chunk behavior
tested is *exhaustion* (a 2nd fetch returns null, 212-219). No test iterates a
result larger than one 2048-row vector → multi-chunk paths are uncovered.

---

## 4. The JIT boundary (§ "JIT Tests", 993-1160)

- Pointers are `intp` throughout numbduck (CLAUDE.md). JIT tests build the same
  open/connect/query/destroy sequence inside `@njit`, passing `array_data_p(buf)`
  (numbox) as the handle-buffer address.
- `jit_prepare_bind_execute` (1061-1131) is the densest JIT test: prepare, bind
  4 typed params (note explicit `numpy.uint64(...)`/`numpy.int32(...)` casts on
  args — 1081-1084), execute, fetch chunk, read 3 columns via `carray`, check a
  NULL via `duckdb_validity_row_is_valid(intp(...), intp(0))`, then destroy in
  reverse order. It asserts the cleanup ordering implicitly (no crash) rather
  than verifying it.
- Invariant relied on: `get_unicode_data_p` extracts the data pointer directly
  (numbox ≥0.5.6) instead of going through NRT meminfo, so it's `@njit`-safe.

---

## 5. Struct-by-value ABI tests (load-bearing, thinly verified)

hugeint/uhugeint/interval/decimal bind (835-906) and the value create/get
round-trips (1263-1356) drive the custom ≤16B by-value lowering described in
CLAUDE.md (`_call_lib_func_struct_in/out`, `_build_packed_interval`, the
hand-rolled intrinsics). Interval (16B `{i32,i32,i64}`) is the one needing the
SysV eightbyte repack — exercised by `test_bind_interval` (871-888) and
`test_create_get_interval` (1299-1316).

**Risk to flag:** `test_struct_size_guard` (1706-1724) only checks the
*arithmetic* `sum(t.bitwidth)/8` against expected byte sizes — it imports numba
types and does no ducklib call. It does **not** validate that the guard actually
gates the codegen, nor that >16B (decimal 24B, varint 24B) take the intrinsic
path. The real ABI correctness rests entirely on the round-trip value
assertions, all on Linux/SysV. The Windows by-pointer/sret branches are not
exercised by this suite.

---

## 6. NRT / MemInfo bridge — the highest-value, most fragile cluster (2794-3617)

This is the subsystem the suite most deeply validates and where the most subtle
assumptions live.

**Mechanism.** DuckDB gives each aggregate a fixed-size state slot (8 or 16
bytes). The init callback allocates a numba structref or numpy array, increfs
its MemInfo, and writes the MemInfo pointer (`intp`) into the slot. Update/
combine/finalize re-`borrow` the structref from the raw pointer; the destructor
callback decrefs. Bridges:
- `_incref_meminfo` (2817-2825) — inlined intrinsic, `context.nrt.incref`.
- `export_meminfo` (2828-2832) — `@njit` wrapper; **relies on the intrinsic
  inlining** because it returns plain `intp`.
- `_deref_structref_raw_ptr` / `borrow_structref` (2835-2853) — rebuild a
  structref value from a raw MemInfo pointer (incref on entry; numba's
  scope-exit decref balances it → net zero for the external owner).
- `_release_meminfo` (2856-2874) — deliberately calls the **C runtime**
  `NRT_MemInfo_release` rather than `context.nrt.decref`, to dodge
  `removerefctpass` stripping (documented 2858-2864).

**The removerefctpass landmine (documented at length 2882-2903).** Because the
bridge functions' signatures contain no NRT-tracked types, `removerefctpass`
strips *all* `NRT_incref/decref` from them. The export's incref and the local
scope-exit decref are stripped *symmetrically*, so net refcount is correct — but
any attempt to observe an intermediate `refcount==2` from JIT fails. Hence the
commented-out `_refcount_of_meminfo` and the design choice to read refcount only
from Python *between* separate `@njit` calls (`_read_refcount`, 2971-2977).

**Verification strategy.**
- Refcount ladders (2999-3085) assert `_read_refcount(p)` equals 1 after
  export+local-drop and after borrow, then release. The array ladder note
  (3042-3053) explains the subtle difference: `numpy.zeros` emits
  `NRT_MemInfo_alloc*`, which itself disables `removerefctpass`, so there the
  inline incref must be used (not the `export_meminfo` wrapper).
- Leak tests (3412-3457, 3460-3534, 3537-3617) use
  `rtsys.get_allocation_stats()` with `memsys_enable_stats()` and assert
  `alloc_delta == free_delta` across a query / 100 cycles. These are the
  strongest correctness checks in the suite.

**Risks to flag (not audited):**
- `_read_refcount` hardcodes that `MemInfo.refct` is the **first field
  (`size_t`)**, "stable since numba 0.50" (2974). A numba layout change silently
  breaks every ladder assertion.
- Allocation-stats deltas are **process-global**. The tests are robust to a
  constant offset (they compare alloc vs free deltas) but assume no other
  concurrent NRT activity between the before/after snapshots; `memsys_enable_stats`
  is enabled per-test and never disabled.
- `_welford_update_impl` explicitly **skips NULL validity checks** (NOTE
  3295-3296) — correct only because the test data has no NULLs. The UDAF
  callbacks generally assume dense, all-valid input vectors.
- The aggregate callback ABI (state-slot arrays indexed by `count`,
  `offset+count` sizing in finalize, e.g. 2281-2290, 3189-3209) mirrors DuckDB's
  C aggregate calling convention by hand. A DuckDB-side convention change would
  corrupt silently rather than fail a signature check.

---

## 7. Cross-cutting coverage gaps / thin spots (map only)

- **No teardown safety.** Cleanup is inline; a failing assertion leaks handles
  and NRT allocations. The leak tests themselves can be perturbed by an earlier
  failing test in the same process.
- **Multi-chunk results untested** (§3d) — only single 2048-row vectors and
  exhaustion.
- **NULL/validity** is tested in only a few places (j_col 222-235, bind_null,
  null-value 1514-1525); most readback tests assume all-valid.
- **Decimal physical width >18** (hugeint-backed DECIMAL) is untested; only the
  int64 `DECIMAL(10,2)` path (891-906, 1319-1327).
- **Windows ABI branches** (struct by-pointer / sret) are not exercised.
- **Version-gated features** (`duckdb_create_varint`/`get_varint` 1341-1344,
  `duckdb_scalar_function_set_init` 2372-2375) are `skipif`-guarded, so they may
  silently not run on the CI DuckDB version.
- **Shared-handle double-free hazard** for `duckdb_get_value_type` on scalars is
  only *documented in a comment* (1692-1694), not asserted/guarded.
- **Hybrid/benchmark tests** depend on `numbduck.pybridge.extract_connection_ptr`
  reaching into a live Python `duckdb.connect()` connection (2564-2569,
  2621-2626, 2701-2724) — an integration point against duckdb-python internals
  not validated for version drift here.
- **Error paths** are covered for invalid SQL, invalid param index, unbound
  params, null DB, and callback `set_error`; but not for allocation failure,
  destroy-of-null/partial handles, or mid-stream cancellation.

---

## 8. One-line takeaways for the defect reviewer

1. The ctypes lifetime idiom (`c_char_p` local kept alive only by being a local,
   §3b) is correct-by-immediacy, not by construction — scan for any deferred use.
2. Struct-by-value correctness rests on round-trip asserts on Linux only;
   `test_struct_size_guard` does not actually test ducklib (§5).
3. The MemInfo bridge is the deepest machinery; its correctness depends on
   `removerefctpass` symmetry, the `MemInfo.refct`-at-offset-0 assumption, and
   process-global alloc stats (§6).
4. Single-chunk, all-valid, in-memory assumptions pervade the result-reading
   tests (§3d, §7).
