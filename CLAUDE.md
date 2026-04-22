# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

numbduck — adapts DuckDB's C API for use inside numba `@njit` code. Built on the [numbox](https://github.com/Goykhman/numbox) bindings toolkit.

## Build & Dev

- Venv: `python3.10 -m venv venv && venv/bin/pip install -e . flake8 pytest`
- Install: `pip install -e .`
- Test: `pytest` — always clean `__pycache__`, `~/.cache/numba`, AND numba cache in numbox (`find venv -name '*.nbi' -delete -o -name '*.nbc' -delete`) before every run. `make_structref` caches compiled code in numbox's `__pycache__` (not the project's), and stale entries cause type identity mismatches.
- Lint: `flake8`
- Line width: 120 characters
- Python: >=3.10
- Key dependencies: `duckdb>=1.3.2,<1.6`, `numbox~=0.5.8`
- Stale numba caches cause type identity mismatches (e.g., structref types look identical but numba sees them as different). `make_structref` uses `highlevel.py` as the source filename for `exec`'d code, so numba caches in numbox's `__pycache__/` — clearing only the project's `__pycache__` is insufficient.

## Architecture

### Bindings (ducklib.py)

Wraps DuckDB C API functions for use in numba JIT code. Same pattern as numbox bindings:

1. **`utils.py`** — finds and loads the DuckDB shared library via `ctypes.CDLL` with `RTLD_GLOBAL`
2. **`ducklib.py`** — registers signatures in numbox's `signatures` dict, then wraps each function with `@cres` + `_call_lib_func`
3. **`duckdb_utils.py`** — allocates numpy buffers for DuckDB C structs (database, connection, result, chunk, vector)

### Adding a New Binding

1. **Check `duckdb.h` first.** Look up the function signature in [`src/include/duckdb.h`](https://github.com/duckdb/duckdb/blob/v1.3.2/src/include/duckdb.h). Trace every typedef to its underlying type (e.g. `idx_t` → `uint64_t` → `uint64`, `duckdb_prepared_statement` is a pointer → use `_p` suffix). Verify all docstring links point to the correct line in `duckdb.h` at implementation time.
2. Add signature to `ducklib.py`: `signatures["duckdb_func"] = return_type(arg_types...)`
3. Add wrapper following this pattern:
```python
@cres(signatures.get("duckdb_func"))
def duckdb_func(arg):
    return _call_lib_func("duckdb_func", (arg,))
```
4. Function names must match the DuckDB C API names exactly
5. Docstring links must use `https://duckdb.org/docs/stable/clients/c/api.html#func_name`, not links to `duckdb.h` source
6. If a function returns a handle (e.g. `duckdb_logical_type`), also bind the corresponding destroy function (e.g. `duckdb_destroy_logical_type`)
7. **Before submitting an upstream PR**, re-verify all signatures, parameter types, naming conventions (`_p`/`_pp`), and docstring links against `duckdb.h` — this is a second check; step 1 is the first

### Struct-by-value helpers (ducklib.py)

For C functions that pass or return structs by value, `ducklib.py` provides:

- **`_call_lib_func_struct_in`** — ≤16-byte struct passed by value (SysV x86-64) or by pointer (Windows)
- **`_call_lib_func_struct_out`** — ≤16-byte struct returned by value (SysV x86-64) or via sret (Windows)
- **`_call_lib_func_byval`** — arg passed by pointer regardless of platform (e.g. `duckdb_result *`)
- **`_emit_byval_call`** — shared codegen helper for alloca+store+call-via-pointer
- **`_build_packed_interval`** — packs `{i32, i32, i64}` interval into `{i64, i64}` (LLVM drops the second i32 on SysV x86-64)

Custom `@intrinsic` functions are used for >16-byte structs (decimal 24B, varint 24B) and interval (16B but needs repacking). These use `byval` + `optnone` on SysV x86-64 to prevent LLVM from optimizing away stack copies. See [llvmlite#300 comment](https://github.com/numba/llvmlite/issues/300#issuecomment-327235846) for the ABI rationale.

### UDAF Type Resolution

The return type of an aggregate function is fixed at registration time via `duckdb_aggregate_function_set_return_type`, independent of the input type set via `duckdb_aggregate_function_add_parameter`. There is no bind-time type inference callback for aggregates (unlike scalar functions which have `duckdb_scalar_function_set_bind`).

To support multiple input types (e.g., `my_min(int)→int` and `my_min(double)→double`), create separate aggregate function objects — each with its own parameter/return type pair — and group them in a function set:

1. `duckdb_create_aggregate_function_set(name)` — create the set
2. For each (input_type, return_type) pair: create a function, add parameter, set return type, set callbacks, add to set
3. `duckdb_register_aggregate_function_set(conn, set)` — register all variants

DuckDB resolves the correct overload at query planning time. Same-type aggregates (min, max) use input type = return type. Type-changing aggregates (avg, stddev) typically return `DOUBLE` regardless of input type. No additional numbduck machinery is needed — the existing C API bindings and function set APIs handle it directly.

## Key Paths

- `numbduck/ducklib.py` — all DuckDB C API bindings
- `numbduck/duckdb_utils.py` — buffer allocators for DuckDB structs
- `numbduck/utils.py` — shared library loader
- `test/test_ducklib.py` — integration tests

## Error Handling

Bindings must mirror the DuckDB C API error-handling protocol exactly — return state codes (`DuckDBSuccess`/`DuckDBError`) and pointers, never inject exceptions. Callers do C-style return code checks. See [numbox issue #5](https://github.com/Goykhman/numbox/issues/5) for rationale.

## Preferences

Cross-project preferences live in the user's MEMORY.md. Only numbduck-specific workflow rules are kept here.

- Always exclude CLAUDE.md, `.github/workflows/numbduck_ci.yml`, and `docs/plans/` from upstream PRs (use a dedicated branch based on `upstream/main`)
- Never merge local feature branches into main — main must always match `upstream/main` (exception: CLAUDE.md and the fork-only CI workflow)
- Feature branches: base off `origin/main` (has CLAUDE.md); upstream PR branches: base off `upstream/main` (no CLAUDE.md)
- Do all coding work on the feature branch (has CLAUDE.md + fork CI), then cherry-pick to the upstream PR branch when ready

## Related Projects

- **[numbox](https://github.com/Goykhman/numbox)** — bindings toolkit that numbduck is built on. Provides `signatures` dict, `_call_lib_func`, `@cres` decorator, and shared library loading patterns. Read numbox source before implementing new binding patterns.
- **[numbarrow](https://github.com/Goykhman/numbarrow)** — bridges PyArrow arrays into numba `@njit` code (Arrow → numpy direction). Read numbarrow source before designing any Arrow-based features in numbduck (e.g., virtual tables). numbduck's Arrow work would be the inverse direction (numpy → Arrow/DuckDB).

## Project Status

- **DuckDB Python issue**: duckdb/duckdb-python#404 — requesting C API symbols be exported from the Python wheel. Filed 2026-03-26; maintainer @evertlammerts responded 2026-04-12 committing to land the fix before 1.5.3. Still open.
- **macOS C API stripping is intentional**: [duckdb-python PR #81](https://github.com/duckdb/duckdb-python/pull/81) deliberately exports only `PyInit__duckdb` + `duckdb_adbc_init` via [CMakeLists.txt L83-L110](https://github.com/duckdb/duckdb-python/blob/main/CMakeLists.txt#L83-L110). macOS `-exported_symbol` enforces it; Linux `--export-dynamic-symbol` is additive so C API survives by accident.
- **Structref-backed UDAF pattern (merged upstream 2026-04-22)**: [Goykhman/numbduck#24](https://github.com/Goykhman/numbduck/pull/24) merged as [`0ff1aee`](https://github.com/Goykhman/numbduck/commit/0ff1aee68c1a91256beaab141bf8be247d8c25e7). Bridges DuckDB aggregate lifecycle to numba structref state via `borrow_structref` (incref + raw deref) and `_deref_structref_raw_ptr` intrinsic. Design doc: [`test/test_ducklib.md`](test/test_ducklib.md) covers aggregate lifecycle, bridge intrinsics, removerefctpass interaction, and the `@cfunc`/`@njit` callback pattern. Reference impl: [`test/test_ducklib.py`](test/test_ducklib.py) (Welford stddev + array UDAF).
- **IRR UDAF example + vector container**: branch `feat/irr-udaf-example`. Fork PR [nelson2005/numbduck#35](https://github.com/nelson2005/numbduck/pull/35) open. Adds `numbduck/vector.py` (generic numba vector container with `make_vector`), capacity-only constructor API (`Float64Vec(64)`), 14 vector tests, and `examples/irr.py` (IRR aggregate using vector state). Bridge intrinsics aligned with upstream-merged pattern from PR #24 (`_incref_meminfo` + `_deref_structref_raw_ptr` split, `structref_meminfo` from numbox). Spec: [`docs/specs/2026-04-17-irr-udaf-example-design.md`](docs/specs/2026-04-17-irr-udaf-example-design.md). Upstream PR planned — scope: both `numbduck/vector.py` (+ `test/test_vector.py`) and `examples/irr.py` — but **deferred until user is happy with the fork branch**. Upstream branch creation per the established CLAUDE.md-exclusion workflow (cherry-pick from feature branch to a new branch off `upstream/main`).
- **NRT decref investigation (resolved)**: `context.nrt.decref()` emits `call void @NRT_decref(...)` during codegen, but `removerefctpass.remove_unnecessary_nrt_usage()` (line 207-210 of `lowering.py`) strips it before `add_ir_module`. The pass runs `_legalize()` which checks if function args/returns involve NRT-tracked types — since our intrinsic takes `intp` and returns `void`, it concludes NRT is unnecessary and removes all `NRT_incref`/`NRT_decref` calls. Not a numba bug: the pass is correct for its purpose; our use case (manual NRT refcount via raw `intp` pointers) is outside the type system's visibility. The `NRT_MemInfo_release` workaround works for two reasons: (1) it does the same atomic decref + dtor call as `NRT_decref`, and (2) its name causes `_legalize()` to return `False` (any `NRT_*` function not in the accepted set aborts the rewrite), protecting the whole function.

**Key patterns for @cfunc + @njit UDF callbacks:**
1. `@cfunc` cannot use `import` inside body → use module-level `@njit` impl + thin `@cfunc` wrapper
2. `@cfunc` signatures must use `nb_types.intp` (not `nb_types.voidptr`) because numbduck uses `intp` for all pointers
3. `carray()` inside `@njit` requires `voidptr`, but numbduck returns `intp` → use numbox's `_cast_int_to_void_p` intrinsic to bridge the two
4. Result reading in Python test bodies: use `(ctypes.c_int32 * N).from_address(data_p)` (not `carray` which only works in JIT)
5. `duckdb_fetch_chunk(tuple(result))` for result fetching (not `duckdb_result_get_chunk` which doesn't exist in numbduck)
6. Destroy handles via buffer: `buf = numpy.array([handle_p], dtype=numpy.intp); destroy_func(buf.ctypes.data)`
7. Each function in a scalar function set must have its name set via `duckdb_scalar_function_set_name` — the name on the set alone is not sufficient
