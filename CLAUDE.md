# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

numbduck adapts DuckDB's C API for use inside numba `@njit` code. Built on the [numbox](https://github.com/Goykhman/numbox) bindings toolkit.

## Build & Dev

- Venv: `python3.12 -m venv venv && venv/bin/pip install -e . flake8 pytest`
- Install: `pip install -e .`
- Test: `pytest`
- Lint: `flake8`
- Python: >=3.10
- Key dependencies: `duckdb>=1.2.2,<2.1.0`, `numbox>=0.7.8`

## Architecture

### Bindings (ducklib.py)

Wraps DuckDB C API functions for use in numba JIT code. Same pattern as numbox bindings:

1. **`utils.py`**: finds and loads the DuckDB shared library via `ctypes.CDLL` with `RTLD_GLOBAL`
2. **`ducklib.py`**: registers signatures in numbox's `signatures` dict, then wraps each function with numbox's `@proxy` (or `@proxy_if_available`, for a function that older duckdb releases lack) around `_call_lib_func`
3. **`duckdb_utils.py`**: allocates numpy buffers for DuckDB C structs (database, connection, result, chunk, vector)

### Adding a New Binding

1. **Check `duckdb.h` first.** Look up the function signature in [`src/include/duckdb.h`](https://github.com/duckdb/duckdb/blob/v1.3.2/src/include/duckdb.h). Trace every typedef to its underlying type (e.g. `idx_t` → `uint64_t` → `uint64`, `duckdb_prepared_statement` is a pointer → use `_p` suffix). Verify all docstring links point to the correct line in `duckdb.h` at implementation time.
2. Add signature to `ducklib.py`: `signatures["duckdb_func"] = return_type(arg_types...)`
3. Add wrapper following this pattern:
```python
@proxy(signatures.get("duckdb_func"), jit_options=jit_options)
def duckdb_func(arg):
    """ https://duckdb.org/docs/2.0/clients/c/api.html#duckdb_func """
    return _call_lib_func("duckdb_func", (arg,))
```
4. Function names must match the DuckDB C API names exactly
5. Docstring links must use the versioned page, `https://duckdb.org/docs/2.0/clients/c/api.html#func_name`, not links to `duckdb.h` source. **Never `/current/` or `/stable/`**: since duckdb 2.0 both serve a 529-byte meta-refresh stub pointing at `/docs/2.0/` (checked 2026-11-09; `/stable/` did the same before), and a meta refresh discards the fragment, so the anchor never resolves. A link checker will not catch it either, because the stub returns HTTP 200. Only a fragment-aware check (lychee `--include-fragments`) sees the breakage. numbox includes a binding's docstring in the cache key of a jitted caller, so any edit to a binding docstring makes every cached caller recompile once (`StaleProxyCacheWarning`); batch such edits
6. If a function returns a handle (e.g. `duckdb_logical_type`), also bind the corresponding destroy function (e.g. `duckdb_destroy_logical_type`)
7. **Before submitting an upstream PR**, re-verify all signatures, parameter types, naming conventions (`_p`/`_pp`), and docstring links against `duckdb.h`. This is a second check; step 1 is the first.

### Structs passed by value

A C function that takes or returns a struct by value uses a numba tuple of the struct's fields in C order (`duckdb_hugeint_ty`, `duckdb_interval_ty`, `duckdb_decimal_ty`, the six-field `duckdb_result`), and numbox's [`_call_lib_func`](https://github.com/Goykhman/numbox/blob/main/numbox/core/bindings/call.py) lowers it to the platform's calling convention. numbduck has no ABI helpers of its own. The `ducklib` module docstring and each such binding's docstring give the tuple; see [llvmlite#300 comment](https://github.com/numba/llvmlite/issues/300#issuecomment-327235846) for the ABI background.

## Key Paths

- `numbduck/ducklib.py`: all DuckDB C API bindings
- `numbduck/duckdb_utils.py`: buffer allocators for DuckDB structs
- `numbduck/utils.py`: shared library loader
- `test/test_ducklib.py`: integration tests

## Error Handling

Bindings must mirror the DuckDB C API error-handling protocol exactly: return state codes (`DuckDBSuccess`/`DuckDBError`) and pointers, never inject exceptions. Callers do C-style return code checks. See [numbox issue #5](https://github.com/Goykhman/numbox/issues/5) for rationale.

## Preferences

Cross-project preferences live in the user's MEMORY.md. Only numbduck-specific workflow rules are kept here.

- Always exclude CLAUDE.md, `.github/workflows/numbduck_ci.yml`, and `docs/plans/` from upstream PRs (use a dedicated branch based on `upstream/main`)
- Never merge local feature branches into main; main must always match `upstream/main` (exception: CLAUDE.md and the fork-only CI workflow)
- Feature branches: base off `origin/main` (has CLAUDE.md); upstream PR branches: base off `upstream/main` (no CLAUDE.md)
- Do all coding work on the feature branch (has CLAUDE.md + fork CI), then cherry-pick to the upstream PR branch when ready

## Related Projects

- **[numbox](https://github.com/Goykhman/numbox)**: bindings toolkit that numbduck is built on. Provides `signatures` dict, `_call_lib_func`, the `@proxy` and `@proxy_if_available` decorators, and shared library loading patterns. **Read the relevant numbox source before planning, reviewing, or implementing anything here that depends on it**, not only new binding patterns. [`numbox/utils/lowlevel.py`](https://github.com/Goykhman/numbox/blob/main/numbox/utils/lowlevel.py) is the one to check first for low-level helpers. We once hand-rolled typed pointer intrinsics (`i32_ptr`, `i64_ptr`, `f64_ptr`) that duplicated numbox's existing [`_cast_int_to_void_p`](https://github.com/Goykhman/numbox/blob/main/numbox/utils/lowlevel.py#L49); review caught it and they were removed.
- **[numbarrow](https://github.com/Goykhman/numbarrow)**: bridges PyArrow arrays into numba `@njit` code (Arrow → numpy direction). Read numbarrow source before designing any Arrow-based features in numbduck (e.g., virtual tables). numbduck's Arrow work would be the inverse direction (numpy → Arrow/DuckDB).

## Project Status

- **DuckDB Python issue**: duckdb/duckdb-python#404, requesting C API symbols be exported from the Python wheel. Filed 2026-03-26; closed 2026-05-18 after [duckdb-python PR #445](https://github.com/duckdb/duckdb-python/pull/445) (merged 2026-05-08) restored the exports, first in duckdb 1.5.3.
- **macOS C API exports**: [duckdb-python PR #81](https://github.com/duckdb/duckdb-python/pull/81) stripped them from the macOS wheels, first in duckdb 1.4.1; PR #445 restored them in 1.5.3. So only the macOS wheels of 1.4.1 to 1.5.2 need the standalone libduckdb (measured 2026-11-09 over every cp312 arm64 release). The Linux and Windows wheels of 1.4.1 to 1.5.2 export it.
- **Structref-backed UDAF pattern (merged upstream 2026-04-22)**: [Goykhman/numbduck#24](https://github.com/Goykhman/numbduck/pull/24) merged as [`0ff1aee`](https://github.com/Goykhman/numbduck/commit/0ff1aee68c1a91256beaab141bf8be247d8c25e7). Bridges DuckDB aggregate lifecycle to numba structref state via `borrow_structref` (incref + raw deref) and `_deref_structref_raw_ptr` intrinsic. Design doc: [`test/test_ducklib.md`](test/test_ducklib.md) covers aggregate lifecycle, bridge intrinsics, removerefctpass interaction, and the `@cfunc`/`@njit` callback pattern. Reference impl: [`test/test_ducklib.py`](test/test_ducklib.py) (Welford stddev + array UDAF).

**Key patterns for @cfunc + @njit UDF callbacks:**
1. A `@cfunc` body may call bindings, `carray` and numbox helpers directly. The tests and examples keep the work in a module-level `@njit` function under a thin `@cfunc` wrapper, a convention that lets the work be called and tested from Python
2. `@cfunc` signatures must use `nb_types.intp` (not `nb_types.voidptr`) because numbduck uses `intp` for all pointers
3. `carray()` inside `@njit` requires `voidptr`, but numbduck returns `intp` → use numbox's `_cast_int_to_void_p` intrinsic to bridge the two
4. Result reading in Python test bodies: use `(ctypes.c_int32 * N).from_address(data_p)` (not `carray` which only works in JIT)
5. `duckdb_fetch_chunk(tuple(result))` for result fetching (not `duckdb_result_get_chunk` which doesn't exist in numbduck)
6. Destroy handles via buffer: `buf = numpy.array([handle_p], dtype=numpy.intp); destroy_func(buf.ctypes.data)`
7. Each function in a scalar function set must have its name set via `duckdb_scalar_function_set_name`; the name on the set alone is not sufficient
