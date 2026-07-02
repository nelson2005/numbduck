# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

numbduck — adapts DuckDB's C API for use inside numba `@njit` code. Built on the [numbox](https://github.com/Goykhman/numbox) bindings toolkit.

## Build & Dev

- Venv: `python3.12 -m venv venv && venv/bin/pip install -e . flake8 pytest`
- Install: `pip install -e .`
- Test: `pytest` (a `benchmark`-marked test is skipped by default via a `conftest.py` collection hook; run it explicitly with `pytest -m benchmark`)
- Lint: `flake8`
- Python: >=3.10
- Key dependencies: `duckdb>=1.3.2,<1.6`, `numbox>=0.5.13,<0.6`

## Architecture

### Bindings (ducklib.py)

Wraps DuckDB C API functions for use in numba JIT code. Same pattern as numbox bindings:

1. **`utils.py`** — finds the DuckDB shared library and loads it via numbox's `load_lib_path` (`ctypes.CDLL` with `RTLD_GLOBAL` on Linux/macOS, `winmode=0` on Windows)
2. **`ducklib.py`** — registers signatures in numbox's `signatures` dict, then wraps each function with `@proxy`/`@proxy_if_available` (from `numbox.core.proxy.proxy`) + `_call_lib_func` (from `numbox.core.bindings.call`), threading `jit_options` (from `numbduck/configurations.py`) through the decorator
3. **`duckdb_utils.py`** — allocates numpy buffers for DuckDB C structs (database, connection, result, chunk, vector)

### Adding a New Binding

1. **Check `duckdb.h` first.** Look up the function signature in [`src/include/duckdb.h`](https://github.com/duckdb/duckdb/blob/v1.3.2/src/include/duckdb.h). Trace every typedef to its underlying type (e.g. `idx_t` → `uint64_t` → `uint64`, `duckdb_prepared_statement` is a pointer → use `_p` suffix).
2. Add signature to `ducklib.py`: `signatures["duckdb_func"] = return_type(arg_types...)`
3. Add wrapper following this pattern:
```python
from numba.core.types import intp
from numbox.core.bindings.call import _call_lib_func
from numbox.core.bindings.signatures import signatures
from numbox.core.proxy.proxy import proxy

from numbduck.configurations import jit_options

signatures["duckdb_func"] = intp(intp)


@proxy(signatures.get("duckdb_func"), jit_options=jit_options)
def duckdb_func(arg):
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_func """
    return _call_lib_func("duckdb_func", (arg,))
```
   For symbols not present on every DuckDB build numbduck supports (`duckdb>=1.3.2,<1.6`), decorate with `@proxy_if_available(duckdb_lib, signatures.get("duckdb_func"), jit_options=jit_options)` instead of `@proxy` — see `duckdb_create_varint`, `duckdb_get_varint`, and `duckdb_scalar_function_set_init` in `ducklib.py` for real examples. `proxy_if_available` stubs the wrapper to raise `NotImplementedError` at call time when the symbol is absent from `duckdb_lib`, instead of a confusing LLVM link error.
4. Function names must match the DuckDB C API names exactly
5. Docstring links must use `https://duckdb.org/docs/stable/clients/c/api.html#func_name`, not links to `duckdb.h` source
6. If a function returns a handle (e.g. `duckdb_logical_type`), also bind the corresponding destroy function (e.g. `duckdb_destroy_logical_type`)
7. **Before submitting an upstream PR**, re-verify all signatures, parameter types, and naming conventions (`_p`/`_pp`) against `duckdb.h`, and re-check that docstring links still use the `duckdb.org` anchor form from step 5 — this is a second check; step 1 is the first

### Struct-by-value ABI (numbox)

For C functions that pass or return structs by value (e.g. `duckdb_result`, `duckdb_hugeint`, `duckdb_interval`), all ABI lowering is handled generically by numbox's `_call_lib_func` intrinsic (`numbox/core/bindings/call.py`, platform classification in `numbox/core/bindings/abi.py`), driven by the tuple-typed signature registered in the `signatures` dict:

- ≤16-byte structs pass/return by value on SysV x86-64 and AAPCS64; on Windows x64, sizes 1/2/4/8 go by value in registers and other sizes go by pointer.
- 16-byte structs whose eightbytes are both INTEGER-class but whose LLVM type isn't already `{i64, i64}` (e.g. `{i32, i32, i64}` — `duckdb_interval`) get repacked via a memory bitcast before the call, working around llvmlite not modeling the eightbyte-packing rule.
- \>16-byte structs pass by pointer everywhere; on SysV x86-64 the `byval` attribute plus `optnone`+`noinline` on the caller keeps LLVM from eliding the stack copy before the callee reads it. See [llvmlite#300 comment](https://github.com/numba/llvmlite/issues/300#issuecomment-327235846) for the ABI rationale.
- \>16-byte struct returns use `sret` (caller-allocated hidden first arg) on every platform.

`ducklib.py` has no local by-value helpers or `@intrinsic` definitions of its own — every wrapper, regardless of struct size, calls `_call_lib_func(name, args)` and lets numbox do the platform dispatch. (numbox also defines a sibling `_call_lib_func_byval` intrinsic for C signatures of the form `func(T*)` — a pointer parameter passed by alloca+store+call — but `ducklib.py` does not currently use it; every numbduck struct parameter is a true by-value `T`.)

## Key Paths

- `numbduck/ducklib.py` — all DuckDB C API bindings
- `numbduck/duckdb_utils.py` — buffer allocators for DuckDB structs
- `numbduck/utils.py` — shared library loader; standalone-libduckdb fallback and dual-runtime coordination guard on macOS
- `numbduck/pybridge.py` — extracts a raw `Connection*` from a Python `duckdb.DuckDBPyConnection` for use with numbduck's C API bindings
- `numbduck/configurations.py` — `jit_options` read from `NUMBDUCK_JIT_OPTIONS` (default `{"cache": True}`), threaded into every `@proxy`/`@proxy_if_available` wrapper
- `test/test_ducklib.py` — integration tests

## Error Handling

Bindings must mirror the DuckDB C API error-handling protocol exactly — return state codes (`DuckDBSuccess`/`DuckDBError`) and pointers, never inject exceptions. Callers do C-style return code checks. See [numbox issue #5](https://github.com/Goykhman/numbox/issues/5) for rationale.

## Preferences

Cross-project preferences live in the user's MEMORY.md. Only numbduck-specific workflow rules are kept here.

- Always exclude CLAUDE.md, `.github/workflows/numbduck_ci.yml`, `docs/plans/`, and `docs/reviews/` from upstream PRs (use a dedicated branch based on `upstream/main`)
- Never merge local feature branches into main — main must always match `upstream/main` (exception: CLAUDE.md and the fork-only CI workflow)
- Feature branches: base off `origin/main` (has CLAUDE.md); upstream PR branches: base off `upstream/main` (no CLAUDE.md)
- Do all coding work on the feature branch (has CLAUDE.md + fork CI), then cherry-pick to the upstream PR branch when ready

## Related Projects

- **[numbox](https://github.com/Goykhman/numbox)** — bindings toolkit that numbduck is built on. Provides the `signatures` dict, the ABI-aware `_call_lib_func`/`_call_lib_func_byval` intrinsics, the `@proxy`/`@proxy_if_available` decorators, and shared library loading (`load_lib_path`). Read numbox source before implementing new binding patterns.
- **[numbarrow](https://github.com/Goykhman/numbarrow)** — bridges PyArrow arrays into numba `@njit` code (Arrow → numpy direction). Read numbarrow source before designing any Arrow-based features in numbduck (e.g., virtual tables). numbduck's Arrow work would be the inverse direction (numpy → Arrow/DuckDB).

## Project Status

- **DuckDB Python C API export**: [duckdb-python#404](https://github.com/duckdb/duckdb-python/issues/404) — filed 2026-03-26 requesting the wheel export DuckDB C API symbols. Fixed upstream in duckdb-python v1.5.3 via [PR #445](https://github.com/duckdb/duckdb-python/pull/445) (merged 2026-05-08). numbduck's supported range is `duckdb>=1.3.2,<1.6`, which still spans wheels built before the fix, so the workaround below stays in place.
- **macOS C API stripping (duckdb 1.4.1–1.5.2)**: [duckdb-python PR #81](https://github.com/duckdb/duckdb-python/pull/81) had deliberately exported only `PyInit__duckdb` + `duckdb_adbc_init`; macOS's `-exported_symbol` enforced it while Linux's `--export-dynamic-symbol` is additive, so the C API survived by accident on Linux. `numbduck/utils.py`'s `load_duckdb()` detects the missing symbols and falls back to a standalone `libduckdb.dylib` (`NUMBDUCK_LIBDUCKDB` env var, a Homebrew install, or an auto-download cached under `~/.numbduck/lib/<duckdb-version>`), RTLD_GLOBAL-loaded independently of the Python `duckdb` module.
- **Dual-runtime coordination guard**: when the standalone-libduckdb fallback is used, two DuckDB runtimes are resident — the wheel's (behind the Python `duckdb` module) and the standalone (bound by numbduck's JIT code). `pybridge.extract_connection_ptr` hands a `Connection*` minted by the wheel to the standalone's C API, which is only sound when both are the same DuckDB build. `utils.py` exposes `libraries_coordinated()`/`loaded_library_version()` to compare the standalone's `duckdb_library_version()` against `duckdb.__version__`; `load_duckdb()` and `pybridge.py` raise `RuntimeError` rather than dereference a pointer under a mismatched internal layout when the two disagree.
- **Structref-backed UDAF pattern (merged upstream 2026-04-22)**: [Goykhman/numbduck#24](https://github.com/Goykhman/numbduck/pull/24) merged as [`0ff1aee`](https://github.com/Goykhman/numbduck/commit/0ff1aee68c1a91256beaab141bf8be247d8c25e7). Bridges DuckDB aggregate lifecycle to numba structref state via numbox's `numbox.utils.meminfo` helpers (`export_meminfo`, `borrow_structref`, `release_meminfo`, wrapping the `_incref_meminfo`/`_deref_structref_raw_ptr`/`_release_meminfo` intrinsics). Design doc: [`test/test_ducklib.md`](test/test_ducklib.md) covers aggregate lifecycle, the bridge functions, `removerefctpass` interaction, and the `@cfunc`/`@njit` callback pattern. Reference impl: [`test/test_ducklib.py`](test/test_ducklib.py) (Welford stddev + array UDAF).

**Key patterns for @cfunc + @njit UDF callbacks:**
1. `@cfunc` cannot use `import` inside body → use module-level `@njit` impl + thin `@cfunc` wrapper
2. `@cfunc` signatures must use `nb_types.intp` (not `nb_types.voidptr`) because numbduck uses `intp` for all pointers
3. `carray()` inside `@njit` requires `voidptr`, but numbduck returns `intp` → use numbox's `_cast_int_to_void_p` intrinsic to bridge the two
4. Result reading in Python test bodies: use `(ctypes.c_int32 * N).from_address(data_p)` (not `carray` which only works in JIT)
5. `duckdb_fetch_chunk(tuple(result))` for result fetching (not `duckdb_result_get_chunk` which doesn't exist in numbduck)
6. Destroy handles via buffer: `buf = numpy.array([handle_p], dtype=numpy.intp); destroy_func(buf.ctypes.data)`
7. Each function in a scalar function set must have its name set via `duckdb_scalar_function_set_name` — the name on the set alone is not sufficient
