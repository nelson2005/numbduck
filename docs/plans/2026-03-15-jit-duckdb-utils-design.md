# JIT-Compatible duckdb_utils Design

**Goal:** Make `duckdb_utils` functions callable from `@njit` context so JIT tests can use named allocators instead of raw `numpy.zeros`.

**Motivation:** Review feedback on PR #9 — raw `numpy.zeros(1, dtype=numpy.int64)` in JIT tests obscures intent. Using `create_duckdb_database()` etc. makes the purpose of each buffer clear.

**Approach:** Add `@njit` to `allocate_buffer` and all `create_*` functions. They already use only numba-compatible operations (`numpy.zeros`). Then update JIT test functions to use them.

**Scope:** `duckdb_utils.py` + JIT tests in `test_ducklib.py`. Part of PR #9.

**Risk:** None — `@njit` functions are callable from both Python and JIT context, so existing non-JIT tests continue to work unchanged.
