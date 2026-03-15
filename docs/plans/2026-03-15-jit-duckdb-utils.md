# JIT-Compatible duckdb_utils Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers-extended-cc:executing-plans to implement this plan task-by-task.

**Goal:** Make `duckdb_utils` allocator functions callable from `@njit` context so JIT tests use named allocators instead of raw `numpy.zeros`.

**Architecture:** Add `@njit` to `allocate_buffer` and all `create_*` functions in `duckdb_utils.py`. Then update the three JIT test functions to use them. Existing non-JIT tests already use these functions and continue to work unchanged.

**Tech Stack:** numba, numpy, DuckDB C API via ducklib

---

### Task 0: Add @njit to duckdb_utils functions

**Files:**
- Modify: `numbduck/duckdb_utils.py`
- Test: `test/test_ducklib.py`

**Step 1: Write a failing test that calls create_duckdb_database from JIT**

Add to `test/test_ducklib.py` after `test_array_data_p` (line ~627):

```python
def test_jit_create_duckdb_database():
    @njit
    def _jit_create():
        db = create_duckdb_database()
        return db.shape[0], db.dtype == numpy.int64, db[0]
    size, is_i64, val = _jit_create()
    assert size == 1
    assert is_i64
    assert val == 0
```

**Step 2: Run test to verify it fails**

Run: `source venv/bin/activate && pytest test/test_ducklib.py::test_jit_create_duckdb_database -v`
Expected: FAIL with `TypingError: Untyped global name 'create_duckdb_database'`

**Step 3: Add @njit to all duckdb_utils functions**

Replace `numbduck/duckdb_utils.py` with:

```python
import numpy
from numba import njit


@njit
def allocate_buffer(sz):
    return numpy.zeros(sz, dtype=numpy.int64)


@njit
def create_duckdb_connection():
    """ https://github.com/duckdb/duckdb/blob/v1.3.2/src/include/duckdb.h#L469 """
    return allocate_buffer(1)


@njit
def create_duckdb_database():
    """ https://github.com/duckdb/duckdb/blob/v1.3.2/src/include/duckdb.h#L464 """
    return allocate_buffer(1)


@njit
def create_duckdb_prepared_statement():
    """ https://github.com/duckdb/duckdb/blob/v1.3.2/src/include/duckdb.h#L480 """
    return allocate_buffer(1)


@njit
def create_duckdb_data_chunk():
    """ https://github.com/duckdb/duckdb/blob/v1.3.2/src/include/duckdb.h#L527 """
    return allocate_buffer(1)


@njit
def create_duckdb_result():
    """ https://github.com/duckdb/duckdb/blob/v1.3.2/src/include/duckdb.h#L454 """
    return allocate_buffer(6)


@njit
def create_duckdb_vector():
    """ https://github.com/duckdb/duckdb/blob/v1.3.2/src/include/duckdb.h#L395 """
    return allocate_buffer(1)
```

**Step 4: Run test to verify it passes**

Run: `source venv/bin/activate && pytest test/test_ducklib.py::test_jit_create_duckdb_database -v`
Expected: PASS

**Step 5: Run full test suite to verify no regressions**

Run: `source venv/bin/activate && pytest -v`
Expected: all tests pass (existing non-JIT callers work unchanged)

**Step 6: Commit**

```bash
git add numbduck/duckdb_utils.py test/test_ducklib.py
git commit -m "Add @njit to duckdb_utils allocator functions"
```

---

### Task 1: Update JIT tests to use duckdb_utils

**Files:**
- Modify: `test/test_ducklib.py`

**Step 1: Update jit_open_close to use create_duckdb_database**

Replace in `jit_open_close` (line ~630):
```python
    db = numpy.zeros(1, dtype=numpy.int64)
```
with:
```python
    db = create_duckdb_database()
```

**Step 2: Update jit_connect_query_disconnect**

Replace in `jit_connect_query_disconnect` (line ~647):
```python
    db = numpy.zeros(1, dtype=numpy.int64)
    conn = numpy.zeros(1, dtype=numpy.int64)
```
with:
```python
    db = create_duckdb_database()
    conn = create_duckdb_connection()
```

**Step 3: Update jit_prepare_bind_execute**

Replace in `jit_prepare_bind_execute` (line ~679):
```python
    db = numpy.zeros(1, dtype=numpy.int64)
    conn = numpy.zeros(1, dtype=numpy.int64)
    stmt = numpy.zeros(1, dtype=numpy.int64)
```
with:
```python
    db = create_duckdb_database()
    conn = create_duckdb_connection()
    stmt = create_duckdb_prepared_statement()
```

And replace the result buffer (line ~703):
```python
    result = numpy.zeros(6, dtype=numpy.int64)
```
with:
```python
    result = create_duckdb_result()
```

And replace the chunk buffer in cleanup (line ~733):
```python
    chunk_buf = numpy.zeros(1, dtype=numpy.int64)
```
with:
```python
    chunk_buf = create_duckdb_data_chunk()
```

**Step 4: Run full test suite**

Run: `source venv/bin/activate && pytest -v`
Expected: all tests pass

**Step 5: Run lint**

Run: `source venv/bin/activate && flake8 numbduck/duckdb_utils.py test/test_ducklib.py`
Expected: only pre-existing E501 warnings

**Step 6: Commit**

```bash
git add test/test_ducklib.py
git commit -m "Use duckdb_utils allocators in JIT tests"
```
