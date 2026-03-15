# JIT Prepared Statements Test Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers-extended-cc:executing-plans to implement this plan task-by-task.

**Goal:** Add a JIT test that exercises prepared statements with parameter binding and result readback from `@njit` context.

**Architecture:** A single `@njit` function (`jit_prepare_bind_execute`) that prepares a `SELECT $1::INTEGER, $2::BIGINT, $3::DOUBLE, $4::INTEGER` statement, binds int32/int64/double/null values, executes, fetches the chunk, reads back each column with the correct typed pointer, checks NULL validity, and cleans up. New `i64_ptr` and `f64_ptr` intrinsics are needed in `jit_utils.py` to cast `intp` to `CPointer(int64)` and `CPointer(float64)` for `carray` readback.

**Tech Stack:** numba, numpy, numbox, DuckDB C API via ducklib

---

### Task 0: Create feature branch

**Files:** none

**Step 1: Create branch off origin/main**

```bash
git checkout origin/main -b jit-prepared-statements
```

**Step 2: Verify branch**

```bash
git branch --show-current
```

Expected: `jit-prepared-statements`

---

### Task 1: Add i64_ptr and f64_ptr intrinsics to jit_utils.py

**Files:**
- Modify: `numbduck/jit_utils.py`
- Test: `test/test_ducklib.py`

**Step 1: Write failing tests for i64_ptr and f64_ptr**

Add to `test/test_ducklib.py` imports and a test after `test_array_data_p`:

```python
# Add to imports at top:
from numbduck.jit_utils import array_data_p, i32_ptr, i64_ptr, f64_ptr

# Add test after test_array_data_p:
def test_i64_ptr():
    @njit
    def _use_i64_ptr():
        arr = numpy.zeros(2, dtype=numpy.int64)
        arr[0] = 42
        arr[1] = 2**40
        return carray(i64_ptr(array_data_p(arr)), (2,))[0], \
               carray(i64_ptr(array_data_p(arr)), (2,))[1]
    v0, v1 = _use_i64_ptr()
    assert v0 == 42
    assert v1 == 2**40


def test_f64_ptr():
    @njit
    def _use_f64_ptr():
        arr = numpy.zeros(1, dtype=numpy.float64)
        arr[0] = 3.14
        return carray(f64_ptr(array_data_p(arr)), (1,))[0]
    v = _use_f64_ptr()
    assert abs(v - 3.14) < 1e-10
```

**Step 2: Run tests to verify they fail**

```bash
source venv/bin/activate && pytest test/test_ducklib.py::test_i64_ptr test/test_ducklib.py::test_f64_ptr -v
```

Expected: ImportError — `i64_ptr` and `f64_ptr` don't exist yet.

**Step 3: Implement i64_ptr and f64_ptr in jit_utils.py**

Add to `numbduck/jit_utils.py`, after `i32_ptr`:

```python
from numba.core.types import int32, int64, float64, intp, CPointer

@intrinsic
def i64_ptr(typingctx, ptr_ty):
    """Cast an intp to CPointer(int64) for use with numba.carray."""
    if ptr_ty != intp:
        raise errors.TypingError(f"i64_ptr expects intp, got {ptr_ty}")
    def codegen(context, builder, signature, args):
        return builder.inttoptr(args[0], llvmir.IntType(64).as_pointer())
    return CPointer(int64)(intp,), codegen


@intrinsic
def f64_ptr(typingctx, ptr_ty):
    """Cast an intp to CPointer(float64) for use with numba.carray."""
    if ptr_ty != intp:
        raise errors.TypingError(f"f64_ptr expects intp, got {ptr_ty}")
    def codegen(context, builder, signature, args):
        return builder.inttoptr(args[0], llvmir.DoubleType().as_pointer())
    return CPointer(float64)(intp,), codegen
```

Update the import line:

```python
from numba.core.types import int32, int64, float64, intp, CPointer
```

**Step 4: Run tests to verify they pass**

```bash
pytest test/test_ducklib.py::test_i64_ptr test/test_ducklib.py::test_f64_ptr -v
```

Expected: PASS

**Step 5: Run full test suite**

```bash
pytest -v
```

Expected: all tests pass

**Step 6: Commit**

```bash
git add numbduck/jit_utils.py test/test_ducklib.py
git commit -m "Add i64_ptr and f64_ptr intrinsics for typed carray readback"
```

---

### Task 2: Add jit_prepare_bind_execute test

**Files:**
- Modify: `test/test_ducklib.py`

**Step 1: Write the @njit function and test**

Add after `test_execute_prepared_unbound_params` (line 549), before the `# --- Error Messages ---` section:

```python
# --- JIT: Prepared Statements ---

@njit
def jit_prepare_bind_execute():
    db = numpy.zeros(1, dtype=numpy.int64)
    conn = numpy.zeros(1, dtype=numpy.int64)
    stmt = numpy.zeros(1, dtype=numpy.int64)

    open_rc = ducklib.duckdb_open(0, array_data_p(db))
    connect_rc = ducklib.duckdb_connect(db[0], array_data_p(conn))
    conn_p = conn[0]

    # prepare: SELECT $1::INTEGER, $2::BIGINT, $3::DOUBLE, $4::INTEGER
    sql = numpy.frombuffer(
        b"SELECT $1::INTEGER, $2::BIGINT, $3::DOUBLE, $4::INTEGER;\x00",
        dtype=numpy.uint8)
    prepare_rc = ducklib.duckdb_prepare(
        conn_p, array_data_p(sql), array_data_p(stmt))
    stmt_p = stmt[0]
    nparams = ducklib.duckdb_nparams(stmt_p)

    # bind values
    bind1_rc = ducklib.duckdb_bind_int32(stmt_p, 1, 99)
    bind2_rc = ducklib.duckdb_bind_int64(stmt_p, 2, 2**40)
    bind3_rc = ducklib.duckdb_bind_double(stmt_p, 3, 3.14)
    bind4_rc = ducklib.duckdb_bind_null(stmt_p, 4)

    # execute
    result = numpy.zeros(6, dtype=numpy.int64)
    exec_rc = ducklib.duckdb_execute_prepared(stmt_p, array_data_p(result))

    # fetch chunk and read back values
    result_tup = (result[0], result[1], result[2],
                  result[3], result[4], result[5])
    chunk_p = _duckdb_fetch_chunk(result_tup)
    chunk_size = ducklib.duckdb_data_chunk_get_size(chunk_p)

    # col 0: int32
    v0_p = ducklib.duckdb_vector_get_data(
        ducklib.duckdb_data_chunk_get_vector(chunk_p, 0))
    col0 = carray(i32_ptr(v0_p), (chunk_size,))[0]

    # col 1: int64
    v1_p = ducklib.duckdb_vector_get_data(
        ducklib.duckdb_data_chunk_get_vector(chunk_p, 1))
    col1 = carray(i64_ptr(v1_p), (chunk_size,))[0]

    # col 2: double
    v2_p = ducklib.duckdb_vector_get_data(
        ducklib.duckdb_data_chunk_get_vector(chunk_p, 2))
    col2 = carray(f64_ptr(v2_p), (chunk_size,))[0]

    # col 3: null check
    v3_p = ducklib.duckdb_data_chunk_get_vector(chunk_p, 3)
    v3_validity_p = ducklib.duckdb_vector_get_validity(v3_p)
    col3_valid = ducklib.duckdb_validity_row_is_valid(
        intp(v3_validity_p), intp(0))

    # cleanup (reverse order)
    chunk_buf = numpy.zeros(1, dtype=numpy.int64)
    chunk_buf[0] = chunk_p
    ducklib.duckdb_destroy_data_chunk(array_data_p(chunk_buf))
    ducklib.duckdb_destroy_result(array_data_p(result))
    ducklib.duckdb_destroy_prepare(array_data_p(stmt))
    ducklib.duckdb_disconnect(array_data_p(conn))
    ducklib.duckdb_close(array_data_p(db))

    return (open_rc, connect_rc, prepare_rc, nparams,
            bind1_rc, bind2_rc, bind3_rc, bind4_rc, exec_rc,
            chunk_size, col0, col1, col2, col3_valid)


def test_jit_prepare_bind_execute():
    """Prepared statement with parameter binding from JIT context.
    https://duckdb.org/docs/stable/clients/c/api.html#duckdb_prepare
    https://duckdb.org/docs/stable/clients/c/api.html#duckdb_nparams
    https://duckdb.org/docs/stable/clients/c/api.html#duckdb_bind_int32
    https://duckdb.org/docs/stable/clients/c/api.html#duckdb_bind_int64
    https://duckdb.org/docs/stable/clients/c/api.html#duckdb_bind_double
    https://duckdb.org/docs/stable/clients/c/api.html#duckdb_bind_null
    https://duckdb.org/docs/stable/clients/c/api.html#duckdb_execute_prepared
    https://duckdb.org/docs/stable/clients/c/api.html#duckdb_destroy_prepare """
    (open_rc, connect_rc, prepare_rc, nparams,
     bind1_rc, bind2_rc, bind3_rc, bind4_rc, exec_rc,
     chunk_size, col0, col1, col2, col3_valid) = jit_prepare_bind_execute()
    assert open_rc == ducklib.DuckDBSuccess, f"open failed, rc={open_rc}"
    assert connect_rc == ducklib.DuckDBSuccess, f"connect failed, rc={connect_rc}"
    assert prepare_rc == ducklib.DuckDBSuccess, f"prepare failed, rc={prepare_rc}"
    assert nparams == 4, f"expected 4 params, got {nparams}"
    assert bind1_rc == ducklib.DuckDBSuccess, f"bind int32 failed, rc={bind1_rc}"
    assert bind2_rc == ducklib.DuckDBSuccess, f"bind int64 failed, rc={bind2_rc}"
    assert bind3_rc == ducklib.DuckDBSuccess, f"bind double failed, rc={bind3_rc}"
    assert bind4_rc == ducklib.DuckDBSuccess, f"bind null failed, rc={bind4_rc}"
    assert exec_rc == ducklib.DuckDBSuccess, f"execute failed, rc={exec_rc}"
    assert chunk_size == 1, f"expected 1 row, got {chunk_size}"
    assert col0 == 99, f"col0: expected 99, got {col0}"
    assert col1 == 2**40, f"col1: expected 2^40, got {col1}"
    assert abs(col2 - 3.14) < 1e-10, f"col2: expected 3.14, got {col2}"
    assert col3_valid == 0, f"col3: expected NULL, validity={col3_valid}"
```

**Step 2: Run the test**

```bash
pytest test/test_ducklib.py::test_jit_prepare_bind_execute -v
```

Expected: PASS (all bindings already exist; this is an integration test)

**Step 3: Run full test suite**

```bash
pytest -v
```

Expected: all tests pass

**Step 4: Commit**

```bash
git add test/test_ducklib.py
git commit -m "Add JIT test for prepared statements with parameter binding"
```

---

### Task 3: Final verification and push

**Files:** none

**Step 1: Run full test suite**

```bash
pytest -v
```

Expected: all tests pass

**Step 2: Run lint**

```bash
flake8
```

Expected: no errors

**Step 3: Push branch**

```bash
git push -u origin jit-prepared-statements
```
