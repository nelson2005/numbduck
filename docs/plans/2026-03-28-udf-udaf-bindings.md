# UDF + UDAF C API Bindings Implementation Plan

**Goal:** Add 33 scalar function and aggregate function C API bindings to `ducklib.py` with integration tests.

**Architecture:** Thin `@cres` wrappers over `_call_lib_func`, same pattern as all existing bindings. Opaque handles map to `intp`/`_p`. Callback function pointers passed as `intp`. One version-conditional binding (`duckdb_scalar_function_set_init`, v1.5+ only via `if_available=True`).

**Prerequisites:** Branch must be rebased onto `upstream/main` to get `_has_symbol`, `if_available`, and the local `cres` wrapper (from merged PR #17).

---

### Task 0: Rebase onto upstream/main

**Goal:** Get the current upstream code (with `_has_symbol`/`if_available`/local `cres`) onto the feature branch.

**Steps:**

- [ ] Rebase `udf-udaf-bindings` onto `upstream/main`
- [ ] Resolve any conflicts (expect CLAUDE.md only)
- [ ] Verify: `python -c "from numbduck.ducklib import _has_symbol; print(_has_symbol('duckdb_open'))"` prints `True`
- [ ] Force-push to origin (rebase rewrites history)

---

### Task 1: Scalar function signatures

**Goal:** Add all 17 scalar function signatures to the `signatures` dict.

**Files:**
- Modify: `numbduck/ducklib.py` (add after line ~328, the last existing signature)

**Steps:**

- [ ] Add signatures block:

```python
# ── Scalar Functions ─────────────────────────────────────────────────
signatures["duckdb_create_scalar_function"] = intp()
signatures["duckdb_destroy_scalar_function"] = void(intp)
signatures["duckdb_register_scalar_function"] = duckdb_state_ty(intp, intp)
signatures["duckdb_scalar_function_set_name"] = duckdb_state_ty(intp, intp)
signatures["duckdb_scalar_function_add_parameter"] = duckdb_state_ty(intp, intp)
signatures["duckdb_scalar_function_set_return_type"] = duckdb_state_ty(intp, intp)
signatures["duckdb_scalar_function_set_function"] = duckdb_state_ty(intp, intp)
signatures["duckdb_scalar_function_set_bind"] = duckdb_state_ty(intp, intp)
signatures["duckdb_scalar_function_set_extra_info"] = duckdb_state_ty(intp, intp, intp)
signatures["duckdb_scalar_function_set_varargs"] = duckdb_state_ty(intp, intp)
signatures["duckdb_scalar_function_set_volatile"] = duckdb_state_ty(intp)
signatures["duckdb_scalar_function_set_special_handling"] = duckdb_state_ty(intp)
signatures["duckdb_scalar_function_set_init"] = duckdb_state_ty(intp, intp)
signatures["duckdb_create_scalar_function_set"] = intp(intp)
signatures["duckdb_destroy_scalar_function_set"] = void(intp)
signatures["duckdb_add_scalar_function_to_set"] = duckdb_state_ty(intp, intp)
signatures["duckdb_register_scalar_function_set"] = duckdb_state_ty(intp, intp)
```

- [ ] Commit: `git add numbduck/ducklib.py && git commit -m "Add scalar function signatures to ducklib"`

**Note on destroy signatures:** Existing destroy bindings (e.g. `duckdb_close`, `duckdb_disconnect`) use `void(intp)` despite the C API taking `type **`. The caller passes the numpy buffer's `.ctypes.data` (a pointer to the stored handle pointer), which is effectively a `**`. The numba type is still `intp` — the double indirection is handled by the caller, not the signature. The existing `duckdb_destroy_logical_type` uses `void(intp)`. Follow that pattern.

---

### Task 2: Scalar function wrappers

**Goal:** Add `@cres` wrapper functions for all 17 scalar function bindings.

**Files:**
- Modify: `numbduck/ducklib.py` (add after the last existing wrapper, before intrinsics section)

**Steps:**

- [ ] Add wrapper functions after the existing `duckdb_vector_get_validity` wrapper block (~line 1415 on upstream). Each follows the exact pattern: `@cres` decorator, function with matching name/args, docstring with API link, `_call_lib_func` call. `duckdb_scalar_function_set_init` uses `if_available=True`.

```python
# ── Scalar Functions ────────────────────────────────────────��────────

@cres(signatures.get("duckdb_create_scalar_function"))
def duckdb_create_scalar_function():
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_create_scalar_function """
    return _call_lib_func("duckdb_create_scalar_function", ())


@cres(signatures.get("duckdb_destroy_scalar_function"))
def duckdb_destroy_scalar_function(scalar_function_pp):
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_destroy_scalar_function """
    return _call_lib_func("duckdb_destroy_scalar_function", (scalar_function_pp,))


@cres(signatures.get("duckdb_register_scalar_function"))
def duckdb_register_scalar_function(connection_p, scalar_function_p):
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_register_scalar_function """
    return _call_lib_func("duckdb_register_scalar_function", (connection_p, scalar_function_p))


@cres(signatures.get("duckdb_scalar_function_set_name"))
def duckdb_scalar_function_set_name(scalar_function_p, name_p):
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_scalar_function_set_name """
    return _call_lib_func("duckdb_scalar_function_set_name", (scalar_function_p, name_p))


@cres(signatures.get("duckdb_scalar_function_add_parameter"))
def duckdb_scalar_function_add_parameter(scalar_function_p, type_p):
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_scalar_function_add_parameter """
    return _call_lib_func("duckdb_scalar_function_add_parameter", (scalar_function_p, type_p))


@cres(signatures.get("duckdb_scalar_function_set_return_type"))
def duckdb_scalar_function_set_return_type(scalar_function_p, type_p):
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_scalar_function_set_return_type """
    return _call_lib_func("duckdb_scalar_function_set_return_type", (scalar_function_p, type_p))


@cres(signatures.get("duckdb_scalar_function_set_function"))
def duckdb_scalar_function_set_function(scalar_function_p, function_p):
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_scalar_function_set_function """
    return _call_lib_func("duckdb_scalar_function_set_function", (scalar_function_p, function_p))


@cres(signatures.get("duckdb_scalar_function_set_bind"))
def duckdb_scalar_function_set_bind(scalar_function_p, bind_p):
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_scalar_function_set_bind """
    return _call_lib_func("duckdb_scalar_function_set_bind", (scalar_function_p, bind_p))


@cres(signatures.get("duckdb_scalar_function_set_extra_info"))
def duckdb_scalar_function_set_extra_info(scalar_function_p, extra_info_p, destroy_p):
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_scalar_function_set_extra_info """
    return _call_lib_func("duckdb_scalar_function_set_extra_info", (scalar_function_p, extra_info_p, destroy_p))


@cres(signatures.get("duckdb_scalar_function_set_varargs"))
def duckdb_scalar_function_set_varargs(scalar_function_p, type_p):
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_scalar_function_set_varargs """
    return _call_lib_func("duckdb_scalar_function_set_varargs", (scalar_function_p, type_p))


@cres(signatures.get("duckdb_scalar_function_set_volatile"))
def duckdb_scalar_function_set_volatile(scalar_function_p):
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_scalar_function_set_volatile """
    return _call_lib_func("duckdb_scalar_function_set_volatile", (scalar_function_p,))


@cres(signatures.get("duckdb_scalar_function_set_special_handling"))
def duckdb_scalar_function_set_special_handling(scalar_function_p):
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_scalar_function_set_special_handling """
    return _call_lib_func("duckdb_scalar_function_set_special_handling", (scalar_function_p,))


@cres(signatures.get("duckdb_scalar_function_set_init"), if_available=True)
def duckdb_scalar_function_set_init(scalar_function_p, init_p):
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_scalar_function_set_init """
    return _call_lib_func("duckdb_scalar_function_set_init", (scalar_function_p, init_p))


@cres(signatures.get("duckdb_create_scalar_function_set"))
def duckdb_create_scalar_function_set(name_p):
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_create_scalar_function_set """
    return _call_lib_func("duckdb_create_scalar_function_set", (name_p,))


@cres(signatures.get("duckdb_destroy_scalar_function_set"))
def duckdb_destroy_scalar_function_set(set_pp):
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_destroy_scalar_function_set """
    return _call_lib_func("duckdb_destroy_scalar_function_set", (set_pp,))


@cres(signatures.get("duckdb_add_scalar_function_to_set"))
def duckdb_add_scalar_function_to_set(set_p, scalar_function_p):
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_add_scalar_function_to_set """
    return _call_lib_func("duckdb_add_scalar_function_to_set", (set_p, scalar_function_p))


@cres(signatures.get("duckdb_register_scalar_function_set"))
def duckdb_register_scalar_function_set(connection_p, set_p):
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_register_scalar_function_set """
    return _call_lib_func("duckdb_register_scalar_function_set", (connection_p, set_p))
```

- [ ] Commit: `git add numbduck/ducklib.py && git commit -m "Add scalar function wrappers to ducklib"`

---

### Task 3: Scalar UDF integration test

**Goal:** Prove the scalar UDF round-trip works: create, configure, register, call from SQL, verify result.

**Files:**
- Modify: `test/test_ducklib.py`

**Steps:**

- [ ] Add test at the end of the test file, before any varint tests:

```python
def test_scalar_function_round_trip():
    """Register a scalar UDF that adds 1 to an integer, call it from SQL."""
    from numba import cfunc, types as nb_types

    # Callback: reads int32 from input chunk column 0, writes int32 + 1 to output vector
    @cfunc(nb_types.void(nb_types.voidptr, nb_types.voidptr, nb_types.voidptr))
    def add_one_cb(info, chunk, output):
        from numbduck.ducklib import (
            duckdb_data_chunk_get_column_count,
            duckdb_data_chunk_get_size,
            duckdb_data_chunk_get_vector,
            duckdb_vector_get_data,
        )
        n = duckdb_data_chunk_get_size(chunk)
        input_vec = duckdb_data_chunk_get_vector(chunk, 0)
        in_data = duckdb_vector_get_data(input_vec)
        out_data = duckdb_vector_get_data(output)
        for i in range(n):
            val = carray(in_data, (n,), dtype=numpy.int32)[i]
            carray(out_data, (n,), dtype=numpy.int32)[i] = val + 1

    duckdb_database, duckdb_connection = aux_connect_db()
    conn_p = duckdb_connection[0]

    # Create and configure scalar function
    func_p = ducklib.duckdb_create_scalar_function()
    assert func_p != 0

    name_p = get_unicode_data_p("add_one")
    rc = ducklib.duckdb_scalar_function_set_name(func_p, name_p)
    assert rc == ducklib.DuckDBSuccess

    int_type_p = ducklib.duckdb_create_logical_type(4)  # DUCKDB_TYPE_INTEGER
    rc = ducklib.duckdb_scalar_function_add_parameter(func_p, int_type_p)
    assert rc == ducklib.DuckDBSuccess
    rc = ducklib.duckdb_scalar_function_set_return_type(func_p, int_type_p)
    assert rc == ducklib.DuckDBSuccess
    ducklib.duckdb_destroy_logical_type(int_type_p)

    rc = ducklib.duckdb_scalar_function_set_function(func_p, add_one_cb.address)
    assert rc == ducklib.DuckDBSuccess

    rc = ducklib.duckdb_register_scalar_function(conn_p, func_p)
    assert rc == ducklib.DuckDBSuccess

    # Destroy function handle (DuckDB copied it during registration)
    func_buf = numpy.array([func_p], dtype=numpy.intp)
    ducklib.duckdb_destroy_scalar_function(func_buf.ctypes.data)

    # Query using the UDF
    result = create_duckdb_result()
    query_p = get_unicode_data_p("SELECT add_one(42)")
    rc = ducklib.duckdb_query(conn_p, query_p, result.ctypes.data)
    assert rc == ducklib.DuckDBSuccess, f"Query failed, rc={rc}"

    # Read result
    chunk_count = ducklib.duckdb_result_chunk_count(result)
    assert chunk_count == 1
    chunk_p = ducklib.duckdb_result_get_chunk(result, 0)
    vec_p = ducklib.duckdb_data_chunk_get_vector(chunk_p, 0)
    data_p = ducklib.duckdb_vector_get_data(vec_p)
    val = carray(data_p, (1,), dtype=numpy.int32)[0]
    assert val == 43, f"Expected 43, got {val}"

    ducklib.duckdb_destroy_data_chunk(chunk_p)
    ducklib.duckdb_destroy_result(result.ctypes.data)
    aux_close_db(duckdb_database, duckdb_connection)
```

- [ ] Run: `find . -name __pycache__ -exec rm -rf {} + 2>/dev/null; rm -rf ~/.cache/numba; pytest test/test_ducklib.py::test_scalar_function_round_trip -v`
- [ ] Commit: `git add test/test_ducklib.py && git commit -m "Add scalar UDF round-trip integration test"`

---

### Task 4: Aggregate function signatures and wrappers

**Goal:** Add all 14 aggregate function signatures and `@cres` wrappers.

**Files:**
- Modify: `numbduck/ducklib.py`

**Steps:**

- [ ] Add signatures after the scalar function signatures block:

```python
# ── Aggregate Functions ──────────────────────────────────────────────
signatures["duckdb_create_aggregate_function"] = intp()
signatures["duckdb_destroy_aggregate_function"] = void(intp)
signatures["duckdb_register_aggregate_function"] = duckdb_state_ty(intp, intp)
signatures["duckdb_aggregate_function_set_name"] = duckdb_state_ty(intp, intp)
signatures["duckdb_aggregate_function_add_parameter"] = duckdb_state_ty(intp, intp)
signatures["duckdb_aggregate_function_set_return_type"] = duckdb_state_ty(intp, intp)
signatures["duckdb_aggregate_function_set_functions"] = duckdb_state_ty(intp, intp, intp, intp, intp, intp)
signatures["duckdb_aggregate_function_set_destructor"] = duckdb_state_ty(intp, intp)
signatures["duckdb_aggregate_function_set_extra_info"] = duckdb_state_ty(intp, intp, intp)
signatures["duckdb_aggregate_function_set_special_handling"] = duckdb_state_ty(intp)
signatures["duckdb_create_aggregate_function_set"] = intp(intp)
signatures["duckdb_destroy_aggregate_function_set"] = void(intp)
signatures["duckdb_add_aggregate_function_to_set"] = duckdb_state_ty(intp, intp)
signatures["duckdb_register_aggregate_function_set"] = duckdb_state_ty(intp, intp)
```

- [ ] Add wrappers after the scalar function wrappers:

```python
# ── Aggregate Functions ──────────────────────────────────────────────

@cres(signatures.get("duckdb_create_aggregate_function"))
def duckdb_create_aggregate_function():
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_create_aggregate_function """
    return _call_lib_func("duckdb_create_aggregate_function", ())


@cres(signatures.get("duckdb_destroy_aggregate_function"))
def duckdb_destroy_aggregate_function(aggregate_function_pp):
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_destroy_aggregate_function """
    return _call_lib_func("duckdb_destroy_aggregate_function", (aggregate_function_pp,))


@cres(signatures.get("duckdb_register_aggregate_function"))
def duckdb_register_aggregate_function(connection_p, aggregate_function_p):
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_register_aggregate_function """
    return _call_lib_func("duckdb_register_aggregate_function", (connection_p, aggregate_function_p))


@cres(signatures.get("duckdb_aggregate_function_set_name"))
def duckdb_aggregate_function_set_name(aggregate_function_p, name_p):
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_aggregate_function_set_name """
    return _call_lib_func("duckdb_aggregate_function_set_name", (aggregate_function_p, name_p))


@cres(signatures.get("duckdb_aggregate_function_add_parameter"))
def duckdb_aggregate_function_add_parameter(aggregate_function_p, type_p):
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_aggregate_function_add_parameter """
    return _call_lib_func("duckdb_aggregate_function_add_parameter", (aggregate_function_p, type_p))


@cres(signatures.get("duckdb_aggregate_function_set_return_type"))
def duckdb_aggregate_function_set_return_type(aggregate_function_p, type_p):
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_aggregate_function_set_return_type """
    return _call_lib_func("duckdb_aggregate_function_set_return_type", (aggregate_function_p, type_p))


@cres(signatures.get("duckdb_aggregate_function_set_functions"))
def duckdb_aggregate_function_set_functions(aggregate_function_p, state_size_p, init_p, update_p, combine_p, finalize_p):
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_aggregate_function_set_functions """
    return _call_lib_func("duckdb_aggregate_function_set_functions", (aggregate_function_p, state_size_p, init_p, update_p, combine_p, finalize_p))


@cres(signatures.get("duckdb_aggregate_function_set_destructor"))
def duckdb_aggregate_function_set_destructor(aggregate_function_p, destroy_p):
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_aggregate_function_set_destructor """
    return _call_lib_func("duckdb_aggregate_function_set_destructor", (aggregate_function_p, destroy_p))


@cres(signatures.get("duckdb_aggregate_function_set_extra_info"))
def duckdb_aggregate_function_set_extra_info(aggregate_function_p, extra_info_p, destroy_p):
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_aggregate_function_set_extra_info """
    return _call_lib_func("duckdb_aggregate_function_set_extra_info", (aggregate_function_p, extra_info_p, destroy_p))


@cres(signatures.get("duckdb_aggregate_function_set_special_handling"))
def duckdb_aggregate_function_set_special_handling(aggregate_function_p):
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_aggregate_function_set_special_handling """
    return _call_lib_func("duckdb_aggregate_function_set_special_handling", (aggregate_function_p,))


@cres(signatures.get("duckdb_create_aggregate_function_set"))
def duckdb_create_aggregate_function_set(name_p):
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_create_aggregate_function_set """
    return _call_lib_func("duckdb_create_aggregate_function_set", (name_p,))


@cres(signatures.get("duckdb_destroy_aggregate_function_set"))
def duckdb_destroy_aggregate_function_set(set_pp):
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_destroy_aggregate_function_set """
    return _call_lib_func("duckdb_destroy_aggregate_function_set", (set_pp,))


@cres(signatures.get("duckdb_add_aggregate_function_to_set"))
def duckdb_add_aggregate_function_to_set(set_p, aggregate_function_p):
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_add_aggregate_function_to_set """
    return _call_lib_func("duckdb_add_aggregate_function_to_set", (set_p, aggregate_function_p))


@cres(signatures.get("duckdb_register_aggregate_function_set"))
def duckdb_register_aggregate_function_set(connection_p, set_p):
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_register_aggregate_function_set """
    return _call_lib_func("duckdb_register_aggregate_function_set", (connection_p, set_p))
```

- [ ] Commit: `git add numbduck/ducklib.py && git commit -m "Add aggregate function signatures and wrappers to ducklib"`

---

### Task 5: Callback-side accessor signatures and wrappers

**Goal:** Add `duckdb_function_get_extra_info` and `duckdb_function_set_error` bindings.

**Files:**
- Modify: `numbduck/ducklib.py`

**Steps:**

- [ ] Add signatures after aggregate function signatures:

```python
# ── Callback-Side Accessors ──────────────────────────────────────────
signatures["duckdb_function_get_extra_info"] = intp(intp)
signatures["duckdb_function_set_error"] = void(intp, intp)
```

- [ ] Add wrappers after aggregate function wrappers:

```python
# ── Callback-Side Accessors ──────────────────────────────────────────

@cres(signatures.get("duckdb_function_get_extra_info"))
def duckdb_function_get_extra_info(info_p):
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_function_get_extra_info """
    return _call_lib_func("duckdb_function_get_extra_info", (info_p,))


@cres(signatures.get("duckdb_function_set_error"))
def duckdb_function_set_error(info_p, error_p):
    """ https://duckdb.org/docs/stable/clients/c/api.html#duckdb_function_set_error """
    return _call_lib_func("duckdb_function_set_error", (info_p, error_p))
```

- [ ] Commit: `git add numbduck/ducklib.py && git commit -m "Add callback-side accessor bindings to ducklib"`

---

### Task 6: Aggregate UDF integration test

**Goal:** Prove the aggregate UDF round-trip works: custom SUM via state_size/init/update/combine/finalize callbacks.

**Files:**
- Modify: `test/test_ducklib.py`

**Steps:**

- [ ] Add test:

```python
def test_aggregate_function_round_trip():
    """Register an aggregate UDF that sums int32 values, call it from SQL."""
    from numba import cfunc, types as nb_types

    STATE_SIZE = 8  # int64 accumulator

    @cfunc(nb_types.uint64(nb_types.voidptr))
    def my_state_size(info):
        return STATE_SIZE

    @cfunc(nb_types.void(nb_types.voidptr, nb_types.voidptr))
    def my_init(info, state):
        carray(state, (1,), dtype=numpy.int64)[0] = 0

    @cfunc(nb_types.void(nb_types.voidptr, nb_types.voidptr, nb_types.voidptr))
    def my_update(info, chunk, states):
        from numbduck.ducklib import (
            duckdb_data_chunk_get_size,
            duckdb_data_chunk_get_vector,
            duckdb_vector_get_data,
        )
        n = duckdb_data_chunk_get_size(chunk)
        input_vec = duckdb_data_chunk_get_vector(chunk, 0)
        in_data = duckdb_vector_get_data(input_vec)
        state_ptrs = carray(states, (n,), dtype=numpy.intp)
        in_vals = carray(in_data, (n,), dtype=numpy.int32)
        for i in range(n):
            acc = carray(state_ptrs[i], (1,), dtype=numpy.int64)
            acc[0] += in_vals[i]

    @cfunc(nb_types.void(nb_types.voidptr, nb_types.voidptr, nb_types.voidptr, nb_types.uint64))
    def my_combine(info, source, target, count):
        src_ptrs = carray(source, (count,), dtype=numpy.intp)
        tgt_ptrs = carray(target, (count,), dtype=numpy.intp)
        for i in range(count):
            src_acc = carray(src_ptrs[i], (1,), dtype=numpy.int64)[0]
            tgt_acc = carray(tgt_ptrs[i], (1,), dtype=numpy.int64)
            tgt_acc[0] += src_acc

    @cfunc(nb_types.void(nb_types.voidptr, nb_types.voidptr, nb_types.voidptr, nb_types.uint64, nb_types.uint64))
    def my_finalize(info, source, result, count, offset):
        from numbduck.ducklib import duckdb_vector_get_data
        out_data = duckdb_vector_get_data(result)
        src_ptrs = carray(source, (count,), dtype=numpy.intp)
        out_vals = carray(out_data, (offset + count,), dtype=numpy.int64)
        for i in range(count):
            acc = carray(src_ptrs[i], (1,), dtype=numpy.int64)[0]
            out_vals[offset + i] = acc

    duckdb_database, duckdb_connection = aux_connect_db()
    conn_p = duckdb_connection[0]

    # Create test table
    result = create_duckdb_result()
    query_p = get_unicode_data_p("CREATE TABLE t AS SELECT * FROM (VALUES (1), (2), (3)) AS t(v)")
    rc = ducklib.duckdb_query(conn_p, query_p, result.ctypes.data)
    assert rc == ducklib.DuckDBSuccess
    ducklib.duckdb_destroy_result(result.ctypes.data)

    # Create and configure aggregate function
    func_p = ducklib.duckdb_create_aggregate_function()
    assert func_p != 0

    name_p = get_unicode_data_p("my_sum")
    rc = ducklib.duckdb_aggregate_function_set_name(func_p, name_p)
    assert rc == ducklib.DuckDBSuccess

    int_type_p = ducklib.duckdb_create_logical_type(4)  # INTEGER
    bigint_type_p = ducklib.duckdb_create_logical_type(5)  # BIGINT
    rc = ducklib.duckdb_aggregate_function_add_parameter(func_p, int_type_p)
    assert rc == ducklib.DuckDBSuccess
    rc = ducklib.duckdb_aggregate_function_set_return_type(func_p, bigint_type_p)
    assert rc == ducklib.DuckDBSuccess
    ducklib.duckdb_destroy_logical_type(int_type_p)
    ducklib.duckdb_destroy_logical_type(bigint_type_p)

    rc = ducklib.duckdb_aggregate_function_set_functions(
        func_p, my_state_size.address, my_init.address,
        my_update.address, my_combine.address, my_finalize.address
    )
    assert rc == ducklib.DuckDBSuccess

    rc = ducklib.duckdb_register_aggregate_function(conn_p, func_p)
    assert rc == ducklib.DuckDBSuccess

    func_buf = numpy.array([func_p], dtype=numpy.intp)
    ducklib.duckdb_destroy_aggregate_function(func_buf.ctypes.data)

    # Query using the UDAF
    result = create_duckdb_result()
    query_p = get_unicode_data_p("SELECT my_sum(v) FROM t")
    rc = ducklib.duckdb_query(conn_p, query_p, result.ctypes.data)
    assert rc == ducklib.DuckDBSuccess, f"Query failed, rc={rc}"

    chunk_p = ducklib.duckdb_result_get_chunk(result, 0)
    vec_p = ducklib.duckdb_data_chunk_get_vector(chunk_p, 0)
    data_p = ducklib.duckdb_vector_get_data(vec_p)
    val = carray(data_p, (1,), dtype=numpy.int64)[0]
    assert val == 6, f"Expected 6, got {val}"

    ducklib.duckdb_destroy_data_chunk(chunk_p)
    ducklib.duckdb_destroy_result(result.ctypes.data)
    aux_close_db(duckdb_database, duckdb_connection)
```

- [ ] Run: `find . -name __pycache__ -exec rm -rf {} + 2>/dev/null; rm -rf ~/.cache/numba; pytest test/test_ducklib.py::test_aggregate_function_round_trip -v`
- [ ] Commit: `git add test/test_ducklib.py && git commit -m "Add aggregate UDF round-trip integration test"`

---

### Task 7: Extra info and error propagation tests

**Goal:** Test `set_extra_info`/`get_extra_info` round-trip and `set_error` propagation.

**Files:**
- Modify: `test/test_ducklib.py`

**Steps:**

- [ ] Add test for extra_info:

```python
def test_scalar_function_extra_info():
    """Verify extra_info pointer round-trips through set/get."""
    from numba import cfunc, types as nb_types

    MAGIC = 0xDEADBEEF

    @cfunc(nb_types.void(nb_types.voidptr, nb_types.voidptr, nb_types.voidptr))
    def extra_info_cb(info, chunk, output):
        from numbduck.ducklib import (
            duckdb_function_get_extra_info,
            duckdb_data_chunk_get_size,
            duckdb_vector_get_data,
        )
        extra = duckdb_function_get_extra_info(info)
        n = duckdb_data_chunk_get_size(chunk)
        out_data = duckdb_vector_get_data(output)
        out_vals = carray(out_data, (n,), dtype=numpy.int64)
        for i in range(n):
            out_vals[i] = extra

    duckdb_database, duckdb_connection = aux_connect_db()
    conn_p = duckdb_connection[0]

    func_p = ducklib.duckdb_create_scalar_function()
    ducklib.duckdb_scalar_function_set_name(func_p, get_unicode_data_p("get_extra"))

    bigint_type_p = ducklib.duckdb_create_logical_type(5)
    ducklib.duckdb_scalar_function_set_return_type(func_p, bigint_type_p)
    ducklib.duckdb_destroy_logical_type(bigint_type_p)

    ducklib.duckdb_scalar_function_set_function(func_p, extra_info_cb.address)
    ducklib.duckdb_scalar_function_set_extra_info(func_p, MAGIC, 0)

    rc = ducklib.duckdb_register_scalar_function(conn_p, func_p)
    assert rc == ducklib.DuckDBSuccess

    func_buf = numpy.array([func_p], dtype=numpy.intp)
    ducklib.duckdb_destroy_scalar_function(func_buf.ctypes.data)

    result = create_duckdb_result()
    query_p = get_unicode_data_p("SELECT get_extra()")
    rc = ducklib.duckdb_query(conn_p, query_p, result.ctypes.data)
    assert rc == ducklib.DuckDBSuccess

    chunk_p = ducklib.duckdb_result_get_chunk(result, 0)
    vec_p = ducklib.duckdb_data_chunk_get_vector(chunk_p, 0)
    data_p = ducklib.duckdb_vector_get_data(vec_p)
    val = carray(data_p, (1,), dtype=numpy.int64)[0]
    assert val == MAGIC, f"Expected {MAGIC}, got {val}"

    ducklib.duckdb_destroy_data_chunk(chunk_p)
    ducklib.duckdb_destroy_result(result.ctypes.data)
    aux_close_db(duckdb_database, duckdb_connection)
```

- [ ] Add test for set_error:

```python
def test_scalar_function_set_error():
    """Verify set_error in callback causes query failure."""
    from numba import cfunc, types as nb_types

    @cfunc(nb_types.void(nb_types.voidptr, nb_types.voidptr, nb_types.voidptr))
    def error_cb(info, chunk, output):
        from numbduck.ducklib import duckdb_function_set_error
        from numbox.utils.lowlevel import get_unicode_data_p as _get_p
        duckdb_function_set_error(info, _get_p("test error from callback"))

    duckdb_database, duckdb_connection = aux_connect_db()
    conn_p = duckdb_connection[0]

    func_p = ducklib.duckdb_create_scalar_function()
    ducklib.duckdb_scalar_function_set_name(func_p, get_unicode_data_p("will_fail"))

    int_type_p = ducklib.duckdb_create_logical_type(4)
    ducklib.duckdb_scalar_function_add_parameter(func_p, int_type_p)
    ducklib.duckdb_scalar_function_set_return_type(func_p, int_type_p)
    ducklib.duckdb_destroy_logical_type(int_type_p)

    ducklib.duckdb_scalar_function_set_function(func_p, error_cb.address)
    rc = ducklib.duckdb_register_scalar_function(conn_p, func_p)
    assert rc == ducklib.DuckDBSuccess

    func_buf = numpy.array([func_p], dtype=numpy.intp)
    ducklib.duckdb_destroy_scalar_function(func_buf.ctypes.data)

    result = create_duckdb_result()
    query_p = get_unicode_data_p("SELECT will_fail(42)")
    rc = ducklib.duckdb_query(conn_p, query_p, result.ctypes.data)
    assert rc == ducklib.DuckDBError, f"Expected DuckDBError, got {rc}"

    ducklib.duckdb_destroy_result(result.ctypes.data)
    aux_close_db(duckdb_database, duckdb_connection)
```

- [ ] Run: `find . -name __pycache__ -exec rm -rf {} + 2>/dev/null; rm -rf ~/.cache/numba; pytest test/test_ducklib.py::test_scalar_function_extra_info test/test_ducklib.py::test_scalar_function_set_error -v`
- [ ] Commit: `git add test/test_ducklib.py && git commit -m "Add extra_info and set_error integration tests"`

---

### Task 8: Scalar function set (overloads) test

**Goal:** Test registering an overloaded scalar function with INTEGER and DOUBLE variants.

**Files:**
- Modify: `test/test_ducklib.py`

**Steps:**

- [ ] Add test:

```python
def test_scalar_function_set_overloads():
    """Register overloaded scalar function with integer and double variants."""
    from numba import cfunc, types as nb_types

    @cfunc(nb_types.void(nb_types.voidptr, nb_types.voidptr, nb_types.voidptr))
    def double_it_int(info, chunk, output):
        from numbduck.ducklib import (
            duckdb_data_chunk_get_size, duckdb_data_chunk_get_vector, duckdb_vector_get_data,
        )
        n = duckdb_data_chunk_get_size(chunk)
        in_data = duckdb_vector_get_data(duckdb_data_chunk_get_vector(chunk, 0))
        out_data = duckdb_vector_get_data(output)
        for i in range(n):
            carray(out_data, (n,), dtype=numpy.int32)[i] = carray(in_data, (n,), dtype=numpy.int32)[i] * 2

    @cfunc(nb_types.void(nb_types.voidptr, nb_types.voidptr, nb_types.voidptr))
    def double_it_dbl(info, chunk, output):
        from numbduck.ducklib import (
            duckdb_data_chunk_get_size, duckdb_data_chunk_get_vector, duckdb_vector_get_data,
        )
        n = duckdb_data_chunk_get_size(chunk)
        in_data = duckdb_vector_get_data(duckdb_data_chunk_get_vector(chunk, 0))
        out_data = duckdb_vector_get_data(output)
        for i in range(n):
            carray(out_data, (n,), dtype=numpy.float64)[i] = carray(in_data, (n,), dtype=numpy.float64)[i] * 2.0

    duckdb_database, duckdb_connection = aux_connect_db()
    conn_p = duckdb_connection[0]

    # Integer variant
    int_func_p = ducklib.duckdb_create_scalar_function()
    int_type_p = ducklib.duckdb_create_logical_type(4)  # INTEGER
    ducklib.duckdb_scalar_function_add_parameter(int_func_p, int_type_p)
    ducklib.duckdb_scalar_function_set_return_type(int_func_p, int_type_p)
    ducklib.duckdb_scalar_function_set_function(int_func_p, double_it_int.address)
    ducklib.duckdb_destroy_logical_type(int_type_p)

    # Double variant
    dbl_func_p = ducklib.duckdb_create_scalar_function()
    dbl_type_p = ducklib.duckdb_create_logical_type(11)  # DOUBLE
    ducklib.duckdb_scalar_function_add_parameter(dbl_func_p, dbl_type_p)
    ducklib.duckdb_scalar_function_set_return_type(dbl_func_p, dbl_type_p)
    ducklib.duckdb_scalar_function_set_function(dbl_func_p, double_it_dbl.address)
    ducklib.duckdb_destroy_logical_type(dbl_type_p)

    # Create function set and register
    set_p = ducklib.duckdb_create_scalar_function_set(get_unicode_data_p("double_it"))
    rc = ducklib.duckdb_add_scalar_function_to_set(set_p, int_func_p)
    assert rc == ducklib.DuckDBSuccess
    rc = ducklib.duckdb_add_scalar_function_to_set(set_p, dbl_func_p)
    assert rc == ducklib.DuckDBSuccess
    rc = ducklib.duckdb_register_scalar_function_set(conn_p, set_p)
    assert rc == ducklib.DuckDBSuccess

    # Cleanup handles
    for p in [int_func_p, dbl_func_p]:
        buf = numpy.array([p], dtype=numpy.intp)
        ducklib.duckdb_destroy_scalar_function(buf.ctypes.data)
    set_buf = numpy.array([set_p], dtype=numpy.intp)
    ducklib.duckdb_destroy_scalar_function_set(set_buf.ctypes.data)

    # Test integer variant
    result = create_duckdb_result()
    rc = ducklib.duckdb_query(conn_p, get_unicode_data_p("SELECT double_it(21::INTEGER)"), result.ctypes.data)
    assert rc == ducklib.DuckDBSuccess
    chunk_p = ducklib.duckdb_result_get_chunk(result, 0)
    vec_p = ducklib.duckdb_data_chunk_get_vector(chunk_p, 0)
    val = carray(ducklib.duckdb_vector_get_data(vec_p), (1,), dtype=numpy.int32)[0]
    assert val == 42, f"Expected 42, got {val}"
    ducklib.duckdb_destroy_data_chunk(chunk_p)
    ducklib.duckdb_destroy_result(result.ctypes.data)

    # Test double variant
    result = create_duckdb_result()
    rc = ducklib.duckdb_query(conn_p, get_unicode_data_p("SELECT double_it(1.5::DOUBLE)"), result.ctypes.data)
    assert rc == ducklib.DuckDBSuccess
    chunk_p = ducklib.duckdb_result_get_chunk(result, 0)
    vec_p = ducklib.duckdb_data_chunk_get_vector(chunk_p, 0)
    val = carray(ducklib.duckdb_vector_get_data(vec_p), (1,), dtype=numpy.float64)[0]
    assert val == 3.0, f"Expected 3.0, got {val}"
    ducklib.duckdb_destroy_data_chunk(chunk_p)
    ducklib.duckdb_destroy_result(result.ctypes.data)

    aux_close_db(duckdb_database, duckdb_connection)
```

- [ ] Run: `find . -name __pycache__ -exec rm -rf {} + 2>/dev/null; rm -rf ~/.cache/numba; pytest test/test_ducklib.py::test_scalar_function_set_overloads -v`
- [ ] Commit: `git add test/test_ducklib.py && git commit -m "Add scalar function set overload integration test"`

---

### Task 9: Final verification and lint

**Goal:** Run full test suite and linter, ensure nothing is broken.

**Steps:**

- [ ] Clean caches and run full suite: `find . -name __pycache__ -exec rm -rf {} + 2>/dev/null; rm -rf ~/.cache/numba; pytest test/test_ducklib.py -v`
- [ ] Run linter: `flake8 numbduck/ducklib.py test/test_ducklib.py`
- [ ] Fix any issues
- [ ] Commit fixes if needed
