# UDF + UDAF C API Bindings

Low-level bindings for DuckDB's scalar function and aggregate function C APIs, enabling registration of numba `@cfunc` callbacks as SQL-callable UDFs.

## Scope

Add 33 C API function bindings to `ducklib.py` (32 baseline + 1 version-conditional). One PR. No new files, no high-level helpers, no decorator API.

### Functions to Bind

**Scalar function lifecycle (3):**

| C API Function | Signature | Notes |
|---|---|---|
| `duckdb_create_scalar_function` | `_p()` | Returns opaque handle |
| `duckdb_destroy_scalar_function` | `void(_pp)` | Takes pointer-to-pointer |
| `duckdb_register_scalar_function` | `duckdb_state_ty(_p, _p)` | connection, function |

**Scalar function configuration (10):**

| C API Function | Signature | Notes |
|---|---|---|
| `duckdb_scalar_function_set_name` | `duckdb_state_ty(_p, _p)` | function, const char* |
| `duckdb_scalar_function_add_parameter` | `duckdb_state_ty(_p, _p)` | function, logical_type |
| `duckdb_scalar_function_set_return_type` | `duckdb_state_ty(_p, _p)` | function, logical_type |
| `duckdb_scalar_function_set_function` | `duckdb_state_ty(_p, _p)` | function, callback ptr |
| `duckdb_scalar_function_set_bind` | `duckdb_state_ty(_p, _p)` | function, bind callback ptr |
| `duckdb_scalar_function_set_extra_info` | `duckdb_state_ty(_p, _p, _p)` | function, data ptr, destroy callback |
| `duckdb_scalar_function_set_varargs` | `duckdb_state_ty(_p, _p)` | function, logical_type |
| `duckdb_scalar_function_set_volatile` | `duckdb_state_ty(_p)` | function |
| `duckdb_scalar_function_set_special_handling` | `duckdb_state_ty(_p)` | function |
| `duckdb_scalar_function_set_init` | `duckdb_state_ty(_p, _p)` | function, init callback; **v1.5+ only** (`if_available=True`) |

**Scalar function sets / overloads (4):**

| C API Function | Signature | Notes |
|---|---|---|
| `duckdb_create_scalar_function_set` | `_p(_p)` | const char* name |
| `duckdb_destroy_scalar_function_set` | `void(_pp)` | pointer-to-pointer |
| `duckdb_add_scalar_function_to_set` | `duckdb_state_ty(_p, _p)` | set, function |
| `duckdb_register_scalar_function_set` | `uint32(_p, _p)` | connection, set |

**Aggregate function lifecycle (3):**

| C API Function | Signature | Notes |
|---|---|---|
| `duckdb_create_aggregate_function` | `_p()` | Returns opaque handle |
| `duckdb_destroy_aggregate_function` | `void(_pp)` | pointer-to-pointer |
| `duckdb_register_aggregate_function` | `duckdb_state_ty(_p, _p)` | connection, function |

**Aggregate function configuration (7):**

| C API Function | Signature | Notes |
|---|---|---|
| `duckdb_aggregate_function_set_name` | `duckdb_state_ty(_p, _p)` | function, const char* |
| `duckdb_aggregate_function_add_parameter` | `duckdb_state_ty(_p, _p)` | function, logical_type |
| `duckdb_aggregate_function_set_return_type` | `duckdb_state_ty(_p, _p)` | function, logical_type |
| `duckdb_aggregate_function_set_functions` | `duckdb_state_ty(_p, _p, _p, _p, _p, _p)` | function, state_size, init, update, combine, finalize |
| `duckdb_aggregate_function_set_destructor` | `duckdb_state_ty(_p, _p)` | function, destroy callback |
| `duckdb_aggregate_function_set_extra_info` | `duckdb_state_ty(_p, _p, _p)` | function, data ptr, destroy callback |
| `duckdb_aggregate_function_set_special_handling` | `duckdb_state_ty(_p)` | function |

**Aggregate function sets / overloads (4):**

| C API Function | Signature | Notes |
|---|---|---|
| `duckdb_create_aggregate_function_set` | `_p(_p)` | const char* name |
| `duckdb_destroy_aggregate_function_set` | `void(_pp)` | pointer-to-pointer |
| `duckdb_add_aggregate_function_to_set` | `duckdb_state_ty(_p, _p)` | set, function |
| `duckdb_register_aggregate_function_set` | `uint32(_p, _p)` | connection, set |

**Callback-side accessors (2):**

| C API Function | Signature | Notes |
|---|---|---|
| `duckdb_function_get_extra_info` | `_p(_p)` | Returns void* from function_info |
| `duckdb_function_set_error` | `void(_p, _p)` | function_info, const char* |

**Total: 33 bindings** (32 baseline + 1 version-conditional)

## Signature Mapping Conventions

All opaque DuckDB handles (`duckdb_scalar_function`, `duckdb_aggregate_function`, `duckdb_function_info`, `duckdb_logical_type`, etc.) are single-field structs wrapping `void *internal_ptr`. They map to `_p` (pointer) in numba signatures.

Callback function pointers (`duckdb_scalar_function_t`, `duckdb_aggregate_init_t`, etc.) are passed as `_p`. Users create callbacks with numba `@cfunc` and pass `.address`.

- `duckdb_state` (enum) -> `duckdb_state_ty` (`int32`)
- `const char *` -> `_p`
- `idx_t` -> `uint64`
- `void *` -> `_p`
- pointer-to-handle (destroy functions) -> `_pp`

## File Changes

- **`numbduck/ducklib.py`** — add signatures + `@cres` wrapper functions in a new "Scalar Functions" and "Aggregate Functions" section after existing value interface bindings. Each wrapper must include a docstring linking to `https://duckdb.org/docs/stable/clients/c/api.html#func_name` per existing convention.
- **`test/test_ducklib.py`** — add integration tests

No changes to `utils.py` or `duckdb_utils.py`.

## Testing Strategy

Tests exercise the full round-trip: create function, configure, register, call from SQL, verify result.

**Scalar UDF round-trip:**
1. Open database + connection
2. Create scalar function, set name `"add_one"`, add INTEGER parameter, set INTEGER return type
3. Create numba `@cfunc(void(voidptr, voidptr, voidptr))` callback that reads input chunk vector data, writes output vector data + 1
4. Set function callback, register with connection
5. `SELECT add_one(42)` -> assert result is 43
6. Destroy function + cleanup

**Aggregate UDF round-trip:**
1. Open database + connection, create table with test data
2. Create aggregate function `"my_sum"` with INTEGER parameter, BIGINT return type
3. Create `@cfunc` callbacks for state_size, init, update, combine, finalize
4. Set functions (monolithic call), register
5. `SELECT my_sum(val) FROM test_data GROUP BY grp` -> verify sums
6. Destroy + cleanup

**Scalar function set (overloads):**
1. Create two scalar functions: one for INTEGER, one for DOUBLE
2. Create function set, add both, register
3. Call with integer arg and double arg, verify both resolve

**Callback-side accessors:**
1. `set_extra_info` with a known pointer, `get_extra_info` inside callback, verify match
2. `set_error` inside callback, verify DuckDB propagates the error

**Version-conditional:**
- `scalar_function_set_init` test skipped on < 1.5 via `_has_symbol`

## Callback Wiring Pattern

User-facing pattern enabled by these bindings (not part of this PR):

```python
from numba import cfunc, types, njit
from numbduck.ducklib import (
    duckdb_create_scalar_function, duckdb_scalar_function_set_name,
    duckdb_scalar_function_add_parameter, duckdb_scalar_function_set_return_type,
    duckdb_scalar_function_set_function, duckdb_register_scalar_function,
    duckdb_destroy_scalar_function,
)
from numbduck.duckdb_utils import get_unicode_data_p

@cfunc(types.void(types.voidptr, types.voidptr, types.voidptr))
def my_scalar(info, input_chunk, output_vector):
    # Use existing numbduck bindings to read/write vectors
    pass

@njit
def register_my_func(conn):
    func = duckdb_create_scalar_function()
    name_ptr = get_unicode_data_p("my_func")
    duckdb_scalar_function_set_name(func, name_ptr)
    # ... configure parameter types, return type ...
    duckdb_scalar_function_set_function(func, my_scalar.address)
    duckdb_register_scalar_function(conn, func)
    duckdb_destroy_scalar_function(func)
```

## Version Compatibility

| Version | Support |
|---|---|
| 1.3.x | Full (32 functions) |
| 1.4.x | Full (32 functions) |
| 1.5.x | Full + `duckdb_scalar_function_set_init` (33 functions) |

Symbol names are stable across all three versions — no renames between 1.3.2 and 1.5.1 for the functions in scope. The `_add_parameter` and `_to_set` naming conventions are consistent.
