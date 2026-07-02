# numbduck subsystem: Connection bridge & C-struct buffers

Scope: `numbduck/pybridge.py` (the pybind11 → C-API `Connection*` extractor) and
`numbduck/duckdb_utils.py` (the numpy buffers that stand in for DuckDB opaque C
structs). This note describes what the code actually does at HEAD
(`review/numbduck-2026-06-29`, `@proxy` tree), the C / JIT / Python boundaries it
crosses, its invariants, and the load-bearing assumptions that a downstream defect
review should scrutinise.

---

## 1. The two files at a glance

- `duckdb_utils.py` — six `@njit`-compiled allocator functions, each returning a
  `numpy.zeros(sz, dtype=int64)` array whose raw bytes are handed to DuckDB C-API
  calls as an opaque out-struct / handle slot. `sz` is chosen so the byte length
  matches (or exceeds) the corresponding `duckdb.h` struct.
- `pybridge.py` — `extract_connection_ptr(conn)`, a pure-Python (ctypes) function
  that walks the pybind11 instance layout of a `duckdb.DuckDBPyConnection` to
  recover the underlying C-API `duckdb_connection` pointer, then proves the pointer
  is live by running `SELECT 1` through the JIT bindings.

The bridge is the seam that lets ordinary Python-created DuckDB connections be
driven by the numba `@njit` C-API bindings in `ducklib.py`.

---

## 2. `duckdb_utils.py` — numpy buffers as opaque C structs

### 2.1 The allocator

`duckdb_utils.py:7-9`

```python
@njit(**jit_options)
def allocate_buffer(sz: int):
    return numpy.zeros(sz, dtype=numpy.int64)
```

- `jit_options` comes from `numbduck/configurations.py` (env `NUMBDUCK_JIT_OPTIONS`
  as JSON, default `{"cache": True}`) — imported at `duckdb_utils.py:4`.
- Each buffer is an `int64` array → **8 bytes per element**. The array is
  zero-initialised, so every handle slot starts as a NULL pointer and every
  out-struct starts fully zeroed, which is what the DuckDB C API expects for an
  uninitialised `duckdb_result` / handle out-param.
- Callers pass `buf.ctypes.data` (Python int address) into the `intp`-typed C-API
  wrappers in `ducklib.py`. The buffer *is* the C struct; DuckDB writes through
  the pointer.

### 2.2 Size table (why each size)

Each wrapper (`duckdb_utils.py:12-51`) calls `allocate_buffer(n)`; bytes = `8 * n`:

| function | `n` | bytes | C type (v1.3.2 duckdb.h) | what lives there |
|---|---|---|---|---|
| `create_duckdb_connection` (`:13`) | 1 | 8 | `duckdb_connection` (opaque ptr) | one handle |
| `create_duckdb_database` (`:19`) | 1 | 8 | `duckdb_database` (opaque ptr) | one handle |
| `create_duckdb_prepared_statement` (`:25`) | 1 | 8 | `duckdb_prepared_statement` | one handle |
| `create_duckdb_data_chunk` (`:31`) | 1 | 8 | `duckdb_data_chunk` | one handle |
| `create_duckdb_value` (`:43`) | 1 | 8 | `duckdb_value` | one handle |
| `create_duckdb_vector` (`:49`) | 1 | 8 | `duckdb_vector` | one handle |
| `create_duckdb_result` (`:37`) | **6** | **48** | `duckdb_result` (a real struct, not a ptr) | full result struct |

The handle types are all `typedef struct {...} *`, i.e. a single pointer — 8 bytes
is exact. `duckdb_result` is the only genuine multi-field struct. In v1.3.2 it is
six 8-byte fields:

```c
typedef struct {
  idx_t deprecated_column_count;   // 8
  idx_t deprecated_row_count;      // 8
  idx_t deprecated_rows_changed;   // 8
  duckdb_column *deprecated_columns;    // 8
  char *deprecated_error_message;       // 8
  void *internal_data;                  // 8
} duckdb_result;                   // = 48 bytes
```

→ `6 * int64 = 48` matches exactly (`duckdb_utils.py:37-39`). This is the size that
"matters": `duckdb_query` / `duckdb_execute_prepared` write the full struct through
this pointer, so an undersized buffer would be a heap-adjacent out-of-bounds write.

Each docstring pins the size decision to a specific `duckdb.h` line at tag `v1.3.2`
(`duckdb_utils.py:14,20,26,32,38,44,50`).

### 2.3 Boundary & lifetime notes for the buffers

- **JIT ↔ Python boundary:** the allocators are `@njit`. Called from Python they
  compile-and-run, returning a real numpy array; called from within other `@njit`
  code they inline. Either way the caller must retain the returned array for as long
  as the C struct pointer is in use (`.ctypes.data` does **not** keep the array
  alive). See `pybridge.py` §3.3 for the one in-repo Python caller, which does hold
  the reference.
- **Type punning:** the struct is declared `int64` purely as a byte carrier. All
  fields (pointers, `idx_t`) are 8-byte aligned, so alignment is fine on LP64.

---

## 3. `pybridge.py` — `extract_connection_ptr`

`pybridge.py:10-74`. Recovers the C-API `duckdb_connection` from a Python
`duckdb.DuckDBPyConnection` by reading raw memory with ctypes.

### 3.1 Type guard

`pybridge.py:52-55` — `isinstance(conn, duckdb.DuckDBPyConnection)` else `TypeError`.
This guards the *Python* type only; it does **not** guarantee the pybind11/ABI
layout matches the offsets below (a different duckdb build with the same class name
would pass this check).

### 3.2 The two-hop pointer walk

Step 1 (`pybridge.py:58-59`):

```python
py_obj_addr = id(conn)
cpp_obj_p = ctypes.c_void_p.from_address(py_obj_addr + 16).value
```

`id(conn)` is the `PyObject*`. On CPython x86-64 the object header is 16 bytes
(`ob_refcnt` 8 + `ob_type` 8). pybind11 lays its `instance` struct immediately after
the header, and for the common **simple-holder** layout the first word of that
struct is the value pointer — the `DuckDBPyConnection` C++ object. So `+16` reads
that C++ object pointer.

Step 2 (`pybridge.py:61-62`):

```python
conn_ptr = ctypes.c_void_p.from_address(cpp_obj_p + 32).value
```

`+32` indexes into the C++ `DuckDBPyConnection` object. The docstring
(`pybridge.py:22-27`) documents the assumed field layout:

```
[0]  enable_shared_from_this weak_ptr  — 16 bytes
[16] shared_ptr<DuckDB> database       — 16 bytes
[32] unique_ptr<Connection> connection — the pointer we want
```

A `unique_ptr<Connection>` is a single pointer, so reading a `c_void_p` at `+32`
yields the raw `Connection*` — which the DuckDB C API treats as
`duckdb_connection`.

Both offsets are hard-coded and are explicitly stated to be validated only on
"duckdb 1.3.2 / Linux x86-64 / libstdc++" (`pybridge.py:31-34`). They are private
implementation details of the duckdb wheel and pybind11.

### 3.3 Validation query (and its ordering)

`pybridge.py:64-72`:

```python
result = create_duckdb_result()          # 48-byte buffer, §2.2
query_p = get_unicode_data_p("SELECT 1")  # ptr to string payload
rc = ducklib.duckdb_query(conn_ptr, query_p, result.ctypes.data)
ducklib.duckdb_destroy_result(result.ctypes.data)
if rc != ducklib.DuckDBSuccess:           # DuckDBSuccess == 0 (ducklib.py:19)
    raise RuntimeError("extracted connection pointer failed validation")
return conn_ptr
```

Control/data flow across boundaries:

- `create_duckdb_result()` — `@njit` allocator (§2.2), returns a live numpy array
  held in local `result` for the whole call → keeps the C struct memory alive.
- `get_unicode_data_p` — numbox `@njit` intrinsic (`numbox/utils/lowlevel.py:279-285`)
  that returns the raw address of a numba unicode string's data payload.
- `ducklib.duckdb_query` / `ducklib.duckdb_destroy_result` — `@proxy`-wrapped njit
  dispatchers (`ducklib.py:1111-1114`, `775-778`). Signatures:
  `duckdb_query = duckdb_state_ty(intp, intp, intp)` (`ducklib.py:221`),
  `duckdb_destroy_result = void(intp)` (`ducklib.py:165`). Called from Python they
  compile-and-run, ultimately reaching the real DuckDB C symbols via numbox's
  `_call_lib_func` and the `RTLD_GLOBAL`-loaded shared library.
- All three arguments are plain Python ints (`conn_ptr`, `query_p`,
  `result.ctypes.data`) matching the `intp` param types — numbduck's convention that
  every pointer is `intp`.

`duckdb_destroy_result` is always called (before the rc check) so the result struct
is freed on both success and query-failure paths — correct, since `duckdb_query`
populates the result even on error.

Return value: `conn_ptr` as a Python int (`intp`-compatible), ready to feed the
`ducklib.py` connection-taking bindings.

---

## 4. Invariants the bridge relies on

1. CPython object header is exactly 16 bytes (→ `+16` finds the pybind11 value ptr).
2. pybind11 uses the simple-holder inline layout for `DuckDBPyConnection`, storing
   the C++ object pointer as the first post-header word.
3. `DuckDBPyConnection`'s `unique_ptr<Connection>` sits at byte offset 32 with the
   16+16 preamble described at `pybridge.py:24-26`.
4. `duckdb_result` is ≤ 48 bytes for every duckdb in the pin range (`>=1.3.2,<1.6`),
   so `create_duckdb_result()`'s buffer is large enough to receive it.
5. Opaque handle types are single pointers (8 bytes) — true for all six 1-element
   allocators.
6. The DuckDB C symbols (`duckdb_query`, `duckdb_destroy_result`) are actually
   exported by the loaded library — the whole point of the numbduck project-status
   notes about macOS symbol stripping; on a stripped build the validation call would
   fail to resolve rather than returning a clean error.

---

## 5. Fragile assumptions / risk flags (noted, not fully audited)

- **Hard-coded ABI offsets (`+16`, `+32`).** `pybridge.py:59,62`. Verified only on
  duckdb 1.3.2 / Linux x86-64 / libstdc++. The pin allows `<1.6`; any change to
  pybind11 holder layout, the `DuckDBPyConnection` field order, or a non-libstdc++
  build silently reads a wrong pointer. Wrong pointer → best case the validation
  query returns `DuckDBError`; **worst case `duckdb_query` dereferences garbage and
  segfaults** before any rc can be checked. The `isinstance` guard does not cover
  this. Win32/other ABIs are entirely out of scope.

- **Validation cannot catch a wild pointer.** The `SELECT 1` check
  (`pybridge.py:64-72`) only distinguishes "live connection, query failed" from
  success. A structurally-wrong `conn_ptr` (from a layout drift) is undefined
  behavior in `duckdb_query`, not a catchable error — so the "validated before
  returned" guarantee in the docstring (`pybridge.py:28-29`) is weaker than it reads.

- **`get_unicode_data_p("SELECT 1")` lifetime (`pybridge.py:66`).** The returned
  int is a bare pointer into the data payload of a numba unicode string materialised
  from the Python `str` argument. Once `get_unicode_data_p` returns, the numba-side
  unicode value (and any meminfo it owns) is released; correctness then depends on
  the pointer aliasing memory that is still valid when `duckdb_query` reads it on the
  next line. The `"SELECT 1"` literal is a code constant of `extract_connection_ptr`
  so the *Python* string stays alive, but whether the returned pointer references
  that CPython buffer vs. a freed numba copy is a numba-internal detail. This is a
  classic dangling-pointer hazard worth a targeted look in the defect review.

- **`duckdb_result` size is a moving target.** `create_duckdb_result` hard-codes 48
  bytes against v1.3.2 (`duckdb_utils.py:37-39`) but the pin permits up to 1.5.x. If
  any release in-range grows `duckdb_result`, `duckdb_query` overruns the 48-byte
  numpy buffer — a heap OOB write. No runtime size check exists.

- **Buffer keep-alive is caller responsibility.** The allocators hand back numpy
  arrays; passing `.ctypes.data` to C means the array must outlive the C usage.
  `extract_connection_ptr` does this correctly (local `result`), but the pattern is
  easy to get wrong elsewhere (e.g. `create_duckdb_result().ctypes.data` inline
  would free the buffer immediately).

- **`int64` type-punning assumes LP64 / 8-byte pointers.** Fine on the supported
  Linux x86-64 target; not portable to ILP32.
