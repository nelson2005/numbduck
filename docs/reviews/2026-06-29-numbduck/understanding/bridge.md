# numbduck subsystem: Connection bridge & C-struct buffers

Scope: the two helpers that let numba/`ctypes` code reach into a live
`duckdb.DuckDBPyConnection` and hand DuckDB's C API correctly-sized scratch
buffers.

- `numbduck/pybridge.py` — `extract_connection_ptr(conn)`
- `numbduck/duckdb_utils.py` — numpy buffer allocators used as opaque DuckDB
  C structs.

Supporting code cited:
- `numbduck/ducklib.py` — `DuckDBSuccess`/`DuckDBError`, `duckdb_query`,
  `duckdb_destroy_result` bindings.
- `numbox/numbox/utils/lowlevel.py` — `get_unicode_data_p` /
  `_get_unicode_data_p`.

---

## 1. The problem this subsystem solves

The DuckDB Python wheel exposes a high-level pybind11 object
(`duckdb.DuckDBPyConnection`) but does **not** expose a way to get the raw C
API `duckdb_connection` handle from it. numbduck's JIT bindings
(`ducklib.py`) all operate on raw pointers passed as `intp`. So to drive the C
API against a connection the user already opened in Python, numbduck must
recover the C API `Connection*` from the opaque Python object. That is the
entire job of `extract_connection_ptr`.

The buffer allocators solve the complementary problem: every C API call that
returns a handle or fills an out-struct (`duckdb_result`, `duckdb_data_chunk`,
etc.) needs caller-provided storage of the exact byte size DuckDB expects.
numbduck materializes that storage as zero-initialized numpy `int64` arrays
and passes `arr.ctypes.data` (or `tuple(arr)` by value) as the pointer.

---

## 2. `extract_connection_ptr` — control & data flow

`numbduck/pybridge.py:10-74`.

### Step 0 — type guard
`pybridge.py:52-55`: rejects anything that is not a
`duckdb.DuckDBPyConnection` with `TypeError`. This is the only structural
precondition checked; it does **not** check that the connection is open or
non-closed.

### Step 1 — read the C++ object pointer out of the pybind11 instance header
`pybridge.py:58-59`:

```python
py_obj_addr = id(conn)
cpp_obj_p = ctypes.c_void_p.from_address(py_obj_addr + 16).value
```

`id(conn)` is the address of the `PyObject`. On CPython 64-bit a plain
`PyObject` header is 16 bytes (`ob_refcnt` 8 + `ob_type` 8). pybind11's
`instance` struct begins right after that header with the held value/holder
pointer, so reading a `c_void_p` at `id + 16` yields the pointer pybind11
stores for the wrapped C++ object — here the `DuckDBPyConnection*`
(the C++ class, not the C API handle). See the docstring at
`pybridge.py:16-19`.

This relies on pybind11's **simple/internal-pointer layout** (one bare value
pointer immediately after `PyObject_HEAD`). It is the common case for a
single-inheritance, default-holder pybind11 class.

### Step 2 — read the `Connection*` from a fixed member offset
`pybridge.py:62`:

```python
conn_ptr = ctypes.c_void_p.from_address(cpp_obj_p + 32).value
```

The `+32` is hand-derived from the assumed `DuckDBPyConnection` member layout
(`pybridge.py:22-27`):

```
[0]  enable_shared_from_this weak_ptr  — 16 bytes
[16] shared_ptr<DuckDB> database       — 16 bytes
[32] unique_ptr<Connection> connection — pointer we want
```

So `*(cpp_obj_p + 32)` is the raw pointer inside `unique_ptr<Connection>`,
i.e. the C++ `Connection*`. This is returned (as a Python int) and used
directly as the C API `duckdb_connection` argument. The fact that the bare
`Connection*` is directly accepted by `duckdb_query` is a property of DuckDB's
C API (the handle is a thin reinterpret of the C++ object), and is exactly
what the validation step confirms.

Both 16-byte sizes assume **libstdc++** layout (`weak_ptr` = 2 pointers = 16B,
`shared_ptr` = 2 pointers = 16B, `unique_ptr` with default deleter = 1
pointer). libc++ / MSVC could differ.

### Step 3 — validation by round-trip query
`pybridge.py:65-72`:

```python
result = create_duckdb_result()
query_p = get_unicode_data_p("SELECT 1")
rc = ducklib.duckdb_query(conn_ptr, query_p, result.ctypes.data)
ducklib.duckdb_destroy_result(result.ctypes.data)
if rc != ducklib.DuckDBSuccess:
    raise RuntimeError("extracted connection pointer failed validation")
```

- `create_duckdb_result()` (`duckdb_utils.py:34-37`) is the 6×int64 = 48-byte
  out-struct buffer for `duckdb_result`.
- `get_unicode_data_p("SELECT 1")` returns an `intp` pointing at the
  null-terminated UTF-8/ASCII data payload of the numba unicode string
  (`numbox/numbox/utils/lowlevel.py:267-285`; the intrinsic extracts field 0,
  the data pointer, and `ptrtoint`s it — `lowlevel.py:272-274`).
- `ducklib.duckdb_query(conn, query_p, out_result_p)` is the JIT-compiled
  binding; signature `duckdb_state_ty(intp, intp, intp)`
  (`ducklib.py:220`, wrapper `ducklib.py:1110-1113`). It writes into the
  result buffer at `result.ctypes.data`.
- `duckdb_destroy_result` (`ducklib.py:164`, `774-777`) frees the result's
  internal data. Note it is called **unconditionally before** the `rc` check,
  which is correct: destroying a result populated by a successful or failed
  query is the documented protocol, and a zero-initialized result is safe to
  destroy too.
- `DuckDBSuccess == 0`, `DuckDBError == 1` (`ducklib.py:18-19`). Error
  handling is C-style return-code, matching numbduck's contract that bindings
  never raise across the boundary; the Python-side `RuntimeError` is raised by
  `extract_connection_ptr` itself, not by the binding.

---

## 3. The buffer allocators (`duckdb_utils.py`)

All are `@njit` functions returning `numpy.zeros(sz, dtype=numpy.int64)`
(`duckdb_utils.py:5-7`). The numpy array **is** the opaque DuckDB C struct;
its backing memory is handed to C as a pointer (`arr.ctypes.data` for
out-params, or `tuple(arr)` by value where the binding takes the struct by
value, e.g. `duckdb_fetch_chunk(duckdb_result_ty)` at `ducklib.py:171`).

| Allocator | `sz` | Bytes | Represents | Cite |
|---|---|---|---|---|
| `create_duckdb_connection` | 1 | 8 | `duckdb_connection` handle (1 ptr) | `duckdb_utils.py:10-13` |
| `create_duckdb_database` | 1 | 8 | `duckdb_database` handle (1 ptr) | `duckdb_utils.py:16-19` |
| `create_duckdb_prepared_statement` | 1 | 8 | `duckdb_prepared_statement` handle | `duckdb_utils.py:22-25` |
| `create_duckdb_data_chunk` | 1 | 8 | `duckdb_data_chunk` handle | `duckdb_utils.py:28-31` |
| `create_duckdb_result` | **6** | **48** | `duckdb_result` struct (by value) | `duckdb_utils.py:34-37` |
| `create_duckdb_value` | 1 | 8 | `duckdb_value` handle | `duckdb_utils.py:40-43` |
| `create_duckdb_vector` | 1 | 8 | `duckdb_vector` handle | `duckdb_utils.py:46-49` |

### Why the sizes matter

- Everything except `duckdb_result` is an **opaque handle**, which in the C
  API is a single pointer (`typedef struct {...}* duckdb_X`). One `int64`
  slot = 8 bytes = one pointer. The C call writes the handle into that slot;
  the numpy array owns the 8 bytes.
- `duckdb_result` is **not** a handle — it is a value struct with 6 members
  (3 deprecated `idx_t` counts + `deprecated_columns*` +
  `deprecated_error_message*` + `internal_data*`), 48 bytes on 64-bit. Hence
  `sz=6`. Under-sizing this buffer (e.g. `sz=1`) would let `duckdb_query`
  write past the array and corrupt heap memory. The docstring link
  (`duckdb_utils.py:36`) points at `duckdb.h` L454 where the struct is
  defined; the `sz=6` is the load-bearing number in this whole file.
- `numpy.zeros` (not `empty`) zero-initializes, so handle slots start NULL and
  result fields start clean — both required for the destroy-on-any-path
  protocol and for handle out-params to be detectably-NULL on failure.

These functions are `@njit` so they can be called from inside other JIT code
(where the array is a native array), but `create_duckdb_result()` is also
invoked from pure Python in `pybridge.py:65` (numba materializes a real numpy
array on return).

---

## 4. Boundaries

- **Python ↔ raw memory (ctypes):** `extract_connection_ptr` Steps 1–2 are
  pure `ctypes.c_void_p.from_address`. No JIT, no DuckDB. This is the most
  ABI-fragile part.
- **Python ↔ JIT:** `create_duckdb_result()` and `get_unicode_data_p(...)` are
  `@njit` dispatchers called from Python; `ducklib.duckdb_query` /
  `duckdb_destroy_result` are `@cres`-compiled C-API wrappers called from
  Python. The pointers crossing are plain Python ints / `intp`.
- **JIT ↔ C:** the `@cres` wrappers lower to direct calls into
  `libduckdb` (loaded `RTLD_GLOBAL` per `utils.py`). Out-params are the numpy
  buffer's `ctypes.data` address.

---

## 5. Invariants

- `conn` must remain alive (not GC'd) for as long as the returned
  `conn_ptr` is used — the pointer is **borrowed**, not owned. Nothing in the
  code enforces or documents this lifetime coupling for callers.
- The result buffer must be exactly 6 int64 wide for `duckdb_query`/
  `duckdb_destroy_result` to operate safely.
- Handle buffers must be ≥1 pointer and zeroed.
- Return-code discipline: bindings return `DuckDBSuccess`/`DuckDBError`; only
  the Python helper translates failure into an exception
  (`pybridge.py:69-72`).

---

## 6. Fragile assumptions & risks (flagged, not audited)

1. **`+16` pybind11 header offset (`pybridge.py:59`)** assumes CPython 64-bit
   PyObject header size (16) **and** pybind11 "simple" instance layout (held
   pointer immediately after the header). A non-simple holder, multiple
   inheritance, or `__dict__`/weakref slots before the value pointer would
   shift this. Only validated on the configuration in the docstring
   (`pybridge.py:31-34`: duckdb 1.3.2 / Linux x86-64 / libstdc++).

2. **`+32` member offset (`pybridge.py:62`)** is hand-derived from an assumed
   `weak_ptr(16) + shared_ptr(16)` prefix. Any reordering of
   `DuckDBPyConnection` members, additional base classes, or a non-libstdc++
   `shared_ptr/weak_ptr`/`unique_ptr` size changes it. No runtime assertion
   guards the layout; the only safety net is the `SELECT 1` query.

3. **Validation can crash instead of failing cleanly.** If the extracted
   `conn_ptr` is a wrong-but-plausible address (or stale/closed connection),
   `duckdb_query(conn_ptr, ...)` (`pybridge.py:67`) may segfault rather than
   return `DuckDBError`. The `rc` check (`pybridge.py:69`) only catches
   pointers that survive a query and return a clean error code — a narrow
   class of failures. The guard is best-effort, not a true validation.

4. **`get_unicode_data_p("SELECT 1")` returns an interior data pointer
   (`pybridge.py:66`, intrinsic at `lowlevel.py:267-285`).** The pointer
   aliases the numba unicode string's buffer; it is only valid while that
   string lives. Here the literal is consumed on the very next line by
   `duckdb_query`, and string literals are persistent code constants, so it
   holds — but the pattern (raw data pointer outliving the function that
   produced it) is inherently fragile if reused with a non-literal/temporary
   string. Also assumes the payload is NUL-terminated (numba unicode storage
   keeps a trailing NUL; `lowlevel.py` `_get_unicode_data_p` relies on this).

5. **No ownership transfer / double-free surface around the C++
   `unique_ptr<Connection>`.** numbduck reads the raw pointer out of a
   `unique_ptr` it does not own (`pybridge.py:62`). It correctly does **not**
   call `duckdb_disconnect`/free on it here, but any caller that does would
   double-free against pybind11's own destructor. Worth checking downstream
   call sites.

6. **`create_duckdb_result` size coupling (`duckdb_utils.py:37`).** The `6`
   is tied to the DuckDB 1.3.2 `duckdb_result` definition. If a DuckDB version
   in the supported range (`duckdb>=1.3.2,<1.6` per CLAUDE.md) changes the
   struct's member count, this silently under/over-allocates. The value is a
   magic literal justified only by a docstring URL, with no compile-time or
   runtime size assertion against the actual ABI.

7. **Single shared mutable allocator for differently-sized handles.** All
   handle allocators delegate to `allocate_buffer(1)`; the distinct function
   names are documentation only. Correct today (all handles are 1 pointer) but
   provides no type safety if a future "handle" were larger.
