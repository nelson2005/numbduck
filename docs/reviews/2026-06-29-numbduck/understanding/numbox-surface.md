# numbduck ↔ numbox boundary contract

Scope: every symbol numbduck pulls from the sibling `numbox` library, the
contract each provides, and how the two cooperate at import-time, typing-time,
and call-time. Citations are `file:line`. numbduck paths are under
`/home/erik/projects/numbduck`; numbox paths under `/home/erik/projects/numbox`.

This is an understanding note feeding a defect review. Risky items are flagged
**[RISK]** inline and collected at the end; they are noted, not fully audited.

---

## 1. The import surface (who imports what)

Grep of `numbduck/` + `test/` for numbox imports:

| numbduck site | numbox symbol | numbox source |
|---|---|---|
| `numbduck/utils.py:8` | `load_lib_path` | `numbox/core/bindings/utils.py:167` |
| `numbduck/ducklib.py:2` | `_call_lib_func`, `_call_lib_func_byval` | `numbox/core/bindings/call.py:21`, `:280` |
| `numbduck/ducklib.py:3` | `signatures` (dict) | `numbox/core/bindings/signatures.py:217` |
| `numbduck/ducklib.py:4` | `cres`, `cres_if_available` | `numbox/utils/highlevel.py:26`, `:41` |
| `numbduck/pybridge.py:7` | `get_unicode_data_p` | `numbox/utils/lowlevel.py:280` |
| `test/test_ducklib.py:13` | `get_unicode_data_p` | `numbox/utils/lowlevel.py:280` |
| `test/test_ducklib.py:14` | `structref_meminfo` | `numbox/utils/meminfo.py:33` |
| `test/test_ducklib.py:24` | `_cast_int_to_void_p` | `numbox/utils/lowlevel.py:49` |
| `test/test_ducklib.py:25` | `array_data_p` | `numbox/utils/lowlevel.py:402` |

The four meminfo-bridge functions named in the task — `borrow_structref`,
`_deref_structref_raw_ptr`, `_incref_meminfo`, `release_meminfo` — live in
`numbox/utils/meminfo.py` (`:132`, `:115`, `:78`, `:156`) but numbduck does
**not** import them. `test/test_ducklib.py` re-defines byte-identical local
copies at `test/test_ducklib.py:2818-2879` (it imports only `structref_meminfo`
from numbox). See §6 and **[RISK-7]**.

---

## 2. Control + data flow at a glance

```
import numbduck.ducklib
  └─ ducklib.py:11  duckdb_lib = load_duckdb()          (utils.py:113)
        └─ find_duckdb_shared_lib()  (utils.py:94)       locate libduckdb
        └─ load_lib_path(path)       (numbox utils.py:167) CDLL(path, RTLD_GLOBAL)
        └─ _has_capi_symbols(lib)    (utils.py:29)        hasattr(lib,"duckdb_open")
  └─ ducklib.py:77-272   signatures["duckdb_*"] = sig    mutate numbox global dict
  └─ ducklib.py:275+     @cres(signatures.get("name"))   EAGER njit-compile each wrapper
        └─ body: _call_lib_func("name", (args,))         numbox intrinsic, ABI lowering
```

Runtime (JIT) call path, e.g. from `pybridge.extract_connection_ptr`:

```
get_unicode_data_p("SELECT 1")            -> intp pointer into the numba str payload
ducklib.duckdb_query(conn_ptr, query_p, result.ctypes.data)
   -> CompileResultWAP wrapper -> _call_lib_func codegen -> extern call to duckdb_query
```

Three distinct boundaries are crossed:
- **Python → JIT**: `cres` turns each Python wrapper into a JIT-callable
  `CompileResultWAP` (FunctionType proxy), invokable from both Python and `@njit`.
- **JIT → C**: `_call_lib_func` / `_call_lib_func_byval` emit an extern LLVM
  declaration resolved by the JIT linker to the `RTLD_GLOBAL`-loaded `libduckdb`.
- **C ↔ NRT**: the UDAF path (meminfo bridge) carries numba structref state
  across DuckDB aggregate callbacks as raw `intp` MemInfo pointers (§6).

---

## 3. Library loading — `load_lib_path` (numbox utils.py:167)

Contract: `CDLL(path, mode=RTLD_GLOBAL)` on Linux/Darwin, `CDLL(path, winmode=0)`
on Windows; **uncached** (`numbox/core/bindings/utils.py:178-183`). numbduck's
`load_duckdb` (`numbduck/utils.py:113`) calls it and stashes the handle in the
module global `duckdb_lib` (`ducklib.py:11`).

Why this is load-bearing:
- **`RTLD_GLOBAL` is mandatory.** `_call_lib_func` validates a symbol at typing
  time with `ll.address_of_symbol(func_name)` (`call.py:72`) and the JIT linker
  resolves the extern at link time. Both only see `duckdb_*` if the library was
  dlopened into the *global* namespace. A local-scope load would make every
  `@cres` decoration raise `TypingError` at import (`call.py:73-74`).
- **Handle retention is mandatory.** `ctypes.CDLL.__del__` calls
  `dlclose`/`FreeLibrary` (documented at numbox `utils.py:131-138`). Because
  `load_lib_path` is uncached, the only thing keeping libduckdb mapped is the
  `duckdb_lib` module global. If it were GC'd, JIT-resolved externs would dangle.
  Currently safe (module-global lifetime).

`numbduck/utils.py` adds a macOS fallback layer on top: the duckdb ≥1.4.1 wheel
strips C-API symbols on macOS, so `load_duckdb` re-loads a standalone
`libduckdb.dylib` (env `NUMBDUCK_LIBDUCKDB`, brew paths, or a versioned download
cache) and re-checks `_has_capi_symbols` (`utils.py:113-130`). On non-Darwin a
missing symbol raises immediately (`utils.py:124-128`).

**[RISK-1]** `find_duckdb_shared_lib` (`utils.py:94`) picks the shared object by
regex over the package dir, requiring **exactly one** match (`utils.py:99`,
`:105`); 0 or >1 → `RuntimeError`. Layout-dependent across duckdb 1.3/1.4+.

---

## 4. The signatures dict — `signatures` (numbox signatures.py:217)

`signatures` is a single process-global `dict` assembled from
`signatures_c | signatures_m | signatures_sqlite` (`signatures.py:217-221`).
numbduck **mutates this shared object in place**, inserting ~230 `duckdb_*`
keys at module import (`ducklib.py:77-272`).

Contract: maps a C function name → a numba `Signature` whose `return_type`/`args`
encode the exact ABI types. `_call_lib_func` re-reads it **by name at codegen
time** (`call.py:75`), independently of the `Signature` object that `@cres`
received via `signatures.get("name")` (`ducklib.py:275`). So each binding is
looked up twice and the two lookups must agree.

**[RISK-2]** Shared mutable global namespace. numbduck and numbox write into the
same dict; a name collision (or a third importer overwriting a `duckdb_*` key
after import) would silently change the ABI used by codegen while the
`cres`-fixed wrapper signature stays stale. No prefix isolation, no collision
check. Today the `duckdb_` prefix avoids clashes with libc/math/sqlite keys.

Type encoding conventions visible in `ducklib.py`:
- All pointers are `intp` (`_p`/`_pp` is naming only; the numba type is `intp`).
- Handle-returning creators are bound with their destroyers (e.g.
  `duckdb_create_logical_type` / `duckdb_destroy_logical_type`).
- Struct-by-value duckdb types are modelled as numba tuples and sized by the ABI
  classifier: `duckdb_hugeint_ty`/`uhugeint_ty`/`interval_ty` are 16 B (`ducklib.py:68-70`);
  `duckdb_decimal_ty` (`:71`) and `duckdb_varint_ty` (`:74`) are 24 B → large class.

---

## 5. The call intrinsics

### 5.1 `_call_lib_func` (call.py:21)

A `@intrinsic(prefer_literal=True)`. Steps:
1. `extract_literal_str` requires the func name be a compile-time literal
   (`call.py:71`, enforced in numbox `utils.py:13`).
2. `ll.address_of_symbol` null-check (`call.py:72-74`) — typing-time presence gate.
3. Signature lookup by name (`call.py:75-77`).
4. Classify return + each arg into scalar / struct ≤16 B / struct >16 B via
   `_classify` (`call.py:80,97`) and lower per host ABI in `codegen`
   (`call.py:118-197`):
   - **scalar** → direct (`call.py:146-149`).
   - **≤16 B struct arg** → by value on SysV/AAPCS64, by value on Win64 only for
     sizes {1,2,4,8} else alloca+pointer (`call.py:150-169`).
   - **>16 B struct arg** → alloca+store+pointer; on SysV gets `byval` plus the
     enclosing function gets `optnone`+`noinline` so LLVM can't elide the
     caller-side copy (`call.py:170-171,183-189`; rationale: llvmlite#300).
   - **≤16 B struct return** → direct, or `sret` on Win64 for non-{1,2,4,8}
     (`call.py:103-111,137-140,173-193`).
   - **>16 B struct return** → `sret` everywhere; **`Record` returns >16 B are
     rejected** with `TypingError` (`call.py:81-90`) because numba's RecordModel
     is a raw pointer and the sret stack slot would dangle.
   - **eightbyte repack**: a 16 B pure-INTEGER/INTEGER aggregate whose LLVM type
     isn't already `{i64,i64}` (the `{i32,i32,i64}` `duckdb_interval` case) is
     repacked via memory bitcast on both arg and return sides
     (`_needs_int_int_eightbyte_repack` `call.py:212`, `_repack_to_i64_pair`
     `:236`, `_repack_from_i64_pair` `:260`), working around llvmlite dropping a
     field during register coercion.

Note the symbol is **declared by name** via `get_or_insert_function`
(`call.py:181`) and resolved by the JIT linker; the `address_of_symbol` int from
step 2 is used only for the existence check, not for the actual call.

### 5.2 `_call_lib_func_byval` (call.py:280)

For C signatures of shape `func(T*)` where the caller holds `T` as a value. It
allocas, stores, and passes the slot address regardless of platform
(`_emit_byval_call`, `call.py:203-209`). numbduck uses it for the three
functions that take/inspect a `duckdb_result` by pointer:
`duckdb_fetch_chunk` (`ducklib.py:945`), `duckdb_result_return_type`
(`ducklib.py:1131`), `duckdb_result_statement_type` (`ducklib.py:1137`), where
the result is the 6-`intp` tuple `duckdb_result_ty` (`ducklib.py:67`). The numba
type system cannot tell `T` from `T*`; **the caller must pick the right
intrinsic by reading the C header** — the only thing preventing a silent ABI
mismatch.

**[RISK-3]** Docs drift: `numbduck/CLAUDE.md` documents struct-by-value helpers
`_call_lib_func_struct_in/out`, `_build_packed_interval`, and three hand-rolled
intrinsics `_duckdb_bind_hugeint/_uhugeint/_interval` "at ducklib.py:1525-1640
(~116 lines)". Those lines **do not exist** — `ducklib.py` is 1432 lines and
`duckdb_bind_hugeint/uhugeint/interval/decimal` (`ducklib.py:1410-1431`) already
route through plain `_call_lib_func`. The migration that CLAUDE.md lists as an
open "Follow-up" appears already landed (the eightbyte repack now lives in
numbox `call.py`). The downstream review should treat the CLAUDE.md
"Struct-by-value helpers" / "Follow-ups" sections as stale.

### 5.3 `cres` / `cres_if_available` (highlevel.py:26 / :41)

`cres(sig)` does `njit(sig)(func)` **eagerly** (compiles at decoration/import
time), asserts a single nopython signature, and wraps the compile result in
`CompileResultWAP` (`highlevel.py:31-37`) — a FunctionType proxy rather than a
`CPUDispatcher`, so the binding is a first-class function value usable as a
function pointer inside `@njit`. Consequence: **importing `ducklib` compiles all
~230 wrappers up front**; any unresolved symbol fails the import, not the call.

`cres_if_available(lib, sig)` guards version-specific symbols: it compiles only
if `hasattr(lib, func.__name__)`, otherwise installs a stub raising
`NotImplementedError` (`highlevel.py:50-57`). numbduck uses it for symbols absent
in older duckdb: `duckdb_create_varint` (`ducklib.py:725`), `duckdb_get_varint`
(`:1050`), `duckdb_scalar_function_set_init` (`:1284`).

---

## 6. String + pointer bridges, and the UDAF meminfo bridge

### 6.1 `get_unicode_data_p` (lowlevel.py:280)

Intrinsic `_get_unicode_data_p` extracts field 0 (the data pointer) of numba's
unicode struct and returns it as `intp` (`lowlevel.py:267-276`). Used in
`pybridge.extract_connection_ptr` to hand `"SELECT 1"` to `duckdb_query`
(`pybridge.py:66-67`).

**[RISK-4]** The returned `intp` aliases the live numba string payload; it is
valid only while that string object is alive and relies on numba strings being
NUL-terminated. It is the raw UTF-8 bytes, not a decoded/copied buffer. Fine for
the immediate-use pattern in `pybridge`, but a latent footgun if a caller stores
the pointer.

### 6.2 `_cast_int_to_void_p` (lowlevel.py:49)

`builder.inttoptr(arg, voidptr_t)` — bridges numbduck's all-`intp` pointer
convention to the `voidptr` that `carray()` requires inside `@njit`. Used in the
UDF callback path (per `numbduck/CLAUDE.md` "Key patterns" #3) and in tests
(`test_ducklib.py:24`).

### 6.3 The meminfo bridge (UDAF state across the C boundary)

numbox `meminfo.py` provides the lifecycle primitives that let a numba structref
serve as DuckDB aggregate state, carried through C callbacks as a raw `intp`
MemInfo pointer:
- `export_meminfo(s)` (`meminfo.py:143`): `structref_meminfo` → `_incref_meminfo`;
  returns `intp` with +1 ref to keep the allocation alive across the boundary.
- `borrow_structref(struct_type, p)` (`meminfo.py:131`): `_incref_meminfo(p)` then
  `_deref_structref_raw_ptr` — reconstructs a live, NRT-participating structref
  from the raw pointer; net-zero for the external owner on scope exit.
- `release_meminfo(p)` (`meminfo.py:155`): `-1` decref, triggers the dtor at 0.

**Load-bearing subtlety:** `_release_meminfo` (`meminfo.py:90-111`) calls
`NRT_MemInfo_release` **directly** instead of `context.nrt.decref`, specifically
so numba's `removerefctpass` won't strip the decref as dead code — its
`void(intp)` signature contains no NRT-tracked types, and `NRT_MemInfo_release`
is not in the pass's accepted-nrtfns allowlist, which keeps `_legalize()` from
enabling the rewrite at all. Symmetrically `_incref_meminfo` (`:77`) inlines an
`nrt.incref` for the same reason. `_require_intp` (`:67`) rejects non-`intp`
pointer args to avoid a truncating `inttoptr` on 64-bit hosts.

**[RISK-5]** This whole bridge depends on undocumented internals of numba's
`removerefctpass` allowlist and the `MemInfo` C struct layout
(`get_nrt_refcount`, `meminfo.py:42-61`). A numba upgrade that changes either
could silently double-free or leak. Noted, not audited.

**[RISK-7]** `test/test_ducklib.py:2818-2879` re-implements `_incref_meminfo`,
`export_meminfo`, `_deref_structref_raw_ptr`, `borrow_structref`,
`_release_meminfo`, `release_meminfo` **locally** rather than importing them from
`numbox.utils.meminfo` (it imports only `structref_meminfo`). Two copies of a
refcount-correctness-critical primitive can drift apart; the numbox canonical
versions are the contract, the test's fork is what actually runs in the test.

---

## 7. `pybridge.extract_connection_ptr` — the most fragile boundary

`pybridge.py:10-74` reaches into the duckdb Python package's C++ internals with
ctypes:
1. `id(conn) + 16` → `DuckDBPyConnection*` (pybind11 instance header offset).
2. `DuckDBPyConnection* + 32` → `Connection*` (the C-API `duckdb_connection`).
3. Validate by running `SELECT 1` through `duckdb_query` and checking the state
   code (`pybridge.py:64-72`).

**[RISK-6]** Both offsets (16, 32) are hardcoded and only validated against
duckdb 1.3.2 / Linux x86-64 / libstdc++ (`pybridge.py:31-34`). They depend on
pybind11's instance layout and the `DuckDBPyConnection` C++ struct layout — both
private implementation details that can shift on any duckdb major bump or a
different C++ stdlib. The `SELECT 1` validation catches a *wrong* pointer that
still maps to readable memory, but a layout shift that yields a plausible-looking
garbage `Connection*` could pass validation by luck. The buffer is destroyed
before the rc check (`pybridge.py:68-69`), which is correct — duckdb populates
the result struct even on error.

---

## 8. Invariants the boundary relies on

- **Import order**: `duckdb_lib = load_duckdb()` (`ducklib.py:11`) must precede
  every `@cres` decoration; `cres` compiles eagerly and `_call_lib_func` checks
  symbol presence at typing time. Currently guaranteed by file order.
- **Global symbol visibility**: libduckdb dlopened with `RTLD_GLOBAL` and the
  handle retained for process lifetime (§3).
- **Signature agreement**: the `Signature` `@cres` receives and the one
  `_call_lib_func` re-reads from the shared dict must match (§4).
- **Pointer width**: everything is `intp`; the `_require_intp` guards and the
  all-`intp` convention assume a 64-bit host throughout.
- **ABI correctness is human-asserted**: tuple shapes/sizes in `ducklib.py` and
  the `_call_lib_func` vs `_call_lib_func_byval` choice encode the C ABI by hand;
  there is no compile-time check that they match `duckdb.h`.

---

## 9. Fragile assumptions (collected)

1. **[RISK-1]** `find_duckdb_shared_lib` requires exactly one regex match; brittle
   across duckdb packaging layouts (`utils.py:94-110`).
2. **[RISK-2]** `signatures` is a shared mutable global; no collision isolation
   between numbox and numbduck keys, and codegen re-reads it by name (`call.py:75`).
3. **[RISK-3]** `numbduck/CLAUDE.md` struct-by-value helper + "Follow-ups"
   sections are stale; those `ducklib.py:1525-1640` intrinsics no longer exist.
4. **[RISK-4]** `get_unicode_data_p` returns a pointer aliasing a live numba
   string; valid only while the string lives and relies on NUL-termination.
5. **[RISK-5]** The meminfo bridge depends on numba `removerefctpass` internals
   and the `MemInfo` struct layout; a numba upgrade could silently break refcounts.
6. **[RISK-6]** `extract_connection_ptr` hardcodes pybind11/`DuckDBPyConnection`
   offsets (16, 32), validated only on duckdb 1.3.2 / Linux x86-64 / libstdc++.
7. **[RISK-7]** `test/test_ducklib.py` forks the meminfo-bridge primitives locally
   instead of importing numbox's canonical versions; drift risk.
