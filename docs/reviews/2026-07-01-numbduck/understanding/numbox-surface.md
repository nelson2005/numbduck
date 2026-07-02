# numbduck ↔ numbox: the dependency surface (boundary contract)

Understanding note for the downstream defect review. Describes **exactly**
what numbduck imports from numbox, what each import promises, and how the
control/data flow crosses the Python / JIT-lowering / native-C boundaries.
Cites are `file:line`. numbduck at HEAD (`review/numbduck-2026-06-29`),
numbox at `/home/erik/projects/numbox`.

Pin: `numbox>=0.5.13` (`pyproject.toml:13`) — open-ended upper bound.
(`CLAUDE.md` still says `numbox~=0.5.6` and describes `@cres`; both are
**stale** — the current tree is 100% `@proxy`.)

---

## 1. The complete import surface

Every symbol numbduck pulls from numbox, grouped by the numbox module that
actually defines it:

### `numbox.core.proxy.proxy`
- `proxy`, `proxy_if_available` — imported at `numbduck/ducklib.py:6`.

### `numbox.core.bindings.call`
- `_call_lib_func`, `_call_lib_func_byval` — imported at `ducklib.py:4`.

### `numbox.core.bindings.signatures`
- `signatures` (the shared mutable dict) — imported at `ducklib.py:5`.

### `numbox.core.bindings.utils`
- `load_lib_path` — imported at `numbduck/utils.py:8`.

### `numbox.utils.lowlevel`
- `get_unicode_data_p` — `pybridge.py:7`, `test/test_ducklib.py:10`.
- `_cast_int_to_void_p` — `test/test_ducklib.py:24` (test-only).
- `array_data_p` — `test/test_ducklib.py:25` (test-only).

### `numbox.utils.meminfo`  (NOT `lowlevel` — the task brief mislabels these)
- `borrow_structref`, `export_meminfo`, `_incref_meminfo`,
  `release_meminfo`, `structref_meminfo` — `test/test_ducklib.py:11-14`
  (test-only; this is the UDAF state-bridge surface).
- `_deref_structref_raw_ptr` is defined at `numbox/utils/meminfo.py:115`
  and is the intrinsic `borrow_structref` composes; numbduck reaches it
  only transitively via `borrow_structref`.

**Boundary-relevant fact:** the *runtime* numbduck package (non-test) depends
on only five numbox names: `proxy`, `proxy_if_available`, `_call_lib_func`,
`_call_lib_func_byval`, `signatures`, `load_lib_path`, `get_unicode_data_p`.
The meminfo/structref surface is exercised only by the UDAF reference tests.

Note that four of these (`_call_lib_func`, `_call_lib_func_byval`,
`_cast_int_to_void_p`, `_incref_meminfo`, `_deref_structref_raw_ptr`,
`_release_meminfo`) are **underscore-private** in numbox. numbduck couples
to numbox internals, so any numbox refactor of these can break numbduck
silently within the `>=0.5.13` pin.

---

## 2. `@proxy` — what it does, and why not plain `@cres`

Source: `numbox/core/proxy/proxy.py:40-133`.

For each `@proxy(sig, jit_options=jit_options)`-decorated stub in ducklib,
at **import time**:

1. **Eager compile of the body** (`proxy.py:75-76`): `func_jit =
   njit(sig, **jit_options)(func)`, then `cres =
   func_jit.get_compile_result(main_sig)`. Because `jit_options` defaults to
   `{"cache": True}` (`configurations.py:11`), the body's machine code is
   written to numba's on-disk cache.

2. **Process-stable symbol alias** (`proxy.py:79-80`, helper `20-37`): numba
   mangles the body's cfunc-wrapper LLVM name with a *process-local*
   `v<N>` abi-tag (`FunctionIdentity.unique_id`, an `itertools.count`) that
   is **not** part of the numba cache key. Two processes therefore give the
   same body different wrapper names. `_stable_cfunc_alias` computes a
   deterministic name `numbox_pxy_<funcname>_<sha256(mod.qualname.sig)[:16]>`
   and registers it with `ll.add_symbol(alias, <ptr to cfunc wrapper>)`.

3. **Generated inline wrapper** (`proxy.py:83-98`, `exec`'d at `128-129`):
   builds source text for a fresh `@intrinsic` + `@njit(sig, inline='always')`
   dispatcher. The intrinsic's codegen emits an *extern declaration* of the
   alias via `cgutils.get_or_insert_function(builder.module, func_ty_ll,
   cfunc_alias)` and `builder.call`s it. The decorated name is rebound to
   this dispatcher; `dispatcher.as_func = CompileResultWAP(cres)`
   (`proxy.py:131`).

4. **Cache-anchor line math** (`proxy.py:108-127`): the generated `@njit`
   source is prepended with blank lines so its decorator lands exactly at
   `func.__code__.co_firstlineno`, working around CPython #122981 and numba's
   `co_consts` cache-key collision. If the wrapped `def` were above the
   anchor minimum (line ~5 of the template), it raises `ValueError`. All
   ducklib stubs sit far down the file (first is `ducklib.py:277`), so this
   never trips here.

**`@proxy` vs plain `@cres`:** `cres` (numbox `utils/highlevel.py`) merely
compiles to a `CompileResultWAP` with an explicit signature — a callable
handle. `@proxy` additionally (a) emits an `inline='always'` intrinsic
wrapper so a caller statically links only the *declaration* of the callee,
and (b) registers the **deterministic alias** so that a `cache=True` caller
which inlines the proxy references a stable symbol name instead of the
process-local `v<N>`. Without (b), two independently-built caches can pair a
body defining `v<Na>` with a caller referencing `v<Nb>` and abort on load
with `LLVM ERROR: Symbol not found: cfunc...` (`proxy.py:26-33`). Since every
ducklib binding is `cache=True`, this stability guarantee is **load-bearing**
for numbduck.

### `proxy_if_available` (`proxy.py:136-164`)

`if hasattr(lib, func.__name__):` → full `proxy`; else returns a **stub**
(`proxy.py:158-163`) named `__<funcname>` that raises
`NotImplementedError(f"{name} is not available")` and has **no** `.as_func`.
`lib` here is `duckdb_lib` (`ducklib.py:12`), the loaded CDLL handle. Used for
version-gated symbols:
- `duckdb_create_varint` (`ducklib.py:726`)
- `duckdb_get_varint` (`ducklib.py:1051`)
- `duckdb_scalar_function_set_init` (`ducklib.py:1285`)

So symbol presence is decided **at import** by `hasattr` on the ctypes
handle, not at call time.

---

## 3. `_call_lib_func` — the ABI trampoline

Source: `numbox/core/bindings/call.py:21-200`.
`@intrinsic(prefer_literal=True)`; called as
`_call_lib_func("duckdb_x", (a, b))` from every non-byval ducklib body.

Control flow at **lowering time**:
1. `func_name = extract_literal_str(...)` — the name **must** be a compile-time
   literal (`call.py:71`).
2. `ll.address_of_symbol(func_name)` (`call.py:72-74`) — a *presence check
   only*; if `None` → `TypingError("... is unavailable in the LLVM
   context")`. The runtime address is **never** baked into IR (that would
   break `cache=True` under ASLR).
3. `func_sig = signatures.get(func_name)` (`call.py:75-77`) — **reads the
   same `signatures` dict numbduck mutated at `ducklib.py:78-273`**. If absent
   → `TypingError("Undefined signature ...")`. Note this is an *independent*
   lookup from the `sig` passed to `@proxy`; the two must agree (they do,
   because ducklib sets both from the same expression).
4. Classifies the return type and each arg as scalar / struct ≤16B /
   struct >16B (`_classify`), then emits an **extern declaration** via
   `get_or_insert_function(builder.module, func_ll_ty, func_name)`
   (`call.py:181`) and lets llvmlite's JIT linker resolve the name at link
   time. This extern-ref (not literal-address) pattern is precisely what
   makes cached objects portable across processes/machines.

ABI handling that numbduck relies on (`call.py:24-70`, `102-197`):
- **Scalars** direct.
- **≤16B structs** by-value on SysV-x86-64 / AAPCS64; on Win-x64 sizes
  1/2/4/8 by value else by pointer.
- **16B pure-INTEGER non-canonical layouts** (e.g. `duckdb_interval_ty =
  {i32,i32,i64}`, `ducklib.py:71`) are **repacked to `{i64,i64}`** via memory
  bitcast before the call (`_needs_int_int_eightbyte_repack` `call.py:212-233`,
  `_repack_to_i64_pair` `236-257`), working around llvmlite dropping fields.
  Return-side repack mirror at `call.py:260-277`.
- **>16B struct args** by pointer + `byval` + `optnone`/`noinline` on
  SysV-x86-64 (`call.py:170-171,185-189`). numbduck's 24-byte by-value
  structs (`duckdb_decimal_ty` 4-field, `duckdb_varint_ty` 3-field —
  `ducklib.py:72,75`) travel this path.
- **>16B struct returns** via `sret` (`call.py:103-111,137-140,183-193`).
- **`Record` LARGE returns are rejected** (`call.py:81-90`). numbduck uses
  `Tuple`/`UniTuple` return shapes, so it stays clear of this.

This means the **entire eightbyte/interval/decimal/hugeint ABI machinery now
lives in numbox**. The `CLAUDE.md` "Follow-ups" note about ~116 lines of
hand-rolled intrinsics at `ducklib.py:1525-1640` is **stale**: the file ends
at line 1433 and `duckdb_bind_hugeint/uhugeint/interval/decimal`
(`ducklib.py:1411-1432`) are ordinary `@proxy` + `_call_lib_func` wrappers.

### `_call_lib_func_byval` (`call.py:280-301`)

Passes the value arg **by pointer on all platforms** (alloca + store + call
via pointer, `_emit_byval_call` `call.py:203-209`). Used where the C header
declares `func(duckdb_result*)` but the caller holds the result as a value:
- `duckdb_fetch_chunk(result)` (`ducklib.py:943-946`)
- `duckdb_result_return_type(result)` (`ducklib.py:1129-1132`)
- `duckdb_result_statement_type(result)` (`ducklib.py:1135-1138`)

The value is `duckdb_result_ty = UniTuple(intp, 6)` (`ducklib.py:68`) — a
48-byte, 6-pointer aggregate matching the C `duckdb_result` struct. numba's
type system can't tell `T` from `T*`, so ducklib picks the intrinsic manually
per the header shape.

---

## 4. Library loading & symbol visibility

- `numbduck/utils.py:113-131` `load_duckdb()` finds the duckdb shared lib
  (`find_duckdb_shared_lib`, handles 1.3.x package-dir vs 1.4+
  `_duckdb.*` site-packages layouts) and calls
  `load_lib_path(lib_path)` (numbox `utils.py:167-183`).
- `load_lib_path` does `CDLL(path, mode=RTLD_GLOBAL)` on Linux/Darwin,
  `CDLL(path, winmode=0)` on Windows. **`RTLD_GLOBAL` is what makes duckdb's
  C symbols visible to LLVM's JIT linker** (`dlsym(RTLD_DEFAULT, ...)` after
  JIT init). `load_lib_path` is **uncached** — numbduck keeps the handle
  alive itself as the module-global `duckdb_lib` (`ducklib.py:12`). If that
  reference were dropped, `CDLL.__del__` → `dlclose` could invalidate
  already-resolved extern refs.
- `_has_capi_symbols` (`utils.py:29-30`) = `hasattr(lib, "duckdb_open")` —
  guards the macOS-wheel-strips-C-API case; falls back to a standalone
  `libduckdb.dylib` (search paths / `NUMBDUCK_LIBDUCKDB` / cached download).

The same `duckdb_lib` handle is then reused as the `lib` argument to every
`proxy_if_available` call (§2).

---

## 5. String & pointer helpers at the boundary

- `get_unicode_data_p(s) -> intp` (numbox `lowlevel.py:280-285`, njit-callable
  intrinsic wrapper) — returns a pointer to a Python unicode's null-terminated
  data payload. In `pybridge.py:66` it feeds `"SELECT 1"` to
  `ducklib.duckdb_query`; the string is a live temporary for the duration of
  the call, so the borrowed pointer is valid. **Caller must keep the `str`
  alive** while the pointer is in use — a general fragile contract.
- `_cast_int_to_void_p(p) -> voidptr` (numbox `lowlevel.py:48-53`) — bridges
  numbduck's `intp`-carried pointers to the `voidptr` that numba's `carray`
  requires inside `@njit` (test-only; see `CLAUDE.md` UDF-callback note 3).
- `array_data_p(arr) -> intp` (numbox `lowlevel.py:403`) — numpy data pointer,
  used in tests to hand buffer addresses to the C API.

---

## 6. UDAF meminfo/structref bridge (test surface)

Source: `numbox/utils/meminfo.py`. Bridges DuckDB's aggregate lifecycle
(raw `intp` state pointers handed across the C boundary) to numba structref
NRT state:

- `borrow_structref(struct_type, p)` (`meminfo.py:131-140`) =
  `_incref_meminfo(p)` (`78-87`, NRT incref of a MemInfo at `intp`) +
  `_deref_structref_raw_ptr(struct_type, p)` (`115-128`, reconstructs a
  structref value whose `.meminfo` is `inttoptr(p)`). Yields a **live**
  structref that participates in normal NRT refcounting; net-zero for the
  external owner if the borrowing scope exits cleanly.
- `export_meminfo(s)` (`143-152`) = `structref_meminfo` + `_incref_meminfo`;
  returns the MemInfo pointer as `intp` with a +1 pin.
- `release_meminfo(p)` (`155-162`) → `_release_meminfo` (`90-111`), which
  calls **`NRT_MemInfo_release` directly** rather than `context.nrt.decref`.
  This is deliberate: a bare `NRT_decref` in a `void(intp)` signature (no
  NRT-tracked types) would be **stripped by numba's `removerefctpass`** as
  dead code; using a symbol outside the pass's allowlist keeps the decref
  alive. This is a numba-version-sensitive internal dependency.
- `_require_intp` (`meminfo.py:67-74`) rejects non-`intp` pointer args to
  avoid width-truncating `inttoptr` — matching numbduck's convention that
  **all pointers are `intp`** (`CLAUDE.md` UDF note 2).

---

## 7. Boundaries summary

| Phase | What happens | numbox pieces |
|---|---|---|
| **Import** | `signatures` dict mutated (`ducklib.py:78-273`); each stub eager-compiled + alias-registered; CDLL loaded | `signatures`, `proxy`/`proxy_if_available`, `load_lib_path` |
| **JIT lowering** | `_call_lib_func` reads `signatures`, classifies ABI, emits extern decls; proxy intrinsic emits call to stable alias | `_call_lib_func(_byval)`, `proxy` codegen |
| **Native runtime** | Actual C calls into libduckdb; symbols resolved via `RTLD_GLOBAL`/`add_symbol` | `RTLD_GLOBAL` load, alias `add_symbol` |

---

## 8. Fragile / risky assumptions (flagged, not audited)

1. **Shared mutable `signatures` dict, two independent lookups.** ducklib
   writes `duckdb_*` keys into numbox's *global* dict at import
   (`ducklib.py:78-273`); `_call_lib_func` re-reads it at lowering
   (`call.py:75`). Correctness depends on (a) no key collision with numbox's
   own `signatures_c/m/sqlite`, and (b) the `@proxy` sig and the dict entry
   agreeing. A typo'd key → `signatures.get(...)` returns `None`; `@proxy`'s
   `main_sig` truthiness logic (`proxy.py:68`) then yields `False` and
   `njit(None)` — a confusing downstream failure, not a clean error.

2. **`proxy_if_available` decides availability at import via `hasattr` on the
   CDLL.** If `load_duckdb` returned a handle missing C API symbols, *every*
   gated symbol silently becomes a `NotImplementedError` stub **and** the
   non-gated `@proxy` bodies would fail at first lowering with "unavailable
   in the LLVM context" (`call.py:74`). The `_has_capi_symbols` guard
   (`utils.py:116-123`) is the only thing preventing a half-bound module.

3. **`cache=True` cross-process symbol stability rests entirely on
   `_stable_cfunc_alias`.** If numbox changes the alias scheme (or numba
   changes cfunc-wrapper naming), stale on-disk caches abort with
   `Symbol not found: cfunc...`. numbduck's default `jit_options`
   (`{"cache": True}`) makes every binding subject to this.

4. **`_call_lib_func_byval` assumes `UniTuple(intp,6)` == C `duckdb_result`
   (48 B, 6 words).** If duckdb changes the `duckdb_result` struct layout
   across the `>=1.3.2,<1.6` range, the three byval result functions
   (`ducklib.py:943,1129,1135`) pass a mis-shaped pointer with no diagnostic.

5. **Interval/decimal/varint ABI correctness is delegated wholesale to
   numbox's eightbyte-repack logic** (`call.py:212-277`). The repack only
   fires for 16-byte **pure-INTEGER** non-canonical layouts on SysV/AAPCS64;
   any duckdb struct that is 16 B but classifies differently, or a platform
   numbox doesn't special-case, would silently mis-pass. numbduck has no
   independent guard.

6. **RTLD_GLOBAL first-match resolution + possible dual libduckdb.** On the
   macOS fallback path (`utils.py:119-130`) a standalone `libduckdb.dylib`
   can be loaded alongside the wheel's `_duckdb.*`; `dlsym(RTLD_DEFAULT)`
   returns the first match in load order. Less acute than sqlite (duckdb is
   not in the dyld shared cache), but the numbox macOS "detect-and-refuse"
   guard that protects sqlite has **no numbduck analogue** for duckdb.

7. **meminfo/structref bridge depends on numba internals** — MemInfo layout
   and `removerefctpass` behavior (`meminfo.py:90-111`). Pinned implicitly to
   numbox's tested numba range (`numba>=0.60.0,<0.66.0` per numbox), but
   numbduck's own pin does not constrain numba directly.

8. **Open-ended numbox pin (`>=0.5.13`) over a private API.** numbduck
   consumes underscore-private numbox intrinsics (`_call_lib_func`,
   `_call_lib_func_byval`, `_incref_meminfo`, `_deref_structref_raw_ptr`,
   `_cast_int_to_void_p`). Any minor numbox release refactoring these can
   break numbduck within the allowed range.
