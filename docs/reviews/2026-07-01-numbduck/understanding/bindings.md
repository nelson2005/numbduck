# numbduck binding layer: `signatures` dict, `@proxy`, `jit_options`, `_call_lib_func`

Scope: how a Python-level DuckDB C-API wrapper in
`numbduck/ducklib.py` becomes a numba `@njit`-callable that jumps directly
into the DuckDB shared library, ABI-correctly, at native speed. Everything
here is derived from the current `@proxy` tree, not from prior reviews or
the (now-stale) `CLAUDE.md` "Struct-by-value helpers / Follow-ups" text —
see "Doc drift" at the end.

Files read in full:
- `/home/erik/projects/numbduck/numbduck/ducklib.py` (1433 lines)
- `/home/erik/projects/numbduck/numbduck/configurations.py`
- `/home/erik/projects/numbox/numbox/core/bindings/call.py`
- `/home/erik/projects/numbox/numbox/core/proxy/proxy.py`
- `/home/erik/projects/numbox/numbox/core/bindings/abi.py`

---

## 1. The four moving parts and how they connect

Each binding is one Python function decorated with `@proxy(...)` whose body
calls `_call_lib_func("name", (args...))`. Two independent registries are
keyed by the **exact C function name string**:

1. The **`signatures` dict** — a single process-global dict *imported from
   numbox* (`from numbox.core.bindings.signatures import signatures`,
   `ducklib.py:5`). numbduck mutates it at import time (`ducklib.py:78-274`)
   by assigning numba function signatures, e.g.
   `signatures["duckdb_open"] = duckdb_state_ty(intp, intp)`
   (`ducklib.py:218`).
2. The DuckDB shared library `duckdb_lib`, loaded once via
   `load_duckdb()` (`ducklib.py:12`; `RTLD_GLOBAL` `ctypes.CDLL` per the
   utils loader), which makes the symbols visible to LLVM's JIT linker.

The name string is the join point twice over:
- `@proxy(signatures.get("duckdb_open"), ...)` looks the signature up **at
  decoration time** to hand numba the eager-compile signature.
- `_call_lib_func("duckdb_open", ...)` looks the **same** name up **again at
  lowering time** (`call.py:75`) to recover the return/arg types for ABI
  classification.

Both lookups must agree; the name literal is passed twice (once in the
decorator, once inside the body), so a mismatch is a latent hazard (see
Fragile Assumptions).

---

## 2. Enum / type aliases (`ducklib.py:17-75`)

Plain Python constants and numba type aliases, no classes:

- `duckdb_state_ty = int32` (`:17`); `DuckDBSuccess = 0`, `DuckDBError = 1`
  (`:19-20`). These are the C-style return codes the DuckDB error protocol
  uses — bindings return them; they never raise (`CLAUDE.md` "Error
  Handling").
- `DUCKDB_TYPE_*` enum ints (`:23-66`) mirroring `duckdb.h`'s `duckdb_type`.
  Note `DUCKDB_TYPE_VARINT = 35` and `DUCKDB_TYPE_BIGNUM = 35` are
  deliberately the **same** value (`:61-62`): DuckDB renamed VARINT→BIGNUM
  at 1.5 with identical enum value and struct layout; both names are kept
  for cross-version portability.
- **Struct-shaped numba type aliases** (`:68-75`), the load-bearing part
  for ABI lowering. Each is a numba `Tuple`/`UniTuple` chosen so numba's
  natural-alignment tuple lowering reproduces the exact C struct byte
  layout:

  | alias | numba type | C struct | size | ABI class |
  |---|---|---|---|---|
  | `duckdb_result_ty` | `UniTuple(intp, 6)` | `duckdb_result` | 48 B | LARGE |
  | `duckdb_hugeint_ty` | `Tuple((uint64, int64))` | `{lower, upper}` | 16 B | SMALL, canonical `{i64,i64}` |
  | `duckdb_uhugeint_ty` | `UniTuple(uint64, 2)` | `{lower, upper}` | 16 B | SMALL, canonical `{i64,i64}` |
  | `duckdb_interval_ty` | `Tuple((int32, int32, int64))` | `{months, days, micros}` | 16 B | SMALL, **non-canonical INT/INT** |
  | `duckdb_decimal_ty` | `Tuple((uint8, uint8, uint64, int64))` | `{width, scale, hugeint{lo,hi}}` | 24 B | LARGE |
  | `duckdb_blob_ty` | `Tuple((intp, uint64))` | `{data, size}` | 16 B | SMALL, canonical |
  | `duckdb_bit_ty` | `Tuple((intp, uint64))` | `{data, size}` | 16 B | SMALL, canonical |
  | `duckdb_varint_ty` | `Tuple((intp, uint64, int8))` | `{data, size, is_negative}` | 24 B | LARGE |

  `decimal` inlines the nested `duckdb_hugeint` as its trailing
  `(uint64, int64)` fields; with `uint8, uint8` at offsets 0/1 and the
  hugeint aligned to offset 8, total 24 B — matches the C layout. `varint`:
  `intp@0, uint64@8, int8@16`, aligned to 24 B. These hand-computed layouts
  are validated by `abi._basetuple_layout` / `_struct_bytes` at lowering
  time, **not** at authoring time — a wrong alias silently corrupts.

All pointers are represented as **signed** `intp` (64-bit), never a numba
pointer type. This is why `intp` fields classify as INTEGER eightbytes.

---

## 3. `jit_options` threading (`configurations.py`, `ducklib.py:8`)

`configurations.get_jit_options()` reads env var `NUMBDUCK_JIT_OPTIONS` as
JSON, defaulting to `{"cache": True}` when unset
(`configurations.py:9-16`); invalid JSON raises `ValueError`. The result is
bound module-global `jit_options` (`configurations.py:19`) and imported into
`ducklib.py:8`. Every wrapper passes it verbatim:
`@proxy(sig, jit_options=jit_options)`.

Inside `proxy()` (`proxy.py:69-71`) it is copied and the wrapper (but not
the eager body compile) also gets `inline='always'`:
`jit_opts.update(jit_opts, inline='always')`. So:
- the **body** njit is `njit(sig, **jit_options)` (`proxy.py:75`) →
  gets `cache=True` (default) but *not* forced inline;
- the **proxy wrapper** njit is `njit(sig, **jit_opts)` (`proxy.py:95`) →
  `cache=True` **and** `inline='always'`.

`cache=True` is why the `_stable_cfunc_alias` machinery exists (see §5): a
cached caller inlines the proxy wrapper and must reference a
process-stable symbol name, not numba's per-process `v<uid>` mangling.

---

## 4. `@proxy` / `@proxy_if_available` wrapping (`proxy.py`)

`proxy(sig, jit_options)` (`proxy.py:40-133`):
1. `main_sig` = the (single) `Signature` (`proxy.py:68`).
2. **Eagerly** JIT-compiles the body: `func_jit = njit(sig, **jit_options)(func)`
   then `cres = func_jit.get_compile_result(main_sig)` (`proxy.py:75-76`).
   Because the signature is explicit, this compiles **at import time** —
   which means `_call_lib_func`'s symbol-presence check and full ABI
   lowering also run at import time (see §6 and Fragile Assumptions).
3. Registers a deterministic process-stable LLVM alias for the body's
   cfunc wrapper: `ll.add_symbol(cfunc_alias, cres.library.get_pointer_to_function(...))`
   (`proxy.py:79-80`). `cfunc_alias` is a sha256-derived name
   (`_stable_cfunc_alias`, `proxy.py:20-37`).
4. Builds, via `exec` of generated source (`proxy.py:83-129`), a private
   `@intrinsic` that emits a direct `builder.call` to `cfunc_alias`, wrapped
   in an `@njit(sig, **jit_opts)` dispatcher named `__<func>`
   (`make_proxy_name`, `proxy.py:16-17`). The generated `@njit` line is
   line-anchored to `func.__code__.co_firstlineno` by prepending blank lines
   (`proxy.py:108-127`) — a cache-key anchoring workaround for CPython
   #122981 / numba `co_consts` collisions.
5. Returns the dispatcher, with `.as_func = CompileResultWAP(cres)` attached
   (`proxy.py:131`) for passing the binding as a function-typed value.

So the object bound to e.g. `duckdb_open` in `ducklib` is a numba dispatcher
that, when called from `@njit` code, inlines a single `call` into the eager
body's cfunc, which itself was lowered by `_call_lib_func` to a direct call
into DuckDB.

`proxy_if_available(lib, sig, jit_options)` (`proxy.py:136-164`): same as
`proxy` **iff** `hasattr(lib, func.__name__)` — i.e. the C symbol exists in
`duckdb_lib`. Otherwise it returns a `stub` that raises
`NotImplementedError` at call and, critically, **skips the eager compile
entirely** (`proxy.py:154-163`). This is the version-gating mechanism.
In `ducklib.py` only three symbols use it:
- `duckdb_create_varint` (`:726`)
- `duckdb_get_varint` (`:1051`)
- `duckdb_scalar_function_set_init` (`:1285`)

The stub has **no `.as_func`** (`proxy.py:145-152`); callers passing it as a
function value must `hasattr`-guard.

---

## 5. `_stable_cfunc_alias` (why it matters here)

`proxy.py:20-37`: numba mangles the cfunc wrapper name with a process-local
`v<N>` uid that is **not** part of the cache key. Two processes name the
same function differently, so a `cache=True` caller that inlined a proxy
could pair a cached body defining `v<Na>` with a cached caller referencing
`v<Nb>` → `LLVM ERROR: Symbol not found`. The sha256 alias, re-registered
per-process via `add_symbol`, keeps cached references valid. Since numbduck
runs with `cache=True` by default, this is directly load-bearing.

---

## 6. `_call_lib_func` / `_call_lib_func_byval` — ABI lowering (`call.py`)

`_call_lib_func(typingctx, func_name_ty, args_ty)` is an
`@intrinsic(prefer_literal=True)` (`call.py:21-200`):

- Extracts the literal name (`call.py:71`), asserts the symbol is present
  via `ll.address_of_symbol(func_name)` — a **presence check only**, the
  int address is never baked into IR (`call.py:72-74`); codegen instead
  emits an extern decl via `get_or_insert_function` (`call.py:181`) so the
  JIT linker resolves it (survives `cache=True` / ASLR).
- Looks the signature up in the shared dict (`call.py:75-77`), pulls
  `ret_ty` and classifies it and each arg via `abi._classify` into
  SCALAR / STRUCT_SMALL (≤16 B) / STRUCT_LARGE (>16 B) (`call.py:79-100`).
- Picks the host convention via `abi._current_platform()` — SysV x86-64,
  AAPCS64 (arm64), or Windows x64 (`call.py:102`, `abi.py:14-41`).

Lowering rules (from the docstring `call.py:22-70` and codegen
`call.py:118-197`):

- **Scalar args/returns** — passed/returned directly (`call.py:146-149`).
- **≤16-B struct args**:
  - SysV x86-64 / AAPCS64 → by value (`pass_by_value`, `call.py:150-158`).
    If the 16-B struct is INT/INT but not canonical `{i64,i64}`
    (`_needs_int_int_eightbyte_repack`, `call.py:212-233`), it is repacked
    to `{i64,i64}` via memory bitcast before the call
    (`_repack_to_i64_pair`, `call.py:160-164, 236-257`). **This is the
    interval case** (`{i32,i32,i64}`) — llvmlite otherwise drops the second
    `i32`.
  - Windows x64 → by value only for sizes 1/2/4/8
    (`_is_windows_register_passable`), else alloca+store+pass-pointer
    (`call.py:150-169`).
- **>16-B struct args** — alloca+store, pass pointer on all platforms; on
  SysV x86-64 the arg gets the `byval` attribute and the enclosing function
  gets `optnone` + `noinline` so LLVM doesn't elide the caller-side copy
  before the callee reads it (`call.py:166-171, 185-189`). **This is the
  decimal (24 B) and varint (24 B) case.**
- **≤16-B struct returns** — direct on SysV/AAPCS64; INT/INT non-canonical
  returns get the mirror repack (`needs_ret_repack`, declare call returning
  `{i64,i64}`, unpack via `_repack_from_i64_pair`, `call.py:112-116,
  175-178, 195-196, 260-277`). Windows x64: sizes 1/2/4/8 return in RAX,
  else `sret` (`call.py:103-111`).
- **>16-B struct returns** — `sret` (hidden caller-allocated first arg, void
  return) on every platform (`call.py:137-140, 183-184, 191-193`). **This is
  `duckdb_get_decimal` → 24-B decimal and `duckdb_get_varint` → 24-B
  varint.**
- **`Record` LARGE returns are hard-rejected** with `TypingError`
  (`call.py:81-90`) — RecordModel is a raw pointer so an sret stack slot
  would dangle. numbduck sidesteps this entirely by using `Tuple`/`UniTuple`
  for every struct return, never `Record`.

`_call_lib_func_byval(typingctx, func_name_ty, arg_ty)` (`call.py:280-301`):
unconditionally passes the single arg **by pointer** (alloca, store, call
via pointer — `_emit_byval_call`, `call.py:203-209`) on all platforms. Used
for C functions that take `duckdb_result *` where the caller holds the
result struct by value. In `ducklib.py`:
- `duckdb_fetch_chunk` (`:943-946`)
- `duckdb_result_return_type` (`:1129-1132`)
- `duckdb_result_statement_type` (`:1135-1138`)

Important subtlety: `_call_lib_func_byval` uses **only the return type** from
the `signatures` dict; it builds `sig = func_sig.return_type(func_name_ty,
arg_ty)` from the **caller's actual `arg_ty`** (`call.py:300`). The declared
arg type in the sig entry (e.g. `duckdb_fetch_chunk = intp(duckdb_result_ty)`,
`:172`) is therefore documentation only for the byval path. It also does
**not** add `byval`/`optnone` — safe because the C callee genuinely takes a
`T*` and reads through it (not by-value semantics), so the store→call
data-dependency keeps the alloca alive.

---

## 7. Struct-by-value cases, mapped to wrappers

- **hugeint** (`Tuple((uint64,int64))`, canonical `{i64,i64}`, no repack):
  arg-in `duckdb_bind_hugeint` (`:1411`), `duckdb_create_hugeint` (`:654`);
  return `duckdb_get_hugeint` (`:979`).
- **uhugeint / uuid** (`UniTuple(uint64,2)`, canonical): arg-in
  `duckdb_bind_uhugeint` (`:1417`), `duckdb_create_uhugeint` (`:714`),
  `duckdb_create_uuid` (`:720`); return `duckdb_get_uhugeint` (`:1039`),
  `duckdb_get_uuid` (`:1045`).
- **interval** (`{i32,i32,i64}`, **non-canonical INT/INT → repack path**):
  arg-in `duckdb_bind_interval` (`:1423`), `duckdb_create_interval`
  (`:660`); return `duckdb_get_interval` (`:985`). This is the *only*
  wrapper exercising `_needs_int_int_eightbyte_repack` on both the arg and
  return side.
- **decimal** (24 B, LARGE → byval+optnone in, sret out): arg-in
  `duckdb_bind_decimal` (`:1429`), `duckdb_create_decimal` (`:648`); return
  `duckdb_get_decimal` (`:973`).
- **varint** (24 B, LARGE, version-gated): arg-in `duckdb_create_varint`
  (`:726`, gated); return `duckdb_get_varint` (`:1051`, gated).
- **bit / blob** (16 B canonical): `duckdb_create_bit` takes `duckdb_bit_ty`
  by value (`:630`); `duckdb_get_bit` returns it (`:955`); `duckdb_get_blob`
  returns `duckdb_blob_ty` (`:961`). (`duckdb_create_blob` takes two scalar
  args `intp, uint64`, not a struct — `:636`.)

---

## 8. Control + data flow (end to end)

Import time (`ducklib` imported):
1. `load_duckdb()` dlopens DuckDB `RTLD_GLOBAL` (`:12`).
2. `signatures[...] = ...` populates the shared dict (`:78-274`).
3. Each `@proxy`/`@proxy_if_available` decorator runs → **eager**
   `njit(sig)` of the body → `_call_lib_func` intrinsic lowers → symbol
   presence check + ABI codegen happen **now** → cfunc alias registered.
   `proxy_if_available` skips this whole step if the symbol is absent.

Call time (from user `@njit` code):
`duckdb_open(path_p, db_pp)` → inlined proxy wrapper (`inline='always'`) →
`builder.call` to the eager body's cfunc alias → the body's lowered
`_call_lib_func` code → direct `call @duckdb_open` into DuckDB, ABI-correct.
Return code (`int32`) / pointer (`intp`) / by-value struct (tuple) flows
back. No Python on the hot path.

Boundaries:
- **Python** — decorator wiring, sig-dict population, env parsing, symbol
  presence checks. All at import.
- **JIT/LLVM** — the intrinsic codegen (ABI classification, repack, sret,
  byval) and the inlined proxy call.
- **C** — the DuckDB library, reached via extern-decl + JIT-linker symbol
  resolution (not a baked address).

---

## 9. Invariants the layer relies on

1. numba tuple lowering = natural-aligned, non-packed C struct layout
   (`abi._basetuple_layout`, `abi.py:87-119`); every `duckdb_*_ty` alias is
   authored to match `duckdb.h` exactly.
2. Every pointer is `intp` (signed 64-bit) — so pointers classify as
   INTEGER eightbytes and never get an SSE lowering.
3. Struct returns use `Tuple`/`UniTuple`, never `Record` (Record LARGE
   returns are rejected, `call.py:81-90`).
4. The name literal in the decorator's `signatures.get(...)` and in the body's
   `_call_lib_func("name", ...)` are identical and both present in the dict.
5. All plain-`@proxy` symbols exist in every DuckDB in the supported pin
   (`duckdb>=1.3.2,<1.6`); only genuinely version-variant symbols are
   `proxy_if_available`-gated.

---

## 10. Fragile assumptions / risk notes (surfaced, not audited)

- **Import-time eager compile of every binding.** `proxy` eager-compiles
  each body (`proxy.py:75-76`), so a missing symbol or a bad signature is an
  **ImportError at `import numbduck.ducklib`**, not a lazy per-call failure.
  This makes `proxy_if_available` gating load-bearing: any symbol wrapped
  with plain `@proxy` that is absent in the linked DuckDB breaks the whole
  module import. Worth checking that the gated set (`create_varint`,
  `get_varint`, `scalar_function_set_init`) is exactly the set of symbols
  that vary across 1.3.2–1.5.x, and nothing else does.
- **Shared global `signatures` dict across libraries.** The same dict holds
  numbox's libc/libm/sqlite entries *and* numbduck's `duckdb_*` entries
  (`call.py:17` vs `ducklib.py:5` import the identical object). A name
  collision between libraries would silently overwrite; and the dict is
  mutated purely by import side effect, so import order matters. DuckDB
  names are prefixed `duckdb_`, so collision is unlikely today but
  structurally possible.
- **Two independent name lookups per binding.** The decorator resolves the
  signature; `_call_lib_func` re-resolves by string at lowering. A typo in
  the body's literal that still matches *some* key would compile against the
  wrong signature. A missing key → `signatures.get` returns `None` → the
  decorator gets `sig=None` and fails at import (`proxy.py:68` yields
  `main_sig=False`), which at least fails loud.
- **`_call_lib_func_byval` ignores the declared arg type** and lowers from
  the caller's actual value type (`call.py:300`). The sig entry's arg type
  for `fetch_chunk` / `result_return_type` / `result_statement_type` is
  decorative; correctness depends entirely on the caller passing the right
  48-byte `duckdb_result` value. It also omits `byval`/`optnone`, unlike the
  LARGE-struct path in `_call_lib_func` — correct for a genuine `T*`
  parameter but a real semantic difference worth keeping in mind.
- **Interval repack is the only INT/INT-non-canonical path exercised.** If a
  future 16-B struct alias were mixed FLOAT/INT and non-canonical, the
  repack only handles pure INT/INT (`call.py:231-233`); SSE eightbytes rely
  on llvmlite's default lowering. Not a current bug, but the repack is
  narrowly scoped.
- **Hand-computed struct layouts.** `decimal` (24 B via inlined hugeint
  fields) and `varint` (24 B) layouts are asserted only implicitly by
  `_struct_bytes`; if a DuckDB struct's real packing ever diverged from the
  tuple encoding, lowering would be silently ABI-wrong (mislocated fields),
  not a crash.
- **`duckdb_create_time_tz` returns scalar `uint64`** (`:115, 672`) — relies
  on DuckDB's `duckdb_time_tz` being a single-`uint64` struct that the ABI
  passes as a scalar; correct but implicit.

---

## Doc drift (informational)

`numbduck/CLAUDE.md` still describes hand-rolled intrinsics
(`_call_lib_func_struct_in/_out`, `_build_packed_interval`) and a "Follow-ups"
item to migrate `duckdb_bind_hugeint/_uhugeint/_interval` off bespoke
intrinsics. In the current tree those are already plain `@proxy` +
`_call_lib_func` wrappers (`ducklib.py:1411-1432`), the generic INT/INT
eightbyte repack lives in numbox (`call.py`/`abi.py`), and no per-struct
intrinsics remain in `ducklib.py`. The CLAUDE.md "Struct-by-value helpers"
and "Follow-ups" sections are stale relative to the reviewed code.
