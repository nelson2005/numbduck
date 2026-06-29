# numbduck binding layer — understanding note

Scope: the binding layer in `numbduck/ducklib.py` (1432 lines) and the ABI
lowering it delegates to in `numbox/core/bindings/call.py`. Covers the
enum/type aliases, the `signatures` dict, the `@cres` wrapping, and how
`_call_lib_func` / `_call_lib_func_byval` lower calls per the host C ABI.

Files cited:
- `/home/erik/projects/numbduck/numbduck/ducklib.py`
- `/home/erik/projects/numbox/numbox/core/bindings/call.py`
- `/home/erik/projects/numbox/numbox/core/bindings/abi.py`
- `/home/erik/projects/numbox/numbox/core/bindings/signatures.py`
- `/home/erik/projects/numbox/numbox/core/bindings/utils.py`
- `/home/erik/projects/numbox/numbox/utils/highlevel.py`

---

## 1. End-to-end picture

`ducklib.py` is a flat list of ~200 thin Python wrappers, one per DuckDB C
API function. Each wrapper is a numba-compiled function value (not a Python
function and not a `CPUDispatcher`) that, when called from inside other
`@njit` code, lowers to a direct LLVM call to the DuckDB shared library
symbol with ABI-correct argument/return passing.

Three collaborating pieces:

1. **`signatures` dict** (numbox-global, mutated by ducklib) — maps the C
   function name string to a numba `Signature` describing arg/return types.
2. **`@cres` / `@cres_if_available`** — eagerly njit-compile each wrapper to
   the exact signature and hand back a `CompileResultWAP` first-class
   function value.
3. **`_call_lib_func` / `_call_lib_func_byval`** — numba `@intrinsic`s
   (defined in numbox `call.py`) that resolve the symbol address and emit
   the LLVM call, classifying each arg/return as scalar / small-struct /
   large-struct and applying the host ABI's struct-passing convention.

Boundaries:
- **Python / import time**: `duckdb_lib = load_duckdb()` (`ducklib.py:11`)
  loads the DuckDB `.so`/`.dll` with `RTLD_GLOBAL` so its symbols land in
  the process's global symbol table where LLVM's JIT linker can find them.
  The `signatures[...] = ...` assignments and all `@cres(...)` decorations
  run at import — i.e. **every binding is compiled eagerly at import**.
- **Typing time** (inside numba): `_call_lib_func` runs as an intrinsic
  typer/codegen pair; it reads the literal function name, looks up the
  signature, classifies types, and resolves the symbol via
  `ll.address_of_symbol` (`call.py:72`).
- **Runtime / JIT code**: the emitted IR is a plain `builder.call` to the
  DuckDB symbol; data crosses as raw registers / stack per the C ABI.

---

## 2. Enum and type aliases (`ducklib.py:16-74`)

### State / enum constants
- `duckdb_state_ty = int32` (`:16`); `DuckDBSuccess = 0`, `DuckDBError = 1`
  (`:18-19`). These mirror the C API's return-code protocol — bindings
  return state codes, never raise (per CLAUDE.md "Error Handling").
- `DUCKDB_TYPE_*` (`:22-65`) are plain Python ints mirroring the `duckdb_type`
  enum in `duckdb.h`. Note the deliberate aliasing
  `DUCKDB_TYPE_VARINT = DUCKDB_TYPE_BIGNUM = 35` (`:60-61`) — same enum value
  and struct layout across DuckDB ≤1.4 (VARINT) and ≥1.5 (BIGNUM).
  `DUCKDB_TYPE_TIME_NS = 39` is 1.5+ only (`:65`).

### Struct type aliases (numba `Tuple`/`UniTuple` standing in for C structs)
These are the load-bearing ABI shapes (`:67-74`):

| alias | numba type | C struct | natural-align size |
|---|---|---|---|
| `duckdb_result_ty` | `UniTuple(intp, 6)` | `duckdb_result` | 48 B |
| `duckdb_hugeint_ty` | `Tuple((uint64, int64))` | `{lower, upper}` | 16 B, canonical `{i64,i64}` |
| `duckdb_uhugeint_ty` | `UniTuple(uint64, 2)` | hugeint | 16 B, canonical `{i64,i64}` |
| `duckdb_interval_ty` | `Tuple((int32, int32, int64))` | `{months, days, micros}` | 16 B, **non-canonical** `{i32,i32,i64}` |
| `duckdb_decimal_ty` | `Tuple((uint8, uint8, uint64, int64))` | `{width, scale, value(hugeint)}` | 24 B (large) |
| `duckdb_blob_ty` | `Tuple((intp, uint64))` | `{data, size}` | 16 B |
| `duckdb_bit_ty` | `Tuple((intp, uint64))` | `{data, size}` | 16 B |
| `duckdb_varint_ty` | `Tuple((intp, uint64, int8))` | `{data, size, is_negative}` | 24 B (large) |

Sizes follow numba's non-packed `StructModel` layout (natural alignment),
computed by `_basetuple_layout` (`abi.py:_basetuple_layout`). All pointers
are modeled as `intp` — a global design choice (see CLAUDE.md note: `_p`
suffix = pointer). This means the layer **assumes 64-bit `intp`**; a struct
embedding `intp` (blob/bit) changes size on 32-bit, but that's out of scope.

---

## 3. The `signatures` dict (`ducklib.py:77-272`)

`signatures` is imported from numbox (`ducklib.py:3` ←
`numbox/core/bindings/signatures.py`). In numbox it is the **merged global**
of libc/libm/sqlite signatures (`signatures.py` bottom). ducklib **mutates
this same global dict in place** at import, adding the ~210 `duckdb_*` keys.

Each entry is a numba `Signature` built by calling a numba scalar/struct type
as a constructor, e.g.:
- `signatures["duckdb_bind_double"] = duckdb_state_ty(intp, uint64, float64)`
  (`:83`) → returns `int32`, args `(ptr, idx, double)`.
- `signatures["duckdb_get_hugeint"] = duckdb_hugeint_ty(intp)` (`:181`) →
  returns a 16-byte struct by value, takes a value pointer.
- `signatures["duckdb_result_return_type"] = int32(duckdb_result_ty)`
  (`:223`) → takes the 48-byte result struct.

The dict is the single source of truth shared by **both** the `@cres`
decorator (which needs the `Signature` to compile the wrapper) and
`_call_lib_func` (which re-looks-up the same name at typing time,
`call.py:75`). The wrapper body passes the name as a **string literal** so
the two lookups always agree on the key.

**Fragile:** because `signatures` is one process-global dict shared across
numbox / numbduck / numbarrow, a duplicate key in any library silently
overwrites. A typo in a `signatures[...]` key vs the `@cres(signatures.get(
"..."))` lookup yields `None` → `@cres` raises `ValueError` at import
(`highlevel.py:29`), so mismatches fail loud at import, not silently.

---

## 4. `@cres` and `@cres_if_available` (`numbox/utils/highlevel.py:26-58`)

```python
def cres(sig, **kwargs):
    def _(func):
        func_jit = njit(sig, **kwargs)(func)          # eager compile to sig
        sigs = func_jit.nopython_signatures
        assert len(sigs) == 1
        func_cres = func_jit.get_compile_result(sigs[0])
        return CompileResultWAP(func_cres)            # first-class fn value
    return _
```

Key points:
- Passing an explicit `sig` to `njit` forces **eager** (ahead-of-call)
  compilation at import. So importing `ducklib` compiles every binding.
- `sig` must be a single `Signature`; ambiguity/`None` raises (`:30-31`,
  `:34`). `signatures.get(name)` returning `None` (missing key) is caught
  here.
- The return is a `CompileResultWAP` (numba `CompileResultWAP` /
  `FunctionType` proxy), **not** a `CPUDispatcher`. This is what lets a
  binding be used as a typed, addressable first-class function value inside
  other `@njit` code (e.g. passed as a callback pointer, stored in a struct)
  — the whole point of `cres` per its docstring (`highlevel.py:27`).

`cres_if_available(lib, sig, **kwargs)` (`highlevel.py:40`): if
`hasattr(lib, func.__name__)` it behaves as `cres`; otherwise it returns a
**stub** that raises `NotImplementedError` at call time (`:51-55`). Used for
version-gated symbols: `duckdb_create_varint` (`:725`), `duckdb_get_varint`
(`:1050`), `duckdb_scalar_function_set_init` (`:1284`). The presence check is
against the **ctypes handle** (`hasattr(lib, name)`), whereas `_call_lib_func`
resolves through the **LLVM symbol table** (`ll.address_of_symbol`). Two
different resolution mechanisms — normally consistent under `RTLD_GLOBAL`,
but a divergence (symbol visible to one and not the other) is a latent risk.

Wrapper shape (the universal pattern, e.g. `:317-320`):
```python
@cres(signatures.get("duckdb_bind_double"))
def duckdb_bind_double(prepared_statement_p, param_idx, val):
    return _call_lib_func("duckdb_bind_double", (prepared_statement_p, param_idx, val))
```
Args are forwarded as a **tuple literal**; the name is a **string literal**.
Zero-arg calls pass `()` (e.g. `duckdb_create_null_value`, `:546-548`).

---

## 5. `_call_lib_func` — ABI lowering (`call.py:21-200`)

`@intrinsic(prefer_literal=True)` so the function-name arg is a compile-time
`Literal[str]`, extracted by `extract_literal_str` (`call.py:71`,
`utils.py:extract_literal_str`).

### Typing phase (`call.py:71-116`)
1. Extract `func_name`; resolve its address with `ll.address_of_symbol`
   (`:72`). `None` → `TypingError "...unavailable in the LLVM context"`
   (`:74`). **This is where a not-yet-loaded library or a version-missing
   symbol fails.**
2. Look up `func_sig = signatures.get(func_name)` (`:75`); `None` →
   `TypingError "Undefined signature"` (`:77`).
3. `ret_class = _classify(ret_ty)` (`:80`). **`Record` returns >16 bytes are
   rejected** with `TypingError` (`:81-90`): numba's `RecordModel` is a raw
   pointer, so a stack-alloca `sret` slot would dangle after the `@njit`
   function returns. (No duckdb binding hits this; all struct returns are
   Tuples.)
4. Classify each arg (`:92-100`). `args_ty == NoneType` → no args; a
   `BaseTuple` → unpack; otherwise a single scalar arg.
5. Decide `use_sret` (`:103-111`) and `needs_ret_repack` (`:112-116`).

### Classification (`abi.py:_classify`, `_struct_bytes`)
- non-`Record`/non-`BaseTuple` → `_CLASS_SCALAR`.
- `Record`/`BaseTuple` ≤16 B → `_CLASS_STRUCT_SMALL`.
- `Record`/`BaseTuple` >16 B → `_CLASS_STRUCT_LARGE`.

### Codegen (`call.py:118-197`) — the four cases

**(a) Scalar args/returns** — passed/returned directly (`:146-149`, default
`func_ll_ty` at `:180`).

**(b) ≤16-byte struct args** (`:150-165`):
- `pass_by_value` when SysV x86-64 or AAPCS64, **or** Windows x64 with size
  ∈ {1,2,4,8} (`_is_windows_register_passable`, `abi.py`). When by value, if
  `_needs_int_int_eightbyte_repack` (16 B, both eightbytes pure-INTEGER, not
  already canonical `{i64,i64}`), the value is repacked to `{i64,i64}` via
  memory bitcast (`_repack_to_i64_pair`, `call.py:236-257`) before the call.
  This works around llvmlite **dropping fields** when register-lowering a
  non-canonical INT/INT 16-byte aggregate (e.g. `{i32,i32,i64}` = interval
  loses the second `i32`). See `call.py:212-233` and the llvmlite#300
  reference.
- Windows non-register-passable small structs fall through to the
  alloca+store+pass-pointer path (`:166-169`).

**(c) >16-byte struct args** (`:166-171`): alloca, store, pass pointer on
every platform. **Only on SysV x86-64** is the `byval` attribute added
(`:170-171`, `:185-186`) and the enclosing function marked `optnone` +
`noinline` (`:187-189`) so LLVM doesn't elide the caller-side stack copy
before the callee reads it (llvmlite#300). On AAPCS64 a bare pointer is the
correct AAPCS64 by-reference convention, so no attribute is needed.

**(d) Struct returns**:
- `use_sret` (`:103-111`) when the return is `_CLASS_STRUCT_LARGE`, **or**
  Windows x64 small-but-not-register-passable. sret = caller allocates the
  slot, passes it as a hidden first pointer arg, function returns void
  (`:137-140`, `:174`, `:183-184`, `:191-193`).
- ≤16-byte small struct returns on SysV/AAPCS64 are returned **directly**;
  if `_needs_int_int_eightbyte_repack`, the LLVM call is declared to return
  `{i64,i64}` (`:175-178`) and the result unpacked back via
  `_repack_from_i64_pair` (`:195-196`, `call.py:260-277`).

### The repack helpers (`call.py:236-277`)
Both allocate an **over-aligned** `{i64,i64}` slot (8-byte aligned),
bitcast it to the original struct pointer type for the store/load mismatch,
and reinterpret the bytes. The over-alignment is the correctness argument:
an `alloca(orig_ll_ty)` for `[4 x i32]` would be 4-byte aligned and the
`{i64,i64}` load would be UB; the `{i64,i64}` alloca is 8-byte aligned so
both the store (via bitcast pointer) and the load are well-aligned
(`call.py:242-249`). **Implicit assumption: little-endian** — the bitcast is
a pure byte reinterpretation, correct on x86-64 and ARM64 (both LE) but
would mis-order fields on a big-endian host.

---

## 6. `_call_lib_func_byval` — always-by-pointer (`call.py:280-301`)

```python
@intrinsic(prefer_literal=True)
def _call_lib_func_byval(typingctx, func_name_ty, arg_ty):
    ...
    def codegen(...):
        return _emit_byval_call(builder, arg, arg_ll_ty, ret_type, func_name)
```

`_emit_byval_call` (`call.py:203-209`): alloca the arg type, store the value,
declare the callee as `func(arg_ll_ty*)`, call with the slot pointer. **No
`byval` attribute, no `optnone`/`noinline`, no classification.** Its
docstring (`call.py:65-69`, `:282-287`) states it is for C signatures of the
form **`func(T*)`** (pointer to struct) where the numba caller holds the
struct as a value — the type system can't tell `T` from `T*`, so the caller
picks the intrinsic.

### Callers in ducklib (the struct-by-value/byref cases)
- `duckdb_fetch_chunk(duckdb_result)` → `_call_lib_func_byval(...,
  duckdb_result)` (`ducklib.py:942-945`), sig `intp(duckdb_result_ty)`
  (`:171`).
- `duckdb_result_return_type(result)` → `_call_lib_func_byval` (`:1128-1131`),
  sig `int32(duckdb_result_ty)` (`:223`).
- `duckdb_result_statement_type(result)` → `_call_lib_func_byval`
  (`:1134-1137`), sig `int32(duckdb_result_ty)` (`:224`).

All three take the 48-byte `duckdb_result_ty` (`UniTuple(intp,6)`).

### Struct-by-value cases routed through `_call_lib_func` instead
(small/large struct args & returns handled by the generic ABI path):
- **16-byte by-value struct args (canonical `{i64,i64}`)**:
  `duckdb_bind_hugeint` (`:1410`), `duckdb_bind_uhugeint` (`:1416`),
  `duckdb_create_hugeint` (`:653`), `duckdb_create_uhugeint` (`:713`),
  `duckdb_create_uuid` (`:719`), `duckdb_create_bit` (`:629`,
  `duckdb_bit_ty`).
- **16-byte by-value struct arg needing the INT/INT eightbyte repack**:
  `duckdb_bind_interval` (`:1422`), `duckdb_create_interval` (`:659`) —
  `{i32,i32,i64}` repacked to `{i64,i64}`.
- **24-byte (large) by-value struct args** → pointer + `byval` on SysV:
  `duckdb_bind_decimal` (`:1428`, `duckdb_decimal_ty`),
  `duckdb_create_decimal` (`:647`), `duckdb_create_varint` (`:725`,
  `duckdb_varint_ty`).
- **16-byte by-value struct returns**: `duckdb_get_hugeint` (`:978`),
  `duckdb_get_uhugeint` (`:1038`), `duckdb_get_uuid` (`:1044`),
  `duckdb_get_blob` (`:960`, `duckdb_blob_ty`), `duckdb_get_bit` (`:954`),
  `duckdb_get_interval` (`:984`, INT/INT repack on return).
- **24-byte (large) struct returns** → `sret`: `duckdb_get_decimal` (`:972`,
  `duckdb_decimal_ty`), `duckdb_get_varint` (`:1050`, `duckdb_varint_ty`).

So the `duckdb_*_ty` 16-byte structs that are *register-canonical* ride the
plain by-value path; **interval** is the one shape that triggers the
eightbyte repack; **decimal** and **varint** (24 B) are the large-struct
byval/sret cases.

---

## 7. Invariants

- Wrapper name == DuckDB C symbol name == `signatures` key == the string
  literal passed to `_call_lib_func`. Any drift breaks at import or typing.
- DuckDB lib must be loaded `RTLD_GLOBAL` **before** any binding compiles;
  guaranteed by `duckdb_lib = load_duckdb()` at `ducklib.py:11` running
  before the `@cres` decorations later in the module.
- Pointers are uniformly `intp`; structs are `Tuple`/`UniTuple` whose numba
  `StructModel` layout must match the C struct's natural-alignment layout
  (the ABI code recomputes this in `_basetuple_layout`).
- Bindings never raise into C — they return state codes / pointers
  (`DuckDBSuccess`/`DuckDBError`).
- The eightbyte repack only fires for exactly-16-byte structs whose two
  eightbytes are both pure-INTEGER and not already `{i64,i64}` — i.e.
  precisely the interval shape among duckdb types.

---

## 8. Risks / fragile assumptions to surface (not fully audited)

1. **`_call_lib_func_byval` vs the DuckDB-by-value C signatures — primary
   risk.** `duckdb_fetch_chunk`, `duckdb_result_return_type`, and
   `duckdb_result_statement_type` use `_call_lib_func_byval`, which emits a
   **bare pointer arg with no `byval` attribute** (`call.py:203-209`). The
   intrinsic's own docstring says it is for `func(T*)` signatures
   (`call.py:65-69`). In the DuckDB C header these three take the result
   **by value** (`func(duckdb_result)`), a 48-byte struct. On SysV x86-64 a
   >16-byte by-value arg is MEMORY-class and `_call_lib_func`'s large-struct
   path passes it as `pointer + byval + optnone/noinline` (`call.py:170-189`)
   — a *different* lowering than the bare pointer `_call_lib_func_byval`
   emits. On AAPCS64 the two coincide (both bare pointer = by-reference), but
   on SysV x86-64 they do not. Worth verifying that these three calls are in
   fact ABI-correct on Linux x86-64 (bare pointer in RDI vs an in-memory
   48-byte argument). Flagging only; not audited.

2. **CLAUDE.md is stale relative to the code.** The repo's `CLAUDE.md`
   describes struct helpers `_call_lib_func_struct_in` / `_struct_out` /
   `_build_packed_interval` and "hand-rolled intrinsics at
   ducklib.py:1525-1640 (~116 lines)" for hugeint/uhugeint/interval/decimal.
   **None of those exist in the current `ducklib.py`** (the file is 1432
   lines and ends at `duckdb_bind_decimal`, `:1432`). The documented
   "Follow-up" migration to `_call_lib_func` has already landed: hugeint
   (`:1410`), uhugeint (`:1416`), interval (`:1422`), decimal (`:1428`) all
   go through the generic `_call_lib_func`. Treat the CLAUDE.md "Struct-by-
   value helpers" and "Follow-ups" sections as outdated when reviewing.

3. **Two symbol-resolution mechanisms.** `cres_if_available` gates on the
   ctypes handle (`hasattr(lib, name)`, `highlevel.py:50`); `_call_lib_func`
   gates on the LLVM symbol table (`ll.address_of_symbol`, `call.py:72`). A
   symbol visible to one but not the other (e.g. partial export, name
   mangling differences) would slip past the stub guard and fail later with
   a `TypingError`. Normally consistent under `RTLD_GLOBAL`.

4. **Global mutable `signatures` dict shared across numbox/numbduck/
   numbarrow.** ducklib mutates the numbox global in place at import
   (`ducklib.py:77-272`). A name collision with another binding library
   silently overwrites the earlier signature.

5. **`optnone` + `noinline` is applied to the whole enclosing `@njit`
   function**, not just the call site, whenever any byval large-struct arg is
   present (`call.py:187-189`). A hot JIT function that binds a single
   decimal/varint arg loses all optimization. Documented, but a performance
   footgun.

6. **Eager compile-at-import cost.** `@cres` compiles all ~200 bindings at
   `import ducklib` (explicit `sig` → eager njit). Import-time cost and any
   per-symbol resolution failure surfaces at import, not lazily.

7. **Little-endian assumption** baked into the eightbyte memory-bitcast
   repack (`call.py:236-277`). Fine for x86-64/ARM64; would corrupt field
   order on a big-endian host.

8. **64-bit `intp` assumption.** Structs embedding `intp` as a pointer
   field (`duckdb_blob_ty`, `duckdb_bit_ty`, `duckdb_varint_ty`) change size
   and classification on a 32-bit `intp`; the whole layer assumes 64-bit.
