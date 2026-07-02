# Understanding: Structref-backed UDAF lifecycle & @cfunc/@njit callbacks

Scope: how numbduck bridges DuckDB's aggregate (UDAF) lifecycle to a numba
structref (or numpy array) held in DuckDB's opaque per-group state slot, using
numbox's `export_meminfo` / `borrow_structref` / `release_meminfo` and the
`@cfunc`-wrapper + module-level `@njit`-impl callback pattern.

Primary sources read in full:
- `/home/erik/projects/numbduck/test/test_ducklib.md` (design doc)
- `/home/erik/projects/numbduck/examples/irr.py` (worked UDAF: IRR)
- numbox bridge intrinsics `/home/erik/projects/numbox/numbox/utils/meminfo.py`
- numbox low-level helpers `/home/erik/projects/numbox/numbox/utils/lowlevel.py`
- numba `removerefctpass` (installed 0.65.1):
  `/home/erik/projects/numbox/venv/lib/python3.12/site-packages/numba/core/removerefctpass.py`

Reference implementations in `test/test_ducklib.py`:
- Welford stddev (structref state): callbacks `test_ducklib.py:3204-3305`, registration `test_ducklib.py:3392-3467`
- Array variance (numpy-array state): callbacks `test_ducklib.py:3038-3163`, registration `test_ducklib.py:3469-3540`
- Refcount ladder tests: structref `test_ducklib.py:2931-2971`, array `test_ducklib.py:2974-3017`, nested-heap dtor cascade `test_ducklib.py:3344-3389`

numba version in the numbox venv: **0.65.1**. (numbduck venv had no numba
installed at review time; numbox is the sibling providing the intrinsics.)


## 1. The core problem: two ownership models, one `void*`

DuckDB's aggregate API hands each group a fixed-size, opaque byte buffer
(`state_size` bytes) whose lifetime DuckDB owns; it never looks inside. numba's
NRT (Numba Runtime) manages structref/array lifetime by reference counting a
`MemInfo` header (refcount, dtor ptr, dtor_info, data ptr, size, allocator);
see numbox's layout comment `meminfo.py:48-58`.

The bridge stores an **`intp` MemInfo pointer** inside DuckDB's slot and keeps
the NRT allocation alive across the six DuckDB callbacks even though NRT cannot
see the slot. `state_size` is set to **8** for the structref case (one `intp`;
`_welford_state_size_impl` `test_ducklib.py:3204-3206`, `_irr_state_size_impl`
`irr.py:104-107`) and **16** for the array case (two `intp`s: meminfo_p +
data_p; `_arr_state_size_impl` `test_ducklib.py:3038-3040`, layout constants
`test_ducklib.py:3031-3035`).


## 2. DuckDB aggregate lifecycle → callbacks

DuckDB drives six C function pointers (all raw pointers; numbduck carries every
pointer as `nb_types.intp`, never `voidptr`):

| DuckDB step   | numba impl (Welford)                         | signature (cfunc)                                  |
|---------------|----------------------------------------------|----------------------------------------------------|
| `state_size`  | `_welford_state_size_impl` `:3204`           | `uint64(intp)` `:3209`                             |
| `init`        | `_welford_init_impl` `:3214-3219`            | `void(intp,intp)` `:3222`                          |
| `update`      | `_welford_update_impl` `:3229-3242`          | `void(intp,intp,intp)` `:3245`                     |
| `combine`     | `_welford_combine_impl` `:3250-3263`         | `void(intp,intp,intp,uint64)` `:3266`             |
| `finalize`    | `_welford_finalize_impl` `:3272-3284`        | `void(intp,intp,intp,uint64,uint64)` `:3287`      |
| `destroy`     | `_welford_destroy_impl` `:3293-3300`         | `void(intp,uint64)` `:3303`                        |

The `states` argument in update/combine/finalize/destroy is a pointer to an
**array of per-row (per-group) state pointers**, each of which points at that
group's `state_size`-byte slot. The impls decode it as
`carray(_cast_int_to_void_p(states), (n,), dtype=numpy.intp)` and then decode
each individual slot as a further length-1 (structref) or length-2 (array)
`carray` of `intp` (e.g. `test_ducklib.py:3234-3240`; `irr.py:144-155`).

Data-flow per callback:
- **init** (`test_ducklib.py:3214-3219`, `irr.py:114-121`): allocate the state
  structref (refcount 1), `export_meminfo(s)` to get its `intp` MemInfo ptr with
  a +1, write that ptr into `slot[0]`.
- **update** (`test_ducklib.py:3229-3242`, `irr.py:129-162`): read chunk vectors
  via `ducklib.duckdb_data_chunk_get_vector` / `..._get_data` / `..._get_size`;
  for each row reconstruct the structref with `borrow_structref(type, slot[0])`
  and mutate it. Welford's impl **skips NULL validity checks** (documented
  caveat `test_ducklib.py:3227-3228`); IRR's impl **does** check validity via
  `duckdb_vector_get_validity` + `duckdb_validity_row_is_valid`
  (`irr.py:140-153`).
- **combine** (`test_ducklib.py:3250-3263`, `irr.py:170-185`): borrow both source
  and target structrefs, merge source into target in place. Source is not
  consumed; DuckDB still destroys it later.
- **finalize** (`test_ducklib.py:3272-3284`, `irr.py:193-206`): borrow, compute
  the scalar result, write into the output vector at `offset + i`. Output buffer
  is decoded as `carray(out_data, (offset+count,), float64)`.
- **destroy** (`test_ducklib.py:3293-3300`, `irr.py:214-220`): `release_meminfo(slot[0])`
  per group — the single -1 that brings refcount to 0 and fires the dtor.

Registration is ordinary Python (not JIT): `duckdb_create_aggregate_function`,
`..._set_name`, `..._add_parameter`, `..._set_return_type`,
`..._set_functions(state_size, init, update, combine, finalize)`,
`..._set_destructor(destroy)`, `duckdb_register_aggregate_function`. Each cfunc
is passed by `.address` (`test_ducklib.py:3422-3431`; `irr.py:244-252`). The
cfuncs must live at module scope so they outlive registration.


## 3. The `@cfunc` wrapper + module-level `@njit` impl pattern

DuckDB needs raw C entry points. A `@cfunc` gives one, but a `@cfunc` body
cannot `import` or use the full numba feature set. So every callback is split:

- a module-level `@njit` **impl** that does all real work (`ducklib` calls,
  `carray`, bridge intrinsics), and
- a thin `@cfunc` **wrapper** with the DuckDB-mandated signature that just
  forwards to the impl (e.g. `test_ducklib.py:3222-3224`; `irr.py:124-126`).

Two boundary rules make this compile:
1. All pointer args are `nb_types.intp` because numbduck's C-API bindings
   produce/consume `intp`, not `voidptr`.
2. `carray` inside `@njit` requires a `voidptr`, so every raw address is bridged
   with numbox's `_cast_int_to_void_p` intrinsic (`lowlevel.py:48-53`), an
   `inttoptr` to `voidptr_t`.

Impls freely call the `ducklib` wrappers (each `@proxy(sig)` over
`_call_lib_func`) — e.g. `duckdb_data_chunk_get_size` — inside `@njit`
(`irr.py:131-143`). That is the JIT↔C boundary; the numba↔DuckDB boundary is the
`@cfunc` entry point address handed to DuckDB.


## 4. The three bridge intrinsics (numbox `meminfo.py`)

- **`export_meminfo(s)`** `meminfo.py:143-152`: `structref_meminfo(s)` extracts
  `(meminfo_p, data_p)` (`meminfo.py:15-39`), then `_incref_meminfo(meminfo_p)`
  (`meminfo.py:77-87`, emits `context.nrt.incref` → an `NRT_incref` call) adds
  +1, returns `meminfo_p` as `intp`. Net contract: **+1**, kept alive until a
  matching `release_meminfo`.
- **`borrow_structref(struct_type, p)`** `meminfo.py:131-140`: `_incref_meminfo(p)`
  then `_deref_structref_raw_ptr` (`meminfo.py:114-128`) builds a struct proxy
  whose `.meminfo` field is `inttoptr(p)`. The reconstructed local is a normal
  NRT value, so the compiler inserts a scope-exit decref. Net contract on the
  external owner: **0** (entry incref balanced by scope-exit decref).
- **`release_meminfo(p)`** `meminfo.py:155-162` → `_release_meminfo`
  `meminfo.py:90-111`: calls **`NRT_MemInfo_release`** directly (not
  `context.nrt.decref`). Net: **-1**; at 0, NRT runs the structref's `imp_dtor`,
  which recursively decrefs nested heap fields (e.g. the two `Float64Vector`s in
  IRRState, or a `typed.List`). The dtor-cascade is proven leak-clean by
  `test_structref_meminfo_bridge_nested_heap` `test_ducklib.py:3344-3389`.

All three reject non-`intp` pointer types via `_require_intp`
(`meminfo.py:67-74`) to avoid a truncating `inttoptr` on 64-bit hosts.


## 5. Why the refcount math actually works — `removerefctpass`

This is the load-bearing and fragile part. numba runs
`remove_unnecessary_nrt_usage` (`removerefctpass.py:99-120`) per lowered
function. `_rewrite_function` (`:23-31`) strips **only** calls named exactly
`NRT_incref` / `NRT_decref` (`_accepted_nrtfns` `:34`). Whether stripping runs at
all is gated by `_legalize` (`:37-96`), which returns `True` (→ strip) only if
**all** of:
- every argument type is `valid_input` — i.e. needs no refcount **or is an
  `Array`** (the `types.Array` exception, `:51-55`);
- the return type is `valid_output` (needs no refcount, `:44-49`);
- every *called* function's return type is `valid_output` (`:86-88`);
- no module function name starts with `NRT_` outside the accepted set (this is
  the "no allocation" gate — `NRT_MemInfo_alloc*` and `NRT_MemInfo_release`
  trip it, `:90-94`);
- no `numba_args_may_always_need_nrt` metadata (`:60-69`).

`release_meminfo` deliberately calls `NRT_MemInfo_release` rather than emitting
`NRT_decref`, for two reasons: (a) `NRT_decref` would be a strip target, and
(b) the `NRT_`-prefixed name trips the allocation gate `:90-94`, disabling the
pass for the whole destroy impl so the release survives regardless of function
complexity. This is exactly the numbox docstring rationale at `meminfo.py:92-100`.

### The structref-vs-array asymmetry (concrete, source-verified)

The single most important subtlety: **`export_meminfo` is safe for structrefs
but would be wrong for arrays**, which is why the array path uses inline
`_incref_meminfo` (`test_ducklib.py:3048-3056`) instead of the `export_meminfo`
wrapper.

Reason, traced to `_legalize`:
- `export_meminfo(s)` with a **structref** arg: the structref is not an `Array`
  and its model needs refcount → `valid_input` is `False` (`:51-55`) → pass
  **disabled** in `export_meminfo` → its `_incref_meminfo` **survives**.
- `export_meminfo(arr)` with an **`Array`** arg: `valid_input` returns `True`
  via the `types.Array` exception, return type `intp` is valid, and the body
  contains no `NRT_` allocation → pass **enabled** → the `_incref_meminfo` gets
  **stripped**. Using it for an array would yield alloc(+1) − scope-exit
  decref(−1) = 0 → premature free.

So the array bridge does the incref **inline** in the allocating impl
(`_arr_init_impl` `test_ducklib.py:3048-3056`), where `numpy.zeros` emits an
`NRT_MemInfo_alloc*` in-module and trips the allocation gate `:90-94`, keeping
the pass disabled so both the inline incref and the array's scope-exit decref
survive symmetrically (net +0 over the allocation's refcount 1).

### Empirical guarantee

The end-to-end correctness does **not** rest on hand-derived stripping accounting
— it is verified two ways: the refcount ladders read `MemInfo.refct` from Python
between separate `@njit` calls (`_read_refcount` `test_ducklib.py:2903-2909`;
ladders `:2931-2971` and `:2974-3017`), and the DuckDB end-to-end tests assert
NRT `alloc == free` via `rtsys.get_allocation_stats()`
(`test_ducklib.py:3462-3466`, `:3344-3389`; the IRR example does the same
`irr.py:265-377`).


## 6. Invariants relied upon

- Slot width: `state_size` == `sizeof(intp)` == 8 (structref) / 16 (array).
  64-bit-host assumption.
- `slot[0]` always holds a live MemInfo `intp` for the group's lifetime; DuckDB
  guarantees init runs before update/combine/finalize and destroy runs exactly
  once per group at the end.
- Exactly one `export_meminfo` (+1) per group balanced by exactly one
  `release_meminfo` (−1) in destroy. `borrow_structref` is always net-0.
- Nested heap fields are freed only via the top structref's `imp_dtor` cascade;
  callbacks never free vectors/lists directly.
- cfunc objects persist at module scope (their `.address` is registered with
  DuckDB).


## 7. Fragile assumptions / risk flags (for the downstream defect review)

1. **removerefctpass dependency (version-sensitive).** Correctness hinges on the
   exact `_legalize` rules in `removerefctpass.py:37-96`, especially the
   `types.Array` exception at `:55` and the `NRT_`-allocation gate at `:90-94`.
   A future numba change to these rules could silently flip a net-0/net-±1 into
   a leak or a use-after-free. The design doc claims stability "since numba 0.43"
   (`test_ducklib.md:161-163`) — plausible but unverified here.
2. **Design-doc mechanism may not match numba 0.65.1.** `test_ducklib.md` frames
   the structref init as *symmetric stripping of both incref and decref*
   (`test_ducklib.md:113-121`). Traced against the actual `_legalize`, the pass
   appears to be **disabled** in these impls (structref ctor/alloc and
   `NRT_`-named release trip the gates), so both ops **survive** rather than
   being stripped — arriving at the same final refcount by the opposite
   mechanism. The *code* is validated empirically, but reviewers should not rely
   on the doc's stripping narrative when reasoning about edge cases. Worth a
   focused check.
3. **Welford update skips NULL validity** (`test_ducklib.py:3227-3228`). Only
   safe because the test data has no NULLs; the IRR example does check validity
   (`irr.py:140-153`). Any structref UDAF copied from the Welford template into
   production without adding validity checks will read garbage for NULL rows.
4. **`states` decoding assumes DuckDB's vectorized-aggregate contract** — that
   `states` is an array of `n` (= chunk size for update; `count` for
   combine/finalize/destroy) pointers, each to a `state_size` slot. If the C API
   contract for `duckdb_aggregate_update_t` / combine / finalize / destroy
   differs from this assumption (e.g. counts, ordering, alignment) the `carray`
   decode reads out of bounds. Not audited against `duckdb.h` here.
5. **`intp` everywhere, 64-bit-only.** Slot sizes, pointer widths, and
   `_require_intp` all assume 8-byte pointers; a 32-bit target would corrupt.
6. **`_read_refcount` reads `MemInfo.refct` as the first `size_t`** field
   (`test_ducklib.py:2903-2909`; numbox `get_nrt_refcount` `meminfo.py:42-61`) —
   depends on NRT's `MemInfo` layout staying refcount-first (true through 0.65.1,
   but a raw-offset assumption).
7. **finalize output aliasing.** `out_vals` is sized `offset+count` and written
   at `offset+i` (`test_ducklib.py:3277-3284`, `irr.py:196-206`); correctness
   depends on DuckDB's `offset` semantics matching (writing only `[offset,
   offset+count)`). Reasonable but not independently verified.
8. **IRR "first non-NULL wins" constant capture.** `investment`/`target_npv` are
   taken from the first non-NULL row and never re-checked (`irr.py:159-162`,
   combine `:182-185`); if a caller passes differing per-row constants within a
   group the result is silently order-dependent (documented contract
   `irr.py:18-22`, but a latent foot-gun).
