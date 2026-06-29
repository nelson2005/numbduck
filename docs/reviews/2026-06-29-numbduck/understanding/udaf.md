# Structref-backed UDAF lifecycle & @cfunc/@njit callbacks

Understanding note for downstream defect review. Subsystem: bridging DuckDB's
aggregate C API to a Numba structref used as per-group state, via the numbox
meminfo intrinsics (`export_meminfo` / `borrow_structref` / `release_meminfo`)
and the `@cfunc`-wrapper + module-level `@njit`-impl callback pattern.

Primary artifacts read in full:
- `/home/erik/projects/numbduck/test/test_ducklib.md` (design doc, Welford stddev UDAF)
- `/home/erik/projects/numbduck/examples/irr.py` (IRR UDAF, the concrete worked example)
- numbox bridge intrinsics: `/home/erik/projects/numbox/numbox/utils/meminfo.py`
- numbox pointer/cast helpers: `/home/erik/projects/numbox/numbox/utils/lowlevel.py`

All `file:line` cites below are to those files.


## 1. The core problem: two ownership domains over one `void*`

A Numba structref is a single pointer to an NRT `MemInfo` header
(`{refct, dtor, dtor_info, data, size, ext_alloc}`); NRT reference-counts it and
runs the destructor when `refct` hits 0 (memory layout: `meminfo.py:48-58`).

DuckDB hands each aggregate group a fixed-size opaque byte buffer and is blind to
its contents (`test_ducklib.md:10`). Both example UDAFs set `state_size = 8`
(`irr.py:104-107`, doc `test_ducklib.md:17`) so the buffer holds exactly one
`intp`-wide pointer — the `MemInfo*`.

The bridge must: (a) move ownership of a freshly-allocated structref out of an
NRT-managed local into DuckDB's slot, (b) keep it alive across many independent C
callbacks where NRT cannot see it, and (c) free it exactly once on destroy.


## 2. DuckDB aggregate lifecycle (control flow)

DuckDB drives six C entry points, each receiving raw state pointer(s). Order and
duties (`test_ducklib.md:15-51`, `irr.py:97-225`):

1. **state_size** -> returns `8` (`irr.py:104-111`).
2. **init** -> allocate structref (refct 1), `export_meminfo` to get `MemInfo*`,
   store it in the 8-byte slot (`irr.py:114-126`).
3. **update** -> per chunk: for each valid row, read slot, `borrow_structref`,
   mutate state (`irr.py:129-167`).
4. **combine** -> per group: borrow source and target, merge src into tgt
   (`irr.py:170-190`).
5. **finalize** -> per group: borrow, compute result, write into the output
   vector at `offset + i` (`irr.py:193-211`).
6. **destroy** -> per group: `release_meminfo(slot[0])`, dropping the slot's
   reference; at refct 0 NRT runs the structref dtor, freeing nested allocations
   such as the typed vectors (`irr.py:214-225`).

Note the **arity differs per callback** and matches DuckDB's C ABI exactly:
update/finalize/combine receive an *array* of state pointers (one slot per group
in the chunk), not a single state. `irr.py` reads that outer array with `carray`
over `states`/`source`/`target` (`irr.py:144`, `:172-173`, `:196`), then for each
group does a second `carray((1,))` over the individual slot to load the
`MemInfo*` (`irr.py:155`, `:176-177`, `:200`). Update additionally pulls the four
input column vectors + validity masks from the data chunk (`irr.py:131-143`) and
skips rows where any of the four inputs is NULL (`irr.py:146-153`).


## 3. The `@cfunc` wrapper + module-level `@njit` impl pattern

DuckDB needs raw C function pointers. Each callback is therefore two functions:

- a module-level `@njit` `_*_impl` doing all real work, and
- a thin `@cfunc` `_*_cb` that only forwards to the impl
  (e.g. `irr.py:114-126`, `:129-167`).

Rationale (`test_ducklib.md:54-75`, numbduck `CLAUDE.md`):
- `@cfunc` bodies can't `import` or use the richer Numba features the impls need.
- **All pointer args are typed `nb_types.intp`, never `voidptr`**, because the
  numbduck/numbox C bindings carry every pointer as `intp`
  (`irr.py:109`, `:124`, `:165`, `:188`, `:209`, `:223`).
- But `carray()` inside `@njit` *requires* a `voidptr`. The gap is bridged by
  numbox's `_cast_int_to_void_p` intrinsic — a bare `inttoptr` to `voidptr_t`
  (`lowlevel.py:48-53`) — applied at every `carray` call site
  (`irr.py:120`, `:136-139`, `:144`, `:155`, `:172-177`, `:196-200`, `:216-219`).

The `@cfunc` exposes a raw entry address via `_*_cb.address`, which is what gets
registered with DuckDB (`irr.py:244-252`).


## 4. The three bridge intrinsics (numbox meminfo.py)

These are the JIT/NRT boundary. They are plain `@numba.njit` wrappers over
`@intrinsic` codegen, so they are *callable from both Python and `@njit`*.

### `export_meminfo(s)` — init only (`meminfo.py:143-152`)
```
meminfo_p, _ = structref_meminfo(s)   # extract MemInfo* as intp (meminfo.py:15-39)
_incref_meminfo(meminfo_p)            # +1 via context.nrt.incref (meminfo.py:77-87)
return meminfo_p
```
`structref_meminfo` reaches into the structref value, pulls the meminfo and data
pointers and returns them as `intp` (`meminfo.py:18-30`). The `+1` is the
external (DuckDB-slot) reference. Used at `irr.py:119`.

### `borrow_structref(struct_type, p)` — update/combine/finalize (`meminfo.py:131-140`)
```
_incref_meminfo(p)                                # +1
return _deref_structref_raw_ptr(struct_type, p)   # rebuild structref value
```
`_deref_structref_raw_ptr` (`meminfo.py:114-128`) builds a struct proxy and sets
its `.meminfo` field to `inttoptr(p)` — it does **not** itself incref. The intent
(docstring `meminfo.py:133-138`) is "net-zero for the external owner": the `+1`
here is meant to be cancelled by the borrowed local's scope-exit decref in the
caller. Used at `irr.py:156`, `:178-179`, `:201`.

### `release_meminfo(p)` — destroy only (`meminfo.py:155-162`)
Wraps `_release_meminfo` (`meminfo.py:90-111`), which emits a **direct call to
`NRT_MemInfo_release`** rather than `context.nrt.decref`. Used at `irr.py:220`.

Type guard: every intrinsic that takes a pointer-as-int calls `_require_intp`
(`meminfo.py:67-74`) and rejects anything but `types.intp`, because an int of the
wrong width would `inttoptr` a truncated address. This is why the whole codebase
is rigid about `intp`.


## 5. Why `release_meminfo` is special — the `removerefctpass` interaction

This is the load-bearing, non-obvious mechanism. After lowering, Numba's
`removerefctpass` inspects a function's **argument and return types only**; if
none is NRT-tracked, it strips *every* `NRT_incref`/`NRT_decref` in that function
as dead code. Only functions literally named `NRT_incref`/`NRT_decref` are
strippable; any other `NRT_*` symbol (e.g. `NRT_MemInfo_release`,
`NRT_MemInfo_alloc*`) in the module makes the legalizer bail and **disables the
pass for that whole function** (design doc `test_ducklib.md:103-127`,
intrinsic docstring `meminfo.py:90-100`).

Consequences exploited here:
- The callback impls have signatures like `void(intp, intp, intp)` — **no
  NRT-tracked types** — so the pass runs and strips the structref incref/decref
  pairs inside them.
- `release_meminfo` must therefore use a direct `NRT_MemInfo_release` call: a
  plain `context.nrt.decref` would emit `NRT_decref`, which the pass would strip,
  leaving destroy a no-op and **leaking every group's state**
  (`meminfo.py:92-100`, `test_ducklib.md:124-127`). The direct call both
  survives *and* (by being a non-allowlisted `NRT_*` symbol) disables the pass
  for the destroy impl.

The intended refcount ladder (doc table `test_ducklib.md:113-121`):
init leaves refct 1 (slot owns it), update/combine/finalize are net-zero borrows,
destroy does -1 -> dtor. The doc's own `refcount_of_meminfo` cautionary tale
(`test_ducklib.md:195-199`) documents that you *cannot* observe the intermediate
refct from inside a no-NRT-signature JIT function, because the pass already
stripped the incref that would have bumped it — only Python-side `_read_refcount`
between separate `@njit` calls is reliable.


## 6. Registration (Python side, `irr.py:230-258`)

`register_irr` runs in interpreter (not JIT) code: create the aggregate function
handle, set name, add 4 DOUBLE params + DOUBLE return, then hand DuckDB the five
`@cfunc` addresses via `duckdb_aggregate_function_set_functions`
(`irr.py:244-251`) and the destructor via
`duckdb_aggregate_function_set_destructor` (`irr.py:252`). Logical-type and
function handles are freed through the numpy-buffer destroy idiom
(`irr.py:241-242`, `:257-258`). After `duckdb_register_aggregate_function`
(`irr.py:254`), a plain SQL `SELECT irr(...)` drives the full callback lifecycle.


## 7. Invariants the design relies on

- **Slot holds exactly one live `MemInfo*` at refct 1** between callbacks; the
  structref is otherwise invisible to NRT.
- **`state_size == 8`** must equal `sizeof(intp)`; the `carray((1,), intp)`
  pattern silently assumes an 8-byte slot (`irr.py:106`, `:120`, `:155`).
- **`removerefctpass` is all-or-nothing per function** (strips both incref and
  decref, or neither). The doc states the symmetry is structural and unchanged
  since numba 0.43 (`test_ducklib.md:161-163`).
- **Per-group-constant capture**: `investment`/`target_npv` are taken from the
  first non-NULL row (update, `irr.py:159-162`) or first partial (combine,
  `irr.py:182-185`) and never overwritten; callers must pass identical values for
  every row of a GROUP BY key (`irr.py:18-22`). Wrong input -> silently wrong
  result, no error.
- **Stable `__module__` for the state type**: `IRRStateType` must be defined in an
  *imported* module, not run as `__main__`, or warm-cache type inference fails
  (`irr.py:24-39`).
- **Single release per group**: destroy is assumed to be called exactly once per
  allocated state. A DuckDB path that allocates a state (init) but never destroys
  it leaks; a double-destroy double-frees.


## 8. Fragile / risky points to surface (noted, not audited)

1. **Borrow refcount-neutrality depends on inlining, and the doc's accounting may
   not match the actual numbox code.** The doc table (`test_ducklib.md:115-121`)
   treats the borrow/export increfs as living *inside* the no-NRT callback impl,
   where `removerefctpass` strips them symmetrically with the local's scope-exit
   decref -> net 0. But in numbox, `export_meminfo` and `borrow_structref` are
   **separate `@njit` units whose signatures contain NRT-tracked types**
   (`export_meminfo: intp(StructRef)`, `borrow_structref: StructRef(TypeRef,
   intp)`). For such signatures `removerefctpass` is **disabled**, so their
   `_incref_meminfo` provably *survives* (numbox docstrings even state "+1
   incref", `meminfo.py:144-152`, and "net-zero ... local decref on drop balances
   the incref", `meminfo.py:133-138`). For the net to actually be zero in
   update/combine/finalize, the caller's scope-exit decref of the borrowed local
   must survive too — but those callback impls have no-NRT signatures, so that
   decref is a strip candidate. The reconciliation requires the helper to be
   **inlined into the callback impl before `removerefctpass` runs** (so both ops
   sit in one function and are stripped/kept together), OR for some other NRT
   symbol in the impl to disable the pass. This inlining is not forced anywhere
   visible; numbox helpers are plain `@njit`. If a numba version or compile flag
   declines to inline them, the surviving helper-incref + stripped caller-decref
   yields **+1 refct per borrowed row** (a leak), or the mirror error frees early.
   A reviewer should empirically measure per-row net refct in update/combine, not
   trust the table.

2. **Different stages are kept correct by different mechanisms.** IRR's `update`
   calls `vector_push` (`irr.py:157-158`) which can trigger a vector realloc ->
   `NRT_MemInfo_alloc*` in the impl -> pass disabled -> borrow incref + scope
   decref both survive -> net 0. The simpler Welford `update` (pure arithmetic,
   no alloc) instead relies on the strip-everything path. So the *same* borrow
   contract is upheld by opposite mechanisms depending on whether the body
   happens to allocate. Fragile and easy to break with an innocuous edit.

3. **Whole pattern is explicitly numba-version-fragile.** The doc itself warns
   that if a future numba changes the pass to be asymmetric, both the direct-call
   and the symmetric-strip approaches break (`test_ducklib.md:161-163`). The
   `_require_intp`/`intp`-everywhere rigidity is the only static guard;
   refcount correctness has no compile-time check.

4. **No NULL/empty-group result error path beyond NaN.** If every row of a group
   is NULL-skipped, the vector stays empty and finalize writes `NaN`
   (`irr.py:203-204`); combine of two empty partials leaves `initialized == 0`.
   Correct by design, but means input errors surface only as `NaN`, never as an
   exception.

5. **`@cfunc` cannot propagate Python exceptions to DuckDB.** Any error raised
   inside an impl (e.g. a bad pointer) crosses the C boundary as undefined
   behavior; there is no try/except in the impls. Out-of-bounds in the manual
   `carray` sizing (`offset + count` in finalize, `irr.py:197`) would corrupt
   memory rather than raise.

6. **Array-state variant uses a deliberately different bridge** (store both
   `meminfo_p` and `data_p`, incref via the `@intrinsic` `_incref_meminfo` so it
   inlines, access via `carray`) precisely because the `@njit`-wrapper incref
   gets stripped asymmetrically for alloc-bearing functions
   (`test_ducklib.md:143-159`). This confirms point 1 is a real, known hazard the
   authors already had to work around once.
