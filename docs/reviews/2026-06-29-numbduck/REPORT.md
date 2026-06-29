# numbduck code review — 2026-06-29

This report documents a full-surface engineering review of **numbduck**, the library that
adapts DuckDB's C API for use inside numba `@njit` code on top of the sibling **numbox**
bindings toolkit. The review covered the runtime library, the example UDF/UDAF patterns,
the integration test suite, the CI/packaging configuration, and the in-repo documentation.
It produced 92 findings, every one of which was independently and adversarially verified
(verifier default stance: refute unless the evidence is conclusive). 89 findings were
confirmed and 3 were refuted. No critical defects were found; the confirmed set is 0
critical / 5 high / 33 medium / 51 low.

- **Repo:** numbduck, branch `review/numbduck-2026-06-29` (based on `origin/main` 6b32f03)
- **Size:** ~6,800 Python LOC
- **Scope:** 34 review units across the source/example/test/build targets x six dimensions
  (memory, correctness, security, design, testing, documentation), plus 9 architecture
  notes. numbox was examined only at the interface boundary it presents to numbduck; numbox
  has its own separate review.
- **Results:** 92 findings — **89 confirmed** (0 critical / 5 high / 33 medium / 51 low),
  **3 refuted**. Verification is complete: every finding carries a verdict.

A companion remediation plan, grouping every confirmed finding into 23 concrete tasks, lives
in `numbduck-review.tasks.json` alongside this report. Task ids (T1-T23) are referenced from
the cluster headings below.

---

## Architecture

numbduck is a **thin binding layer, not a framework**. Its one structural job is to make
each DuckDB C API function callable from inside numba `@njit` code at native speed, with no
Python or ctypes crossing per call. Everything else — scalar UDFs, prepared-statement query
loops, aggregate UDAFs — is application code built on top of those bindings and lives in
`examples/` and `test/`, not in the package. The package has only four source modules:
`utils.py` locates and `dlopen`s `libduckdb` with `RTLD_GLOBAL` (so its symbols enter the
process-global table where LLVM's JIT linker can resolve them); `ducklib.py` is the binding
table (~200 DuckDB functions, each registered into numbox's global `signatures` dict and
wrapped with `@cres` + `_call_lib_func`); `duckdb_utils.py` provides `@njit` allocators that
return numpy buffers sized to DuckDB's out-param structs; and `pybridge.py` recovers the raw
C `Connection*` out of a live Python `duckdb` connection. The whole adaptation rests on
numbox's bindings toolkit — `cres`, `_call_lib_func`/`_call_lib_func_byval`, the shared
`signatures` dict, the meminfo bridge, `_cast_int_to_void_p`, `get_unicode_data_p`,
`make_structref` — and adds no new lowering machinery of its own. Two load-bearing
invariants underpin everything: `libduckdb` must be loaded `RTLD_GLOBAL` before any `@cres`
wrapper compiles (guaranteed by `duckdb_lib = load_duckdb()` at `ducklib.py:11`), and every
pointer is uniformly modelled as `intp`, which assumes a 64-bit host.

The connection bridge is the project's most fragile boundary. Because the DuckDB Python wheel
exposes no way to obtain the C-API `duckdb_connection` from its pybind11
`DuckDBPyConnection` object, `pybridge.extract_connection_ptr` walks **hardcoded byte
offsets** into that object's memory — `id(conn)+16` to reach the C++ object, then `+32` to
read the `unique_ptr<Connection>` — and validates the result by running `SELECT 1` through
the C API before returning. Those offsets were validated only on duckdb 1.3.2 / Linux
x86-64 / libstdc++, yet the project supports `duckdb>=1.3.2,<1.6` and a macOS fallback path,
so any layout shift across that range yields a wrong pointer that the `SELECT 1` probe
cannot reliably catch. DuckDB out-params are modelled as zero-initialized numpy `int64`
buffers whose `.ctypes.data` address is passed as an `intp`; the only size that matters is
the 48-byte (`6xint64`) `duckdb_result` struct, every other handle being a single pointer.

Three UDF patterns are built on this spine, all sharing one rule: DuckDB calls raw C
function pointers and `@cfunc` bodies cannot `import` or use rich numba features, so real
work lives in a module-level `@njit` impl and a thin `@cfunc` trampoline forwards to it, with
`intp` (never `voidptr`) pointer signatures bridged to `carray` via `_cast_int_to_void_p`.
**Pattern A (scalar chunk callback)** registers a JIT function whose callback runs once per
~2048-row data chunk, reading each input vector's raw buffer directly and writing the output
vector in one tight loop (`haversine.py`, `fraud_score.py`). **Pattern B (JIT query loop)**
runs an entire per-event loop inside one `@njit(nogil=True)` function that drives a prepared
statement — bind, execute, fetch chunk, read, destroy — with zero Python per iteration so it
scales across threads (`online_scoring.py`). **Pattern C (structref-backed UDAF)** is the
most intricate: DuckDB hands each aggregate group an 8-byte opaque slot, and numbduck stores
a numba structref there by smuggling its NRT `MemInfo*` through that `void*`, using numbox's
`export_meminfo`/`borrow_structref`/`release_meminfo` to move ownership across the six C
callbacks. Its correctness depends on numba's `removerefctpass` being effectively disabled
for the callback functions (so the increfs/decrefs that balance the borrow survive), and on
`release_meminfo` calling `NRT_MemInfo_release` directly rather than `context.nrt.decref`
(`irr.py`, design doc `test/test_ducklib.md`).

| Subsystem | What it covers | Note |
|---|---|---|
| Overall architecture & the three UDF patterns | End-to-end mechanics, invariants, fragile spots | [understanding/architecture.md](understanding/architecture.md) |
| Shared-library loading & platform handling | `utils.py` load path, macOS standalone/download fallback | [understanding/loader.md](understanding/loader.md) |
| Binding layer | `ducklib.py` signatures, `@cres`, ABI lowering via numbox | [understanding/bindings.md](understanding/bindings.md) |
| Connection bridge & C-struct buffers | `pybridge.py` offsets, `duckdb_utils.py` allocators | [understanding/bridge.md](understanding/bridge.md) |
| numbox boundary contract | Every numbox symbol numbduck depends on | [understanding/numbox-surface.md](understanding/numbox-surface.md) |
| Structref-backed UDAF lifecycle | meminfo bridge, `@cfunc`/`@njit` callbacks, `removerefctpass` | [understanding/udaf.md](understanding/udaf.md) |
| Examples as usage contracts | The four reference UDF/UDAF examples | [understanding/examples.md](understanding/examples.md) |
| Test architecture & coverage shape | `test/test_ducklib.py` structure and gaps | [understanding/tests.md](understanding/tests.md) |
| Packaging, CI, versioning | `pyproject.toml`, the GitHub Actions workflows | [understanding/build.md](understanding/build.md) |

---

## Findings by severity

Findings are grouped into the remediation clusters used in `numbduck-review.tasks.json`.
Within each cluster they are listed strictly by effective severity (high -> medium -> low).
Each entry gives the finding id, `file:line`, its effective severity, a short description of
the defect and its impact, and the recommended fix.

### High-priority clusters

#### macOS dual-runtime handle mismatch (T1)

- **XC-ver-3** — `numbduck/utils.py:113` (high). On macOS with a C-API-stripped duckdb-python
  wheel (>=1.4.1), `load_duckdb()` loads the wheel library `RTLD_GLOBAL` first and then loads a
  *second*, full `libduckdb` `RTLD_GLOBAL`, so two DuckDB runtimes coexist in one process. The
  Python `duckdb` module's connections live in the wheel runtime, while numbduck's bindings
  may resolve to the other; `extract_connection_ptr` then passes a `Connection*` across the
  runtime boundary — undefined behavior that can corrupt state or crash on exactly the
  platform the fallback targets. *Fix:* ensure both the Python module and numbduck bind to a
  single library when the wheel lacks symbols, or have pybridge detect the dual-runtime
  condition and refuse rather than mix handles.

#### Runtime duckdb version/layout guard for pybridge (T2)

- **PBR-DES-1** — `numbduck/pybridge.py:31-34, 58-62` (high). The `+16`/`+32` offsets are
  hardcoded and validated only on duckdb 1.3.2, but nothing reads `duckdb.__version__` to
  confirm the running version matches; on a supported-but-unvalidated 1.4/1.5 release the code
  silently uses 1.3.2 offsets. *Fix:* gate the offsets behind a per-version lookup so an
  unknown version fails loudly; centralize the verified-version constant beside the offsets.
- **PBR-COR-1** — `numbduck/pybridge.py:62` (medium). The `+32` member offset depends on a
  specific libstdc++ smart-pointer layout and member ordering; any 1.4/1.5 reorder or a
  libc++/MSVC build shifts it, and the read returns whatever bytes sit there with no
  assertion. *Fix:* either tighten the dependency to the validated ABI or add a version-gated
  layout assertion at import/use.
- **PBR-DES-3** — `numbduck/pybridge.py:59-67` (medium). No NULL/sanity check guards the two
  intermediate pointers before they are dereferenced and handed to `duckdb_query`; the
  `SELECT 1` net only catches pointers that survive a query, so the most likely layout-drift
  failures segfault instead of raising. *Fix:* add `if not cpp_obj_p` / `if not conn_ptr`
  guards that convert the common NULL cases into a clean `RuntimeError`.
- **XC-ver-1** — `numbduck/pybridge.py:59` (medium). Same hardcoded offsets are also wrong on
  Windows/MSVC (different pybind11 header and smart-pointer sizes) and unverified on the 1.4
  packaging reorg, yet macOS/Windows and 1.4/1.5 are all in scope. *Fix:* gate extraction on
  `(version, platform, C++ runtime)` and refuse un-validated combinations; strengthen
  validation beyond `SELECT 1` (alignment/range sanity before the first dereference).

#### CI matrix coverage (T3)

- **BLD-DES-1** — `.github/workflows/numbduck_ci.yml:22` (high). No workflow includes a macOS
  runner, so the entire macOS-only loader surface (standalone-dylib discovery, the network
  download fallback, the versioned cache, the double-load interaction) — the most failure-prone
  part of the project — has zero automated coverage. *Fix:* add at least one `macos-latest`
  (ARM64) job that exercises `load_duckdb` end-to-end, ideally with `NUMBDUCK_LIBDUCKDB`
  pointing at a brew-installed library so the standalone path runs deterministically.
- **BLD-DES-2** — `pyproject.toml:26` (high). The `benchmark` marker is described as
  "deselected in CI", but nothing implements deselection (no `addopts`, no `conftest.py`), and
  CI runs a bare `pytest`; `test_udf_benchmark` hard-codes 10K/100K/1M row counts (the 1M
  Python path alone is ~2 minutes) and therefore runs in full on every matrix job. *Fix:* add
  `addopts = "-m 'not benchmark'"` (or `pytest -m "not benchmark"` in CI) and correct the
  marker description.
- **BLD-DES-6** — `.github/workflows/numbduck_ci.yml:29` (low). Windows is restricted to a
  single slice (Python 3.11 x duckdb 1.5.1), so the Windows-specific struct-by-value ABI
  branches are validated against one Python and one duckdb version. *Fix:* document the
  tradeoff and consider one Windows job at the duckdb floor (1.3.2) to exercise the lower
  bound on the divergent ABI.

#### CLAUDE.md accuracy (T4)

- **DOC-acc-2** — `CLAUDE.md:55-62` (high). The "Follow-ups" section directs the reader to
  migrate hand-rolled intrinsics "at `ducklib.py:1525-1640`", but that migration already
  landed: the file is 1431 lines, there are zero `@intrinsic` definitions, and
  `duckdb_bind_hugeint/uhugeint/interval/decimal` are ordinary `_call_lib_func` wrappers. The
  cited range is past EOF. *Fix:* delete the section (the work is done) or replace it with the
  remaining open numbox dependency.
- **DOC-acc-1** — `CLAUDE.md:43-53` (medium). The "Struct-by-value helpers (ducklib.py)"
  section names five helpers (`_call_lib_func_struct_in/out`, `_emit_byval_call`,
  `_build_packed_interval`, ...) that do not exist in `ducklib.py`; the real ABI logic lives in
  numbox. *Fix:* rewrite to reflect that `ducklib.py` only imports `_call_lib_func` /
  `_call_lib_func_byval` from numbox.
- **DOC-acc-3** — `CLAUDE.md:53` (medium). Claims "custom `@intrinsic` functions are used for
  decimal/varint/interval"; `ducklib.py` defines no intrinsics — these route through numbox's
  generic `_call_lib_func` classification. *Fix:* state that decimal/varint/interval are
  handled by numbox's generic path (byval+optnone on SysV; the interval eightbyte repack).
- **DOC-acc-4** — `CLAUDE.md:16` (medium). Lists `numbox~=0.5.6`, but `pyproject.toml` pins
  `numbox~=0.5.11`; `examples/README.md` adds a third (0.5.8) reference. *Fix:* update to
  `numbox~=0.5.11` and keep in sync with pyproject (the source of truth).
- **DCK-wrap-DES-1** — `CLAUDE.md:55-62` (medium). Duplicate confirmation of the stale
  "Follow-ups" section from the binding-layer perspective: a developer will hunt for ~116
  lines of intrinsics at a nonexistent location. *Fix:* delete or replace with a one-line note
  that the binds now route through `_call_lib_func`.
- **DCK-wrap-DES-2** — `CLAUDE.md:43-53` (medium). Duplicate confirmation that the struct
  helper list attributes a substantial ABI surface to `ducklib.py` that lives entirely in
  numbox; the only by-value helper used is the imported `_call_lib_func_byval` (3 call sites).
  *Fix:* rewrite the section and move the ABI rationale into numbox docs with a cross-link.
- **DOC-acc-5** — `CLAUDE.md:30` (low). "Adding a New Binding" step 1 says to verify docstring
  links against `duckdb.h` line numbers, contradicting step 5 (docstrings must link to
  `api.html`); the code follows step 5. *Fix:* reword step 1 to "verify the signature against
  duckdb.h" and drop the docstring-link clause.
- **BLD-DES-5** — `CLAUDE.md:16` (low). The documented numbox pin (0.5.6) understates the real
  floor (0.5.11). *Fix:* update to `numbox~=0.5.11`, or reference the pin location rather than
  restating the value.

### Medium-priority clusters

#### Downloaded-library integrity & version match (T5)

- **DCK-wrap-SEC-1** — `numbduck/utils.py:83` (medium). The macOS download path
  (`urlopen(url).read()` -> unzip -> `dlopen` `RTLD_GLOBAL`) performs no integrity check on the
  bytes (no SHA-256, no signature, no size sanity); TLS is the only guarantee, so a
  compromised release asset or tampered cache file becomes arbitrary code execution. *Fix:*
  ship a per-version SHA-256, verify before extracting/loading, and re-verify on each cache
  hit.
- **UTL-SEC-1** — `numbduck/utils.py:83-90` (medium). Same root issue from the loader side,
  with detail: `NUMBDUCK_LIBDUCKDB_DOWNLOAD=1` silently authorizes the unverified download and
  the interactive prompt does not disclose that the artifact is unverified. *Fix:* pin and
  verify the digest before load, do not let the env var bypass the check, and make the prompt
  state the verification status.
- **XC-ver-2** — `numbduck/utils.py:44` (medium). Standalone/Homebrew/env libraries are never
  version-checked against `duckdb.__version__`; the only acceptance test is a single-symbol
  probe. A brew vs pip version skew silently loads a mismatched runtime whose struct/enum
  layout may differ. *Fix:* query `duckdb_library_version()` after loading and warn/raise on
  mismatch.
- **UTL-SEC-2** — `numbduck/utils.py:41-43` (low). A cache hit loads the dylib on existence
  alone with no re-verification, the cache dir is created with default umask, and the file is
  written non-atomically; a one-shot tamper persists across all future imports. *Fix:*
  re-verify the pinned digest on every cache hit, create the dir `0o700`, and write via temp +
  `os.replace`.
- **BLD-SEC-3** — `.github/workflows/numbduck_ci.yml:67` (low). numbox resolves through the
  `~=0.5.11` range with no lock and no `--require-hashes`, so a new/compromised 0.5.x release
  runs in CI with no repo change. *Fix:* pin numbox exactly in the CI install or adopt a
  hash-pinned constraints file.

#### pybridge input validation (T6)

- **PBR-SEC-1** — `numbduck/pybridge.py:52` (medium). The lone `isinstance` guard is spoofable
  via a `__class__` property; a crafted non-connection object then drives `id+16`/`+32`
  pointer arithmetic into an arbitrary memory read before `SELECT 1` ever runs. *Fix:* use an
  exact-type check, `type(conn) is duckdb.DuckDBPyConnection`.
- **PBR-SEC-2** — `numbduck/pybridge.py:62` (medium). A closed connection leaves the
  `unique_ptr<Connection>` reset, so the read yields NULL (`c_void_p(0).value is None`),
  producing a misleading exception, a `None` return, or a use-after-free inside the validation
  query. *Fix:* reject closed connections via a liveness probe and explicitly test for NULL
  before issuing the query.
- **PBR-SEC-3** — `numbduck/pybridge.py:67` (medium). On any layout change the wild
  `conn_ptr` is passed straight into `duckdb_query`, where it is dereferenced; the `rc` check
  only catches pointers that survive a full query, so it is a smoke test, not the
  memory-safety validation the docstring implies. *Fix:* pin/verify the duckdb version, add a
  cheap address sanity check, and reword the docstring.
- **PBR-COR-3** — `numbduck/pybridge.py:62` (low). `c_void_p.from_address(...).value` returns
  `None` (not 0) for a NULL slot, so `None + 32` raises a bare `TypeError`, contradicting the
  documented `TypeError`/`RuntimeError` contract. *Fix:* treat a `None`/0 read as a validation
  failure and raise the documented `RuntimeError`.
- **XC-err-1** — `numbduck/pybridge.py:62` (low). A closed/null-but-type-correct connection
  surfaces as a numba unboxing error or a `TypeError` that bypasses the `rc` check entirely,
  so callers catching `RuntimeError` per the docstring miss it. *Fix:* guard `cpp_obj_p` and
  `conn_ptr` and funnel all failure modes into the documented `RuntimeError`.

#### idx_t signedness & validity pointer typing (T7)

- **DCK-sig-COR-1** — `numbduck/ducklib.py:105` (low). Six entries map DuckDB's unsigned
  `idx_t` to signed `intp` (`duckdb_column_count`, `duckdb_row_count`,
  `duckdb_data_chunk_get_column_count`/`get_size`, the index arg of `get_vector`, and the row
  arg of `validity_row_is_valid`) while siblings correctly use `uint64`; ABI-neutral on
  64-bit but a numba int64/uint64->float64 unification footgun. *Fix:* type every `idx_t` as
  `uint64`.
- **DCK-sig-COR-2** — `numbduck/ducklib.py:236` (low). `duckdb_vector_get_validity` returns a
  `uint64_t*` but is typed `uint64(intp)`, breaking the pointer-as-`intp` convention used by
  the adjacent `duckdb_vector_get_data`. *Fix:* type the return as `intp`.
- **DCK-wrap-COR-1** — `numbduck/ducklib.py:105, 155, 156, 157, 225` (low). Duplicate of the
  `idx_t`->`intp` inconsistency from the wrapper perspective. *Fix:* type all `idx_t`
  returns/args as `uint64`.
- **DCK-wrap-COR-2** — `numbduck/ducklib.py:236` (low). Duplicate of the validity-pointer
  mistype. *Fix:* return `intp`.
- **XC-numbox-COR-1** — `numbduck/ducklib.py:236` (low). The `uint64` validity return forces
  an unsafe `uint64->int64` coercion when fed to `duckdb_validity_row_is_valid` (whose validity
  arg is `intp`), violating numbox's all-pointers-are-`intp` contract. *Fix:* set the
  signature to `intp(intp)`.
- **XC-numbox-COR-2** — `numbduck/ducklib.py:233` (low). Duplicate of the `idx_t`->`intp`
  inconsistency cross-checked against numbox's convention. *Fix:* type the listed
  returns/args as `uint64`.

#### CI workflow hardening (T8)

- **BLD-SEC-1** — `.github/workflows/numbduck_ci.yml:53` (medium). The version step
  interpolates `${{ github.ref_name }}` directly into a `run:` script and into
  `numbduck/__init__.py`; a ref name containing shell metacharacters yields command execution
  / Python injection. Currently latent (no `tags:` trigger), but the block exists to be used.
  *Fix:* pass the ref through an `env:` block, reference the quoted shell variable, and
  validate against a strict regex.
- **BLD-DES-4** — `.github/workflows/numbduck_ci.yml:61` (medium). numba and duckdb are pinned
  to matrix values but numbox is left floating via the editable install, so a new numbox 0.5.x
  patch can flip CI with no repo change — exactly the dependency numbduck is most tightly
  coupled to. *Fix:* `pip install "numbox==<floor>"` (or a numbox matrix dimension) before the
  editable install.
- **BLD-SEC-2** — `.github/workflows/numbduck_ci.yml:43` (low). Actions are pinned to mutable
  major tags (`@v6`, the third-party `lychee-action@v2`) rather than commit SHAs. *Fix:* pin
  to full 40-char SHAs with a version comment and adopt Dependabot; prioritize the third-party
  action.

#### Committed `__version__` (T9)

- **BLD-DES-3** — `numbduck/__init__.py:1` (medium). The committed `__version__ = ""` makes a
  local `python -m build` / `pip install -e .` resolve to an empty version; only CI's overwrite
  step produces a meaningful value. *Fix:* commit a real fallback (e.g. `"0.0.0.dev0"`) or move
  version resolution to setuptools-scm.

#### DuckDB handle leaks in tests (T10)

- **TST-MEM-1** — `test/test_ducklib.py:107-137, 153-160` (medium). `aux_query_1` returns an
  `out_result` that `test_query` and the column/row-count test tear down without ever calling
  `duckdb_destroy_result`, leaking the result's backing memory (sibling tests destroy it
  correctly). *Fix:* destroy the result before `aux_close_db`, or give `aux_query_1` a paired
  teardown helper.
- **TST-MEM-2** — `test/test_ducklib.py:172-236` (medium). Five consumers of
  `aux_get_data_vector` destroy neither the fetched chunk nor the result. *Fix:* destroy chunk
  and result before `aux_close_db`, or return a teardown closure.
- **TST-MEM-3** — `test/test_ducklib.py:1163-2599` (medium). The NRT/meminfo side has strong
  alloc==free leak accounting, but the DuckDB C-handle side has none, so a missing-destroy
  regression (as in TST-MEM-1/2) or a double-free passes silently — the `test_get_value_type`
  double-free hazard is only a comment. *Fix:* add a lightweight C-handle balance harness and
  fix the omitted destroys so the suite acts as a teardown guard.

#### UDAF `@cfunc` exception guard (T11)

- **XC-numbox-MEM-1** — `examples/irr.py:156` (medium). None of the six callback impls wrap
  their body in try/except; a `MemoryError` from a `vector_push`/`vector_extend` realloc (or
  any raise) while a borrowed structref `s` is live skips the scope-exit decref, leaking the
  group's MemInfo, and the `@cfunc` swallows the exception so DuckDB returns a silently wrong
  aggregate. *Fix:* wrap each impl body in a bare try/except (not try/finally, which reraises
  on numba 0.65.1), releasing/decrefing the borrowed state and signalling failure on the error
  path.

#### irr convergence & bracket (T12)

- **EX-irr-COR-1** — `examples/irr.py:82-94` (medium). `irr_bisect` converges only on the
  absolute test `abs(npv) < 1e-9`, whose floor scales with `|dNPV/dr|`; for realistic
  (large-magnitude) cashflows the gate can never be met even after `r_mid` has converged to
  float resolution, so the function returns `NaN` despite finding the root — indistinguishable
  from the documented empty-group `NaN`. *Fix:* converge on the bracket width in `r` and return
  `(r_lo + r_hi)/2` after the loop, or make the NPV gate relative to scale.
- **EX-irr-COR-2** — `examples/irr.py:80-81` (low). The bracket `[-0.99, 10.0]` assumes a sign
  change; an out-of-range-but-valid IRR returns `NaN`, again indistinguishable from "no data".
  *Fix:* document the monthly-rate assumption and detect the no-sign-change case distinctly.

#### Loader legacy-cache & lib-preference (T13)

- **UTL-COR-1** — `numbduck/utils.py:40` (medium). The legacy-cache `os.remove` is unguarded
  and runs before the versioned-cache and Homebrew checks, so an un-removable stale file aborts
  loading even when a usable library is two lines further down. *Fix:* wrap the removal in
  `try/except OSError`, or run cleanup only after a library is located.
- **UTL-COR-2** — `numbduck/utils.py:98` (low). `find_duckdb_shared_lib` returns the 1.3.x
  package-dir match before checking the 1.4+ `_duckdb*` site-packages library, and is
  strict-singleton, so a transitional layout can pick the wrong library or raise despite a
  valid one being present (regexes are also not end-anchored). *Fix:* make selection
  version-aware, anchor the regexes, and pick deterministically on multiple matches.

#### Design-doc citations (T14)

- **TMD-DOC-1** — `test/test_ducklib.md:51` (medium). The Welford callback cite
  `L3075-L3176` points at the unrelated array-state variant; the real callbacks are
  ~`L3273-L3375`. *Fix:* update both cites (lines 51 and 75).
- **TMD-DOC-2** — `test/test_ducklib.md:192` (medium). The registration cite `L3280-L3304`
  lands on init callbacks, not registration (actual ~`L3477-L3500`). *Fix:* update both cites.
- **TMD-DOC-3** — `test/test_ducklib.md:199` (medium). The
  `test_structref_meminfo_bridge_refcount_ladder` anchor `L3179` lands in an unrelated combine
  impl; the function is at `L2999`. *Fix:* change the anchor to `L2999`.
- **TMD-DOC-4** — `test/test_ducklib.md:197` (low). The commented-intrinsic cite
  `L2877-L2914` starts on live code and truncates the example. *Fix:* adjust to ~`L2882-L2919`.

#### Orphaned EOF wrappers (T15)

- **DCK-sig-DES-1** — `numbduck/ducklib.py:1398-1432` (medium). Four bind wrappers and two
  scalar wrappers sit appended at EOF, far from their families (residue of the
  hugeint/interval/decimal migration), while their signatures are grouped correctly — so the
  dict order and wrapper order disagree and a maintainer scanning the bind run sees only 18 of
  22. *Fix:* relocate the four binds into the bind run and the two scalar wrappers into the
  scalar block, and move the trailing sig keys into the scalar sig block.

### Low-priority clusters

#### Test coverage gaps (T16)

- **TST-cover-1** — `test/test_ducklib.py:1706` (medium). `test_struct_size_guard` only asserts
  pure-Python byte arithmetic and never drives any ducklib binding or the <=16B/>16B
  classification, giving false ABI confidence. *Fix:* rename to reflect what it tests, or make
  it round-trip a >16B struct (decimal/varint) through the intrinsic path.
- **TST-cover-2** — `test/test_ducklib.py:212` (medium). Only single-chunk results and
  exhaustion are tested; no result exceeds one 2048-row vector, so the real multi-chunk loop is
  never driven. *Fix:* add a `SELECT * FROM range(5000)` test that loops `duckdb_fetch_chunk`
  until null and asserts values across a chunk boundary.
- **TST-cover-3** — `test/test_ducklib.py:891` (medium). Decimal tests only exercise the int64
  physical path (width <=18); the int128/hugeint-backed DECIMAL(19..38) readback is untested.
  *Fix:* add a DECIMAL(38,x) bind+execute reading the column back as a 16-byte int128, plus a
  `get_decimal` round-trip exceeding 2**63.
- **TST-cover-4** — `test/test_ducklib.py:628` (low). The invalid-param-index error branch is
  covered for only a few bind wrappers; the struct binds (hugeint/interval/decimal) and most
  scalars lack it. *Fix:* parametrize a single invalid-index check across all `bind_*`
  wrappers.
- **TST-cover-5** — `test/test_ducklib.py:1634` (low). `test_create_array_value` asserts only
  `av != 0` and never verifies the contained values. *Fix:* read the array back and assert its
  elements, mirroring the list/struct tests.
- **TST-cover-6** — `test/test_ducklib.py:1687` (low). `duckdb_get_value_type` is checked only
  for non-null, not for the correct type id. *Fix:* assert
  `duckdb_get_type_id(type_p) == DUCKDB_TYPE_INTEGER`.
- **TST-cover-7** — `test/test_ducklib.py:1359` (low). `test_create_get_bit` asserts only the
  size, not the round-tripped bytes. *Fix:* read the payload and assert it equals the input,
  matching the blob/varint tests.
- **TST-COR-1** — `test/test_ducklib.py:3255` (low). No test ever reads `WelfordState.mean`
  after a combine, so a sign/precedence regression in the merged-mean line would pass silently
  (finalize depends only on m2 and count). *Fix:* assert the combined `.mean` against the
  pooled mean.
- **TST-COR-2** — `test/test_ducklib.py:267` (low). `aux_read_inline_string`/the blob reader
  assume the inlined `string_t` layout (length <=12); for longer strings the characters live
  behind a pointer at offset 8, so the helpers would return prefix+pointer bytes. Current tests
  pass only because every value is <=12 bytes. *Fix:* document the <=12-byte precondition or
  branch on length.

#### Example UDF hardening (T17)

- **EX-os-COR-1** — `examples/online_scoring.py:117-123` (low). The loop discards the
  `duckdb_state` returns of `bind_int64`/`execute_prepared`, so an execute failure feeds a
  garbage result struct to `fetch_chunk` instead of aborting — a deviation from the stated
  C-style return-code contract in the reference example. *Fix:* check both return codes before
  fetching.
- **EX-os-COR-2** — `examples/online_scoring.py:123-136` (low). The fetched chunk is read with
  no `chunk_p != 0` or row-count check; correct only because the data guarantees exactly one
  matching row. *Fix:* guard `if chunk_p == 0` and document the one-row precondition.
- **EX-os-MEM-1** — `examples/online_scoring.py:123-136` (low). The same unchecked chunk read
  dereferences NULL inside `nogil` JIT on a miss/empty/error result — a hard segfault. *Fix:*
  check `chunk_p`/`get_size`, skip with a sentinel, and still destroy the result.
- **EX-os-MEM-2** — `examples/online_scoring.py:155-161` (low). `duckdb_prepare` allocates a
  statement object even on error, but the `assert rc == DuckDBSuccess` precedes
  `duckdb_destroy_prepare`, so a failed prepare leaks it (no try/finally). *Fix:* wrap
  prepare+loop in try/finally so destroy always runs.
- **EX-hav-1** — `examples/haversine.py:105-113` (low). The chunk loop reads input vectors with
  no validity check, so a NULL input is read as a garbage double; safe only for the dense
  generated data, but this is the reference scalar-UDF pattern. *Fix:* document the non-NULL
  precondition or fetch validity masks as `irr.py` does.
- **EX-hav-MEM-1** — `examples/haversine.py:134-137` (low). `duckdb_destroy_scalar_function`
  runs after the `assert rc == DuckDBSuccess`, so a failed `register` leaks the builder handle.
  *Fix:* destroy `func_p` before asserting or wrap register/assert in try/finally.
- **EX-fraud-1** — `examples/fraud_score.py:118-144` (low). Same no-validity scalar-UDF gap as
  haversine. *Fix:* document the dense-data precondition or fetch validity per input.
- **EX-fraud-MEM-1** — `examples/fraud_score.py:125-144` (low). The loop folds uninitialized
  NULL-row bytes into the score (in-bounds read, wrong value). *Fix:* add an inline non-NULL
  comment or fetch validity and propagate NULL.
- **EX-irr-MEM-1** — `examples/irr.py:170-190, 300-346, 372-377` (low). The NRT alloc/free leak
  guard never exercises `_irr_combine_impl` because all test datasets are tiny and run
  single-threaded, leaving the combine borrow/release balance (where the subtlest hazards live)
  unchecked. *Fix:* force parallel aggregation (raise threads / enlarge the table) so combine
  runs inside the guarded region.

#### Example test helpers (T18)

- **EX-common-1** — `examples/_common.py:45-60` (low). `format_table` never checks that each
  row's cell count matches the header count: a longer row raises `IndexError`, a shorter one is
  silently truncated by `zip`. *Fix:* validate `len(row) == len(headers)` alongside the
  alignment check.
- **EX-common-2** — `examples/_common.py:73` (low). The cross-check uses `first != other`, so
  identical `NaN` results spuriously fail (and numpy arrays raise an ambiguous-truth error).
  *Fix:* use a NaN-aware comparison (treat both-NaN as equal) and document scalar-only support.

#### ducklib cleanup (T19)

- **DCK-sig-DES-2** — `numbduck/ducklib.py:121` (low). `duckdb_connect` splits the otherwise
  alphabetical `create_*` run. *Fix:* move it to its sorted slot and keep the dict and wrappers
  consistently ordered.
- **DCK-sig-DES-3** — `numbduck/ducklib.py:13` (low). `_is_win = sys.platform == "win32"` and
  the `import sys` are dead (all platform branching lives in numbox). *Fix:* remove both.
- **DCK-sig-DES-4** — `numbduck/ducklib.py:727, 1052` (low). The two varint docstrings link to
  a version-pinned `docs/1.3/...api#...` URL without `.html`, deviating from the canonical
  `docs/stable/...api.html#...` form. *Fix:* normalize both links.
- **DCK-sig-DES-5** — `numbduck/ducklib.py:77-1432` (low). The function name is repeated five
  times per binding; the sig-key/`@cres`/`_call_lib_func`/def-name quartet is checked at
  import, but the docstring URL is not (the source of DCK-sig-DES-4). *Fix:* add a tiny test
  asserting each wrapper's docstring anchor matches its def name.
- **DCK-wrap-DES-3** — `numbduck/ducklib.py:275-1431` (low). All ~190 wrappers are pure
  boilerplate with the name repeated; a typo in the `_call_lib_func("...")` literal surfaces
  only at runtime. *Fix:* add a test asserting the call string equals the def name and is a
  registered signature key (preferred over a generator, which would lose the mandated
  per-function docstrings).
- **DCK-wrap-DES-4** — `numbduck/ducklib.py:727` (low). Duplicate of the varint
  non-canonical-URL issue from the wrapper view. *Fix:* normalize both links (or comment the
  exception if deliberate).
- **DCK-wrap-COR-3** — `numbduck/ducklib.py:727, 1052` (low). Duplicate of the varint URL
  deviation cross-checked against the convention. *Fix:* normalize both links.

#### utils refactor (T20)

- **UTL-DES-1** — `numbduck/utils.py:41, 87, 88` (medium). The cached-dylib filename literal is
  duplicated across producer and consumer; a mismatch silently breaks the cache hit. *Fix:*
  factor `_LIBDUCKDB_FILENAME`/`_LIBDUCKDB_CACHE_FILE` constants and reference them.
- **UTL-DES-2** — `numbduck/utils.py:50-91` (medium). `_download_libduckdb` does consent,
  fetch, unzip, and cache-write in one body with no injectable collaborators, so the
  extract-and-cache logic cannot be unit-tested without monkeypatching `input`/`urlopen`/module
  globals. *Fix:* split into a pure-policy consent function and a `_fetch_and_cache(url, dest)`
  taking URL and destination as parameters.
- **UTL-DES-3** — `numbduck/utils.py:37-40` (medium). The "find" locator mutates the filesystem
  (unconditional, unguarded `os.remove` of the legacy cache) on every fallback load, including
  on Linux/Windows where it is meaningless, and an OSError aborts loading. *Fix:* move the
  one-time cleanup into an explicit migration step (or gate on Darwin) and guard the remove.
- **UTL-DES-4** — `numbduck/utils.py:124-128` (low). The non-Darwin "set
  `NUMBDUCK_LIBDUCKDB`" error fires even when the user already set it to a missing path,
  offering no diagnostic and not stating the macOS-only download asymmetry. *Fix:* distinguish
  unset vs set-but-missing (echo the path) and state the platform asymmetry.
- **UTL-DES-5** — `numbduck/utils.py:51-56` (low). The download URL couples the package version
  string to a core git tag and a flat archive member name; any divergence raises a bare
  `HTTPError`/`KeyError` with none of the guidance the consent-decline branch provides. *Fix:*
  wrap fetch/extract and re-raise as a `RuntimeError` carrying brew/env guidance and the
  attempted URL; normalize the version.
- **UTL-COR-3** — `numbduck/utils.py:51` (low). Same package-version-to-release-tag assumption;
  a post/dev suffix yields a 404, and a Homebrew dylib carries no ABI-version guarantee. *Fix:*
  normalize/validate the version before forming the tag and surface a clear error on a missing
  asset.
- **XC-ver-4** — `numbduck/utils.py:51` (low). The release URL and the hardcoded
  `libduckdb.dylib` member name are assumed stable across 1.3.2-1.5.x with no fallback. *Fix:*
  handle 404/asset-name changes with an actionable error, allow a version/URL override, and
  verify the extracted member name.

#### pybridge polish (T21)

- **PBR-DES-2** — `numbduck/pybridge.py:59, 62` (low). The `16`/`32` offsets are bare literals
  whose meaning lives only in the docstring. *Fix:* hoist them to named module constants with
  the derivation comment attached.
- **PBR-DES-4** — `numbduck/pybridge.py:70-72` (low). The validation `RuntimeError` carries no
  context (version, offsets, likely cause). *Fix:* include `duckdb.__version__` and a re-verify
  hint in the message.
- **PBR-COR-2** — `numbduck/pybridge.py:67` (low). The docstring overstates the `SELECT 1`
  round-trip as offset validation; it only confirms a working `Connection*`, not that the
  offsets are correct. *Fix:* document it as best-effort and make the version/ABI gate the real
  safeguard.
- **PBR-MEM-1** — `numbduck/pybridge.py:74` (low). The returned pointer is borrowed from
  `conn`'s `unique_ptr<Connection>` but escapes as a plain int with no documented lifetime
  coupling; if `conn` is GC'd or closed, the pointer dangles. *Fix:* document the borrow
  contract, or have callers retain `conn` alongside the pointer.

#### Version-gated bindings raise for JIT callers (T22)

- **XC-err-2** — `numbduck/ducklib.py:725` (low). The three `@cres_if_available` bindings degrade
  to a Python stub that raises `NotImplementedError` — but only for Python callers; from `@njit`
  the stub is an untyped global and numba raises an opaque `TypingError`, so the degraded path
  fails confusingly for exactly the JIT consumer it protects. *Fix:* make the unavailable-symbol
  stub `@njit`-introspectable (an overload/intrinsic that raises a clear numba error), or have
  numbduck guard with a Python-level capability check before compiling dependent JIT code
  (fix lands in numbox).

#### duckdb_utils dangling-pointer footgun (T23)

- **DBU-MEM-1** — `numbduck/duckdb_utils.py:7` (low). The allocators return a bare owned ndarray
  with no coupling to the handle DuckDB writes into it, so `p = create_duckdb_result().ctypes.data`
  leaves `p` dangling when the temporary is collected. Latent (every current caller binds the
  array to a named local first). *Fix:* document that the returned array — not the extracted handle
  int — must outlive all DuckDB use of its `.ctypes.data`.

---

## Refuted (3)

- **TMD-DOC-5** — `test/test_ducklib.md:161` (medium). Claimed the doc's stability framing gives
  false confidence because `removerefctpass` is removed in numba main. Refuted: the pass is
  already effectively *skipped* for every callback via other `_legalize` bail-outs (Array
  outputs, NRT-typed call returns, the `NRT_MemInfo_release` symbol), so removing it from numba
  has zero effect — the bridge does not depend on the pass and the correctness/portability claim
  holds.
- **TST-cover-8** — `test/test_ducklib.py:833` (low). Claimed Windows struct-ABI and macOS loader
  paths are entirely unexercised. Refuted: the struct bind/value tests carry no `skipif` and run
  on the `windows-latest` and `ubuntu-24.04-arm` CI hosts, so the Windows x64 sret/by-pointer and
  AAPCS64 branches are exercised every run; the dominant claim is factually wrong.
- **XC-numbox-MEM-2** — `examples/irr.py:201` (low). Claimed the finalize borrow's scope-exit
  decref survives only when `borrow_structref` is inlined. Refuted: protection comes from
  `removerefctpass._legalize` checking all call-site return types — `borrow_structref` returns an
  NRT-tracked `StructRef`, so the pass is disabled for the whole unit at type-inference time,
  independent of inlining; the decref always survives and the vector ops are irrelevant to it.

---

## Investigated and dismissed

A completeness check proposed an additional review unit, `DCK-abi-MEM`, on the premise that
`ducklib.py` contains hand-rolled `@intrinsic` / `byval` / `sret` codegen for the 24-byte
decimal and varint structs. This was checked against the source and is **false for the
reviewed tree**: `ducklib.py` defines no such codegen. Decimal, varint, hugeint, and interval
all route through numbox's `_call_lib_func`, which `ducklib.py` imports at line 2; there are
zero `@intrinsic` definitions in the file. The struct-by-value ABI codegen lives entirely in
numbox and is outside this review's boundary scope. The proposal conflated the stale CLAUDE.md
"Struct-by-value helpers" section (itself flagged by DOC-acc-1 / DCK-wrap-DES-2) with
`ducklib.py`'s actual content. No unit was opened.

---

## Coverage

Coverage is complete across the in-scope surface. Every source module (`utils.py`,
`ducklib.py`, `duckdb_utils.py`, `pybridge.py`), every example (`haversine.py`,
`fraud_score.py`, `online_scoring.py`, `irr.py`/`run_irr.py`, `_common.py`), the integration
test suite (`test/test_ducklib.py` and its design doc `test/test_ducklib.md`), the CI and
packaging configuration (the four GitHub Actions workflows, `pyproject.toml`,
`numbduck/__init__.py`), and the in-repo documentation (`CLAUDE.md`) is covered by at least
one architecture note and at least one finding unit; the numbox boundary is covered by a
dedicated interface note and the `XC-*` cross-cutting units. Verification is complete: all 92
findings carry a verdict (89 confirmed, 3 refuted), and every confirmed finding is mapped to
exactly one remediation task in `numbduck-review.tasks.json`.
