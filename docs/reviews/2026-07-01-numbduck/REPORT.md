# numbduck code review — 2026-07-01

This report documents a full-surface engineering review of **numbduck**, the library that
adapts DuckDB's C API for use inside numba `@njit` code on top of the sibling **numbox**
bindings toolkit. The review covered the runtime library (now built on numbox's `@proxy`
wrappers), the example UDF/UDAF patterns, the integration test suite, the CI / packaging /
release configuration, and the in-repo documentation. It produced 108 findings — every one
independently and adversarially verified under a default-refute stance — of which **99 are
confirmed** and **9 refuted**. No critical defects were found; the confirmed set is
0 critical / 6 high / 33 medium / 60 low.

- **Review id:** `2026-07-01-numbduck`
- **Date:** 2026-07-02
- **Repo / tree state:** numbduck, branch `review/numbduck-2026-06-29`, HEAD `3ed20bc` — the
  current `@proxy` / `numbox>=0.5.13` tree (the source modules were migrated from the earlier
  `@cres` binding pattern to numbox's `@proxy`/`@proxy_if_available` decorators, and a
  `numbduck/configurations.py` jit-options knob and a `.github/workflows/release.yml` PyPI
  Trusted-Publishing pipeline were added since the previous review).
- **Scope:** 37 review units across the source / example / test / build / release / doc
  targets x six dimensions (memory, correctness, security, design, testing, documentation),
  plus nine architecture notes. numbox was examined only at the interface boundary it presents
  to numbduck; numbox has its own separate review.
- **Results:** 108 findings — **99 confirmed** (0 critical / 6 high / 33 medium / 60 low),
  **9 refuted**. The 108 comprise 91 first-pass findings plus 17 adoptions carried over from
  the superseded 2026-06-29 review. Verification is complete: every finding carries a verdict.

A companion remediation plan, grouping every confirmed finding into 24 concrete tasks, lives
in [`numbduck-review.tasks.json`](numbduck-review.tasks.json) alongside this report. Task ids
(T1–T24) are referenced from the cluster headings below.

---

## Method

The review proceeded in four stages:

1. **Comprehension pass.** The whole in-scope surface was read and summarized into nine
   architecture notes (linked below) before any defect hunting, so that findings could be
   reasoned against a shared model of the loader, the binding layer, the connection bridge,
   the numbox boundary, the UDAF lifecycle, the examples, the tests, and the build.
2. **Per-unit review (37 units).** Each unit is one `(target × dimension)` slice — e.g.
   `PBR-SEC` (pybridge × security), `TST-cover` (tests × coverage), `BLD-DES` (build × design).
   Every unit produced a findings file under [`findings/`](findings/).
3. **Independent adversarial verification.** Every candidate finding was re-examined against
   the actual tree under a default-refute stance: a finding is confirmed only when the evidence
   is conclusive, and its severity is corrected where the reported severity overstated or
   understated the real risk. Nine findings did not survive; several confirmed findings were
   re-rated (e.g. `DOC-acc-1/4` from high to medium, `DCK-wrap-DES-1` from high to medium).
   **Throughout this report, the corrected `effectiveSeverity` is used, not the as-reported
   severity.**
4. **Reconcile and completeness audit.** All 89 still-confirmed findings from the superseded
   2026-06-29 review were mapped onto the current findings; gaps were adopted as `REC-*`
   findings (and themselves verified), and a completeness audit
   ([`reconcile/AUDIT.json`](reconcile/AUDIT.json)) checked that every old finding is accounted
   for exactly once. See [Reconcile & completeness](#reconcile--completeness) below.

---

## Tallies

| Effective severity | Confirmed |
|---|---:|
| Critical | 0 |
| High | 6 |
| Medium | 33 |
| Low | 60 |
| **Confirmed total** | **99** |
| Refuted | 9 |
| Uncertain | 0 |
| **All findings** | **108** |

The 108 findings = 91 first-pass findings + 17 carried-over adoptions (`REC-*`); of the
adoptions, 14 were confirmed and 3 refuted. Every confirmed finding maps to exactly one of the
24 remediation tasks; no refuted finding is mapped.

---

## Architecture

numbduck is a **thin binding layer, not a framework**. Its one structural job is to make each
DuckDB C API function callable from inside numba `@njit` code at native speed, with no Python
or ctypes crossing per call. Everything else — scalar UDFs, prepared-statement query loops,
aggregate UDAFs — is application code built on top of those bindings and lives in `examples/`
and `test/`, not in the package. The package has five source modules: `utils.py` locates and
`dlopen`s `libduckdb` with `RTLD_GLOBAL` (so its symbols enter the process-global table where
LLVM's JIT linker can resolve them); `configurations.py` reads `NUMBDUCK_JIT_OPTIONS`
(defaulting to `{"cache": True}`) into a module-level `jit_options` dict; `ducklib.py` is the
binding table (~190 DuckDB functions, each registered into numbox's global `signatures` dict
and wrapped with `@proxy(...)` / `@proxy_if_available(...)` + `_call_lib_func`, threading
`jit_options`); `duckdb_utils.py` provides `@njit` allocators that return numpy buffers sized
to DuckDB's out-param structs; and `pybridge.py` recovers the raw C `Connection*` out of a
live Python `duckdb` connection. The adaptation rests entirely on numbox's toolkit
(`proxy`/`proxy_if_available`, `_call_lib_func`/`_call_lib_func_byval`, the shared `signatures`
dict, the meminfo bridge, `_cast_int_to_void_p`, `get_unicode_data_p`, `make_structref`) and
adds no new lowering machinery of its own. Two load-bearing invariants underpin everything:
`libduckdb` must be loaded `RTLD_GLOBAL` before any wrapper compiles, and every pointer is
uniformly modelled as `intp`, which assumes a 64-bit host.

The connection bridge is the project's most fragile boundary. Because the DuckDB Python wheel
exposes no way to obtain the C-API `duckdb_connection` from its pybind11 `DuckDBPyConnection`
object, `pybridge.extract_connection_ptr` walks **hardcoded byte offsets** into that object's
memory — `id(conn)+16` to reach the C++ object, then `+32` to read the
`unique_ptr<Connection>` — and validates the result by running `SELECT 1` through the C API
before returning. Those offsets were validated only on duckdb 1.3.2 / Linux x86-64 / libstdc++,
yet the project supports `duckdb>=1.3.2,<1.6` and a macOS fallback path. On macOS the wheel
strips the C API, so `load_duckdb()` loads a *second*, standalone `libduckdb` `RTLD_GLOBAL`;
that second runtime can be a different build/version from the wheel that minted the
`Connection*`, and numbduck has no coordination guard (the sibling numbox bridge does). These
two seams — the offset assumptions and the dual-runtime fallback — account for most of the
high/medium risk in this review.

Three UDF patterns are built on this spine: **Pattern A (scalar chunk callback)** reads each
input vector's raw buffer and writes the output vector in one tight loop (`haversine.py`,
`fraud_score.py`); **Pattern B (JIT query loop)** drives a prepared statement entirely inside
one `@njit(nogil=True)` function (`online_scoring.py`); and **Pattern C (structref-backed
UDAF)** smuggles a numba structref's NRT `MemInfo*` through DuckDB's 8-byte aggregate slot
using numbox's `export_meminfo`/`borrow_structref`/`release_meminfo` (`irr.py`, design doc
`test/test_ducklib.md`).

| Subsystem | What it covers | Note |
|---|---|---|
| Overall architecture & the three UDF patterns | End-to-end mechanics, invariants, fragile spots | [understanding/architecture.md](understanding/architecture.md) |
| Shared-library loading & platform handling | `utils.py` load path, macOS standalone/download fallback | [understanding/loader.md](understanding/loader.md) |
| Binding layer | `ducklib.py` signatures, `@proxy`, ABI lowering via numbox | [understanding/bindings.md](understanding/bindings.md) |
| Connection bridge & C-struct buffers | `pybridge.py` offsets, `duckdb_utils.py` allocators | [understanding/bridge.md](understanding/bridge.md) |
| numbox boundary contract | Every numbox symbol numbduck depends on | [understanding/numbox-surface.md](understanding/numbox-surface.md) |
| Structref-backed UDAF lifecycle | meminfo bridge, `@cfunc`/`@njit` callbacks, `removerefctpass` | [understanding/udaf.md](understanding/udaf.md) |
| Examples as usage contracts | The four reference UDF/UDAF examples | [understanding/examples.md](understanding/examples.md) |
| Test architecture & coverage shape | `test/test_ducklib.py` structure and gaps | [understanding/tests.md](understanding/tests.md) |
| Packaging, CI, versioning, release | `pyproject.toml`, the GitHub Actions workflows | [understanding/build.md](understanding/build.md) |

---

## Findings

Findings are listed by effective severity (high → medium → low) and grouped into the
remediation clusters used in [`numbduck-review.tasks.json`](numbduck-review.tasks.json). Each
entry gives the finding id, `file:line`, its effective severity, a short description of the
defect, why it matters, and the recommended fix. `REC-*` ids are adoptions carried over from
the 2026-06-29 review; each notes which old finding it preserves.

### High severity (6)

- **BLD-DES-1** — `.github/workflows/numbduck_ci.yml:79` (high, T3). The `benchmark` marker is
  documented in `pyproject.toml:26` as "deselected in CI", but nothing implements the
  deselection: there is no `addopts`, no `conftest.py`, and the CI step runs a bare `pytest`.
  `test_udf_benchmark` (`test/test_ducklib.py:2684`) is not env-gated and unconditionally
  builds and times a Python scalar UDF over `[10_000, 100_000, 1_000_000]` rows (~127 s per its
  own docstring) on every one of the ~51 matrix jobs. *Why it matters:* tens of billable CI
  minutes are wasted per run, the stated contract is silently violated, and any future test
  tagged `@pytest.mark.benchmark` inherits the non-deselection. *Fix:* run `pytest -m "not
  benchmark"` in CI (or add `addopts = "-m 'not benchmark'"` so local runs opt in), and add
  `--durations=20` per project convention.

- **DCK-byval-MEM-1** — `numbduck/ducklib.py:946` (high, T1). Three functions that take
  `duckdb_result` **by value** in duckdb.h — `duckdb_fetch_chunk` (946),
  `duckdb_result_return_type` (1132), `duckdb_result_statement_type` (1138) — are wrapped with
  numbox's `_call_lib_func_byval`, which implements `func(T*)` (pointer-parameter) lowering:
  alloca + store + call-through-pointer, with no `byval` attribute. `duckdb_result` is a
  48-byte struct modelled as `UniTuple(intp, 6)`; on SysV x86-64 a 48-byte struct is MEMORY
  class and must be copied inline into the outgoing stack argument area, but
  `_call_lib_func_byval` passes only the alloca address and never populates that area, so the
  real callee reads its 48 bytes from stale stack. *Why it matters:* on Linux x86-64 (the
  primary target) the calls return correct results today only because numba's stack layout
  happens to place the copy where the callee reads; a numba/LLVM version bump, a `jit_options`
  change, or altered surrounding stack usage silently moves the read, yielding a wrong enum or
  a garbage `internal_data` pointer that DuckDB dereferences — a wild-pointer read / segfault
  on the central result-fetch path. *Fix:* route all three through numbox's ABI-aware
  `_call_lib_func` (which applies `byval`+`optnone`+`noinline` on SysV x86-64 and a plain
  pointer on Windows x64 / AAPCS64); the registered signatures already fit, so no signature
  change is needed. Reserve `_call_lib_func_byval` for genuine `func(T*)` parameters, and lock
  the fix with a fetch-and-validate round-trip test at low optimization.

- **DOC-acc-2** — `CLAUDE.md:34` (high, T4). The "Adding a New Binding" template (lines 33–37),
  the Architecture line ("wraps each function with `@cres` + `_call_lib_func`"), and the
  Related-Projects entry all describe a `@cres(signatures.get("duckdb_func"))` wrapper. The
  current `ducklib.py` imports `proxy`/`proxy_if_available` and every one of the ~190 wrappers
  is `@proxy(signatures.get("name"), jit_options=jit_options)`; `@cres` appears nowhere in the
  module. *Why it matters:* this is the single most-followed section for the project's core
  task, and following it verbatim produces code that does not match the codebase (wrong
  decorator, missing `jit_options`, no `proxy_if_available` variant). *Fix:* rewrite steps 2–3
  and the Architecture / Related-Projects lines to the `@proxy` / `@proxy_if_available` +
  `_call_lib_func` pattern with `jit_options` threaded in.

- **DOC-acc-3** — `CLAUDE.md:43` (high, T4). The "Struct-by-value helpers (ducklib.py)"
  section claims `ducklib.py` provides `_call_lib_func_struct_in`, `_call_lib_func_struct_out`,
  `_emit_byval_call`, `_build_packed_interval`, and "Custom `@intrinsic` functions ... for
  >16-byte structs". `ducklib.py` defines none of these; it imports only `_call_lib_func` /
  `_call_lib_func_byval` from numbox and contains zero `@intrinsic` definitions — all
  struct-in/out/byval/repack lowering lives in numbox (`call.py`/`abi.py`). *Why it matters:*
  anyone searching `ducklib.py` for these named helpers finds nothing and is misled about where
  ABI lowering happens. *Fix:* replace the section with a description of the real mechanism —
  numbox's `_call_lib_func` handles struct-by-value/repack/sret based on the tuple-typed
  signatures; `ducklib.py` only uses `_call_lib_func`/`_call_lib_func_byval`.

- **TMD-DOC-1** — `test/test_ducklib.md:80` (high, T5). The "Bridge intrinsics" section states
  the three ownership-transfer functions are "Defined at test_ducklib.py L2796-2874". They are
  not defined there at all: `export_meminfo`, `borrow_structref`, and `release_meminfo` are
  imported from numbox (`test/test_ducklib.py:11-14`) and defined in `numbox/utils/meminfo.py`,
  and lines 2794–2874 hold only a comment block plus a commented-out `_refcount_of_meminfo`
  intrinsic. *Why it matters:* a maintainer following the link finds no such definitions and
  cannot locate the incref/deref/release logic (which lives in numbox and governs the whole
  pattern). *Fix:* state the three functions are imported from `numbox.utils.meminfo` (cite
  `test_ducklib.py:11-14`), link their definitions into numbox, and drop the false
  "Defined at test_ducklib.py L2796-2874" claim.

- **XC-ver-1** — `numbduck/utils.py:44` (high, T2). On the macOS / env-override fallback,
  `load_duckdb()` loads a second, standalone `libduckdb` to supply the C-API symbols the wheel
  strips. Only the download/cache path is version-keyed; the Homebrew paths
  (`/opt/homebrew/lib`, `/usr/local/lib`) and the `NUMBDUCK_LIBDUCKDB` override are accepted
  with **no check that the library's version matches the installed Python `duckdb` wheel**.
  `pybridge.extract_connection_ptr` then reads a `Connection*` minted by the wheel's DuckDB
  build and passes it straight into `duckdb_query`, which resolves against the standalone lib.
  *Why it matters:* if the two DuckDB versions differ (plausible: a pip `duckdb==1.5.1`
  alongside a Homebrew duckdb at a different release), the internal object layouts disagree and
  the C API operates on a mis-typed pointer — silent memory corruption / crash at query time,
  with no error at load time. The sibling numbox bridge guards exactly this with
  `libraries_coordinated()`. *Fix:* before returning a standalone/brew/env-sourced handle,
  compare its `duckdb_library_version()` against `duckdb.__version__` and warn/refuse on
  mismatch, applying the check to the env and brew branches, not just the version-keyed
  cache/download.

### Medium severity (33)

#### Library-coordination / single-runtime guard (T2)

- **PBR-SEC-1** — `numbduck/pybridge.py:67` (medium, T2). `extract_connection_ptr` hands the
  wheel-minted `Connection*` to `ducklib.duckdb_query`/`duckdb_destroy_result`, whose symbols
  are resolved by the JIT linker against whatever `libduckdb` `load_duckdb()` put in the global
  namespace. On the macOS/standalone fallback that can be a *different* build/version than the
  wheel's, so a `Connection*` from build A is dereferenced by build B's C API — undefined
  behavior that the `SELECT 1` probe cannot catch (a wrong-layout deref is UB, not a returnable
  rc). *Why it matters:* interpreter segfault or silent corruption at the first C-API call on
  exactly the platform the fallback targets. *Fix:* add the numbox-style coordination check
  (compare `duckdb.__version__` against the loaded lib's `duckdb_library_version()` and refuse
  on mismatch), or at minimum document that the bridge is sound only when numbduck's loaded
  `libduckdb` is the wheel's own build.

- **XC-ver-2** — `numbduck/pybridge.py:62` (medium, T2). The `+16`/`+32` offsets are validated
  only on duckdb 1.3.2 / Linux / libstdc++, yet the pin spans `>=1.3.2,<1.6` across platforms.
  The only safety net is the post-extraction `SELECT 1` rc check, which catches a wrong-but-live
  pointer but not a layout shift that makes `conn_ptr` a garbage address — that segfaults inside
  `duckdb_query` before any rc is produced, an unrecoverable crash rather than the intended
  clean `RuntimeError`. *Why it matters:* on an in-range release/platform whose pybind11 /
  `DuckDBPyConnection` layout differs from the 1.3.2 baseline, extraction can crash instead of
  raising. *Fix:* do not treat the rc check as portability protection; gate the offsets per
  validated `(version, platform)` and document/enforce which versions are actually validated.

- **UTL-DES-5** — `numbduck/utils.py:121` (medium, T2). On the macOS fallback the symbol-less
  wheel is loaded `RTLD_GLOBAL` and then the standalone dylib is also loaded `RTLD_GLOBAL`, so
  two complete DuckDB runtimes (allocator, extension registry, memory manager) are resident.
  The Python `duckdb` module keeps using the wheel runtime while numbduck's JIT code binds to
  the standalone; any handle created in one and consumed by the other is interpreted by a
  different library instance — free-across-allocators / mismatched-layout UB. *Why it matters:*
  latent under current usage (handles stay within the standalone runtime) but the design
  silently permits a corruption hazard with no guard or documentation at the seam. *Fix:*
  document the single-runtime invariant at the load site, and consider not retaining the
  symbol-less wheel handle before loading the standalone, or a code comment stating all
  JIT-used DuckDB handles must originate from the numbduck-loaded runtime.

#### CI matrix coverage (T3)

- **REC-BLD-DES-1** — `.github/workflows/numbduck_ci.yml:22` (medium, T3; carries forward
  2026-06-29 BLD-DES-1). The CI arch matrix is `["ubuntu-latest", "ubuntu-24.04-arm",
  "windows-latest"]` with no macOS runner, yet numbduck's most bespoke platform code exists
  precisely because the macOS wheel strips the C API: standalone-dylib discovery, the network
  download fallback with consent gate and versioned cache, the legacy-cache cleanup, and the
  Darwin-gated double-load. None of it is reached by any test or CI job. *Why it matters:* the
  platform the project documents as its hardest case can regress silently for all macOS users.
  *Fix:* add at least one `macos-latest` (ARM64) job exercising `load_duckdb` end-to-end
  (ideally with `NUMBDUCK_LIBDUCKDB` pointing at a brew libduckdb so the standalone path runs
  deterministically). This is a coverage gap, not an active defect — the specific defects
  inside those functions are the `UTL-*` findings.

#### CLAUDE.md accuracy (T4)

- **DCK-wrap-DES-1** — `CLAUDE.md:43` (medium, T4). The Architecture, "Adding a New Binding",
  "Struct-by-value helpers", "Follow-ups", and "Related Projects" sections collectively
  describe a since-completed migration and helpers/intrinsics that do not exist in the current
  `@proxy` tree (the same staleness surfaced by DOC-acc-2/3/4 from the binding-layer view).
  *Why it matters:* the single authoritative onboarding doc for this ~190-wrapper module
  actively misdescribes it. *Fix:* rewrite all five sections to the current `@proxy` /
  `@proxy_if_available` + `_call_lib_func` design (no local intrinsics; ABI lowering lives in
  numbox; the struct-by-value migration is complete) and delete the stale Follow-ups item.

- **DOC-acc-1** — `CLAUDE.md:16` (medium, T4). "Key dependencies" states `numbox~=0.5.6` while
  `pyproject.toml:13` pins `numbox>=0.5.13`; the two specifiers are inconsistent and the doc's
  floor is seven patch releases stale (and below the versions whose `proxy`/repack machinery
  the code needs). *Why it matters:* a contributor trusting the doc could pin an unusable
  numbox and hit import-time failures. *Fix:* change line 16 to `numbox>=0.5.13` (match
  pyproject), or drop the explicit version and point to pyproject as the source of truth.

- **DOC-acc-4** — `CLAUDE.md:57` (medium, T4). The "Follow-ups" item says to migrate
  "hand-rolled intrinsics at `ducklib.py:1525-1640` (~116 lines)" and references sibling
  wrappers at `ducklib.py:298-433`. `ducklib.py` is 1432 lines (so 1525-1640 is past EOF), the
  named binds are already plain `@proxy` wrappers at 1411-1426, and the numbox INT/INT repack
  the item gates on has already landed. *Why it matters:* the doc directs a maintainer to
  line ranges past EOF and to work that is already merged. *Fix:* delete the item (the
  migration is done) or rewrite it to reflect the current state.

- **DOC-acc-5** — `CLAUDE.md:30` (medium, T4). Steps 1 and 7 tell the reader to "verify all
  docstring links point to the correct line in `duckdb.h`", but step 5 mandates docstring links
  use the `duckdb.org/docs/stable/clients/c/api.html#...` form, and all 191 docstrings follow
  step 5 (no duckdb.h line link exists). *Why it matters:* the instruction in the most-used doc
  workflow is self-contradictory and cannot be satisfied as written. *Fix:* scope the duckdb.h
  verification in steps 1/7 to signatures/typedefs/parameter types, and remove the
  "docstring links against duckdb.h" phrasing.

#### Design-doc citations (T5)

- **TMD-DOC-2** — `test/test_ducklib.md:51` (medium, T5). The Welford callback anchor
  (`L3075-3176`, cited at lines 51 and 75) lands inside the *array-backed* UDAF; the Welford
  callbacks are at ~`L3205-3305`. *Why it matters:* readers land on the wrong aggregate
  implementation, contradicting the surrounding prose. *Fix:* update both anchors to
  ~`L3205-3305`.

- **TMD-DOC-3** — `test/test_ducklib.md:51` (medium, T5). The registration anchor
  (`L3280-3304`, cited at lines 51 and 192) points at Welford finalize/destroy definitions;
  the actual `set_functions`/`set_destructor`/`register` block is at ~`L3409-3433`. *Why it
  matters:* the "registration" links resolve to callback bodies, misleading anyone tracing how
  the UDAF is registered. *Fix:* point the registration anchors to ~`L3409-3433`.

- **TMD-DOC-4** — `test/test_ducklib.md:199` (medium, T5). The
  `test_structref_meminfo_bridge_refcount_ladder` link points at `L3179`; the function is at
  `L2931`. *Why it matters:* the link jumps to unrelated array-UDAF code. *Fix:* change the
  anchor to `#L2931`.

#### NUMBDUCK_JIT_OPTIONS validation (T6)

- **CFG-COR-1** — `numbduck/configurations.py:14` (medium, T6). `get_jit_options()` returns
  `json.loads(as_str)` with no check that the parsed value is a dict. Valid-but-non-object JSON
  (`["cache"]`, `true`, `5`, `"cache"`) is returned as-is and then normalized by numbox's proxy
  to `{}`, so every binding is JIT-compiled with **no options at all** — in particular the
  compile cache is silently off, with no error or warning. *Why it matters:* silent,
  hard-to-diagnose loss of the compile cache (and any other intended option) for the entire
  binding set. *Fix:* after `json.loads`, raise a `ValueError` naming `NUMBDUCK_JIT_OPTIONS`
  when the parsed value is not a dict.

- **CFG-COR-2** — `numbduck/configurations.py:19` (medium, T6). `jit_options` runs at import
  and is spread into `njit(sig, **jit_options)` for the first `@proxy` wrapper. An unknown key
  (e.g. `{"cache": true, "foobar": 1}`) surfaces as a numba `KeyError` raised the instant
  `ducklib.py` is imported, and the traceback never mentions `NUMBDUCK_JIT_OPTIONS`. *Why it
  matters:* a single typo'd key renders the whole package unimportable with an error that does
  not identify the misconfiguration source. *Fix:* whitelist the accepted keys in
  `get_jit_options()` and raise a `ValueError` naming the env var and the offending key.

#### release.yml hardening (T7)

- **BLD-SEC-1** — `.github/workflows/release.yml:25` (medium, T7). The `release-build` job
  interpolates `${{ github.ref_name }}` directly into `run:` shell text (line 25) and into
  `numbduck/__init__.py` (line 31). A tag name may legally contain backticks / `$(...)`, so a
  crafted tag executes arbitrary shell in the runner — the canonical GitHub Actions
  script-injection pattern. The job builds the wheel that the downstream `pypi-publish` job
  uploads to PyPI via OIDC Trusted Publishing, so injected code can poison a published artifact.
  *Why it matters:* arbitrary command execution during a tag-triggered release with a
  public-PyPI supply-chain sink (gated by release/tag write access). *Fix:* pass the values
  through an `env:` map and reference quoted shell variables (or write the file via a small
  `python -c`), so environment values are never re-parsed as script.

- **BLD-SEC-3** — `.github/workflows/release.yml:66` (medium, T7). Every action across the
  workflows is pinned to a mutable ref; the highest-value case is
  `pypa/gh-action-pypi-publish@release/v1` — a moving *branch*, not even a tag — running in the
  `id-token: write` publish job. `lychee-action@v2` (with `issues: write`) and the `actions/*`
  set are also tag-pinned. *Why it matters:* a force-moved or compromised upstream ref runs
  attacker code in the OIDC-privileged publish job and could push a malicious numbduck release
  to PyPI. *Fix:* pin every action to a full 40-char commit SHA with a version comment
  (prioritizing the publish and `issues: write` jobs) and enable Dependabot for Actions.

#### numbox pin (T8)

- **BLD-DES-2** — `pyproject.toml:13` (medium, T8). The runtime pin is `numbox>=0.5.13` with no
  upper bound, yet numbduck imports deep private internals (`numbox.core.bindings.call`,
  `numbox.core.bindings.signatures`, `numbox.core.proxy.proxy`) that are not a stable public
  surface and that numbox's own history shows churning. *Why it matters:* a future numbox that
  moves/renames these breaks numbduck with a bare `ImportError` and no version guard; contrast
  the deliberate `<1.6` cap on duckdb. *Fix:* add an upper cap reflecting the tested range
  (e.g. `numbox>=0.5.13,<0.6`) and bump it deliberately as releases are validated.

#### Downloaded-library integrity (T9)

- **UTL-SEC-1** — `numbduck/utils.py:88` (medium, T9). `_download_libduckdb` fetches
  `libduckdb-osx-universal.zip` over HTTPS and writes the extracted member to the on-disk cache
  with **no integrity check** (no SHA-256, no signature), then `load_duckdb` `dlopen`s it
  `RTLD_GLOBAL` (without even re-running `_has_capi_symbols`) and trusts the cached file forever
  after. `makedirs` uses default umask (world-readable), so a same-user process can poison the
  cache. TLS only protects transport. *Why it matters:* a compromised release asset or a
  poisoned per-user cache becomes arbitrary native-code execution in the importing process; the
  `NUMBDUCK_LIBDUCKDB_DOWNLOAD=1` env var makes the download+dlopen fully silent. *Fix:* verify
  a pinned per-version SHA-256 before persisting/loading and re-verify on every cache hit,
  refuse on mismatch (the env var must not bypass), create the cache dir/file `0o700`/`0o600`,
  and support a digest override.

- **UTL-DES-2** — `numbduck/utils.py:129` (medium, T9). Every other load path validates the
  handle with `_has_capi_symbols` (wheel at 116, standalone at 122), but the download branch
  returns `load_lib_path(downloaded)` directly with no symbol probe. *Why it matters:* a
  stripped/truncated/wrong-architecture download passes the loader silently and only fails much
  later as an opaque LLVM "symbol not found" link error with no pointer back to the bad file.
  *Fix:* run `_has_capi_symbols` on the freshly downloaded handle and raise a branded
  `RuntimeError` (naming the cached path) on failure, mirroring the standalone branch.

#### Loader refactor & robustness (T10)

- **REC-UTL-DES-2** — `numbduck/utils.py:50-91` (medium, T10; carries forward 2026-06-29
  UTL-DES-2). `_download_libduckdb` fuses consent (a blocking `input()`), network fetch, unzip,
  and cache-write into one body with no injectable collaborators, so the extract-and-cache
  logic cannot be unit-tested without monkeypatching `input`/`urlopen`/module globals and can
  never exercise the happy path without hitting GitHub. *Why it matters:* the fallback path most
  likely to break on a duckdb release/layout change is effectively untestable, so regressions
  ship undetected. *Fix:* split into a pure-policy consent function and a
  `_fetch_and_cache(url, dest)` that a test can drive with a local `file://` URL and a tmp dest.

- **UTL-DES-3** — `numbduck/utils.py:67` (medium, T10). When the macOS fallback is reached and
  `NUMBDUCK_LIBDUCKDB_DOWNLOAD != "1"`, the flow calls `input("Download now? [y/N] ")`. Under
  CI / piped / non-TTY stdin, `input()` raises an uncaught `EOFError`, and because the loader
  runs at `ducklib.py` import time, a bare `import numbduck.ducklib` becomes an `EOFError`
  traceback instead of the intended branded RuntimeError with brew/env instructions. *Why it
  matters:* headless macOS-without-C-API imports crash with an unrelated-looking error. *Fix:*
  guard the prompt (`sys.stdin` not a TTY, or wrap in `try/except EOFError`) and raise the same
  instructional RuntimeError, noting `NUMBDUCK_LIBDUCKDB_DOWNLOAD=1` to auto-download.

#### pybridge input validation & docstring (T11)

- **PBR-COR-2** — `numbduck/pybridge.py:62` (medium, T11). `cpp_obj_p` (line 59) and `conn_ptr`
  (line 62) come from `ctypes.c_void_p.from_address(...).value`, which returns `None` on a
  zero word, and neither is validated. After `conn.close()` the `+32` read yields `None`, which
  is passed into `duckdb_query` and reproduced as a numba dispatch `TypeError` — not the
  documented `TypeError`/`RuntimeError`. *Why it matters:* a foreseeable use-after-close call
  produces a confusing, undocumented error (or, in the coerced-null case, a NULL C call). *Fix:*
  after each read, raise a clear `RuntimeError` when the value is falsy, before pointer
  arithmetic or the query — mirroring the numbox null-pointer guard.

- **PBR-MEM-2** — `numbduck/pybridge.py:62` (medium, T11). The same missing guard from the
  memory-safety view: `conn_ptr` is passed straight into `duckdb_query` with no non-null/live
  check; a null holder makes `cpp_obj_p + 32` raise a bare `TypeError`, and a stale/dangling
  non-null pointer is dereferenced by the `SELECT 1` "validation" — a use-after-free before any
  rc can be returned. *Why it matters:* the docstring's "validated before returned" overstates
  the guarantee, and the failure modes escape the documented contract. *Fix:* guard `cpp_obj_p`
  and `conn_ptr` for `None`/0 and raise the documented `RuntimeError` before the query;
  document that the query cannot catch a structurally invalid pointer.

- **PBR-DES-1** — `numbduck/pybridge.py:28` (medium, T11). The docstring promises the pointer
  "is validated by running SELECT 1 ... before it is returned" and that a `RuntimeError` is
  raised on failure, but for the one failure mode it warns about (offset drift) a wild
  `conn_ptr` segfaults inside `duckdb_query` before any rc — so the `RuntimeError` branch is
  effectively dead, and for a correct pointer `SELECT 1` essentially always succeeds. *Why it
  matters:* a maintainer upgrading duckdb may trust the "validated" language as protection
  against layout change, when the drift scenario is uncatchable UB. *Fix:* reword the docstring
  to state the query is a best-effort liveness smoke-test, not ABI-offset validation, and move
  the offset-drift warning next to it.

#### DuckDB handle leaks in tests (T12)

- **TST-MEM-1** — `test/test_ducklib.py:172` (medium, T12). `aux_get_data_vector` does
  `duckdb_result = tuple(out_result)` and returns the tuple but not the `out_result` numpy
  buffer — the only handle `duckdb_destroy_result` can be called on — so the buffer is freed on
  return while four consumer tests keep the value-copied tuple. *Why it matters:* four tests
  unconditionally leak a result and can never exercise the destroy path, so a result-leak /
  destroy-time regression is invisible. *Fix:* return `out_result` and have each consumer
  destroy the result (and fetched chunk) before `aux_close_db`.

- **TST-MEM-2** — `test/test_ducklib.py:212` (medium, T12). Several tests allocate DuckDB
  handles and never destroy them: `test_query` and the column/row-count test leak `out_result`;
  `test_duckdb_fetch_chunk_exhausted` leaks both the first fetched chunk and the result; the
  `aux_get_data_vector` consumers also leak the chunk. Closing the connection does not free
  outstanding results/chunks. *Why it matters:* per-test leaks, and because these tests never
  touch the destroy path they cannot detect a leak regression in the very bindings they cover.
  *Fix:* destroy every result and fetched chunk before `aux_close_db` (and the first chunk
  before the second fetch in the exhaustion test).

- **TST-MEM-3** — `test/test_ducklib.py:3462` (medium, T12). The only leak accounting is
  `rtsys.get_allocation_stats()` (alloc==free) in the two UDAF tests and the nested-heap dtor
  test — which counts only numba NRT allocations and is blind to DuckDB-side malloc. A leaked
  `duckdb_result`/`data_chunk`/`value`/`logical_type` passes unchanged. *Why it matters:*
  handle-lifetime regressions on the DuckDB side (leaks, or a bind wrapper that drops a handle)
  are undetectable; the leak-clean claims only hold for numba-managed state. *Fix:* add a
  C-handle accounting harness (or per-test create/destroy balance assertions / loop-and-watch)
  for at least the value and logical_type families.

#### High-value test coverage (T13)

- **TST-cover-1** — `test/test_ducklib.py:1706` (medium, T13). `test_struct_size_guard` claims
  to verify the size computation used by the struct-in/out lowering but only computes
  `sum(bitwidth)/8` over numba tuple types and asserts the integers — it never calls any
  wrapper or exercises the ≤16 / >16-byte branch, and the assertions would still pass if the
  threshold logic were deleted or inverted. *Why it matters:* the one test named as the ABI
  size guard provides zero protection over the most delicate part of the binding. *Fix:*
  round-trip a 16-byte and a >16-byte (24-byte decimal/varint) struct through the real
  wrappers, or rename the test so it is not mistaken for a guard.

- **TST-cover-2** — `test/test_ducklib.py:212` (medium, T13). Every result comes from a 3-row
  query or a single-value SELECT, so no result exceeds one 2048-row vector; the multi-chunk
  `duckdb_fetch_chunk` loop is never driven, and every UDF/UDAF finalize runs only with
  `offset == 0`. *Why it matters:* a defect in multi-chunk fetch or in a finalize that
  mishandles a non-zero output offset — the large-table real-world path — ships undetected.
  *Fix:* select >2048 rows (e.g. `range(5000)`), loop `duckdb_fetch_chunk` to null asserting
  the row total and values across a chunk boundary, and run a UDAF so finalize sees `offset != 0`.

- **TST-cover-3** — `test/test_ducklib.py:891` (medium, T13). Decimal tests only bind/create
  `DECIMAL(10,2)` values with the hugeint upper word 0 (INT64 physical path); the width>18
  (INT128) regime that actually needs the upper 64 bits is never bound, created, or read back.
  *Why it matters:* a bug that drops or mis-packs the decimal struct's upper 64 bits — exactly
  the eightbyte-ABI hazard the large-struct path exists to prevent — passes all decimal tests.
  *Fix:* add a `DECIMAL(38,x)` case whose magnitude needs the upper word, and assert both int128
  halves round-trip.

- **TST-cover-4** — `test/test_ducklib.py:3229` (medium, T13). Every UDF/UDAF callback reads
  `duckdb_vector_get_data` and iterates all rows without ever calling
  `duckdb_vector_get_validity`; the tests' own NOTE says "test data has no NULLs". *Why it
  matters:* a callback that reads garbage or produces wrong aggregates for NULL rows (the common
  real case) passes the whole suite. *Fix:* add at least one scalar-UDF and one UDAF test over a
  NULL-containing column with a validity-aware callback, asserting a NULL-aware reference result.

#### Example UDF error paths (T14)

- **EX-os-MEM-1** — `examples/online_scoring.py:156` (medium, T14). `score_jit` allocates a
  prepared statement via `duckdb_prepare` and frees it only on the straight-line happy path,
  with no `try/finally`. Per duckdb.h the statement must be destroyed even when prepare fails,
  but the `assert rc == DuckDBSuccess` sits between prepare and destroy, so an assert-fail (or
  any raise inside the nogil loop) leaks the statement (and possibly an in-flight chunk/result).
  *Why it matters:* as the canonical prepared-execute pattern, invoked once per worker per T, it
  teaches an unsafe pattern and accumulates leaks on recurring errors. *Fix:* wrap prepare +
  loop in `try/finally` calling `duckdb_destroy_prepare` in the finally, keeping the assert
  inside the try.

#### irr convergence (T15)

- **EX-irr-COR-1** — `examples/irr.py:88` (medium, T15). `irr_bisect` exits successfully only
  when `abs(npv) < 1e-9`, with no bracket-width fallback; otherwise it returns `math.nan`. The
  achievable residual floor scales with cashflow magnitude, so for large-magnitude cashflows the
  gate can never be met even after `r_mid` has resolved to the true root — and the function
  returns NaN, indistinguishable from the documented empty-group NaN. Reproduced: at the shipped
  scale it returns the correct IRR, but scaling amounts ×1000 (~10 million) already returns NaN
  though the IRR is scale-invariant. *Why it matters:* any GROUP BY key whose magnitudes reach
  the ~10-million range (routine for enterprise finance) silently produces NaN; the example's
  own small-amount tests mask it. *Fix:* converge on bracket width in `r` (or make the NPV
  tolerance scale-relative) and return the converged rate rather than NaN.

#### Examples README (T16)

- **EX-readme-DOC-1** — `examples/README.md:33` (medium, T16). The README calls the scripts
  "Runnable" and links `irr.py`, but `irr.py` refuses direct execution (`sys.exit(1)`,
  instructing the user to run `run_irr.py`), and the "Running" section lists only the other
  three examples — the word `run_irr` appears nowhere. *Why it matters:* a reader cannot run the
  IRR/UDAF tutorial as documented; the Scripts link points at a file that immediately errors.
  *Fix:* add `python examples/run_irr.py` to the Running section and note that `irr.py` is
  imported by `run_irr.py` and must not be run directly.

### Low severity (60)

Low-severity findings are listed compactly, grouped by remediation task. Each gives the id,
`file:line`, a one-line description, and the fix.

**T2 — Add a duckdb library-coordination / single-runtime guard for the standalone fallback**

- **PBR-DES-2** — `numbduck/pybridge.py:59`. Hard-coded ABI offsets with no named constants, no duckdb version guard, and no programmatic re-verify mechanism. *Fix:* Promote the offsets to named module-level constants with a one-line rationale each (CPython object header = 16; unique_ptr<Connection> at +32). Add a fail-fast/warn guard that checks `duckdb.__version__` against a known-verified…
- **UTL-COR-3** — `numbduck/utils.py:121`. macOS fallback leaves a dual DuckDB runtime (symbol-less wheel + standalone) globally loaded with no guard against symbol shadowing. *Fix:* Document the single-engine constraint; consider asserting the resolved duckdb_open address belongs to the standalone (e.g. via dladdr) when taking the fallback path, and warn if the wheel also exports C-API symbols.

**T3 — Fix the CI matrix and test step (deselect benchmark, add macOS, tidy matrix)**

- **BLD-DES-4** — `.github/workflows/numbduck_ci.yml:74`. flake8 excludes a path that does not exist in the tree. *Fix:* Drop the `--exclude=...` argument (or replace it with a real exclude if one is actually needed).
- **REC-BLD-DES-6** — `.github/workflows/numbduck_ci.yml:39` _(carries forward 2026-06-29 BLD-DES-6)_. Windows CI exercises the divergent struct ABI at a single duckdb version; the duckdb floor (1.3.2/1.4.0) is never run on Windows. *Fix:* Accept the cost tradeoff if intended but document it explicitly, or add one Windows job at the duckdb floor (1.3.2) so the lower bound of the supported duckdb range is exercised at least once on the divergent Windows ABI.

**T5 — Fix stale line/anchor citations in the UDAF design doc**

- **TMD-DOC-5** — `test/test_ducklib.md:197`. Commented-out intrinsic range (L2877-2914) is stale; it is at L2836-2851. *Fix:* Update the range to L2836-2851 (or L2814-2851 to include the explanatory WARNING).

**T6 — Validate NUMBDUCK_JIT_OPTIONS in get_jit_options (shape + key whitelist)**

- **CFG-COR-3** — `numbduck/configurations.py:13`. Env var injects arbitrary keys into numba jit options without a whitelist. *Fix:* Restrict get_jit_options() to an explicit allowed-key set (e.g. {'cache','nogil','parallel','boundscheck','fastmath','error_model'}) and reject unknown keys with a clear ValueError; this simultaneously fixes CFG-COR-2's opaque…
- **DCK-proxy-COR-1** — `numbduck/configurations.py:14`. get_jit_options accepts valid-but-non-object JSON, silently discarding config and disabling caching for all bindings. *Fix:* After json.loads, validate the type: `if not isinstance(as_json, dict): raise ValueError("NUMBDUCK_JIT_OPTIONS must be a JSON object")` (or equivalent), so both malformed and non-object config raise the same clean ValueError.

**T7 — Harden release.yml against script injection and unpinned actions**

- **BLD-DES-5** — `.github/workflows/release.yml:15`. release.yml drifts from other workflows: pinned-action lag, no sdist, inconsistent fallback minor. *Fix:* Sync release.yml action pins to the @v6/@v7 versions used elsewhere; add `python -m build` (both sdist+wheel) or explicitly document wheel-only intent; unify the synthetic-version minor with numbduck_ci.yml.

**T8 — Cap and pin numbox**

- **BLD-SEC-4** — `.github/workflows/numbduck_ci.yml:70`. CI pulls floating, upper-bound-less numbox (arbitrary latest) via pip install -e .. *Fix:* Add an upper cap to the numbox dependency in pyproject.toml (e.g. `numbox>=0.5.13,<0.6`) matching the internals numbduck relies on, and/or pin numbox explicitly in the CI install step alongside numba and duckdb so the matrix…

**T9 — Verify integrity of the downloaded/cached libduckdb**

- **UTL-DES-6** — `numbduck/utils.py:83`. Download failures (404 / missing zip member) surface as raw urllib/zipfile errors, and version-range comment is rot-prone. *Fix:* Wrap the fetch/extract in try/except (HTTPError, URLError, KeyError) re-raising the branded RuntimeError, pass a `timeout=` to `urlopen`, and replace the fixed version-range comment with a reference to the tracking issue / a note…
- **UTL-SEC-2** — `numbduck/utils.py:83`. urlopen has no timeout: a hung/slow network path blocks library import indefinitely (availability). *Fix:* Pass an explicit, bounded timeout (e.g. urlopen(url, timeout=30)) and handle the resulting socket.timeout/URLError by raising the same actionable RuntimeError used elsewhere (brew install / set NUMBDUCK_LIBDUCKDB), so the failure…

**T10 — Refactor the loader for testability and robustness**

- **REC-UTL-DES-4** — `numbduck/utils.py:124-128` _(carries forward 2026-06-29 UTL-DES-4)_. Non-Darwin error message gives circular NUMBDUCK_LIBDUCKDB guidance and hides the macOS-only download asymmetry. *Fix:* Distinguish 'env var unset' from 'env var set but file missing' in the message (echo the offending path), and state the platform asymmetry (download is macOS-only) so a non-Darwin user understands the env var is their only…
- **UTL-COR-1** — `numbduck/utils.py:98`. find_duckdb_shared_lib regex is not end-anchored, so a sidecar can trip the len==1 invariant and hide a valid lib (or select the wrong file). *Fix:* Anchor the extension at end of string (add '$' or use re.fullmatch), and prefer selecting the actual C-API-exporting object explicitly (e.g. verify with _has_capi_symbols before accepting a candidate) rather than relying solely…
- **UTL-COR-2** — `numbduck/utils.py:40`. Legacy-cache os.remove is an unconditional filesystem mutation inside a discovery ('find') function and can crash import. *Fix:* Wrap the removal in try/except (swallow/log OSError), or move the one-time migration cleanup out of the read-flavoured discovery function entirely so a failed unlink cannot abort loading.
- **UTL-DES-1** — `numbduck/utils.py:38, 41, 87, 88`. Cache dylib path and filename literal duplicated across two functions. *Fix:* Introduce a `_LIBDUCKDB_DYLIB_NAME = "libduckdb.dylib"` constant and a derived `_LIBDUCKDB_CACHE_FILE = os.path.join(_LIBDUCKDB_CACHE_DIR, _LIBDUCKDB_DYLIB_NAME)` next to the existing constants, and reference those from both…
- **UTL-DES-4** — `numbduck/utils.py:37`. Read-flavoured _find_standalone_libduckdb performs an unconditional os.remove side effect. *Fix:* Move the legacy-cache cleanup into its own explicitly-named migration step (called once from load_duckdb, or lazily) wrapped in try/except OSError, and keep `_find_standalone_libduckdb` side-effect free.

**T11 — Harden pybridge input validation and correct its docstring**

- **PBR-COR-3** — `numbduck/pybridge.py:28`. Docstring claims the pointer is 'validated' but the SELECT 1 check cannot detect a structurally-wrong pointer. *Fix:* Soften the docstring to state the validation only confirms a live connection responds to a trivial query and cannot detect a wrong pointer from ABI/layout drift; pair it with the version guard from PBR-COR-1 so the trusted-offset…
- **PBR-MEM-1** — `numbduck/pybridge.py:74`. Returned Connection* is a borrowed pointer with an undocumented keep-alive / use-after-close contract. *Fix:* Add to the docstring the same borrowed-pointer contract the sibling documents: state in Returns that the pointer is borrowed from and owned by `conn`, and add a Warning that the caller must retain `conn` (alive and open) for the…
- **PBR-SEC-2** — `numbduck/pybridge.py:52`. isinstance guard is not an ABI-layout proof: a spoofed/subclassed object turns the two chained from_address reads into attacker-influenced pointer dereferences. *Fix:* Treat the isinstance check as a convenience type-hint, not a safety guarantee, and document it as such. Where feasible tighten to an exact-type check (type(conn) is duckdb.DuckDBPyConnection) to reject subclasses, and pair the…
- **PBR-SEC-3** — `numbduck/pybridge.py:42`. Borrowed / use-after-free ownership contract of the returned pointer is undocumented (sibling documents it explicitly). *Fix:* Add a Returns/Warning note mirroring numbox pysqlite_bridge: the returned int is a borrowed duckdb_connection owned by conn, valid only while conn is alive and open; the caller must retain conn for the full lifetime of any @njit…

**T14 — Harden example UDF error paths (handle leaks and chunk guards)**

- **EX-fraud-1** — `examples/fraud_score.py:170`. func_p scalar-function handle leaked on the register-failure path. *Fix:* Destroy func_p before asserting, e.g. capture rc, run the numpy-buffer destroy idiom on func_p, then 'assert rc == ducklib.DuckDBSuccess'; or wrap registration in try/finally so the handle is freed on both paths.
- **EX-hav-2** — `examples/haversine.py:135`. Scalar-function handle leaked if registration assertion fails. *Fix:* Destroy func_p before asserting (capture rc, destroy the handle via the numpy-buffer idiom, then assert), so the failure path does not leak the scalar-function object.
- **EX-os-COR-1** — `examples/online_scoring.py:156`. Prepared statement leaked when duckdb_prepare fails (cleanup missing on error path). *Fix:* Destroy the statement before asserting, e.g. capture rc, and on failure call duckdb_destroy_prepare(stmt.ctypes.data) before raising (or wrap the loop + destroy in try/finally). This mirrors the numpy-buffer destroy idiom already…
- **EX-os-COR-2** — `examples/online_scoring.py:123`. No chunk_p != 0 / empty-result guard before dereferencing the fetched chunk. *Fix:* Add `if chunk_p == 0: continue`/handle-miss after fetch, and check `duckdb_data_chunk_get_size(chunk_p) > 0` before reading; optionally check the bind/execute duckdb_state == DuckDBSuccess. At minimum add a comment stating the…
- **EX-os-MEM-2** — `examples/online_scoring.py:123`. No NULL/size guard on fetched chunk before dereferencing it. *Fix:* After duckdb_fetch_chunk, guard on chunk_p != 0 and on duckdb_data_chunk_get_size(chunk_p) >= 1 before reading vectors; skip/emit NaN and still destroy the (empty or NULL-safe) result when no row is present. At minimum document…

**T15 — Fix irr convergence tolerance and document the bracket**

- **EX-irr-COR-2** — `examples/irr.py:80`. Fixed bisection bracket [-0.99, 10.0]; roots outside the bracket silently return NaN. *Fix:* Document the [-0.99, 10.0] monthly-rate bracket and the all-positive-cashflow (single sign change) assumption in the irr_bisect docstring, and/or widen the bracket. At minimum this limitation should be stated so users porting the…

**T16 — Correct the examples README run instructions and links**

- **EX-readme-DOC-2** — `examples/README.md:20`. numbox clock.py doc link pinned to tag 0.5.8, below the numbox>=0.5.13 dependency pin. *Fix:* Repin the URL to the supported numbox version (e.g. /blob/0.5.13/... or a tag matching the pin), consistent with the other numbox links.

**T17 — Fix idx_t signedness and validity-pointer typing in the signatures**

- **DCK-sig-COR-1** — `numbduck/ducklib.py:106`. idx_t mapped to signed intp instead of uint64 in several count/size/index signatures (inconsistent with duckdb.h and with sibling entries). *Fix:* For strict fidelity, change these five entries to use uint64 for the idx_t return/arg positions: duckdb_column_count -> uint64(intp); duckdb_row_count -> uint64(intp); duckdb_data_chunk_get_column_count -> uint64(intp)…
- **DCK-sig-COR-2** — `numbduck/ducklib.py:237`. duckdb_vector_get_validity return typed uint64 but the C return is a pointer (uint64_t *) — violates the 'all pointers are intp' invariant. *Fix:* Change the signature at :237 to intp(intp) to match every other pointer-returning binding and the module's pointer convention: signatures['duckdb_vector_get_validity'] = intp(intp). Low priority given zero runtime impact.
- **DCK-wrap-COR-1** — `numbduck/ducklib.py:106`. idx_t returns/args inconsistently typed as signed intp instead of uint64 (and one uint64_t* return typed uint64). *Fix:* Normalize all idx_t returns/args to uint64 (change L106, L156, L157, L226, and the second arg of L234 to uint64) and, if the all-pointers-are-intp convention is intended, change duckdb_vector_get_validity (L237) return to intp.…
- **XC-numbox-COR-1** — `numbduck/ducklib.py:237`. duckdb_vector_get_validity return typed uint64 instead of intp, breaking the all-pointers-are-intp contract. *Fix:* Change the signature to intp(intp): `signatures["duckdb_vector_get_validity"] = intp(intp)` so the pointer return matches the intp-for-all-pointers convention and unifies with duckdb_validity_row_is_valid without casts. After the…

**T18 — ducklib cleanup: reorder stranded wrappers, remove dead code, normalize URLs, add a name-integrity test**

- **DCK-sig-DES-1** — `numbduck/ducklib.py:1411`. Struct-by-value bind wrappers stranded at file end; wrapper/section grouping diverges from the signatures dict. *Fix:* Move duckdb_bind_hugeint/uhugeint/interval/decimal up next to the other duckdb_bind_* wrappers, and move duckdb_scalar_function_get_extra_info/set_error into the scalar-function wrapper group. Adopt one deterministic order…
- **DCK-sig-DES-2** — `numbduck/ducklib.py:14`. Dead code: _is_win and its sole enabling import sys are unused. *Fix:* Delete line 14 (_is_win) and line 1 (import sys).
- **DCK-sig-DES-3** — `numbduck/ducklib.py:728`. Two varint wrappers use a divergent, versioned docstring URL instead of the mandated stable form. *Fix:* Change both docstrings to 'https://duckdb.org/docs/stable/clients/c/api.html#duckdb_create_varint' and '#duckdb_get_varint' to match the rest of the module and the CLAUDE.md rule.
- **DCK-wrap-COR-2** — `numbduck/ducklib.py:728`. varint wrapper docstrings use non-standard versioned doc links (violates api.html convention). *Fix:* Change both to https://duckdb.org/docs/stable/clients/c/api.html#duckdb_create_varint and ...#duckdb_get_varint (or verify the anchor exists on the stable page; if varint is absent from stable docs, keep a versioned link but at…
- **DCK-wrap-DES-2** — `numbduck/ducklib.py:1411`. Four struct-by-value bind wrappers appended at EOF instead of in their sorted slot with the other duckdb_bind_* wrappers. *Fix:* Relocate the four wrappers into the duckdb_bind_* cluster near lines 288-411 (adjacent to bind_double/bind_float and the other typed binds) so the family is contiguous.
- **DCK-wrap-DES-3** — `numbduck/ducklib.py:276`. ~190 near-identical 4-line wrappers each repeat the C function-name string three times. *Fix:* Accept the per-wrapper def (needed for proxy's cache anchoring) but reduce the hazard: derive the body's function-name literal from the wrapper (or add an import-time assert that each def name matches its signatures key), so a…
- **REC-DCK-sig-DES-2** — `numbduck/ducklib.py:122` _(carries forward 2026-06-29 DCK-sig-DES-2)_. signatures dict ordering inconsistent: duckdb_connect wedged inside the create_* run. *Fix:* Move duckdb_connect to its alphabetical slot (before the create_* keys) and keep the dict consistently sorted; mirror that order in the wrapper section.

**T19 — Add @cfunc exception guards to example callbacks and exercise the combine leak path**

- **EX-irr-MEM-1** — `examples/irr.py:156`. Borrowed structref ref leaks (and result is silently wrong) if an impl raises inside a @cfunc-wrapped callback. *Fix:* Wrap each impl body in a bare try/except that swallows-and-signals (or at minimum releases the borrowed ref) so a raise cannot both leak the ref and be silently discarded; e.g. compute results under try/except and, on failure…
- **EX-scalar-MEM-1** — `examples/haversine.py:117`. Scalar-UDF @cfunc callbacks have no exception guard; a raise inside the impl is swallowed at numba's cfunc boundary, handing DuckDB a silent 'success' with a partially-written/garbage output vector and no error signalled. *Fix:* For parity with the numbox phase-3 UDAF guard and XC-numbox-MEM-1, either wrap each impl body in a bare try/except (NOT try/finally -- it re-raises on numba 0.65.1) that writes a defined sentinel to the output on failure, or…
- **REC-EX-irr-MEM-1** — `examples/irr.py:170` _(carries forward 2026-06-29 EX-irr-MEM-1)_. Combine refcount/borrow-balance path is never exercised by the in-file NRT leak guard. *Fix:* Force the combine path to run inside the guarded region: run at least one query against a table large enough (or with `PRAGMA threads` raised / forced parallelism) to produce multiple partial states per group, keeping the…
- **XC-numbox-MEM-1** — `examples/irr.py:165`. UDAF @cfunc callbacks have no exception guard; a raise inside an impl is swallowed by numba's cfunc boundary, leaking the borrow_structref incref and handing DuckDB a silent zero/void default. *Fix:* Wrap each impl body in a bare try/except (the documented cure -- do NOT use try/finally, which re-raises on numba 0.65.1) so the frame runs its NRT cleanup on the error path and the callback returns a defined default; for…

**T20 — Handle NULL validity in the example scalar UDFs**

- **REC-EX-fraud-1** — `examples/fraud_score.py:118` _(carries forward 2026-06-29 EX-fraud-1)_. Chunk callback reads inputs and writes output with no validity/NULL handling. *Fix:* Either add a comment at the loop documenting the dense-data precondition, or (matching irr.py's reference handling at irr.py:140-153) fetch duckdb_vector_get_validity for each input vector and gate each row via…
- **REC-EX-fraud-MEM-1** — `examples/fraud_score.py:125-144` _(carries forward 2026-06-29 EX-fraud-MEM-1)_. Chunk callback reads input vectors without validity (NULL) guard. *Fix:* Either (a) document inline that the callback requires non-NULL inputs (matching the online_scoring/haversine caveat) and is correct only for the dense generated data, or (b) for parity with the irr.py reference, fetch each input…
- **REC-EX-hav-1** — `examples/haversine.py:105` _(carries forward 2026-06-29 EX-hav-1)_. Chunk callback reads no validity mask; the reference scalar-UDF pattern is silently wrong on NULL input. *Fix:* Either add a one-line precondition comment at the loop asserting inputs are non-NULL, or (better for a reference example) fetch each input's validity via duckdb_vector_get_validity and gate each row with…

**T21 — Add value assertions and edge-case coverage to the test suite**

- **REC-TST-COR-1** — `test/test_ducklib.py:3187` _(carries forward 2026-06-29 TST-COR-1)_. welford_combine's merged-mean arithmetic is never asserted (too loose). *Fix:* In test_welford_numba_only._compute_combined, also return the combined state's mean and assert it against the expected pooled mean (e.g. compare sb.mean to numpy.mean(xs) within tolerance), or add a dedicated assertion reading…
- **REC-TST-cover-4** — `test/test_ducklib.py:628` _(carries forward 2026-06-29 TST-cover-4)_. Bind out-of-range param-index error branch tested for only 5 of ~19 bind_* wrappers. *Fix:* Parametrize a single invalid-param-index check (index 999 -> assert DuckDBError) across all bind_* wrappers, or at minimum add the missing variants for the four struct-by-value binds.
- **REC-TST-cover-7** — `test/test_ducklib.py:1359` _(carries forward 2026-06-29 TST-cover-7)_. test_create_get_bit asserts only the byte length, not the bit payload. *Fix:* Read raw = (ctypes.c_char * size).from_address(result[0]) and assert it equals the input bytes, matching the blob/varint tests.
- **TST-COR-1** — `test/test_ducklib.py:1691`. test_get_value_type asserts only non-null, never verifies the returned type id. *Fix:* Add `assert ducklib.duckdb_get_type_id(type_p) == ducklib.DUCKDB_TYPE_INTEGER` after the non-null check (do not destroy type_p separately -- the existing comment correctly notes it aliases the value for scalar types).
- **TST-COR-2** — `test/test_ducklib.py:1641`. test_create_array_value never reads back element contents. *Fix:* Read back and assert at least the array size and one element (e.g. via duckdb_array_type / list-child accessors on the value or its type), matching the depth of the list/map/struct value tests.
- **TST-cover-11** — `test/test_ducklib.py:267`. Only the short (inline) string_t layout is read back; long (>12 char, out-of-line) strings untested. *Fix:* Add a bind_varchar / get_varchar round-trip with a >12-character string and a >12-byte blob, reading back via the out-of-line pointer layout (or via duckdb_value/string_at) and asserting equality.
- **TST-cover-5** — `test/test_ducklib.py:1165`. Destroy of value/logical-type/function handles is never verified (no slot-zero or leak assert). *Fix:* Where DuckDB zeroes the handle on destroy, assert buf[0]==0 after the destroy call (as done for DB/conn). Where it does not, add at least one long-loop create/destroy test with an OS/process RSS or DuckDB-side allocation check to…
- **TST-cover-6** — `test/test_ducklib.py:934`. test_column_logical_type executes but never asserts the type is correct. *Fix:* Add assert duckdb_get_type_id(logical_type_p) == DUCKDB_TYPE_INTEGER before destroying.
- **TST-cover-7** — `test/test_ducklib.py:1634`. test_create_array_value asserts only non-null; array value contents are never read back. *Fix:* Read back the array's length and/or child values (matching the list/map/struct tests) and assert they equal the inputs 1,2,3.
- **TST-cover-8** — `test/test_ducklib.py:2376`. test_scalar_function_set_init never verifies the init callback runs; init impl is a no-op. *Fix:* Have the init callback write an observable marker (e.g. via extra_info or a state slot) and assert the marker after the query, or otherwise verify the init callback fired.
- **TST-cover-9** — `test/test_ducklib.py:1029`. JIT (@njit) tests assert Success only; no in-JIT error/edge branch is covered. *Fix:* Add an @njit test that issues an invalid query and/or an out-of-range bind and asserts the returned rc == DuckDBError from within compiled code.

**T22 — Fix the example test helpers (_common.py)**

- **EX-common-1** — `examples/_common.py:73`. assert_results_match spuriously fails when all variants produce NaN. *Fix:* Handle NaN before the inequality, e.g. treat two values equal when both are NaN: `if first != other and not (isinstance(first,float) and isinstance(other,float) and math.isnan(first) and math.isnan(other))`, or use…
- **EX-common-2** — `examples/_common.py:50`. format_table does not validate row length; wrong-width rows crash or silently truncate. *Fix:* Add an explicit check, e.g. `for row in rows: if len(row) != len(headers): raise ValueError('each row must match headers length')`, before computing widths.

**T23 — Commit a real fallback __version__**

- **REC-BLD-DES-3** — `numbduck/__init__.py:1` _(carries forward 2026-06-29 BLD-DES-3)_. Committed __version__ is the empty string, so any non-CI build silently reports version 0.0.0. *Fix:* Commit a real fallback version (e.g. `__version__ = "0.0.0.dev0"`) or move version resolution to a tool that reads git metadata (setuptools-scm), so a local build yields a meaningful version without the CI overwrite.

**T24 — Make version-gated bindings raise a clear error for JIT callers**

- **REC-XC-err-2** — `numbduck/ducklib.py:726` _(carries forward 2026-06-29 XC-err-2)_. Version-gated bindings surface a numba TypingError, not NotImplementedError, for JIT callers on a libduckdb missing the symbol. *Fix:* Make the unavailable-symbol stub @njit-introspectable so JIT callers get a clear NotImplementedError-equivalent — e.g. have proxy_if_available register an @intrinsic/overload that raises a numba error naming the function at…

---

## Refuted (9)

Each refuted finding is listed with the reason it did not survive verification. None represents
an open defect.

- **EX-hav-1** — `examples/haversine.py:101` (reported high). Claimed the scalar-function
  wrapper passes constant-vector arguments un-flattened, so the kernel reads out of bounds past
  a 1-element buffer. Refuted: DuckDB's C-API scalar wrapper calls `DataChunk::Flatten()`
  unconditionally before invoking the callback (verified identical at v1.3.2 and v1.5.1, the
  pin endpoints), so the argument vectors are already flat n-element arrays and the kernel is
  correct.
- **BLD-DES-3** — `numbduck/__init__.py:1` (reported medium). Claimed the empty committed
  `__version__` makes a local build fail or emit an invalid PEP 440 version. Refuted: with the
  pinned toolchain, setuptools silently defaults the empty string to `0.0.0` and both
  `pip install -e .` and `python -m build` succeed — the real effect is a cosmetic placeholder
  version, not a failure (the milder, correct claim is carried by the confirmed REC-BLD-DES-3).
- **BLD-SEC-2** — `.github/workflows/numbduck_ci.yml:56` (reported low). Claimed the same
  `github.ref_name` script-injection as release.yml is exploitable here. Refuted as
  unreachable: this workflow triggers only on `push: branches` and `pull_request` (no `tags:`
  key), so `github.ref_type == "tag"` can never be true and the vulnerable branch is dead code
  — a latent copy-paste nit, not an active vulnerability.
- **DBU-MEM-1** — `numbduck/duckdb_utils.py:39` (reported low). Claimed the hard-coded 48-byte
  `duckdb_result` buffer could be overflowed if an in-range duckdb release enlarges the struct.
  Refuted: the struct is byte-identical at 1.3.2 and empirically 48 bytes at 1.4.0/1.5.1 (live
  ABI probe), and DuckDB's opaque-`internal_data` design plus its C-API no-ABI-break policy make
  growth in-range implausible — the premise is actively contradicted, not merely unconfirmed.
- **PBR-COR-1** — `numbduck/pybridge.py:62` (reported high, corrected to low → not adopted).
  Claimed an in-range duckdb release (1.4/1.5) could silently shift the `+32` layout so the read
  returns a wrong word. Refuted: green CI across the 1.3.2/1.4.0/1.5.1 matrix plus direct
  reproduction on all three versions show the offsets are stable across the pinned range on
  Linux x86-64; what remains is a much narrower low-value hardening point already carried by
  PBR-DES-2 / XC-ver-2.
- **REC-DBU-MEM-1** — `numbduck/duckdb_utils.py:8` (reported low; oldId DBU-MEM-1). The
  `create_duckdb_*().ctypes.data` dangling-temporary hazard is real in the abstract but was
  already consciously analyzed and declined by the current review (documented in
  `findings/DBU-MEM.json` notes) as a caller responsibility with no live in-repo defect —
  adopting it would duplicate a reasoned exclusion, not surface a missed defect.
- **REC-DCK-sig-DES-1** — `numbduck/ducklib.py:1399-1408` (reported low; oldId DCK-sig-DES-1).
  Refuted as a duplicate: the stranded `get_extra_info`/`set_error` scalar wrappers are already
  covered in full by the confirmed current DCK-sig-DES-1 (which names those exact wrappers and
  line numbers); the reconcile mapper read only the truncated gist and wrongly flagged a gap.
- **REC-PBR-DES-4** — `numbduck/pybridge.py:70` (reported low; oldId PBR-DES-4). Refuted as a
  duplicate: the missing version/offset-drift diagnostic in the validation-error path is already
  addressed in combination by the confirmed PBR-DES-1/PBR-DES-2 (which locate the fix at the
  up-front version guard, not the reactive message); no separate finding is warranted.
- **TST-cover-10** — `test/test_ducklib.py:835` (reported low). Compound claim that Windows
  struct-ABI and macOS loader paths are both unexercised. Refuted: its severity-driving half is
  false — the struct-by-value tests carry no skip and run on `windows-latest` in CI, driving
  numbox's Windows x64 sret/byval branches (and the cited `_call_lib_func_struct_in/out` no
  longer exist). The surviving macOS-loader half is carried by the confirmed REC-BLD-DES-1.

---

## Reconcile & completeness

The previous campaign (2026-06-29) confirmed 89 findings (of 92; three were refuted:
`TMD-DOC-5`, `TST-cover-8`, `XC-numbox-MEM-2`). Every one of those 89 was classified exactly
once against the current review ([`reconcile/AUDIT.json`](reconcile/AUDIT.json), which passed):

- **74 covered** by a confirmed current finding (same root cause, ≥1 confirmed covering
  finding). Four of the five old high-severity findings are among these and verify as
  genuine root-cause matches; the fifth (`BLD-DES-1`) had only a refuted covering finding,
  so its still-valid substance is instead carried by the confirmed adoption
  `REC-BLD-DES-1` in the bucket below.
- **0 stale** — no old finding was found no-longer-applicable.
- **15 missed and adopted** as `REC-*` findings. Of these adoptions, **14 were confirmed**
  (REC-BLD-DES-1, REC-BLD-DES-3, REC-BLD-DES-6, REC-DCK-sig-DES-2, REC-EX-fraud-1,
  REC-EX-fraud-MEM-1, REC-EX-hav-1, REC-EX-irr-MEM-1, REC-TST-COR-1, REC-TST-cover-4,
  REC-TST-cover-7, REC-UTL-DES-2, REC-UTL-DES-4, REC-XC-err-2) and one — REC-DBU-MEM-1 — was
  refuted because the current review had already consciously assessed that `ctypes.data`
  lifetime hazard as a caller-responsibility non-defect.

Two further `REC-*` candidates were raised out of caution for old findings that were already
*covered* and refuted as duplicates: **REC-DCK-sig-DES-1** (old substance already carried in
full by the confirmed current DCK-sig-DES-1) and **REC-PBR-DES-4** (already carried in
combination by the confirmed PBR-DES-1/PBR-DES-2). Together with REC-DBU-MEM-1 these are the
three refuted adoptions; none creates a coverage gap. In total 17 `REC-*` were raised (14
confirmed + 3 refuted).

**One bookkeeping blemish (PBR-COR-1).** Old `PBR-COR-1` (the hardcoded `+16/+32` offsets with
no runtime version guard) is mapped "covered" *solely* to the new `PBR-COR-1`, which was
refuted, and no `REC-PBR-COR-1` was raised — so by the strict audit rule its covered-claim is
unresolved. This is an attribution error, not a loss of substance: the surviving "no version
guard / no re-verify mechanism" content is in fact carried by the confirmed current findings
**PBR-DES-2** and **XC-ver-2** (same offsets, same lines), which the mapper attributed to old
PBR-DES-1 instead. The audit-adoptions round caught the analogous sole-covered-by-refuted cases
for BLD-DES-1 / BLD-DES-3 (rescued via REC adoptions) but missed this one. It is a broken
mapper row pointing PBR-COR-1's coverage at a refuted finding rather than at its confirmed
twins — bookkeeping, not a genuine gap.

**Conclusion.** The current review plus the 14 confirmed `REC-*` adoptions is a complete
superset of everything still valid from 2026-06-29: every still-valid old finding's substance
is present in some confirmed current finding or confirmed adoption. The only residue is
attribution/bookkeeping (the PBR-COR-1 row, and two refuted RECs that were really
already-covered duplicates) plus the single low-severity `ctypes.data` hazard that the current
review deliberately downgraded to a documented non-issue. No substantive old defect is left
uncovered.

---

## Delta vs 2026-06-29

The 2026-06-29 review examined a tree that predated the `@proxy` migration (it reviewed the
`@cres`-wrapped bindings, with the hand-rolled hugeint/interval/decimal intrinsics already
migrated to `_call_lib_func`). This review is against the current `@proxy` / `numbox>=0.5.13`
tree (HEAD `3ed20bc`), and consequently covers surface the old review could not:

- **`numbduck/configurations.py`** — the new `get_jit_options()` / `NUMBDUCK_JIT_OPTIONS` knob
  and the module-level `jit_options` dict threaded into every wrapper. New findings CFG-COR-1/2,
  CFG-COR-3, and DCK-proxy-COR-1 all target its missing shape/key validation.
- **`.github/workflows/release.yml`** — the new tag-triggered PyPI Trusted-Publishing pipeline.
  New findings BLD-SEC-1 (ref-name script injection into the publish artifact), BLD-SEC-3
  (`pypa/gh-action-pypi-publish` pinned to a moving branch in the `id-token: write` job), and
  BLD-DES-5 (workflow drift) all live here.
- **The `@proxy` + `jit_options` wiring** — the migration itself is the subject of the two
  high-severity CLAUDE.md findings (DOC-acc-2/3) and DCK-wrap-DES-1, because the doc still
  describes the removed `@cres` pattern and nonexistent local helpers; and it is the setting for
  the high-severity ABI finding DCK-byval-MEM-1 on the by-value `duckdb_result` path.

Findings that recur from 2026-06-29 (idx_t typing, pybridge offset/validation gaps, loader
robustness, example error paths, test coverage, design-doc citations) are either re-confirmed
against the current tree or carried forward as the confirmed `REC-*` adoptions noted above.
