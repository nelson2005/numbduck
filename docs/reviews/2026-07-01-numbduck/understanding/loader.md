# numbduck subsystem: Shared-library loading & platform handling

Scope: how numbduck locates, loads, and version-gates the DuckDB C API
shared library so that its symbols become resolvable by numba/LLVM's JIT
linker. Primary file: `numbduck/utils.py`. Downstream consumer:
`numbduck/ducklib.py`. Backing primitive: `numbox/core/bindings/utils.py`.

All line cites are against the current tree (`review/numbduck-2026-06-29`).

---

## 1. Entry point and control flow

The whole subsystem runs **once, at import time** of `ducklib.py`:

- `numbduck/ducklib.py:9` — `from numbduck.utils import load_duckdb`
- `numbduck/ducklib.py:12` — `duckdb_lib = load_duckdb()`

`duckdb_lib` is a module-level global holding the `ctypes.CDLL` handle. It
is used later for **version gating** — passed to
`proxy_if_available(duckdb_lib, sig, ...)` at `ducklib.py:726`, `:1051`,
`:1285` (varint/bignum + scalar-function-set-init symbols). Tests also
probe it directly: `test/test_ducklib.py:1342`, `:2373`
(`hasattr(ducklib.duckdb_lib, 'duckdb_create_varint')`, etc.).

`load_duckdb` (`utils.py:113-130`) is the orchestrator:

```
1. lib_path = find_duckdb_shared_lib()          # locate the wheel's shared object
2. lib = load_lib_path(lib_path)                # CDLL(..., RTLD_GLOBAL)
3. if _has_capi_symbols(lib): return lib        # happy path (Linux, older macOS)
4. standalone = _find_standalone_libduckdb()    # env / versioned cache / brew paths
5. if standalone: load it; if it has symbols, return it
6. if not Darwin: raise (tell user to set NUMBDUCK_LIBDUCKDB)
7. downloaded = _download_libduckdb()           # macOS: prompt + download universal dylib
8. return load_lib_path(downloaded)
```

Steps 4-8 only matter on macOS with a DuckDB Python wheel that strips the
C API (`utils.py:118` comment: `1.5.2 >= duckdb >= 1.4.1`). See project
CLAUDE.md "macOS C API stripping is intentional".

---

## 2. Locating the shared object — `find_duckdb_shared_lib` (`utils.py:94-110`)

Two layouts, discriminated by DuckDB packaging version:

- **duckdb 1.3.x** (`utils.py:95-100`): the shared lib lives *inside* the
  `duckdb/` package dir. It lists the top level of that dir
  (`next(iter(os.walk(duckdb_dir)))[2]` — files only, non-recursive) and
  regex-matches `duckdb[\w.-]*\.(so|dll|dylib|pyd)` (`utils.py:98`).
- **duckdb 1.4+** (`utils.py:101-106`): the lib moved to the parent
  `site-packages/` as `_duckdb.*`; regex `_duckdb[\w.-]*\.(so|dll|dylib|pyd)`
  (`utils.py:104`).

Invariant: **exactly one** match in whichever tier hits. `len == 1` is
required (`:99`, `:105`); anything else (0 or >1) falls through to a
`RuntimeError` listing both candidate sets (`:107-110`).

`getfile(duckdb)` (`:95`) resolves the package location via the imported
`duckdb` module object — so it tracks whatever `import duckdb` at
`utils.py:1` bound, i.e. the active environment's wheel.

---

## 3. Loading — `load_lib_path` (numbox `bindings/utils.py:167-183`)

numbduck delegates the actual `dlopen` to numbox's `load_lib_path`
(imported at `utils.py:8`). Behaviour:

- **Linux / Darwin** (`bindings/utils.py:178-180`):
  `CDLL(path, mode=RTLD_GLOBAL)`.
- **Windows** (`:181-182`): `CDLL(path, winmode=0)`.
- Other platforms raise (`:183`).

**`RTLD_GLOBAL` is the load-bearing detail.** It promotes the library's
symbols into the global symbol namespace so that when numba/LLVM's JIT
linker resolves the *extern declarations* emitted by `_call_lib_func`
(numbox emits `get_or_insert_function` extern refs, resolved at link time —
see numbox CLAUDE.md "LLVM symbol resolution and macOS"), `dlsym`/the JIT
resolver finds them. Without `RTLD_GLOBAL` the JIT link would fail at call
time.

`load_lib_path` is **uncached** (contrast numbox's `load_lib` /
`_loaded_libs` cache at `bindings/utils.py:115-144`). Lifetime is instead
guaranteed by numbduck holding the result in the module global
`duckdb_lib` (`ducklib.py:12`). This matters: `ctypes.CDLL.__del__` calls
`dlclose`; if the handle were dropped, the OS refcount could hit zero and
invalidate symbols LLVM already resolved (numbox `bindings/utils.py:130-137`
documents exactly this hazard). Intermediate handles in `load_duckdb`
(e.g. the symbol-less wheel handle at `utils.py:115`, or a standalone that
also lacked symbols at `:121`) are *not* retained and may be `dlclose`d on
GC — acceptable because only the final returned handle is used.

---

## 4. Symbol presence check — `_has_capi_symbols` (`utils.py:29-30`)

`hasattr(lib, "duckdb_open")`. `ctypes.CDLL.__getattr__` does a `dlsym`
for the name; missing → `AttributeError` → `hasattr` False. This is a
**single-symbol probe**: it certifies only that *some* C API is present,
not that any particular newer function exists. Per-version differences are
handled separately by `proxy_if_available` (§6), which does the same
`hasattr(lib, func.__name__)` probe per gated symbol
(numbox `proxy.py:155`).

Consequence (note, not audited): any binding that is **not** wrapped in
`proxy_if_available` but whose symbol is absent in the loaded lib will not
fail here — it will surface as an LLVM link error at first JIT call. The
invariant "only the three symbols at `ducklib.py:726/1051/1285` vary by
version" is therefore load-bearing.

---

## 5. macOS standalone fallback

### 5a. Discovery — `_find_standalone_libduckdb` (`utils.py:33-47`)

Search order:
1. `NUMBDUCK_LIBDUCKDB` env var, if it names an existing file
   (`:34-36`). This is the user override / escape hatch.
2. **Migration side effect** (`:37-40`): if the *old unversioned* cache
   `~/.numbduck/lib/libduckdb.dylib` exists, it is deleted. Note: this is
   an unconditional filesystem mutation performed inside a "find"
   (read-flavoured) function.
3. Versioned cache `~/.numbduck/lib/<duckdb.__version__>/libduckdb.dylib`
   (`:41-43`), where the base/dir constants are `utils.py:16-21`.
4. Homebrew / `/usr/local` paths in `_MACOS_LIBDUCKDB_SEARCH_PATHS`
   (`utils.py:23-26`, iterated `:44-46`).
5. Else `None`.

Note this function runs on **any** platform (`load_duckdb:119` is not
Darwin-gated). On Linux a wheel that lacked symbols could still be
rescued by `NUMBDUCK_LIBDUCKDB` pointing at a `.so`; the cache/brew paths
are `.dylib` and macOS-specific so they simply won't match on Linux.

### 5b. Download — `_download_libduckdb` (`utils.py:50-91`)

Reached only on the Darwin branch (`load_duckdb:124-129`):
1. Builds URL for the **universal** macOS zip of the *exact* wheel
   version: `.../releases/download/v{duckdb.__version__}/libduckdb-osx-universal.zip`
   (`:52-56`).
2. Consent gate: unless `NUMBDUCK_LIBDUCKDB_DOWNLOAD == "1"` (`:57-58`),
   it prints an explanation and calls `input("Download now? [y/N] ")`
   (`:67-69`); a non-yes answer raises `RuntimeError` with brew/env
   instructions (`:70-77`).
3. `urlopen(url).read()` (`:83-84`), open the zip from memory
   (`:85`), `makedirs` the versioned cache dir (`:86`), extract the member
   literally named `"libduckdb.dylib"` (`:88`) to
   `<cache>/libduckdb.dylib` (`:87-89`), return the path (`:91`).
4. `load_duckdb:130` then loads it via `load_lib_path` (with no re-check of
   `_has_capi_symbols` — the download is trusted to be correct).

---

## 6. Version gating boundary — `proxy_if_available` (numbox `proxy.py:136-164`)

`ducklib.py` wraps most functions with `@proxy(sig, jit_options=...)`, but
symbols that only exist in newer DuckDB use
`@proxy_if_available(duckdb_lib, sig, jit_options=...)`. That decorator
(`proxy.py:154-163`): if `hasattr(lib, func.__name__)` it behaves like
`proxy`; otherwise it returns a **stub** that raises `NotImplementedError`
at call time (`:158-159`) and does **not** expose `.as_func` (`:145-152`).
So the loaded `duckdb_lib` handle is not just a symbol source — it is the
oracle for compile-time feature detection.

`jit_options` originates from `numbduck/configurations.py`
(`get_jit_options`): reads `NUMBDUCK_JIT_OPTIONS` as JSON, default
`{"cache": True}`, raising `ValueError` on invalid JSON. Not part of the
loader per se but flows into every wrapper built against `duckdb_lib`.

---

## 7. Boundaries summary

- **Python / ctypes**: package discovery (`os.walk`, regex), `CDLL`
  loading, `hasattr` symbol probes, download/zip/prompt I/O.
- **OS dynamic loader**: `dlopen` with `RTLD_GLOBAL` makes symbols
  process-global; `dlsym` backs both the presence checks and JIT linkage.
- **numba / LLVM JIT**: emits extern refs (in numbox `_call_lib_func`)
  resolved at link time against the globally-loaded symbols; requires the
  handle to stay alive (module global) so `dlclose` never runs.

---

## 8. Invariants

1. Exactly one shared-object candidate is found (`find_duckdb_shared_lib`).
2. The final handle is retained for process lifetime via `ducklib.duckdb_lib`.
3. The library is loaded `RTLD_GLOBAL` so JIT extern refs resolve.
4. `duckdb_open` present ⇒ "the C API is present"; per-version symbol
   differences are handled only for the three explicitly gated functions.
5. Cache is keyed by `duckdb.__version__`, and the downloaded standalone
   release tag is assumed to equal that same version string.

---

## 9. Fragile assumptions / risk notes (surfaced, not fully audited)

- **No integrity verification of the download.** `_download_libduckdb`
  (`utils.py:83-89`) fetches over HTTPS from GitHub but performs no
  checksum/signature check and no `urlopen` timeout; a MITM or a hung
  connection is unguarded. Supply-chain-sensitive.
- **Version-string ↔ release-tag coupling.** The URL uses
  `v{duckdb.__version__}` and the cache dir uses the same string
  (`:16-21`, `:52-56`). If the Python wheel version does not correspond to
  a DuckDB GitHub release that ships `libduckdb-osx-universal.zip` (e.g.
  post/pre-release suffixes, or a version where the asset name differs),
  the `urlopen` 404s / the `zf.open("libduckdb.dylib")` KeyErrors. The
  exact member name `"libduckdb.dylib"` (`:88`) is also assumed.
- **Downloaded lib is trusted without a symbol re-check.**
  `load_duckdb:129-130` returns the download result directly; a
  wrong-arch or symbol-stripped asset would only fail later at JIT link.
- **`input()` in non-interactive contexts.** Without
  `NUMBDUCK_LIBDUCKDB_DOWNLOAD=1`, import of `ducklib` calls `input()`
  (`utils.py:67`), which raises `EOFError` under CI / piped stdin,
  turning a library import into a crash.
- **Filesystem mutation inside discovery.** `_find_standalone_libduckdb`
  unconditionally `os.remove`s the legacy cache (`:38-40`) as a side
  effect of a lookup; concurrent processes or a read-only home could make
  this throw / race.
- **Regex breadth in `find_duckdb_shared_lib`.** `duckdb[\w.-]*\.(so|dll|dylib|pyd)`
  / `_duckdb[\w.-]*\.…` (`:98`, `:104`) could match unexpected sidecar
  files (versioned sonames, debug copies), yielding `len != 1` and a hard
  `RuntimeError` even when a usable lib exists. The scan is non-recursive
  (`next(iter(os.walk(...)))`), so a nested layout is invisible.
- **Multiple globally-loaded DuckDB copies on macOS.** On the fallback
  path the symbol-less wheel is loaded `RTLD_GLOBAL` first (`:115`), then
  a standalone/downloaded dylib is loaded `RTLD_GLOBAL` too (`:121`/`:130`).
  If the wheel ever exported a *partial* C API, `dlsym(RTLD_DEFAULT)`'s
  first-in-load-order rule could bind the JIT to the wrong copy. Today the
  wheel exports none of the gated symbols, so it is latent, not active.
- **`_has_capi_symbols` under-specifies.** A single-symbol probe cannot
  detect a lib that has `duckdb_open` but is otherwise older/newer than
  the ungated bindings expect; mismatches surface as call-time link
  errors, not at load.
