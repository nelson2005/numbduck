# Subsystem: Shared-library loading & platform handling

Scope: how numbduck locates and loads the DuckDB native library so that its
C API symbols (`duckdb_open`, …) become resolvable by numba/llvmlite
JIT-compiled code. Primary file:
`/home/erik/projects/numbduck/numbduck/utils.py`. Underlying loader:
`/home/erik/projects/numbox/numbox/core/bindings/utils.py`.

This note feeds a downstream defect review; risks are flagged inline as
**RISK** but not fully audited.

---

## 1. Entry point and control flow: `load_duckdb()`

`load_duckdb()` (`utils.py:113-130`) is the single public entry. Linear flow:

1. `lib_path = find_duckdb_shared_lib()` (`utils.py:114`) — locate the shared
   library shipped inside the installed `duckdb` Python wheel.
2. `lib = load_lib_path(lib_path)` (`utils.py:115`) — delegate to numbox's
   `load_lib_path` (`numbox/core/bindings/utils.py:167-183`), which does
   `CDLL(path, mode=RTLD_GLOBAL)` on Linux/Darwin or `CDLL(path, winmode=0)`
   on Windows.
3. `_has_capi_symbols(lib)` (`utils.py:116`, def at `29-30`) — probe whether
   the loaded handle exports `duckdb_open`. If yes, return the handle
   (`utils.py:117`). **This is the only path taken on Linux/Windows and on
   macOS wheels that still export the C API.**
4. If the wheel lacks the C API (macOS, duckdb-python ≥ 1.4.1 strips C API
   symbols — see CLAUDE.md "macOS C API stripping is intentional"), fall to
   `standalone = _find_standalone_libduckdb()` (`utils.py:119`). If found,
   load it (again via `load_lib_path`, RTLD_GLOBAL) and re-probe
   (`utils.py:120-123`); return on success.
5. If still no symbols and `platform.system() != "Darwin"`, raise — the
   download fallback is macOS-only (`utils.py:124-128`).
6. Otherwise `downloaded = _download_libduckdb()` then
   `return load_lib_path(downloaded)` (`utils.py:129-130`).

### Data returned

The function returns a `ctypes.CDLL` handle. There is **no caching** in this
path: `load_lib_path` is uncached (contrast numbox's `load_lib` /
`_loaded_libs` cache at `numbox/.../utils.py:115-144`). Lifetime of the
returned handle is the caller's responsibility — see Invariants.

---

## 2. Locating the wheel library: `find_duckdb_shared_lib()`

`utils.py:94-110`. Uses `getfile(duckdb)` (`utils.py:95`) → directory of the
`duckdb` package. `next(iter(os.walk(duckdb_dir)))[2]` (`utils.py:96`) grabs
**only the top-level filenames** of that directory (first tuple from the walk
generator, index 2 = filenames).

Two layouts handled:

- **duckdb 1.3.x** (`utils.py:97-100`): library lives inside the package dir,
  named `duckdb*.{so,dll,dylib,pyd}` (regex `duckdb[\w.-]*\.(so|dll|dylib|pyd)`).
  Exactly one match → return it.
- **duckdb 1.4+** (`utils.py:101-106`): library moved up to
  `site-packages/` as `_duckdb*.{so,dll,dylib,pyd}` (regex `_duckdb[\w.-]*…`).
  `parent_dir = dirname(duckdb_dir)` is site-packages. Exactly one match →
  return it.
- Neither → `RuntimeError` listing both candidate lists (`utils.py:107-110`).

**RISK (ambiguity / layout):** the "exactly one" requirement is strict. Any
deviation (zero matches, two matches, editable installs that scatter the
`.so`, walk seeing both a `duckdb*.so` and the 1.4 `_duckdb*.so` during a
transitional layout) either raises or silently picks the 1.3.x branch first
without checking the 1.4 one. The walk is top-level-only, so a nested lib is
invisible.

---

## 3. macOS standalone discovery + cache: `_find_standalone_libduckdb()`

`utils.py:33-47`. Resolution order:

1. **Env override** `NUMBDUCK_LIBDUCKDB` (`utils.py:34-36`): if set and points
   at an existing file, return it verbatim. Platform-agnostic (also consulted
   on Linux/Windows before the eventual raise).
2. **Old-cache cleanup** (`utils.py:38-40`): unconditionally `os.remove` the
   legacy *unversioned* cache `~/.numbduck/lib/libduckdb.dylib`
   (`_LIBDUCKDB_CACHE_BASE`, `utils.py:16-18`) if present.
3. **Versioned cache** (`utils.py:41-43`):
   `~/.numbduck/lib/<duckdb.__version__>/libduckdb.dylib`
   (`_LIBDUCKDB_CACHE_DIR`, `utils.py:19-21`). Return if it exists.
4. **Homebrew search paths** (`utils.py:44-46`, `_MACOS_LIBDUCKDB_SEARCH_PATHS`
   at `23-26`): `/opt/homebrew/lib/libduckdb.dylib` then
   `/usr/local/lib/libduckdb.dylib`.
5. Else `None`.

Cache directory is keyed by `duckdb.__version__`, so a Python-side duckdb
upgrade transparently invalidates the old cached dylib (forces re-discovery
or re-download for the new version).

**RISK:** `os.remove(old_cached)` is unguarded against permission/race errors.
The homebrew dylib (step 4) is whatever version brew installed — there is **no
version check** that it matches `duckdb.__version__`; a brew/`pip` version skew
loads a mismatched runtime (see ABI risk in §6).

---

## 4. Download fallback: `_download_libduckdb()`

`utils.py:50-91`. macOS-only (callers gate on Darwin at `utils.py:124`).

- URL built from `duckdb.__version__`:
  `…/duckdb/duckdb/releases/download/v{version}/libduckdb-osx-universal.zip`
  (`utils.py:51-56`).
- **Consent gate** `NUMBDUCK_LIBDUCKDB_DOWNLOAD` (`utils.py:57-77`): if the env
  var is not exactly `"1"`, print an explanation and **block on `input()`**
  for a `y/N` prompt; a non-yes answer raises `RuntimeError` with brew/env
  instructions. `=="1"` skips the prompt (CI/non-interactive path).
- Download + extract (`utils.py:78-90`): `urlopen(url).read()` into memory,
  open as `zipfile.ZipFile(io.BytesIO(...))`, `makedirs` the versioned cache,
  extract the single member named `"libduckdb.dylib"` into
  `<cache>/libduckdb.dylib`, return that path.

**RISK (multiple):**
- Blocking `input()` (`utils.py:67`) hangs or raises `EOFError` in any
  non-interactive context that forgot to set `NUMBDUCK_LIBDUCKDB_DOWNLOAD=1`.
- `v{version}` assumes the Python package version string maps 1:1 to a DuckDB
  GitHub release tag and that a `libduckdb-osx-universal.zip` asset exists for
  it. Any divergence (post/dev suffix, package vs core version skew) → 404 /
  `HTTPError`.
- `zf.open("libduckdb.dylib")` (`utils.py:88`) assumes a flat archive member
  name; a layout change → `KeyError`.
- No checksum / signature verification of the downloaded binary; full network
  trust on the release host over plain `urlopen` (TLS only).
- Whole archive is read into memory (`resp.read()`), then the member read into
  memory again — fine for current sizes, noted.

---

## 5. The C / JIT / Python boundary

- **Python/ctypes side:** loading is pure ctypes. `load_lib_path`
  (`numbox/.../utils.py:167-183`) returns a `CDLL`. Symbol probing
  (`_has_capi_symbols`, `utils.py:29-30`) is `hasattr(lib, "duckdb_open")` —
  ctypes `CDLL.__getattr__` performs a `dlsym`; a missing symbol raises
  `AttributeError`, so `hasattr` → `False`. This is the *only* mechanism by
  which numbduck distinguishes a full-C-API library from the symbol-stripped
  macOS wheel.
- **JIT/C linkage:** the load-bearing mechanism is `RTLD_GLOBAL`
  (`numbox/.../utils.py:178-180`). Loading the duckdb library into the
  **global** symbol namespace makes `duckdb_*` symbols resolvable by
  llvmlite's JIT linker when numba lowers the `@cres`/`_call_lib_func`
  bindings in `ducklib.py` (the externs are resolved by the process-wide
  dynamic loader, not passed explicitly). Without `RTLD_GLOBAL` the JIT-emitted
  external references would fail to resolve. Windows uses `winmode=0`
  (`numbox/.../utils.py:181-182`).
- numbox `load_lib_path` raises for any platform other than Darwin/Linux/
  Windows (`numbox/.../utils.py:183`).

---

## 6. Invariants and fragile assumptions

**Invariant — handle retention.** `load_lib_path` is uncached and
`ctypes.CDLL.__del__` calls `dlclose`/`FreeLibrary` (documented at
`numbox/.../utils.py:131-138`). If the handle `load_duckdb()` returns is not
pinned for the process lifetime, the OS refcount can drop to zero, the library
is unloaded, and any JIT-resolved `duckdb_*` extern goes dangling → crash.
**RISK:** unlike numbox's `load_lib` (which pins handles in `_loaded_libs`),
this path provides no internal retention; correctness depends entirely on the
caller in `ducklib.py` keeping the returned handle alive at module scope.

**Symbol-presence heuristic.** `_has_capi_symbols` checks exactly one symbol
(`duckdb_open`). It assumes presence of that single symbol implies the entire
C API surface numbduck needs is present. A partially-exported library would be
mis-classified as good.

**RISK — double-load of two duckdb runtimes (macOS fallback).** On the
standalone/download path the wheel library is *already* loaded RTLD_GLOBAL at
`utils.py:115` (it just lacks the exported `duckdb_open`), and then a second,
full `libduckdb.dylib` is loaded RTLD_GLOBAL at `utils.py:121`/`130`. Two
copies of the DuckDB runtime then coexist in one process with global symbol
visibility. Whichever symbol definition the loader binds first wins for
subsequent resolution; numbduck relies on the standalone copy providing
`duckdb_open` while the wheel's are hidden. Mixing handles created by the
Python `duckdb` module (wheel runtime) with numbduck's C-API handles (standalone
runtime) would cross runtime boundaries — handles/allocations are not
interchangeable across two independent duckdb instances. Not audited; flagged.

**Version-coupling.** Three places key off `duckdb.__version__`: cache dir
(`utils.py:20`), download URL (`utils.py:51`), and (implicitly) what the
homebrew/standalone dylib *should* be. Nothing verifies the discovered or
downloaded native version actually equals the Python wheel's version except by
cache-path naming; the homebrew search path (§3 step 4) bypasses even that.

**Platform gating.** Download is Darwin-only (`utils.py:124-128`); Linux/Windows
wheels are assumed to always export the C API (Linux `--export-dynamic-symbol`
is additive, per CLAUDE.md). If a future Linux wheel strips symbols, the only
recourse is `NUMBDUCK_LIBDUCKDB`, after which `load_duckdb` raises.

**Non-interactive safety.** The interactive `input()` consent prompt
(`utils.py:67`) is the default behavior; `NUMBDUCK_LIBDUCKDB_DOWNLOAD=1` is the
only non-interactive escape, and it silently authorizes an unverified network
download.

---

## 7. Environment variables (summary)

- `NUMBDUCK_LIBDUCKDB` (`utils.py:34`) — absolute path to a prebuilt library;
  highest-priority standalone source; consulted on all platforms. The
  non-Darwin error message recommends it (`utils.py:127`).
- `NUMBDUCK_LIBDUCKDB_DOWNLOAD` (`utils.py:57`) — `"1"` to auto-approve the
  macOS standalone download and skip the `input()` prompt.
- (numbox loader docstrings also mention `DYLD_LIBRARY_PATH`/`LD_LIBRARY_PATH`
  influencing `find_library`/loader search — relevant to `load_lib`, not the
  explicit-path `load_lib_path` numbduck uses; noted for completeness,
  `numbox/.../utils.py:125-129`.)
