# Cross-platform benchmark CI + restored macOS job

**Branch:** `motivating-examples-spec`
**Head at pause:** `e6a8c2b` (Add motivating examples directory... + CI smoke-test haversine)
**PR:** nelson2005/numbduck#28 (OPEN)
**Paused:** 2026-04-09

## Goal

Extend CI so every example (`haversine.py`, `online_scoring.py`, `fraud_score.py`)
runs end-to-end under `NUMBDUCK_BENCH_TINY=1` on every OS: Linux x86_64,
Linux ARM64, Windows, macOS. Also restore the top-level `macos` pytest job
that was present on PR #24's `udf-udaf-bindings` branch but never merged to
main and has since been forgotten.

## Approved workflow shape (3 jobs)

### Job 1: `build` — existing matrix, one change
- Keep: `python-version × arch × numba-version × duckdb-version` with current
  excludes (44 cells).
- **Remove** the haversine smoke step added in `e6a8c2b` — it becomes
  redundant once the `benchmarks` job runs haversine on all 4 OSes.
- Keep `--durations=20` on the pytest line.

### Job 2: `macos` — restored verbatim from [`fff8e38b`](https://github.com/nelson2005/numbduck/blob/fff8e38b5aaaf7e5311e9d9554045eb976a2c397/.github/workflows/numbduck_ci.yml)
```yaml
macos:
  runs-on: macos-latest
  env:
    NUMBDUCK_LIBDUCKDB_DOWNLOAD: "1"
  steps:
    - actions/checkout@v4
    - setup-python@v5 with python-version: "3.13"
    - echo "__version__ = '0.0.1'" > numbduck/__init__.py
    - pip install pytest "numba==0.64.0" "duckdb==1.5.1" && pip install -e .
    - pytest -m "not benchmark" --durations=20   # add --durations=20 per MEMORY.md rule
```

### Job 3: `benchmarks` — new, 4-cell OS matrix
```yaml
benchmarks:
  needs: [build, macos]
  strategy:
    fail-fast: false
    matrix:
      os: [ubuntu-latest, ubuntu-24.04-arm, windows-latest, macos-latest]
  runs-on: ${{ matrix.os }}
  env:
    NUMBDUCK_BENCH_TINY: "1"
    NUMBDUCK_BENCH_REPEATS: "1"
    NUMBDUCK_LIBDUCKDB_DOWNLOAD: "1"   # only consulted on macOS, harmless elsewhere
  steps:
    - actions/checkout@v4
    - setup-python@v5 with python-version: "3.12"
    - echo "__version__ = '0.0.1'" > numbduck/__init__.py
    - pip install flake8 pyarrow "numba==0.64.0" "duckdb==1.5.1" && pip install -e .
    - python examples/haversine.py
    - python examples/online_scoring.py
    - python examples/fraud_score.py
```

Pinned single python/numba/duckdb combo per user instruction.

## Uncommitted state at pause

```
 M docs/superpowers/plans/2026-04-07-numbduck-motivating-examples.md.tasks.json
 M examples/fraud_score.py        (NUMBDUCK_BENCH_TINY support — done)
 M examples/online_scoring.py     (uses _jit_clock.monotonic_ns + TINY support — SEGFAULTS)
?? examples/_jit_clock.py         (new cross-platform monotonic clock — works in isolation)
```

The `tasks.json` diff is just status bookkeeping — safe to leave as-is.

## Cross-platform clock helper — `examples/_jit_clock.py`

**Status:** file exists, **standalone test passes**:
```
$ venv/bin/python -c "
import sys, numpy
sys.path.insert(0, 'examples')
from numba import njit
from numba.core.types import intp
from _jit_clock import monotonic_ns

@njit
def tst():
    scratch = numpy.zeros(2, dtype=numpy.int64)
    p = intp(scratch.ctypes.data)
    t0 = monotonic_ns(p)
    t1 = monotonic_ns(p)
    return t1 - t0

print('result:', tst())
"
result: 44
```

API: `monotonic_ns(scratch_p: intp) -> int64` — caller owns a 16-byte
int64 scratch buffer, passes its address; zero per-call allocations.

Implementation:
- **Linux/Darwin**: binds `clock_gettime(CLOCK_MONOTONIC, &ts)` via
  `signatures["clock_gettime"] = int32(int32, intp)` + `@cres` wrapper +
  `carray` readback of `(tv_sec, tv_nsec)` → nanoseconds.
- **Windows**: binds `QueryPerformanceCounter(&counter)` from `kernel32.dll`,
  explicitly `ll.load_library_permanently("kernel32.dll")` so LLVM's
  `address_of_symbol` finds it, reads `QueryPerformanceFrequency` once at
  module import via ctypes and bakes it into the `@cres` body as a
  compile-time constant.

Rationale: numbox's `_call_lib_func` uses `llvmlite.binding.address_of_symbol`
which calls `dlsym(RTLD_DEFAULT)` on POSIX (picks up libc automatically) but
on Windows requires the DLL to be registered with LLVM's dynamic-library
table. `load_library_permanently` handles that.

## BLOCKER: online_scoring.py segfaults

Running `NUMBDUCK_BENCH_REPEATS=1 venv/bin/python examples/online_scoring.py`
crashes with exit 139 (SIGSEGV, core dumped). Happens at both default size
(50K events) and `NUMBDUCK_BENCH_TINY=1` (200 events). `_jit_clock.monotonic_ns`
works in isolation (see standalone test above) so the bug is in the
`online_scoring.py` integration, not the clock helper itself.

**Not yet bisected.** No output produced at all — crash happens before
`print_env()` runs, or inside the first `_score_jit_loop` warm-up. Need to
narrow down.

### Things to try when resuming

1. **Print early** — add `print("alive", flush=True)` at the very top of
   `main()` and after each major step (before `run_latency_block`, after
   the warmup, etc.) to pin down where the segfault lands.
2. **Compare against last-good** — `git diff e6a8c2b -- examples/online_scoring.py`
   to see the full delta. The last-known-working version (on `ee52fe3` /
   `e6a8c2b`) used inline `signatures["clock_gettime"]` + explicit
   `ts = numpy.zeros(2); ts_p = intp(ts.ctypes.data); clock_gettime(...)`.
   The new version replaces two call sites with `monotonic_ns(ts_p)`.
3. **Suspect 1: @cres-calling-@cres from @njit.** `_score_jit_loop` is
   `@njit(nogil=True)` and calls `monotonic_ns` which is `@cres` and
   internally calls `_clock_gettime` which is also `@cres`. This two-level
   @cres nesting might not work the same way the original one-level
   `clock_gettime` call did. **Test:** inline the clock_gettime call back
   into `monotonic_ns` (don't go through a `_clock_gettime` helper) — i.e.
   call `_call_lib_func("clock_gettime", ...)` directly inside `monotonic_ns`.
4. **Suspect 2: removed import.** I removed `from numbduck.duckdb_utils
   import create_duckdb_result` — verify it really wasn't used. (Grep for
   `create_duckdb_result` in the file.) If it was used, the resulting
   NameError would surface as... no, that would be an ImportError or
   NameError at import time, not a segfault. Probably not it.
5. **Suspect 3: removed `time` import side-effect.** I did NOT remove `import
   time` — verify. `time.perf_counter` is still used in `run_scaling_block`.
6. **Run it under gdb** — `gdb --args venv/bin/python examples/online_scoring.py`,
   `run`, then `bt` on the crash, to see whether the stack frame points to
   `clock_gettime`, `QueryPerformanceCounter`, `_cast_int_to_void_p`,
   `carray`, or somewhere inside DuckDB.
7. **Clean cache before every retry** — `rm -rf ~/.cache/numba
   examples/__pycache__` (per the MEMORY.md rule). Stale pycache from the
   old clock binding could mask the real state.

## Not-yet-done after the segfault is fixed

1. Remove haversine smoke step from `build` job in
   `.github/workflows/numbduck_ci.yml` (the step added in `e6a8c2b`).
2. Restore `macos` job in the workflow — verbatim from `fff8e38b`, plus
   add `--durations=20` per MEMORY.md.
3. Add `benchmarks` job in the workflow per the shape above.
4. Local verification on Linux:
   ```
   NUMBDUCK_BENCH_TINY=1 NUMBDUCK_BENCH_REPEATS=1 venv/bin/python examples/haversine.py
   NUMBDUCK_BENCH_TINY=1 NUMBDUCK_BENCH_REPEATS=1 venv/bin/python examples/online_scoring.py
   NUMBDUCK_BENCH_TINY=1 NUMBDUCK_BENCH_REPEATS=1 venv/bin/python examples/fraud_score.py
   ```
   All three must exit 0 before pushing.
5. Commit (feature branch first), push, watch the full matrix land — 44
   build cells + 1 macos cell + 4 benchmarks cells.
6. Then reply to the two remaining flagged PR comments from the prior
   review cycle if needed, and finish the branch via
   `superpowers-extended-cc:finishing-a-development-branch` (already on
   PR #28).

## Resume command

Drop into the project and run:

```
claude "resume work from docs/superpowers/plans/2026-04-09-cross-platform-benchmark-ci.md — first priority is bisecting the online_scoring.py segfault"
```
