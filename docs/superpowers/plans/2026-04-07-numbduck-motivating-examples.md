# Numbduck Motivating Examples Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers-extended-cc:subagent-driven-development (recommended) or superpowers-extended-cc:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build three runnable narrative-style example scripts under `examples/` that demonstrate where numbduck wins (and where it doesn't) compared to stock DuckDB Python+Arrow approaches.

**Architecture:** One file per example, plus a tiny shared `_common.py` (env print, timing helper, table format, result-match assertion). No package, no `__init__.py`, no CI integration. Each example is self-contained: data generation, three (or two) variants, cross-check, timed comparison, honest reporting.

**Tech Stack:** Python 3.10+, numbduck, numba, duckdb-python, pyarrow (where applicable), numpy. Existing reference: `test/test_ducklib.py:2678` (`test_udf_benchmark`) shows the canonical scalar-UDF registration dance.

**Spec:** [`docs/superpowers/specs/2026-04-07-numbduck-motivating-examples-design.md`](../specs/2026-04-07-numbduck-motivating-examples-design.md)

**Hard rule (from spec):** every numeric table in the spec is a placeholder. Every example task ends by running on this machine, capturing real numbers, and rewriting the example's "Last measured on" docstring block AND honesty paragraph from those numbers. The story bends to the numbers, not the other way around. If an example's measured story doesn't earn its place (e.g., fraud-score JIT-vs-Arrow gap < 2×), drop the example.

---

## File Structure

```
examples/
  README.md             # one paragraph per example, links and final summary
  _common.py            # ~80 LOC max; print_env, time_median, format_table, assert_results_match
  haversine.py          # Task 1
  online_scoring.py     # Task 2
  fraud_score.py        # Task 3 (may be dropped per measured-numbers rule)
docs/superpowers/
  specs/2026-04-07-numbduck-motivating-examples-design.md   # already committed
  plans/2026-04-07-numbduck-motivating-examples.md          # this file
```

`examples/` is **not a package**. Scripts are run as `python examples/<file>.py`. Python adds the script's directory to `sys.path`, so each example can `import _common` directly.

---

### Task 0: Scaffold examples/ and _common.py

**Goal:** Land the shared harness module and an empty README so subsequent tasks have a place to write to.

**Files:**
- Create: `examples/_common.py`
- Create: `examples/README.md`

**Acceptance Criteria:**
- [ ] `examples/_common.py` exposes `print_env()`, `time_median(fn, repeats=3)`, `format_table(headers, rows, alignments)`, `assert_results_match(*results, label)`
- [ ] `_common.py` is < 100 lines
- [ ] `_common.py` has no DuckDB-specific imports
- [ ] `examples/README.md` exists with a one-paragraph stub for each of the three planned examples (placeholders OK; real summaries land in Task 4)
- [ ] Running `python examples/_common.py` exercises each helper via a `__main__` block and exits 0

**Verify:**
```bash
cd /home/erik/projects/numbduck && python examples/_common.py
```
Expected: prints an environment block and a small demo table, exits 0.

**Steps:**

- [ ] **Step 1: Write `examples/_common.py`**

```python
"""Shared utilities for numbduck example scripts.

Intentionally tiny. If this file grows past ~80 lines or gains
DuckDB-specific knowledge, shrink it or inline its bits back into
the example files.
"""
import os
import platform
import statistics
import sys
import time

import duckdb
import numba
import numpy


def print_env() -> None:
    """Print a one-line environment block. Every example output starts with this."""
    pyver = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    print(
        f"  env: python {pyver}, duckdb {duckdb.__version__}, "
        f"numba {numba.__version__}, numpy {numpy.__version__}, "
        f"{platform.machine()}, {os.cpu_count()} cores"
    )


def time_median(fn, repeats: int = 3) -> float:
    """Run fn() `repeats` times under perf_counter; return median wall time in seconds.

    No auto-warmup — caller is responsible. We pick the median, not the min,
    to dampen the occasional outlier without hiding real variance.
    """
    repeats = int(os.environ.get("NUMBDUCK_BENCH_REPEATS", repeats))
    if repeats < 1:
        raise ValueError("repeats must be >= 1")
    timings = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        fn()
        timings.append(time.perf_counter() - t0)
    return statistics.median(timings)


def format_table(headers: list[str], rows: list[list[str]], alignments: list[str]) -> str:
    """Format a small table for stdout. alignments: list of '<', '>', '^'."""
    if len(headers) != len(alignments):
        raise ValueError("headers and alignments must be the same length")
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    def fmt_row(cells):
        return "  " + "  ".join(
            f"{c:{a}{w}s}" for c, a, w in zip(cells, alignments, widths)
        )
    out = [fmt_row(headers)]
    for row in rows:
        out.append(fmt_row(row))
    return "\n".join(out)


def assert_results_match(*results, label: str) -> None:
    """Cross-check that all variants produced the same answer.

    Catches 'your fast variant is fast because it's wrong'. Allows numpy floats
    to compare equal at full precision; if you need tolerance, do it before calling.
    """
    if len(results) < 2:
        return
    first = results[0]
    for i, other in enumerate(results[1:], start=1):
        if first != other:
            raise AssertionError(
                f"{label}: variant 0 produced {first!r} but variant {i} produced {other!r}"
            )


if __name__ == "__main__":
    print_env()
    demo = format_table(
        headers=["Variant", "Time", "Speedup"],
        rows=[
            ["Python", "1.000s", "1.0x"],
            ["JIT", "0.010s", "100.0x"],
        ],
        alignments=["<", ">", ">"],
    )
    print(demo)
    assert_results_match(42, 42, label="demo")
    print("  _common.py self-test OK")
```

- [ ] **Step 2: Write `examples/README.md` stub**

```markdown
# Numbduck examples

Runnable narrative-style scripts that compare numbduck against the closest
stock-DuckDB-Python equivalents. Each script generates its own data, runs
all variants under timing, and prints honest results — including the cases
where numbduck wins by a lot, where it wins moderately, and (eventually)
where it doesn't.

## Scripts

- **[haversine.py](haversine.py)** — *throughput axis.* Per-row great-circle
  distance computation. Compared against a Python scalar UDF and a PyArrow
  expression UDF. *(Numbers TBD until Task 1 lands.)*

- **[online_scoring.py](online_scoring.py)** — *latency + GIL-free axis.*
  Per-event feature lookup and scoring inside a single `@njit(nogil=True)`
  loop. Compared against a pure-Python `conn.execute` loop. Demonstrates
  measurable parallel scaling under multithreading. *(Numbers TBD until
  Task 2 lands.)*

- **[fraud_score.py](fraud_score.py)** — *branchy logic axis.* Per-row
  business rules with several `if/else` branches. Shows that numbduck also
  wins in Arrow's wheelhouse, moderately. *(Numbers TBD until Task 3 lands.
  This example may be dropped if the measured JIT-vs-Arrow gap is < 2×.)*

## Running

```bash
python examples/haversine.py
python examples/online_scoring.py
python examples/fraud_score.py

# Larger row counts (~30s+ each):
NUMBDUCK_BENCH_BIG=1 python examples/haversine.py

# Tighter medians:
NUMBDUCK_BENCH_REPEATS=5 python examples/haversine.py
```
```

- [ ] **Step 3: Run the self-test**

```bash
cd /home/erik/projects/numbduck && python examples/_common.py
```

Expected output: env line, a small two-row table, and `_common.py self-test OK`. Exits 0.

- [ ] **Step 4: Commit**

```bash
git add examples/_common.py examples/README.md
git commit -m "Scaffold examples/ directory with shared harness

Add examples/_common.py with print_env, time_median, format_table,
and assert_results_match helpers. Add README stub describing the
three planned narrative examples. Tasks 1-3 will land the actual
scripts."
```

---

### Task 1: examples/haversine.py — throughput axis

**Goal:** A standalone script that runs Python / Arrow / numbduck JIT versions of a haversine UDF over synthetic customer points, prints a comparison table from real measurements, and tells the honest story in its docstring.

**Files:**
- Create: `examples/haversine.py`
- Modify: `examples/README.md` (replace haversine bullet with real one-line summary)

**Acceptance Criteria:**
- [ ] Script runs to completion in < 30 s at default row counts
- [ ] All three variants produce identical `count(*)` results, asserted via `assert_results_match`
- [ ] Output table is generated from real wall-clock measurements on this machine
- [ ] "Last measured on" docstring block contains the captured table and date
- [ ] Honesty paragraph at the bottom of `main()` reflects the *measured* numbers, not the spec's predicted ones
- [ ] `NUMBDUCK_BENCH_BIG=1` enables a 10M-row tier
- [ ] `examples/README.md` haversine bullet updated with the real headline number

**Verify:**
```bash
cd /home/erik/projects/numbduck && python examples/haversine.py
```
Expected: env block, a result table with three rows (Python / Arrow / JIT), a one-paragraph honesty section, exits 0. Total runtime < 30 s.

**Steps:**

- [ ] **Step 1: Validate `math.*` lowering inside `@cfunc`**

Before writing the full example, confirm that `math.sin/cos/asin/sqrt` lower correctly under numba `@cfunc` for our signature. Write this as a temporary scratch script (do not commit):

```python
# /tmp/scratch_haversine_math.py
import math
import numba

@numba.cfunc(numba.types.float64(numba.types.float64, numba.types.float64))
def hv(lat1, lon1):
    return math.asin(math.sqrt(math.sin(lat1) ** 2 + math.cos(lon1)))

print("OK", hv.address)
```

```bash
python /tmp/scratch_haversine_math.py
```

Expected: prints `OK <address>`. If this fails, **stop and report** — the example design needs revisiting.

- [ ] **Step 2: Write `examples/haversine.py`**

Use `test/test_ducklib.py:2678` (`test_udf_benchmark`) as the reference for the JIT UDF registration dance — the same `duckdb_create_scalar_function` + `duckdb_scalar_function_set_name` + `duckdb_scalar_function_add_parameter` + `duckdb_scalar_function_set_function` + `duckdb_register_scalar_function` sequence applies. Differences from the reference: four DOUBLE inputs instead of one BIGINT; return type DOUBLE not BIGINT; cfunc body is haversine, not square.

```python
"""Haversine distance UDF — throughput axis.

Story: a retail analytics question. For each of N synthetic customer
locations, how far is the customer from a fixed store at (37.7749, -122.4194)?
The query runs `SELECT count(*) FROM customers WHERE haversine(...) < 50`.
The bottleneck is the per-row distance computation.

Three variants:
  1. Python scalar UDF — round-trips through the interpreter per row.
  2. PyArrow expression UDF — chained pc.sin/pc.cos/pc.atan2/pc.sqrt over chunks.
  3. numbduck JIT UDF — @cfunc body with math.sin/cos/asin/sqrt, registered
     via duckdb_register_scalar_function.

Run:
    python examples/haversine.py
    NUMBDUCK_BENCH_BIG=1 python examples/haversine.py    # adds 10M-row tier

Last measured on: <DATE>, <MACHINE>, <python>, <duckdb>, <numba>
    <PASTE TABLE FROM REAL RUN>
"""
import ctypes
import math
import os
import sys

import duckdb
import numba
import numpy
import pyarrow.compute as pc

# import sibling _common.py
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _common import assert_results_match, format_table, print_env, time_median  # noqa: E402

import numbduck.ducklib as ducklib  # noqa: E402
from numbduck.pybridge import extract_connection_ptr  # noqa: E402

STORE_LAT = 37.7749
STORE_LON = -122.4194
RADIUS_KM = 50.0
ROW_COUNTS = [100_000, 1_000_000]
if os.environ.get("NUMBDUCK_BENCH_BIG") == "1":
    ROW_COUNTS.append(10_000_000)


def haversine_py(lat1, lon1, lat2, lon2):
    R = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def haversine_arrow(lat1, lon1, lat2, lon2):
    R = 6371.0
    pi_180 = 3.141592653589793 / 180.0
    p1 = pc.multiply(lat1, pi_180)
    p2 = pc.multiply(lat2, pi_180)
    dp = pc.multiply(pc.subtract(lat2, lat1), pi_180)
    dl = pc.multiply(pc.subtract(lon2, lon1), pi_180)
    a = pc.add(
        pc.power(pc.sin(pc.divide(dp, 2)), 2),
        pc.multiply(
            pc.multiply(pc.cos(p1), pc.cos(p2)),
            pc.power(pc.sin(pc.divide(dl, 2)), 2),
        ),
    )
    return pc.multiply(2 * R, pc.asin(pc.sqrt(a)))


@numba.cfunc(
    numba.types.float64(
        numba.types.float64, numba.types.float64,
        numba.types.float64, numba.types.float64,
    )
)
def haversine_jit(lat1, lon1, lat2, lon2):
    R = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2.0) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2.0) ** 2
    return 2.0 * R * math.asin(math.sqrt(a))


def register_jit_udf(conn):
    """Register haversine_jit as a DuckDB scalar function. See test_udf_benchmark
    in test/test_ducklib.py for the canonical pattern."""
    conn_ptr = extract_connection_ptr(conn)
    func_p = ducklib.duckdb_create_scalar_function()
    name_buf = (ctypes.c_char * 16)(*b"hv_jit\x00")
    ducklib.duckdb_scalar_function_set_name(func_p, ctypes.addressof(name_buf))
    dbl_p = ducklib.duckdb_create_logical_type(ducklib.DUCKDB_TYPE_DOUBLE)
    for _ in range(4):
        ducklib.duckdb_scalar_function_add_parameter(func_p, dbl_p)
    ducklib.duckdb_scalar_function_set_return_type(func_p, dbl_p)
    type_buf = numpy.array([dbl_p], dtype=numpy.intp)
    ducklib.duckdb_destroy_logical_type(type_buf.ctypes.data)
    ducklib.duckdb_scalar_function_set_function(func_p, haversine_jit.address)
    rc = ducklib.duckdb_register_scalar_function(conn_ptr, func_p)
    assert rc == ducklib.DuckDBSuccess
    func_buf = numpy.array([func_p], dtype=numpy.intp)
    ducklib.duckdb_destroy_scalar_function(func_buf.ctypes.data)


def setup_data(conn, n):
    conn.execute("SELECT setseed(0.42)")
    conn.execute(
        f"CREATE OR REPLACE TABLE customers AS "
        f"SELECT random()*180-90 AS lat, random()*360-180 AS lon FROM range({n})"
    )


def run_one(conn, n):
    setup_data(conn, n)
    sql_py = f"SELECT count(*) FROM customers WHERE hv_py(lat, lon, {STORE_LAT}, {STORE_LON}) < {RADIUS_KM}"
    sql_arrow = sql_py.replace("hv_py", "hv_arrow")
    sql_jit = sql_py.replace("hv_py", "hv_jit")

    # Warm up
    conn.execute(sql_py)
    conn.execute(sql_arrow)
    conn.execute(sql_jit)

    # Capture results once for cross-check
    r_py = conn.execute(sql_py).fetchone()[0]
    r_arrow = conn.execute(sql_arrow).fetchone()[0]
    r_jit = conn.execute(sql_jit).fetchone()[0]
    assert_results_match(r_py, r_arrow, r_jit, label=f"haversine n={n}")

    t_py = time_median(lambda: conn.execute(sql_py).fetchone())
    t_arrow = time_median(lambda: conn.execute(sql_arrow).fetchone())
    t_jit = time_median(lambda: conn.execute(sql_jit).fetchone())
    return t_py, t_arrow, t_jit


def main():
    print_env()
    print(f"  Haversine UDF benchmark — {ROW_COUNTS} rows, radius {RADIUS_KM} km")
    print()

    conn = duckdb.connect()
    conn.create_function("hv_py", haversine_py, ["DOUBLE"] * 4, "DOUBLE")
    conn.create_function(
        "hv_arrow", haversine_arrow, ["DOUBLE"] * 4, "DOUBLE", type="arrow"
    )
    register_jit_udf(conn)

    rows = []
    for n in ROW_COUNTS:
        t_py, t_arrow, t_jit = run_one(conn, n)
        rows.append([
            f"{n:,d}",
            f"{t_py:.3f}s",
            f"{t_arrow:.3f}s",
            f"{t_jit:.3f}s",
            f"{t_py/t_jit:.0f}x",
            f"{t_arrow/t_jit:.0f}x",
        ])

    print(format_table(
        headers=["Rows", "Python", "Arrow", "JIT", "Py/JIT", "Arr/JIT"],
        rows=rows,
        alignments=[">", ">", ">", ">", ">", ">"],
    ))
    print()
    print("  <HONESTY PARAGRAPH — REWRITE FROM REAL NUMBERS BEFORE COMMIT>")
    conn.close()


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Run the script and capture real numbers**

```bash
cd /home/erik/projects/numbduck && python examples/haversine.py
```

Expected: env block, table, placeholder honesty paragraph, exits 0. Verify all three variants returned the same `count(*)` (the script asserts this internally).

- [ ] **Step 4: Rewrite the docstring's "Last measured on" block from the captured table**

Paste the actual table the script printed into the docstring, replacing the `<PASTE TABLE FROM REAL RUN>` placeholder. Include date, machine (run `uname -m`), Python version, duckdb version, numba version on the "Last measured on" line.

- [ ] **Step 5: Rewrite the in-script honesty paragraph from real numbers**

Replace `<HONESTY PARAGRAPH — REWRITE FROM REAL NUMBERS BEFORE COMMIT>` with a paragraph that:
- Names the actual JIT-vs-Python and JIT-vs-Arrow ratios from the run
- Says where the win comes from (no per-chunk Python invocation, LLVM-fused math, no intermediate arrays)
- If Arrow turned out close to JIT, say so plainly — do not exaggerate
- If Arrow turned out way behind JIT, say so plainly — do not soften

- [ ] **Step 6: Update `examples/README.md` haversine bullet**

Replace the placeholder bullet with a one-line summary like:
> **[haversine.py](haversine.py)** — *throughput axis.* Great-circle distance over N synthetic points. Measured: JIT is **N×** faster than Python scalar UDF and **N×** faster than PyArrow at 1M rows.

- [ ] **Step 7: Re-run to confirm clean output**

```bash
cd /home/erik/projects/numbduck && python examples/haversine.py
```

Expected: same table as captured, exits 0. No tracebacks, no warnings.

- [ ] **Step 8: Commit**

```bash
git add examples/haversine.py examples/README.md
git commit -m "Add haversine UDF example (throughput axis)

Compares Python scalar / PyArrow expression / numbduck JIT versions
of haversine distance over synthetic customer points. Numbers in the
docstring and honesty paragraph captured from a real run on this
machine."
```

---

### Task 2: examples/online_scoring.py — latency + GIL-free axis

**Goal:** A standalone script that runs a per-event feature-lookup-and-score loop in two ways — pure Python `conn.execute` loop vs `@njit(nogil=True)` loop calling DuckDB C API directly — and reports per-event latency percentiles plus parallel-scaling under multithreading.

**Files:**
- Create: `examples/online_scoring.py`
- Modify: `examples/README.md` (replace online_scoring bullet)

**Acceptance Criteria:**
- [ ] Script runs to completion in < 30 s at default settings
- [ ] Both variants produce identical scores arrays (cross-checked via `assert_results_match` on a hash or sum of the scores)
- [ ] Per-event latency table reports total / events-per-sec / p50 / p95 / p99 / max for both variants
- [ ] Parallel-scaling table reports total wall time at T=1, 2, 4, 8 worker threads for both variants
- [ ] JIT variant is decorated `@njit(nogil=True)` AND verified to actually release the GIL (the parallel-scaling test is the natural reveal — JIT must show measurable speedup, Python must not)
- [ ] If parallel-scaling test reveals the JIT path secretly holds the GIL, **stop and report** — do not paper over by removing the test
- [ ] `NUMBDUCK_BENCH_BIG=1` bumps event count to 500K
- [ ] "Last measured on" docstring block + honesty paragraph rewritten from real numbers

**Verify:**
```bash
cd /home/erik/projects/numbduck && python examples/online_scoring.py
```
Expected: env block, latency table, parallel-scaling table, honesty paragraph, exits 0. Total runtime < 30 s.

**Steps:**

- [ ] **Step 1: Validate `nogil=True` is honored end-to-end**

Before writing the full script, confirm two things with throwaway scratch scripts (not committed):

(a) `time.monotonic_ns` works inside `@njit`:

```python
# /tmp/scratch_njit_time.py
import numba, time

@numba.njit(nogil=True)
def loop(n):
    out = 0
    for i in range(n):
        out += time.monotonic_ns()
    return out

print(loop(1000))
```

Run: `python /tmp/scratch_njit_time.py`. Expected: prints a large integer. If it fails, the example needs an alternative timing strategy — **stop and report**.

(b) A trivial DuckDB call from inside `@njit(nogil=True)` actually releases the GIL. Use `_call_lib_func` with one of the prepared-statement bindings against a dummy connection. Hold the GIL on the main thread (e.g., `threading.Thread(target=...)` for the JIT call while the main thread spins) and confirm the JIT thread completes. If it deadlocks, the JIT path silently re-acquires the GIL inside `_call_lib_func` or one of the @intrinsic helpers. **Stop and report** — this is the very risk the spec calls out as a release blocker.

- [ ] **Step 2: Write `examples/online_scoring.py`**

```python
"""Online event scoring loop — latency + GIL-free axis.

Story: a real-time scoring service. Events arrive one at a time. For each
event we look up the entity's stored features in a DuckDB table and compute
a 4-element dot-product score. The metric the operator cares about is
per-event latency, not throughput — batching events to amortize Python
overhead is exactly what the SLA forbids.

Two variants:
  1. Pure-Python loop calling conn.execute(...).fetchone() per event.
  2. numbduck @njit(nogil=True) loop calling duckdb_execute_prepared and
     reading the result chunk via the bound C API, with no Python crossings
     between iterations.

This example also measures parallel scaling on 1/2/4/8 worker threads. The
expected (and dramatic) shape: the Python loop is GIL-bound and shows zero
or negative scaling; the JIT loop scales roughly linearly with cores. If the
parallel-scaling test does NOT show this shape, the JIT path is silently
acquiring the GIL — investigate before publishing.

Run:
    python examples/online_scoring.py
    NUMBDUCK_BENCH_BIG=1 python examples/online_scoring.py    # 500K events

Last measured on: <DATE>, <MACHINE>, <python>, <duckdb>, <numba>
    <PASTE LATENCY TABLE>
    <PASTE SCALING TABLE>
"""
# Implementation skeleton — full code lands during execution.
#
# Major components, in order:
#
# 1. setup_features(conn, k=100_000) — populate features(id, w0..w3) with seeded RNG
#
# 2. setup_events(n_events, k_features, seed=43) — return numpy arrays
#       ids[int64], x[float64, n_events x 4]
#
# 3. score_python(conn, ids, x, scores_out, latencies_ns_out) — pure-Python loop
#       per iteration: conn.execute("SELECT w0,w1,w2,w3 FROM features WHERE id = ?",
#                                   [ids[i]]).fetchone()
#                      compute dot product, store, stamp latency
#
# 4. score_jit — @njit(nogil=True) function. Signature takes connection_ptr,
#    prepared_stmt_ptr, ids, x, scores_out, latencies_ns_out. Body:
#       for i in range(n):
#           t0 = time.monotonic_ns()
#           ducklib.duckdb_bind_int64(prepared_stmt_ptr, 1, ids[i])
#           # execute, fetch one chunk via bound API, extract four doubles,
#           # compute dot product, store, destroy chunk
#           latencies_ns_out[i] = time.monotonic_ns() - t0
#
#    See test/test_ducklib.py for examples of: bind_int64, execute_prepared,
#    fetch_chunk, data_chunk_get_vector, vector_get_data patterns.
#
# 5. percentiles(latencies_ns) — return (p50, p95, p99, max) in microseconds
#
# 6. run_latency_block(conn, ids, x) — warm up, run both variants, cross-check
#    scores via assert_results_match on scores.sum(), return latency stats
#
# 7. run_scaling_block(conn, ids, x) — for T in [1,2,4,8]: run T copies of each
#    variant in a ThreadPoolExecutor, report total wall time
#
# 8. main() — print_env, build features, build events, run latency block,
#    run scaling block, print both tables, print placeholder honesty paragraph
```

Note: this task's code is more involved than Task 1's. The full implementation will land during execution; the skeleton above documents the structure and the cross-references to existing test patterns. Keep it under ~250 lines.

- [ ] **Step 3: Run the script and capture real numbers**

```bash
cd /home/erik/projects/numbduck && python examples/online_scoring.py
```

Verify: both tables print, scores cross-check passes, parallel scaling shows the expected divergent shape (Python flat-or-worse, JIT scaling down with thread count). If JIT scaling is flat, **STOP**: the GIL is not being released. Investigate the binding helpers in `numbduck/ducklib.py` before continuing.

- [ ] **Step 4: Rewrite "Last measured on" block + honesty paragraph from real numbers**

Same drill as Task 1 step 4-5. The honesty paragraph for this example is the most important of the three because the parallel-scaling story is unique to numbduck and the numbers must be exactly right.

- [ ] **Step 5: Update `examples/README.md` online_scoring bullet**

One-liner with the actual headline numbers (e.g., "JIT variant scales to N× on 8 cores; Python loop holds steady at T=1 due to GIL contention").

- [ ] **Step 6: Re-run to confirm clean output**

- [ ] **Step 7: Commit**

```bash
git add examples/online_scoring.py examples/README.md
git commit -m "Add online scoring loop example (latency + GIL-free axis)

Per-event feature lookup and dot-product score, comparing a pure-Python
conn.execute loop against an @njit(nogil=True) loop using prepared
statements. Reports per-event latency percentiles and parallel-scaling
on 1/2/4/8 worker threads. Numbers captured from a real run."
```

---

### Task 3: examples/fraud_score.py — branchy logic axis

**Goal:** A standalone script that runs Python / Arrow / numbduck JIT versions of a branchy fraud-score UDF, prints a comparison table, and tells the honest story — including dropping the example entirely if the JIT-vs-Arrow gap turns out to be < 2×.

**Files:**
- Create: `examples/fraud_score.py` (or delete during step 5 if dropped)
- Modify: `examples/README.md`

**Acceptance Criteria:**
- [ ] Script runs to completion in < 30 s at default settings
- [ ] All three variants produce identical aggregate sums, cross-checked
- [ ] Output table generated from real measurements
- [ ] Decision step: if measured JIT-vs-Arrow gap < 2×, the script is **deleted** and the README bullet is removed; the design's escape-hatch rule is honored, not papered over
- [ ] If kept, "Last measured on" block + honesty paragraph reflect real numbers

**Verify:**
```bash
cd /home/erik/projects/numbduck && python examples/fraud_score.py
```
Expected (if kept): env block, table, honesty paragraph, exits 0. (If dropped: file does not exist.)

**Steps:**

- [ ] **Step 1: Write `examples/fraud_score.py`**

Same skeleton as `haversine.py`. Differences:

- Data table is `transactions(amount DOUBLE, country_code TINYINT, home_country_code TINYINT, hour TINYINT, merchant_risk_tier TINYINT, num_recent_txns INTEGER)` generated deterministically from `range(N)` in DuckDB
- Six-input UDF
- Body is the literal branchy ruleset from the spec (amount tiers, country mismatch, off-hours, risk tier, txn count)
- Query: `SELECT sum(fraud_score(...)) FROM transactions`
- Arrow variant uses `pc.if_else` chains and `pc.add` over masks; write it idiomatically, do not sandbag
- Default `N = 1_000_000`, `NUMBDUCK_BENCH_BIG=1` bumps to 10M
- Cross-check sums are exact integers (the score formula returns an integer per row)

The JIT registration dance is the same as haversine — six DOUBLE/TINYINT/INTEGER parameters instead of four DOUBLEs. Reference `test/test_ducklib.py:2678` for the type-handling pattern; the haversine task is the closer reference for multi-parameter calls.

- [ ] **Step 2: Run the script and capture real numbers**

```bash
cd /home/erik/projects/numbduck && python examples/fraud_score.py
```

Note the JIT-vs-Arrow ratio specifically.

- [ ] **Step 3: Decision point — keep or drop?**

If `t_arrow / t_jit >= 2.0`: keep the example. Proceed to step 4.

If `t_arrow / t_jit < 2.0`: **drop the example**. Run:

```bash
rm examples/fraud_score.py
```

Update `examples/README.md` to remove the fraud_score bullet entirely and add a one-line note in the README explaining the choice (e.g., "We tried a branchy fraud-score example but on this machine the JIT-vs-Arrow gap was only N×, which doesn't motivate switching tools. Arrow is the right tool for vectorized branchy work."). Skip steps 4-5; jump to step 6.

- [ ] **Step 4: Rewrite "Last measured on" + honesty paragraph from real numbers**

Per spec: this is the most important honesty section because it's the only example that *acknowledges Arrow as a serious competitor*. Write it carefully:
- Lead with Arrow's actual speedup over Python — give credit
- Then state the JIT-over-Arrow ratio plainly
- Be explicit that the case for numbduck strengthens with branchiness, in-JIT calls (point at example 2), or GIL-free needs
- Do NOT use the word "blazingly"

- [ ] **Step 5: Update `examples/README.md` fraud_score bullet**

- [ ] **Step 6: Re-run (if kept) or verify deletion (if dropped)**

If kept: `python examples/fraud_score.py` succeeds. If dropped: `ls examples/fraud_score.py` reports no such file.

- [ ] **Step 7: Commit**

```bash
# If kept:
git add examples/fraud_score.py examples/README.md
git commit -m "Add fraud-score branchy UDF example (Arrow's wheelhouse)

Per-row business rules with several if/else branches. Compares Python
scalar / PyArrow if_else chains / numbduck JIT and shows JIT also wins
in Arrow's wheelhouse, by N×. Numbers and honesty paragraph captured
from a real run."

# If dropped:
git add examples/README.md
git commit -m "Decline to add fraud-score example (Arrow gap too small)

Tried a branchy fraud-score UDF as the third motivating example.
Measured JIT-vs-Arrow ratio was <2x, which doesn't motivate switching
tools — Arrow is the right tool for vectorized branchy work. Honoring
the design spec's measured-numbers escape-hatch rule rather than
papering over the result."
```

---

### Task 4: Finalize examples/README.md and link from project README

**Goal:** Replace the README stub with a polished overview that uses real headline numbers from Tasks 1-3, and add a link from the project's top-level `README.md` to `examples/`.

**Files:**
- Modify: `examples/README.md`
- Modify: `README.md` (project root)

**Acceptance Criteria:**
- [ ] `examples/README.md` summarizes each landed example in one paragraph using real numbers from its docstring's "Last measured on" block
- [ ] Project root `README.md` contains a clear link to `examples/`
- [ ] No example bullet in `examples/README.md` references an example that wasn't landed (e.g., if Task 3 was dropped, no fraud_score bullet)

**Verify:**
```bash
cd /home/erik/projects/numbduck && cat examples/README.md && echo "---" && cat README.md
```
Expected: examples/README.md has real numbers, project README has an examples link.

**Steps:**

- [ ] **Step 1: Rewrite `examples/README.md`**

Use the docstring "Last measured on" blocks from `examples/haversine.py`, `examples/online_scoring.py`, and (if kept) `examples/fraud_score.py` as the source of headline numbers. One paragraph per example. Mention the honest caveats — this is the README, not the marketing page.

- [ ] **Step 2: Add link from project root `README.md`**

The current root README is essentially empty (6 lines per the project state). Add a small section:

```markdown
## Examples

Runnable narrative-style examples comparing numbduck against the closest stock
DuckDB Python+Arrow approaches live in [`examples/`](examples/). Each script is
self-contained, generates its own data, and prints honest measured numbers.
```

- [ ] **Step 3: Commit**

```bash
git add examples/README.md README.md
git commit -m "Polish examples/README and link from project README

Replace the placeholder examples/README with one-paragraph summaries
using the real measured numbers from each example's docstring. Add a
brief examples section to the project README pointing readers at the
examples directory."
```

---

## Self-Review

**Spec coverage:**
- Goal & scope (spec §1): Task 0 sets up the directory structure; Tasks 1-3 land the three examples; Task 4 polishes the README. ✓
- Hard rule on numbers: every example task ends with "rewrite docstring + honesty paragraph from real run". ✓
- Directory layout: matches spec exactly. ✓
- Example 1 — haversine: Task 1 covers data, three variants, query, measurement, validation steps (math.* lowering check). ✓
- Example 2 — online scoring: Task 2 covers setup, both variants, latency + parallel-scaling measurements, nogil validation as a release blocker. ✓
- Example 3 — fraud score: Task 3 covers all three variants and the explicit "drop if gap < 2×" decision. ✓
- Shared harness: Task 0. ✓
- File structure conventions: each example task references the docstring template via the "Last measured on" rewrite step. ✓
- Determinism: seeded RNG noted in each example task. ✓
- Env-var gates: `NUMBDUCK_BENCH_BIG` and `NUMBDUCK_BENCH_REPEATS` covered in Task 0 (`time_median` reads `REPEATS`) and in each example's row-count constants. ✓
- "Not doing" list: no argparse, no JSON, no plots, no CI, no `__init__.py`, no per-example subdirs — none of these appear in any task. ✓
- Risks summary: each risk is either addressed by a validation step or by a decision rule (drop fraud_score example). ✓

**Placeholder scan:** No `TBD`/`TODO`/"implement later" in step instructions. The phrase `<HONESTY PARAGRAPH — REWRITE FROM REAL NUMBERS BEFORE COMMIT>` is intentional — it's the placeholder string that goes *into the script* for the engineer to replace at run time. Same for `<PASTE TABLE FROM REAL RUN>` and `<DATE>` etc. These are in-script markers, not plan-step omissions.

**Type consistency:** Helper signatures used in plan: `print_env() -> None`, `time_median(fn, repeats=3) -> float`, `format_table(headers: list[str], rows: list[list[str]], alignments: list[str]) -> str`, `assert_results_match(*results, label: str) -> None`. Each example task's main() calls them with matching argument shapes. ✓

**Task granularity:** 5 tasks. Each is committable, each is independently verifiable, each has a clear deliverable. Task 2 is the largest (skeleton + nogil validation + parallel-scaling test) and may take longest, but cannot be split without producing an interim commit that doesn't make sense on its own.

No issues found. Ready for execution handoff.
