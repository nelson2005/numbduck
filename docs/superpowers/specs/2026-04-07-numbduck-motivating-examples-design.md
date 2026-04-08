# Numbduck motivating examples — design

**Status:** approved design, awaiting implementation plan
**Date:** 2026-04-07
**Branch:** `motivating-examples-spec`

## Goal

Build an `examples/` directory containing three runnable, self-contained narrative-style scripts that demonstrate where numbduck wins (and where it doesn't) compared to the closest stock-DuckDB-Python equivalents. The audience is a developer reading the README or a blog post who wants an honest answer to "is this thing worth using." Tone is candid: we report measured numbers, name the cases where Arrow wins or ties, and rewrite the story if the numbers contradict the prediction.

## Non-goals

- Not a regression test suite. These are demos. They are not run in CI.
- Not a benchmark microframework. No `argparse`, no JSON output, no plot generation. Plots happen in whatever blog/talk consumes the numbers.
- Not a replacement for `test_udf_benchmark`. That test stays where it is and continues to cover the trivial `x*x` case under pytest.
- Not Arrow-bashing. Arrow is fast for vectorized math; we say so.

## Hard rule on numbers

**Every numeric table and every honesty paragraph in this document is a placeholder.** The predictions below are extrapolations from the existing `test_udf_benchmark` (`x*x` at 1M rows: Python 119s, Arrow 0.24s, JIT 0.001s), not measurements. Before any example ships:

1. Run all variants on the publishing machine.
2. Replace every predicted table with the measured one.
3. If real numbers contradict the predicted *shape* of the story (e.g., the JIT-vs-Arrow gap on the fraud-score example turns out to be < 2× instead of 4–10×), the honesty paragraph rewrites to match reality. The numbers do not bend to the story; the story bends to the numbers.
4. If an example's measured story turns out to not earn its place at all, drop the example.

This rule overrides everything else in this document.

## Directory layout

```
examples/
  README.md              # one paragraph per example, links to scripts and (eventually) blog post
  haversine.py           # example 1 — throughput axis
  online_scoring.py      # example 2 — latency + GIL-free axis
  fraud_score.py         # example 3 — branchy logic axis
  _common.py             # tiny shared module: env print, timing, table format, result match
```

One file per example. No `__init__.py`, no per-example subdirectory. A reader opens one file and sees the whole story end-to-end.

## Example 1 — Haversine distance UDF

### Story

A retail analytics question: "for each of our 10M customers, how far is their home from store #42?" The query is `SELECT count(*) FROM customers WHERE haversine(lat, lon, store_lat, store_lon) < 50`. The bottleneck is the per-row distance computation. Showcase axis: **throughput**.

### Synthetic data

A `customers(lat DOUBLE, lon DOUBLE)` table generated in DuckDB:

```sql
SELECT setseed(0.42);
CREATE TABLE customers AS
SELECT random()*180-90 AS lat, random()*360-180 AS lon FROM range(N);
```

Row counts: 100K, 1M, and (gated by `NUMBDUCK_BENCH_BIG=1`) 10M. Default off so a `python examples/haversine.py` finishes in seconds.

If `setseed` interacts oddly with parallel scans, fall back to deterministic rows: `SELECT (i*9301+49297)%180 - 90 AS lat, (i*4096+150889)%360 - 180 AS lon FROM range(N) AS t(i)`.

### Variants under test

1. **Python scalar UDF.** `conn.create_function("hv_py", lambda lat, lon, slat, slon: ..., [...], "DOUBLE")`. The "do not do this" baseline.
2. **PyArrow expression UDF.** Registered with `type="arrow"`. Body uses `pc.sin / pc.cos / pc.atan2 / pc.sqrt` chained on input chunks. The "honest fast Python" baseline.
3. **numbduck JIT UDF.** `@cfunc` body uses `math.sin/cos/asin/sqrt` (numba-supported), wrapped via the same `duckdb_create_scalar_function` + `duckdb_register_scalar_function` dance used in `test_udf_benchmark`.

### Query

`SELECT count(*) FROM customers WHERE <udf>(lat, lon, store_lat, store_lon) < 50`. Aggregating to a scalar count avoids result-materialization cost biasing the comparison.

### Measurement

- Warm up each variant on a 1-row table.
- Run the query 3 times under `time.perf_counter()`, report the median (configurable via `NUMBDUCK_BENCH_REPEATS`).
- Cross-check that all three variants return the same count via `assert_results_match` (catches "your fast variant is fast because it's wrong").

### Output (PLACEHOLDER — measured numbers will replace this)

```
  Haversine distance UDF (10M rows, Python 3.12, duckdb 1.5.x):
       Variant     Time     Rows/sec   Speedup vs Python
        Python   42.10s        237K            1.0x
         Arrow    0.91s         11M           46.3x
   numbduck JIT    0.18s         55M          233.9x
```

### Honesty paragraph (PLACEHOLDER)

To be rewritten from real numbers. Expected shape: Arrow is competitive, JIT wins because (a) no per-chunk Python invocation, (b) numba's LLVM lowering can fuse the math operations into one pass, (c) no intermediate arrays.

### Validation steps before publishing

- Confirm `math.asin` (and friends) lower correctly under `@cfunc` — write a one-row test before building the full example.
- Confirm 10M case completes in < 5 minutes for the Python variant (or drop 10M from the default-off tier).

## Example 2 — Online event scoring loop

### Story

A real-time scoring service. Events arrive one at a time (clicks, transactions, sensor readings). For each event we look up the entity's stored features in a DuckDB table and compute a score. The metric the operator cares about is **per-event latency**, not throughput — batching events to amortize Python overhead is exactly what the SLA forbids. Showcase axes: **per-event latency**, **GIL-free multithreaded scaling**.

This example exists to make a sharper point than example 1: there is no Arrow path. Arrow UDFs are query-internal — they speed up `SELECT ... FROM table` plans, not "for each thing in this Python list, run a query." The honest comparison is "Python loop calling `conn.execute` per event" vs. "`@njit(nogil=True)` loop calling `duckdb_execute_prepared` per event."

### Setup

- `features(id BIGINT PRIMARY KEY, w0 DOUBLE, w1 DOUBLE, w2 DOUBLE, w3 DOUBLE)` populated with 100K rows of random weights, fixed seed.
- Events: a numpy array of N random `id`s in `[0, 100K)` plus an N×4 random feature inputs `x[0..3]`. Generated up front so neither variant pays generation cost in the timed region.
- N defaults to 50K. Bumpable to 500K via `NUMBDUCK_BENCH_BIG=1`.
- The score is `dot(w, x)` — a 4-element dot product. Trivial math; the point is the lookup-and-compute loop, not the math.

### Variants under test

1. **Pure-Python loop.** `conn.execute("SELECT w0,w1,w2,w3 FROM features WHERE id = ?", [ids[i]]).fetchone()` per iteration, dot product in Python.
2. **numbduck JIT loop.** A single `@njit(nogil=True)` function that:
   - Receives the connection pointer (extracted via `pybridge.extract_connection_ptr` once before the JIT region), the prepared-statement pointer (created via `duckdb_prepare`), the `ids` array, the `x` matrix, and pre-allocated output `scores` and `latencies_ns` arrays.
   - Loops over events, calls `duckdb_bind_int64` then `duckdb_execute_prepared`, fetches one chunk via `duckdb_fetch_chunk`, reads four columns via `duckdb_data_chunk_get_vector` + `duckdb_vector_get_data`, computes the dot product, stores into `scores`, destroys the chunk and result.
   - Stamps per-event latency into the `latencies_ns` array (see Risks).
   - Returns when done — pure JIT, no Python crossings between iterations.

### Measurements

**Per-event latency:**
- Warm up both variants on 100 events.
- Run each over the full N events. Record total wall time, mean, p50, p95, p99, max per-event latency in ns.

**Parallel scaling** (the unique selling point):
- Run the scoring workload on 1, 2, 4, 8 worker threads via `concurrent.futures.ThreadPoolExecutor`.
- Repeat for both variants. Report total wall time per thread count.
- Expected and dramatic shape: Python loop is roughly constant or worse as threads increase (GIL contention); JIT loop scales roughly linearly down to core count.

### Output (PLACEHOLDER)

```
  Online scoring loop (50K events, 100K-row feature table):
       Variant     Total   events/sec      p50      p95      p99      max
   Python loop    8.42s         5.9K     162us    310us    480us    9.1ms
  numbduck JIT    0.21s        238K       3.8us    6.2us    9.1us   140us

  Parallel scaling (50K events × T threads, total wall time):
            T=1     T=2     T=4     T=8
    Python  8.4s    8.6s    9.1s   10.2s     (GIL-bound, zero scaling)
    JIT     0.21s   0.11s   0.06s   0.04s     (~linear to core count)
```

### Honesty paragraph (PLACEHOLDER)

To be rewritten from real numbers. Expected shape: this is *not* an Arrow comparison — Arrow UDFs operate inside a query, not across queries. The JIT win is both throughput and tail latency. The parallel scaling result is the characteristic numbduck advantage that matters for embedding in services: scoring loops on background threads without sharding processes, simplifying deployment compared to multiprocess workarounds.

### Validation steps before publishing

- **Confirm `nogil=True` is honored end-to-end.** Numba is silent if you ask for `nogil` but a feature you use needs the GIL. Validate by holding the GIL on the main thread (e.g., spinning Python work) during a JIT run and confirming the JIT function still completes. The parallel-scaling test is the natural reveal — if Python and JIT scale identically, the JIT path is secretly acquiring the GIL somewhere in `_call_lib_func` or one of the @intrinsic helpers in `ducklib.py`. Investigate before publishing.
- **Confirm `time.monotonic_ns` (or equivalent) is JIT-supported and cheap enough for tight-loop sampling.** If sampling cost dominates measurement, fall back to stamping every Kth event or measuring batches.
- **Confirm chunk lifetime is leak-free.** Each iteration produces a chunk that must be destroyed before the next bind. Validate with a 5-minute soak before publishing numbers.
- **Sanity-assert ids hit.** Every `id` should be in `[0, 100K)` and the table has exactly that range. Add an `assert` inside the JIT loop for the first few iterations.

## Example 3 — Branchy fraud-score UDF

### Story

A common analytics shape: per-row business logic that vectorized kernels can't easily express. We score each transaction with a small ruleset:

```
score = 0
if amount > 10_000:                       score += 40
elif amount > 1_000:                      score += 10
if country_code != home_country_code:     score += 25
if hour < 6 or hour > 22:                 score += 15
if merchant_risk_tier >= 3:               score += 20
if num_recent_txns > 5:                   score += 10
return score
```

This isn't math; it's branches. Showcase axis: **JIT also wins in Arrow's wheelhouse, but moderately**. Arrow can express it (`pc.if_else` chains, `pc.greater` masks, additions over masks) but the expression is verbose, materializes a mask column per branch, and is bottlenecked on memory bandwidth across many intermediate arrays. JIT compiles the seven branches into ~20 instructions of straight-line code that fits in L1.

### Synthetic data

A `transactions` table with 1M rows: `amount DOUBLE`, `country_code TINYINT`, `home_country_code TINYINT`, `hour TINYINT`, `merchant_risk_tier TINYINT`, `num_recent_txns INTEGER`. Generated in DuckDB with deterministic expressions over `range(N)` so every run produces the same data and the same final aggregate score.

### Variants under test

1. **Python scalar UDF** — the literal Python function above.
2. **PyArrow expression UDF** — written honestly and idiomatically, no sandbagging. If a better Arrow phrasing is suggested during review, adopt it.
3. **numbduck JIT UDF** — the literal branchy code as a `@cfunc`.

### Query

`SELECT sum(fraud_score(amount, country_code, home_country_code, hour, merchant_risk_tier, num_recent_txns)) FROM transactions`. Aggregating to a scalar avoids materialization-cost bias.

### Measurement

Same harness as example 1: warm up, time three runs, report median, cross-check via `assert_results_match`.

### Output (PLACEHOLDER)

```
  Fraud score UDF (1M rows, Python 3.12, duckdb 1.5.x):
       Variant     Time     Rows/sec   Speedup vs Python   Speedup vs Arrow
        Python   89.30s        11.2K           1.0x              n/a
         Arrow    0.31s         3.2M         288x                1.0x
   numbduck JIT    0.04s        25M         2,232x                7.7x
```

### Honesty paragraph (PLACEHOLDER, mandatory rewrite)

To be rewritten from real numbers. The most important honesty section of the three. Expected shape:

- Acknowledge Arrow is fast here — by no means embarrassing; ~hundreds of x faster than Python is a real win.
- Acknowledge the JIT-vs-Arrow gap shrinks for purely numerical work and grows as branchiness increases. Cite the haversine and `x*x` numbers as the contrast.
- For users with a working Arrow UDF who aren't bottlenecked, switching to numbduck for an extra ~7× is a judgment call. The case for numbduck strengthens when (a) the logic is branchy, (b) you need to call from inside a JIT loop (example 2), or (c) you need GIL-free multithreaded execution.
- Be explicit about purpose: this example shows JIT *also* wins in Arrow's wheelhouse, moderately, not dramatically.

### Validation steps before publishing

- Run all three variants once before writing the honesty section. If the JIT-vs-Arrow gap is < 2×, consider dropping the example entirely and saying "Arrow is the right tool for vectorized branchy work."
- Have the Arrow expression for the rule set checked by someone who actually writes Arrow expressions.
- Cross-check that all three variants produce identical aggregate sums.

## Shared harness — `_common.py`

Tiny by design. If during implementation it grows past ~80 lines or gains DuckDB-specific knowledge, that's a smell and the helper should be shrunk or deleted with the bits inlined back into the example files.

### Contents

- `print_env()` — prints `python_version`, `numba.__version__`, `duckdb.__version__`, `numpy.__version__`, `platform.machine()`, `os.cpu_count()`. Every output block carries its environment with it.
- `time_median(fn, repeats=3) -> float` — runs `fn` `repeats` times under `perf_counter`, returns the median wall time. No auto-warmup; warmup is each example's responsibility.
- `format_table(headers, rows, alignments) -> str` — the one bit of formatting annoying to write three times. Returns a string. Writes nothing.
- `assert_results_match(*results, label: str)` — cross-checks that all variants computed the same thing, fails loud if not.

### Explicitly NOT in `_common.py`

- Anything DuckDB-specific. No "register UDF for me" helper; no "extract connection pointer" wrapper.
- Data-generation helpers. Each example's data lives in that example's file.
- Percentile/latency stats. That's example 2 only and lives in `online_scoring.py`.

## File structure conventions

Every example file follows this skeleton:

```python
"""<one-line title>

<2-3 paragraph story: what problem this represents, what's measured,
why the comparison is fair, the honest caveats.>

Run:
    python examples/<file>.py
    NUMBDUCK_BENCH_BIG=1 python examples/<file>.py   # larger row counts

Last measured on: <date>, <machine>, <Python>, <duckdb>, <numba>
    <copy of the table this run produced>
"""

# 1. Imports
# 2. Constants (row counts, seeds, env-var gates)
# 3. Synthetic data generation (one function)
# 4. Variant 1: Python  (one function)
# 5. Variant 2: Arrow   (one function, or skip with reason)
# 6. Variant 3: numbduck JIT  (registration + the @cfunc body)
# 7. main():
#    - print_env()
#    - generate data
#    - warm up each variant
#    - time each variant via time_median
#    - assert_results_match
#    - print_table
#    - optional: print honesty paragraph
# 8. if __name__ == "__main__": main()
```

The "Last measured on" docstring block is the **canonical published numbers**, updated whenever the example is re-run for publication. The script's stdout is the *current* numbers; the docstring is the *blessed* numbers. The two diverge between publications and that's fine.

## Determinism conventions

- Every random source seeded: DuckDB's `setseed(0.42)`, numpy's `default_rng(42)`, Python's `random.seed(42)` if used.
- Every script asserts identical aggregate results across variants. If they don't match, the script fails loud rather than printing fake numbers.
- Row counts are constants at the top of the file, not buried mid-function.

## Environment variable gates

- `NUMBDUCK_BENCH_BIG=1` — enables the largest row-count tier (10M for haversine, 500K for scoring, 10M for fraud score). Default off.
- `NUMBDUCK_BENCH_REPEATS=N` — overrides default repeat count (3).

No other knobs. Resist configurability.

## What we are explicitly NOT doing

- No `argparse`. Env vars are enough.
- No JSON output mode.
- No matplotlib / plot generation. Plots happen in whatever consumes the numbers.
- No CI integration. These are demos, not regression tests.
- No `examples/__init__.py`. Not a package.

## Risks summary

| Risk | Example | Mitigation |
|---|---|---|
| Predicted numbers don't match measured | all | Hard rule above: rewrite honesty sections, drop examples that don't earn their place |
| `nogil=True` silently violated by binding helpers | 2 | Parallel-scaling test reveals it; investigate before publishing |
| `math.*` lowering issues under `@cfunc` | 1 | One-row sanity test before building full example |
| Chunk lifetime leaks under tight loop | 2 | 5-minute soak before publishing |
| Arrow phrasing for branchy logic is non-idiomatic | 3 | Have an Arrow user review |
| 10M row tier too slow for default-on | 1, 3 | Gated behind `NUMBDUCK_BENCH_BIG` |
| `time.monotonic_ns` cost dominates per-iter | 2 | Sample every Kth event or measure batches |
| `_common.py` accretes complexity | all | If > 80 lines or gains DuckDB knowledge, shrink/delete |

## Success criteria

This effort is successful when:

1. Three example scripts exist in `examples/`, each runnable as `python examples/<file>.py`.
2. Each script runs to completion in < 30 seconds at default settings on a developer laptop.
3. Each script prints an environment block, a result table from real measurements on the running machine, and (if applicable) an honesty paragraph that matches the measured numbers, not the predicted ones.
4. Each script's "Last measured on" docstring block has been updated from a publishing-machine run.
5. All three variants in each script return identical aggregate results, asserted in code.
6. Example 2 demonstrates measurable parallel scaling with the JIT variant and measurable GIL contention with the Python variant — or, if it doesn't, the honesty section explains why and we investigate the cause before deciding what to publish.
7. The `examples/README.md` summarizes each example in one paragraph and links the script.
