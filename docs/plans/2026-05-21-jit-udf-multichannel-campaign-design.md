# JIT'd DuckDB UDFs — multichannel campaign design

Date: 2026-05-21
Status: Spec (pre-writing)
Repo: [`Goykhman/numbduck`](https://github.com/Goykhman/numbduck) / [`nelson2005/numbduck`](https://github.com/nelson2005/numbduck)

## Context

numbduck wraps DuckDB's C API for use inside [numba](https://numba.pydata.org/) `@njit` code, built on the [numbox](https://github.com/Goykhman/numbox) bindings toolkit. The recent additions make this campaign timely:

- **Structref-backed UDAF pattern** merged upstream as [Goykhman/numbduck#24](https://github.com/Goykhman/numbduck/pull/24) (2026-04-22) — bridges DuckDB's aggregate lifecycle to numba structref state via `borrow_structref` + `_deref_structref_raw_ptr` intrinsics. Design doc at [`test/test_ducklib.md`](../../test/test_ducklib.md). **Critically: DuckDB Python has no native UDAF registration path** — [`con.create_function`](https://duckdb.org/docs/current/clients/python/function) is scalar-only; there is no `create_aggregate`. Custom Python aggregates in DuckDB have been a [requested feature since Oct 2022](https://github.com/duckdb/duckdb/issues/5116) (filed by Olivier Grisel) and remain unavailable. numbduck doesn't just make UDAFs *faster* — it makes them *possible* from Python at all.
- **Four narrative-style benchmark scripts** in [`examples/`](../../examples/) measure JIT-callback throughput, latency, GIL-free parallelism, and branchy logic against the closest stock-DuckDB approaches.
- **Existing public footprint** to leverage:
  - Goykhman's numba discourse thread [Fully JIT'ed DuckDB](https://numba.discourse.group/t/fully-jited-duckdb/3060), opened Sept 2025, last post Oct 2025 — no benchmarks ever posted there.
  - nelson2005 + Goykhman April 2026 comment in [DuckDB Discussion #4797](https://github.com/duckdb/duckdb/discussions/4797), the 3.5-year-old vectorized-Python-UDF feature request originally raised by NickCrews and answered by Hannes Mühleisen in 2022.

## Goal

Reach three audiences with three channel-tailored anchor articles. Each article is **independently strong** — readers in any one channel get a complete story without needing to follow cross-references. The "pause anywhere and learn something; keep going and learn more" requirement applies within each article (section-level), not across articles.

Optimize for **traction over completeness**: a reader who only reads the first two sections should walk away with a coherent takeaway.

**The two-part headline of the campaign** (woven through every article in proportions suited to its channel):

1. **Scalar UDFs at C speed.** Concrete: haversine 400×, fraud_score 1750× at 1M rows, online_scoring 2.4× parallel on 8 threads. This is the *quantitative* story — "the same thing you already do, but a lot faster."
2. **Aggregate UDFs at all.** DuckDB Python ships no `create_aggregate` method. Olivier Grisel filed the request in [Oct 2022](https://github.com/duckdb/duckdb/issues/5116); it's still not available. numbduck's structref-backed UDAF pattern lets you write them today. This is the *qualitative* story — "a capability that wasn't there before."

The qualitative story is often *more* compelling than the quantitative one for general readers (HN, DuckDB community) — "didn't exist; now exists" beats "Nx faster" for narrative weight.

## Prior art (must engage, with attribution)

| Source | Used as |
| --- | --- |
| [Discussion #4797 (NickCrews + Hannes, 2022; nelson2005 + Goykhman, 2026)](https://github.com/duckdb/duckdb/discussions/4797) | Anchor for the DuckDB-channel piece. Hannes's "three problems" (function-call overhead, GIL, serialization cost) is the framing scaffold. |
| [bnmoch3: DuckDB JIT Compiled UDFs with Numba](https://bnmoch3.org/p/duckdb-jit-udfs-numba/) + [bnmoch3/duckdb-udf-numba-jit](https://github.com/bnmoch3/duckdb-udf-numba-jit) | Cited as the prior attempt that measured 23.4s vs 26.7s (~1.1×). Their setup was the right test of *standard* DuckDB UDFs; numbduck changes the integration shape. **Frame respectfully — they found a real result; we explain it and show what changes.** |
| [cpcloud/numbsql](https://github.com/cpcloud/numbsql) + [PyData NYC 2018 talk](https://pydata.org/nyc2018/schedule/presentation/40/) | Direct precedent for SQLite (Phillip Cloud, 2018). Credited in numbduck's README. Use as "this idea worked for SQLite; here it is for DuckDB, in a different ballpark." |
| [DuckDB blog: From Waddle to Flying (2023)](https://duckdb.org/2023/07/07/python-udf) | DuckDB Labs's own scalar-UDF announcement. Tonally the natural predecessor; our piece is the 2026 sequel — "From Flying to Machine Code." |
| [YeSQL VLDB 2022 paper](https://www.vldb.org/pvldb/vol15/p2270-foufoulas.pdf) | Optional academic credibility citation. |
| [duckdb/duckdb-python#404](https://github.com/duckdb/duckdb-python/issues/404) | nelson2005's open issue requesting C API symbol export from the Python wheel. Cited as caveat ("on macOS today, numbduck needs the system DuckDB library; #404 will fix this in 1.5.3"). |
| [duckdb/duckdb#5116](https://github.com/duckdb/duckdb/issues/5116) (Olivier Grisel, Oct 2022) | Original feature request for Python UDAFs with combiners. Closed and moved to discussion. **The headline citation for "UDAFs didn't exist in DuckDB Python before now."** Cite by author + date to ground the impossibility-until-now framing. |
| [duckdb/duckdb#5117](https://github.com/duckdb/duckdb/discussions/5117) | Discussion form of #5116. Amplification target after publish. |
| [duckdb/duckdb#3658](https://github.com/duckdb/duckdb/discussions/3658) "Custom aggregate function in Python" | Earlier discussion thread on the same topic. Amplification target after publish. |
| [duckdb/duckdb#15906](https://github.com/duckdb/duckdb/discussions/15906) "Help with creating user defined aggregate function" | 2024 user still asking for this feature. Concrete evidence the gap is felt. Amplification target after publish. |

All external URLs must be `curl -sIL`-verified before each article publishes, per [`feedback_verify_external_links`](https://github.com/nelson2005/config/blob/main/claude/memory/feedback_verify_external_links.md).

## Architecture decisions

| Decision | Choice | Rationale |
| --- | --- | --- |
| Number of anchor pieces | 3 (numba / DuckDB / HN) | Each channel deserves a native-feeling article; shared-spine teasers under-serve every channel. |
| Shared spine across pieces | **No.** | Each article gets the structure that fits its channel's reader. |
| Shared facts | **Yes — via existing repo assets.** | All numbers come from [`examples/*.py`](../../examples/) (each script prints its measured numbers); UDAF mechanism from [`test/test_ducklib.md`](../../test/test_ducklib.md); project status from [`CLAUDE.md`](../../CLAUDE.md). Re-run examples to refresh numbers before each publish. |
| Canonical hub article | **None.** | Each channel piece is canonical for its channel. |
| HN content | Dedicated essay (not a re-post of the DuckDB piece) | Structurally different audience; concrete-numbers framing is too punchy for DuckDB news but right for HN. |
| Amplification (Discord, Issues, reddit, lobsters, social) | **Deferred.** | Separate follow-on plan once anchors are written. |
| Hub URL location for HN essay | **Deferred to publish time.** | Candidates: numbduck repo `docs/`, GitHub Pages, dev.to, Medium, own blog. Affects logistics not structural design. |

## Channel pieces

### Article 1 — Numba discourse: "Where this stands now"

| | |
| --- | --- |
| Audience | Numba experts; comfortable with `@njit`, structref, codegen, ABI |
| Venue | Reply to [Fully JIT'ed DuckDB](https://numba.discourse.group/t/fully-jited-duckdb/3060) (Milton's existing Sept 2025 thread) |
| Length | ~800-1200 words |
| Tone | Collaborative, technical, generous credit to Milton (Goykhman) |
| Author voice | nelson2005 |
| Title | (Reply; no separate title) |

Sections:

1. **What's new since October** — one paragraph status: PR #24 merged upstream (structref UDAF), four bench scripts written, libc/`_call_lib_func` consolidation in numbox landed (and brought the ABI tower with it).
2. **The struct-passing wall, dissolved** — Milton's Oct 2025 post described copying 6×8-byte `duckdb_result` to stack + passing pointer to dodge platform ABI divergence. numbox's unified [`_call_lib_func`](https://github.com/Goykhman/numbox/blob/main/numbox/core/bindings/call.py) now generalizes that pattern (SysV x86-64 by-value ≤16B, by-pointer otherwise; Windows by-pointer always; `byval` + `optnone` to defeat LLVM's stack-copy elision). One paragraph + link to numbox source.
3. **Benchmarks (with mechanism)** — haversine 400× (per-row Python scalar UDF vs JIT chunk callback, 10K rows); fraud_score 1750× (Arrow `pc.if_else` chain vs JIT chunk callback, 1M rows); online_scoring 2.4× parallel scaling on 8 threads vs Python's GIL plateau. One paragraph per scenario explaining *why* the number is what it is.
4. **UDAF pattern** — structref state + 6 lifecycle callbacks (`size`/`init`/`update`/`combine`/`finalize`/`destroy`) + bridge intrinsics (`borrow_structref`, `_deref_structref_raw_ptr`). Link to [`test/test_ducklib.md`](../../test/test_ducklib.md) for the full design. Note `removerefctpass` interaction. IRR example as the worked use case. One-sentence aside: this is also the only path to Python-side custom aggregates in DuckDB today — [`create_aggregate` isn't in the Python API](https://duckdb.org/docs/current/clients/python/function) — so the pattern doubles as the answer to a [3-year-old DuckDB feature request](https://github.com/duckdb/duckdb/issues/5116). (For this audience, the codegen is the headline; mention the capability gap once, don't dwell.)
5. **Open questions for the numba community** — `@cfunc`/`@njit` callback dance (using module-level `@njit` impl + thin `@cfunc` wrapper because `@cfunc` can't `import`); `nb_types.intp` vs `voidptr` for pointers + the `_cast_int_to_void_p` bridge; macOS C API symbol stripping waiting on [duckdb-python#404](https://github.com/duckdb/duckdb-python/issues/404). Invite discussion.

Numbers/code references to draw from: [`examples/haversine.py`](../../examples/haversine.py), [`examples/online_scoring.py`](../../examples/online_scoring.py), [`examples/fraud_score.py`](../../examples/fraud_score.py), [`test/test_ducklib.md`](../../test/test_ducklib.md), [`numbduck/ducklib.py`](../../numbduck/ducklib.py).

Publish protocol: reply to Milton's existing thread (preserves continuity); do not open a fresh thread. Confirm with Milton before posting (he opened the thread; collegial check-in is warranted).

### Article 2 — DuckDB news / DuckDB discourse: "Three problems Hannes flagged in 2022, dissolved"

| | |
| --- | --- |
| Audience | DuckDB users; familiar with Python-DuckDB integration; not assumed to know numba |
| Venue (primary) | Pitch to [duckdb.org/news/](https://duckdb.org/news/) |
| Venue (fallback) | DuckDB Discourse (community.duckdb.org) or numbduck repo `docs/` (if news pitch declined) |
| Length | ~1500-2000 words |
| Tone | Integration-flavored; respectful of DuckDB Labs history; community-aware; less numba jargon |
| Author voice | nelson2005 + Goykhman (joint byline if DuckDB news accepts) |
| Title candidates | "Python UDAFs in DuckDB, Finally — Plus Scalar UDFs at C Speed"; "From Flying to Machine Code: JIT'd DuckDB UDFs (and the First Python UDAFs)"; "Answering Two DuckDB Feature Requests at Once: Python UDAFs and Vectorized UDFs Without the Python"; "Numba-JIT'd UDFs in DuckDB: Three Problems Dissolved" |

Sections:

1. **The 2022 conversation** — Discussion #4797 opens with NickCrews's request. Hannes's three concerns: function-call overhead, GIL, serialization cost. These are correct constraints for the standard Python UDF path. Set them up as a checklist.
2. **What predecessors got right and wrong** — [NumbSQL](https://github.com/cpcloud/numbsql) (2018, Phillip Cloud) — right idea for SQLite, proven at small scale. [bnmoch3](https://bnmoch3.org/p/duckdb-jit-udfs-numba/) (2024) — tried numba+DuckDB through DuckDB's Python UDF API, measured 1.1×. The reason their result was negligible IS the punchline that motivates everything else: they JIT'd the function body, but DuckDB still called it as a Python function on every row. The boundary remained.
3. **The C API wedge** — DuckDB exposes vectorized chunks via [its C API](https://duckdb.org/docs/stable/clients/c/api.html). numba can compile against C ABIs (`@cfunc`). numbduck bridges them: DuckDB hands your `@cfunc` callback a `duckdb_data_chunk *`, the callback reads vectors, computes in registers, writes the result chunk. No Python on the hot path. One paragraph + small code sample (the haversine `@cfunc` registration).
4. **Three real scenarios** (with numbers) — haversine throughput / online_scoring latency + parallelism / fraud_score branchy logic. One paragraph per scenario. Cite [`examples/`](../../examples/) for runnable scripts; readers can reproduce.
5. **Aggregates: from impossible to possible** — open this section with the capability gap, not the speed story. DuckDB Python ships `con.create_function` for scalars; it does not ship `create_aggregate`. Olivier Grisel filed [issue #5116 in Oct 2022](https://github.com/duckdb/duckdb/issues/5116) asking for Python UDAFs with combiners; it was closed and moved to [discussion #5117](https://github.com/duckdb/duckdb/discussions/5117); the gap is still real in 2026 (see also [#15906](https://github.com/duckdb/duckdb/discussions/15906), 2024 user asking again; [#3658](https://github.com/duckdb/duckdb/discussions/3658), even earlier). numbduck answers the request: a structref-backed state via [`make_structref`](https://github.com/Goykhman/numbox/blob/main/numbox/utils/highlevel.py); the six lifecycle callbacks bridge DuckDB's aggregate protocol; result fetched via `duckdb_fetch_chunk`. Walk through IRR as the worked example. Link to [`test/test_ducklib.md`](../../test/test_ducklib.md) and [`examples/irr.py`](../../examples/irr.py) for depth. **This is the most important section of the article for the DuckDB community** — it's not "1750× faster," it's "now exists."
6. **Caveats and what's next** — duckdb-python wheel ships with C API stripped on macOS today (workaround: install system DuckDB; fix tracked in [duckdb-python#404](https://github.com/duckdb/duckdb-python/issues/404)); pinned to duckdb 1.3-1.5; doesn't yet cover the full C API surface; doesn't yet do window functions. Close with: "we'd love to hear what you'd use this for."

Numbers/code references to draw from: same as Article 1, plus [`numbduck/duckdb_utils.py`](../../numbduck/duckdb_utils.py) for buffer-allocation patterns if needed.

Publish protocol:
1. Draft article.
2. Pitch to DuckDB Labs (email via their contact or open a [duckdb/blog](https://github.com/duckdb/blog) discussion if such a venue exists; check current submission path before pitching).
3. If accepted: collaborate on edits; publish under their schedule.
4. If declined or no response in 2 weeks: fall back to DuckDB Discourse (post under "Show & Tell" or equivalent) and/or commit to numbduck repo as `docs/jit-duckdb-udfs.md` (rendered via GitHub).
5. Either way, cross-link from Article 1 (numba) and Article 3 (HN essay).

### Article 3 — HN essay: "Removing the Python boundary from DuckDB UDFs"

| | |
| --- | --- |
| Audience | General tech-curious; no DuckDB or numba assumed |
| Venue (host) | TBD at publish time — candidates: numbduck repo `docs/` rendered on GitHub Pages, dev.to under nelson2005, Medium, own blog |
| Venue (submission) | [news.ycombinator.com](https://news.ycombinator.com/submit) |
| Length | ~1500-2000 words |
| Tone | First-person, technical-but-accessible, concrete numbers early, minimal jargon, link liberally for depth |
| Author voice | nelson2005 |
| Title candidates (HN) | "DuckDB Python doesn't have aggregate UDFs. We added them (and made scalars 1750× faster)"; "Python UDAFs in DuckDB Python — a feature missing since 2022, now possible"; "Removing the Python boundary from DuckDB UDFs (1750× faster than Arrow at 1M rows)"; "How we JIT-compiled DuckDB UDFs and got 400×–1750× over Python and Arrow" |

Sections:

1. **The boundary cost** — open concrete: a per-row Python UDF in DuckDB pays ~50ns of Python-C-Python crossing per row; the actual work in many UDFs is ~5ns. The remaining ~45ns is overhead. Two-paragraph setup; one number, one diagram of "row → C → Python → C → row."
2. **Why JIT-compiling the UDF body isn't enough** — bnmoch3's 2024 measurement: 23.4s vs 26.7s, ~1.1×. Their result is *correct* — they JIT'd the function body, but DuckDB invoked it as a Python callable, so each row still paid the boundary. **The body is fast; the call is slow.** Cite their post respectfully; their setup was the right test of standard DuckDB UDFs.
3. **The structural fix** — call DuckDB's C API from inside `@njit`. `@cfunc` callbacks. DuckDB hands you a vector chunk; you compute in registers; you write the output chunk. No Python on the hot path. One paragraph + a small code sample (haversine registration, ~15 lines).
4. **What this buys you** — one figure showing the three benchmarks (haversine 400× / online_scoring 2.4× parallel / fraud_score 1750× at 1M). One paragraph per scenario explaining the regime where it wins (throughput / latency+parallelism / branchy logic). Link to runnable scripts in [`examples/`](https://github.com/Goykhman/numbduck/tree/main/examples).
5. **Aggregates: the "wait, you couldn't even do *that* before?" moment** — DuckDB Python today exposes [`con.create_function`](https://duckdb.org/docs/current/clients/python/function) for scalar UDFs. It does not expose `create_aggregate`. Custom Python aggregates in DuckDB have been a [requested feature since Oct 2022](https://github.com/duckdb/duckdb/issues/5116) (filed by Olivier Grisel) and still aren't there. numbduck's structref-backed pattern is the first working answer from the Python side. UDAF is harder than scalar because state has to survive across chunks and threads; numbduck uses [numbox structref](https://github.com/Goykhman/numbox) for the state; six lifecycle callbacks bridge DuckDB's aggregate protocol. Walk through IRR (Internal Rate of Return) as the worked example: accumulate `(cashflow, period)` pairs, bisect for the rate. Link to [`examples/irr.py`](https://github.com/Goykhman/numbduck/blob/main/examples/irr.py). The "speedup" frame doesn't apply here — there's no baseline to compare against in DuckDB Python; this is a capability that wasn't there.
6. **Where to read source, what's still hard, follow along** — links to numbduck repo, [`test/test_ducklib.md`](https://github.com/Goykhman/numbduck/blob/main/test/test_ducklib.md), Discussion #4797, Milton's numba discourse thread, [duckdb-python#404](https://github.com/duckdb/duckdb-python/issues/404). Honest list of what's missing: window functions, full C API coverage, macOS pre-1.5.3 friction.

Numbers/code references: same as Articles 1+2. HN essay especially needs the inline code sample in §3 (~15 lines, copy-paste-runnable).

Publish protocol:
1. Draft article.
2. Choose host location (TBD; not blocking design).
3. Publish on host.
4. Submit to HN with sharp title. Best window: weekday morning US-Eastern. Watch comments for ~6 hours; engage technically, not defensively.
5. Cross-link from Articles 1 and 2 after they're up.

## Shared facts and source-of-truth

| Fact | Source-of-truth | Refresh protocol |
| --- | --- | --- |
| haversine 400× throughput | [`examples/haversine.py`](../../examples/haversine.py) (runs and prints numbers) | Re-run before each article publishes; numbers may shift ±10% across runs |
| online_scoring 2.2× latency, 2.4× parallel scaling | [`examples/online_scoring.py`](../../examples/online_scoring.py) | Same |
| fraud_score 60× (Arrow over Python) / 16× (JIT over Arrow) at 10K / 1750× at 1M | [`examples/fraud_score.py`](../../examples/fraud_score.py) | Same |
| IRR UDAF mechanism | [`examples/irr.py`](../../examples/irr.py) + [`test/test_ducklib.md`](../../test/test_ducklib.md) | Stable; mechanism doesn't drift |
| UDAF lifecycle, bridge intrinsics, `removerefctpass` | [`test/test_ducklib.md`](../../test/test_ducklib.md) | Stable |
| Project status (PR #24 merged date, duckdb-python#404 status) | [`CLAUDE.md`](../../CLAUDE.md) "Project Status" section | Check at write time |
| ABI lowering (`_call_lib_func`, `byval`/`optnone`, struct-by-value rules) | [numbox source](https://github.com/Goykhman/numbox/blob/main/numbox/core/bindings/call.py) + [llvmlite#300 comment](https://github.com/numba/llvmlite/issues/300#issuecomment-327235846) | Stable |

## Deferred decisions

| Decision | Defer to | Notes |
| --- | --- | --- |
| Host URL for HN essay | Publish time of Article 3 | Doesn't affect spec; affects logistics |
| Final article titles | Write time | 3-5 candidates listed per article; pick before publish |
| Sequencing across the 3 articles | After Article 1 published | Default: 1 → 2 → 3, ~1 week apart if traction is steady; adjust based on response |
| Whether Goykhman co-bylines Article 2 | When pitching DuckDB Labs | Depends on his preference; default offer is joint byline |
| Amplification plan (Discord, Issues, reddit, lobsters, social) | Separate follow-on design after anchors written | Out of scope here |

## Out of scope

- Article prose (drafting happens in writing-plans phase).
- Discord-specific writing (Discord is amplification only).
- DuckDB Issues commentary (amplification only).
- Re-running benchmarks at publish time (handled at article-write time).
- Sequencing strategy beyond "Article 1 first" (decide after traction is visible).

## Open questions for write-time

| Question | Default if not resolved |
| --- | --- |
| Milton's preference: reply in his thread vs. fresh thread? | Reply in his thread (Article 1) |
| Goykhman's preference on bnmoch3 framing? | Name them respectfully; their result was the right test of standard DuckDB UDFs |
| Goykhman's co-byline on Article 2? | Offer; default to joint byline if he wants it |
| HN title final wording? | Use highest-impact number ("1750×") in title, with channel-appropriate framing |
| Whether to include the 2018 NumbSQL PyData talk video (if public)? | Yes if YouTube/Vimeo recording exists; verify before linking |
| Whether to mention the macOS C API stripping caveat in HN essay's first half? | No — defer to §6; it's a friction not a story |

## Definition of done (for this campaign)

- All 3 articles written, reviewed, and published to their respective channels.
- All external links verified via `curl -sIL` immediately before each publish.
- All benchmark numbers re-measured (run the relevant `examples/` script in a clean venv) within 1 week of each publish.
- Cross-links added between articles where natural and channel-appropriate (optional — each article stands alone without them).
- Amplification plan written as separate follow-on design and executed.
- Done is *not* "go viral" — that's an outcome, not a deliverable. Deliverable is the published anchors.
