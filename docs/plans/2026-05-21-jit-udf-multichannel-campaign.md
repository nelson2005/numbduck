# JIT'd DuckDB UDFs — multichannel campaign implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers-extended-cc:subagent-driven-development (recommended) or superpowers-extended-cc:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce and publish three channel-tailored articles (numba discourse reply, DuckDB news/discourse, HN essay) telling the numbduck JIT-UDF + first-Python-UDAF story, per the design spec at [`2026-05-21-jit-udf-multichannel-campaign-design.md`](2026-05-21-jit-udf-multichannel-campaign-design.md).

**Architecture:** Each article is a standalone markdown draft in `docs/plans/articles/`. All three share a `benchmarks-snapshot.md` (re-run before publish). Drafts are reviewed against the spec, URLs verified via `lychee`, then published manually by the user to their respective venues. The `docs/plans/` directory is fork-only (excluded from upstream per `CLAUDE.md`).

**Tech Stack:** Markdown, the existing [`examples/*.py`](https://github.com/Goykhman/numbduck/tree/main/examples) scripts, `lychee` for link verification, the numbduck venv at `/home/erik/projects/numbduck/venv`.

---

## File Structure

| File | Responsibility |
| --- | --- |
| `docs/plans/articles/benchmarks-snapshot.md` | Fresh `examples/*.py` outputs (haversine/online_scoring/fraud_score/irr). Source-of-truth for all numbers cited in articles. |
| `docs/plans/articles/article-1-numba-discourse-reply.md` | ~800-1200w reply for Milton's numba discourse thread. |
| `docs/plans/articles/article-2-duckdb-news.md` | ~1500-2000w post for duckdb.org/news/ pitch (or DuckDB Discourse fallback). |
| `docs/plans/articles/article-3-hn-essay.md` | ~1500-2000w essay hosted at [HOST_URL] (TBD) for HN submission. |
| `docs/plans/articles/url-check-report.md` | `lychee` output verifying all URLs in the three drafts resolve. |

All paths relative to repo root `/home/erik/projects/numbduck/`.

---

## Task 0: Snapshot fresh benchmark numbers

**Goal:** Run all four [`examples/*.py`](https://github.com/Goykhman/numbduck/tree/main/examples) scripts in a clean state and capture their printed numbers as the source-of-truth for article citations.

**Files:**
- Create: `docs/plans/articles/benchmarks-snapshot.md`

**Acceptance Criteria:**
- [ ] [`examples/haversine.py`](https://github.com/Goykhman/numbduck/blob/main/examples/haversine.py) runs without error; output captured
- [ ] [`examples/online_scoring.py`](https://github.com/Goykhman/numbduck/blob/main/examples/online_scoring.py) runs without error; output captured (median latency + 8-thread parallel scaling)
- [ ] [`examples/fraud_score.py`](https://github.com/Goykhman/numbduck/blob/main/examples/fraud_score.py) runs without error; output captured at default size (10K) and `NUMBDUCK_BENCH_BIG=1` (1M)
- [ ] [`examples/irr.py`](https://github.com/Goykhman/numbduck/blob/main/examples/irr.py) runs without error; output captured (computed IRR matches known-answer assertion)
- [ ] Snapshot file includes: timestamp, machine spec (CPU/cores/OS), Python version, numba version, numbduck commit SHA
- [ ] Snapshot file committed to branch

**Verify:** Snapshot file exists with all 4 script outputs visible; file size > 1KB; commit contains only the snapshot file.

**Steps:**

- [ ] **Step 1: Confirm venv state**

```bash
git -C /home/erik/projects/numbduck status --short
git -C /home/erik/projects/numbduck branch --show-current
/home/erik/projects/numbduck/venv/bin/python --version
/home/erik/projects/numbduck/venv/bin/pip list | grep -E 'numba|numbox|duckdb|pyarrow'
```

Expected: working tree clean, on `docs/jit-udf-campaign-design` (or `main`), Python 3.12.x, numba + numbox + duckdb + pyarrow installed.

If pyarrow missing: `/home/erik/projects/numbduck/venv/bin/pip install pyarrow`.

- [ ] **Step 2: Clear caches per `feedback_clean_cache_before_tests`**

```bash
/home/erik/projects/numbduck/venv/bin/python -c "import shutil, pathlib; [shutil.rmtree(p, ignore_errors=True) for p in pathlib.Path('/home/erik/projects/numbduck').rglob('__pycache__')]; shutil.rmtree(pathlib.Path.home() / '.cache' / 'numba', ignore_errors=True)"
```

- [ ] **Step 3: Run each script and capture output**

```bash
cd /home/erik/projects/numbduck && venv/bin/python examples/haversine.py 2>&1 | tee /tmp/haversine.out
cd /home/erik/projects/numbduck && venv/bin/python examples/online_scoring.py 2>&1 | tee /tmp/online_scoring.out
cd /home/erik/projects/numbduck && venv/bin/python examples/fraud_score.py 2>&1 | tee /tmp/fraud_score.out
cd /home/erik/projects/numbduck && NUMBDUCK_BENCH_BIG=1 venv/bin/python examples/fraud_score.py 2>&1 | tee /tmp/fraud_score_big.out
cd /home/erik/projects/numbduck && venv/bin/python examples/irr.py 2>&1 | tee /tmp/irr.out
```

Expected: each exits 0; numbers print to stdout.

- [ ] **Step 4: Compose `benchmarks-snapshot.md`**

Structure:

```markdown
# Benchmarks snapshot — YYYY-MM-DD

- **Machine:** <CPU, cores, OS>
- **Python:** <version>
- **numba:** <version>, **numbox:** <version>, **duckdb:** <version>
- **numbduck HEAD:** <sha>
- **Run cmd:** `venv/bin/python examples/<script>.py`

## haversine.py
```
<paste full output>
```

## online_scoring.py
```
<paste full output>
```

## fraud_score.py (default, 10K rows)
```
<paste full output>
```

## fraud_score.py (NUMBDUCK_BENCH_BIG=1, 1M rows)
```
<paste full output>
```

## irr.py
```
<paste full output>
```

## Headline numbers for citation

- haversine: <Nx> JIT chunk vs per-row Python UDF (10K); <Mx> JIT chunk vs Arrow expression UDF (1M)
- online_scoring: <Px> lower median latency vs pure Python; <Qx> parallel scaling on 8 threads
- fraud_score: Arrow ~60x over Python (10K); JIT ~16x over Arrow (10K); JIT ~1750x over Arrow (1M)
- irr: computed IRR = <value>; matches known answer
```

- [ ] **Step 5: Commit**

```bash
git -C /home/erik/projects/numbduck add docs/plans/articles/benchmarks-snapshot.md
git -C /home/erik/projects/numbduck commit -m "docs: snapshot benchmarks for article source-of-truth"
```

---

## Task 1: Draft Article 1 — Numba discourse reply

**Goal:** Write ~800-1200w reply matching the spec's Article 1 spine, in the numbduck dev team voice, citing fresh numbers from Task 0.

**Files:**
- Create: `docs/plans/articles/article-1-numba-discourse-reply.md`
- Read: `docs/plans/2026-05-21-jit-udf-multichannel-campaign-design.md` § "Article 1"
- Read: `docs/plans/articles/benchmarks-snapshot.md`
- Read: [`test/test_ducklib.md`](https://github.com/Goykhman/numbduck/blob/main/test/test_ducklib.md) for UDAF mechanism reference

**Acceptance Criteria:**
- [ ] 5 sections matching spec: What's new since October / Struct-passing wall dissolved / Benchmarks (with mechanism) / UDAF pattern / Open questions
- [ ] Word count between 800 and 1200 (verify with `wc -w`)
- [ ] All numbers match Task 0 snapshot exactly (no rounded-from-memory values)
- [ ] Team voice throughout — no "I" / no "my colleague" / no "Milton's" / no "Goykhman's"
- [ ] All numbduck/numbox links point to `Goykhman/numbduck` or `Goykhman/numbox` (no `nelson2005/numbduck`)
- [ ] Numba doc link is `https://numba.readthedocs.io/en/stable/` (not pydata.org)
- [ ] UDAF section includes the one-sentence aside about [`create_aggregate` gap](https://github.com/duckdb/duckdb/issues/5116)
- [ ] No placeholders, TBDs, or "to be filled in" markers
- [ ] Closes with invitation, not a sell

**Verify:** `wc -w docs/plans/articles/article-1-numba-discourse-reply.md` returns 800-1200; `grep -E 'nelson2005|Milton|TBD|TODO|pydata\.org' docs/plans/articles/article-1-numba-discourse-reply.md` returns nothing.

**Steps:**

- [ ] **Step 1: Draft the article**

Open the file, write the 5 sections per spec. Lead each section with a one-sentence takeaway (per "pause-anywhere" requirement). Cite fresh numbers from `benchmarks-snapshot.md`. Link liberally to [numbox source](https://github.com/Goykhman/numbox/blob/main/numbox/core/bindings/call.py) for the struct-passing dissolution explanation, [`test_ducklib.md`](https://github.com/Goykhman/numbduck/blob/main/test/test_ducklib.md) for UDAF depth.

- [ ] **Step 2: Word count check**

```bash
wc -w /home/erik/projects/numbduck/docs/plans/articles/article-1-numba-discourse-reply.md
```

Expected: 800-1200. If under, expand the mechanism explanation in §3. If over, trim §1's status paragraph.

- [ ] **Step 3: Voice and link audit**

```bash
grep -nE 'nelson2005|Milton|\bI\b|\bmy\b|TBD|TODO|pydata\.org' /home/erik/projects/numbduck/docs/plans/articles/article-1-numba-discourse-reply.md
```

Expected: empty (no matches).

- [ ] **Step 4: Commit**

```bash
git -C /home/erik/projects/numbduck add docs/plans/articles/article-1-numba-discourse-reply.md
git -C /home/erik/projects/numbduck commit -m "docs: draft article 1 — numba discourse reply"
```

---

## Task 2: Draft Article 2 — DuckDB news / discourse

**Goal:** Write ~1500-2000w post matching the spec's Article 2 spine, opening with Hannes's 2022 framing, leading the UDAF section with "impossible to possible."

**Files:**
- Create: `docs/plans/articles/article-2-duckdb-news.md`
- Read: `docs/plans/2026-05-21-jit-udf-multichannel-campaign-design.md` § "Article 2"
- Read: `docs/plans/articles/benchmarks-snapshot.md`
- Read: [Discussion #4797](https://github.com/duckdb/duckdb/discussions/4797) for Hannes's 2022 framing quotes

**Acceptance Criteria:**
- [ ] 6 sections matching spec: The 2022 conversation / Predecessors / C API wedge / Three real scenarios / Aggregates impossible→possible / Caveats
- [ ] Word count between 1500 and 2000
- [ ] Hannes's "three problems" (function-call overhead, GIL, serialization cost) named in §1 with link to [Discussion #4797](https://github.com/duckdb/duckdb/discussions/4797)
- [ ] bnmoch3 cited respectfully with link to [their post](https://bnmoch3.org/p/duckdb-jit-udfs-numba/) and 23.4s/26.7s numbers
- [ ] NumbSQL cited with link to [cpcloud/numbsql](https://github.com/cpcloud/numbsql) and [PyData NYC 2018 talk](https://pydata.org/nyc2018/schedule/presentation/40/)
- [ ] §3 includes a ~15-line code sample showing haversine `@cfunc` registration
- [ ] §5 leads with [Olivier Grisel's issue #5116](https://github.com/duckdb/duckdb/issues/5116) and DuckDB Python's lack of `create_aggregate`
- [ ] §6 mentions [duckdb-python#404](https://github.com/duckdb/duckdb-python/issues/404) caveat
- [ ] Team voice, Goykhman-only links, no placeholders
- [ ] Closes inviting use-case feedback

**Verify:** `wc -w` in range; same `grep` audit as Task 1 returns empty; code sample in §3 is syntactically valid Python (`python -m py_compile <(sed -n '/```python/,/```/p' ...)` or extract and check).

**Steps:**

- [ ] **Step 1: Pull verbatim quotes from #4797**

```bash
gh api 'repos/duckdb/duckdb/discussions/4797/comments' --paginate --jq '.[] | select(.user.login == "hannes") | {body, createdAt}' 2>&1 | head -50
```

Capture Hannes's exact wording of the three problems — paraphrasing is OK in the article but anchor with one verbatim quote.

- [ ] **Step 2: Draft the article**

Six sections per spec. Lead each with a takeaway sentence. Include the 15-line haversine code sample in §3 (copy from [`examples/haversine.py`](https://github.com/Goykhman/numbduck/blob/main/examples/haversine.py), trim to the registration block, ensure imports are present). Use `benchmarks-snapshot.md` numbers in §4.

- [ ] **Step 3: Word count, voice, link audit**

```bash
wc -w /home/erik/projects/numbduck/docs/plans/articles/article-2-duckdb-news.md
grep -nE 'nelson2005|Milton|\bI\b|\bmy\b|TBD|TODO|pydata\.org' /home/erik/projects/numbduck/docs/plans/articles/article-2-duckdb-news.md
```

Expected: count 1500-2000; grep empty.

Note: `pydata.org/nyc2018/...` is a legitimate citation (PyData NYC schedule) — if the grep flags it, verify it's the NYC talk link not the numba doc, and adjust grep accordingly.

- [ ] **Step 4: Commit**

```bash
git -C /home/erik/projects/numbduck add docs/plans/articles/article-2-duckdb-news.md
git -C /home/erik/projects/numbduck commit -m "docs: draft article 2 — duckdb news pitch"
```

---

## Task 3: Draft Article 3 — HN essay

**Goal:** Write ~1500-2000w stand-alone essay matching spec's Article 3 spine, opening with concrete boundary-cost numbers, building to the UDAF "couldn't even do that before" reveal.

**Files:**
- Create: `docs/plans/articles/article-3-hn-essay.md`
- Read: `docs/plans/2026-05-21-jit-udf-multichannel-campaign-design.md` § "Article 3"
- Read: `docs/plans/articles/benchmarks-snapshot.md`

**Acceptance Criteria:**
- [ ] 6 sections per spec: Boundary cost / Why JIT-the-body isn't enough / Structural fix / Benchmarks / Aggregates / Follow-along
- [ ] Word count 1500-2000
- [ ] §1 includes a concrete per-row Python-boundary timing number (~50ns) and a small ASCII flow diagram
- [ ] §2 cites bnmoch3's 23.4s/26.7s respectfully; explains *why* the boundary remained
- [ ] §3 includes the inline 15-line `@cfunc` registration code sample (copy-paste-runnable)
- [ ] §5 frames UDAFs as "couldn't even do that before now" — names [Olivier Grisel's issue #5116](https://github.com/duckdb/duckdb/issues/5116)
- [ ] §6 has follow-along link list: [numbduck repo](https://github.com/Goykhman/numbduck), [`test_ducklib.md`](https://github.com/Goykhman/numbduck/blob/main/test/test_ducklib.md), [Discussion #4797](https://github.com/duckdb/duckdb/discussions/4797), [numba thread](https://numba.discourse.group/t/fully-jited-duckdb/3060), [duckdb-python#404](https://github.com/duckdb/duckdb-python/issues/404)
- [ ] HN title candidate written at top of file as a comment (so submitter has it handy)
- [ ] Team voice, Goykhman-only links, no placeholders

**Verify:** Word count in range; voice/link audit clean; code sample passes `python -m py_compile` against extracted form.

**Steps:**

- [ ] **Step 1: Verify the boundary-cost claim**

```bash
/home/erik/projects/numbduck/venv/bin/python -c "import time; import duckdb; conn = duckdb.connect(); conn.create_function('noop', lambda x: x, [int], int); N = 100000; data = list(range(N)); conn.register('t', __import__('pyarrow').table({'x': data})); t0 = time.perf_counter_ns(); conn.sql('SELECT noop(x) FROM t').fetchall(); t1 = time.perf_counter_ns(); print(f'per-row overhead: {(t1-t0)/N:.1f} ns')"
```

Expected: a per-row number you can cite (likely 50-500ns depending on CPU). Use the actual measured number in §1.

- [ ] **Step 2: Draft the article**

Six sections per spec. Lead each with a takeaway. Include the small ASCII flow diagram in §1 (e.g. `row → C → Python → C → row` with timing). 15-line code sample in §3 reused from Article 2.

- [ ] **Step 3: Audit**

```bash
wc -w /home/erik/projects/numbduck/docs/plans/articles/article-3-hn-essay.md
grep -nE 'nelson2005|Milton|\bI\b|\bmy\b|TBD|TODO' /home/erik/projects/numbduck/docs/plans/articles/article-3-hn-essay.md
```

Expected: count 1500-2000; grep empty (`\bI\b` here is also forbidden — HN essay uses team "we" voice).

- [ ] **Step 4: Commit**

```bash
git -C /home/erik/projects/numbduck add docs/plans/articles/article-3-hn-essay.md
git -C /home/erik/projects/numbduck commit -m "docs: draft article 3 — HN essay"
```

---

## Task 4: Verify all URLs in all three drafts

**Goal:** `curl -sIL` every external URL in the three drafts; replace dead URLs; commit a verification report.

**Files:**
- Modify: any draft files with dead URLs
- Create: `docs/plans/articles/url-check-report.md`

**Acceptance Criteria:**
- [ ] Every URL in all three drafts returns 200 or 301 (final redirect target) on `curl -sIL`
- [ ] Any dead URL has been replaced (prefer stable upstream sources per [`feedback_verify_external_links`](https://github.com/nelson2005/config/blob/main/claude/memory/feedback_verify_external_links.md))
- [ ] Report file lists every URL checked + final status

**Verify:** Re-run `lychee` (or curl loop) on the three drafts after fixes; report shows zero errors.

**Steps:**

- [ ] **Step 1: Extract all URLs from the three drafts**

```bash
grep -ohE 'https?://[^)[:space:]]+' /home/erik/projects/numbduck/docs/plans/articles/article-{1,2,3}-*.md | sort -u > /tmp/urls.txt
wc -l /tmp/urls.txt
```

- [ ] **Step 2: Check each URL**

```bash
while IFS= read -r url; do
  status=$(curl -sIL -o /dev/null -w "%{http_code}" --max-time 10 "$url")
  echo "$status $url"
done < /tmp/urls.txt | tee /tmp/url-status.txt
```

Expected: all lines start with `200` or `301` (with `Location:` redirect captured).

- [ ] **Step 3: Fix any non-200/301 URLs**

For each dead URL, find a stable replacement (prefer canonical upstream — man7 / POSIX / GitHub mirror / web.archive.org). Edit the affected article(s). Re-run Step 2 until clean.

- [ ] **Step 4: Write report**

```markdown
# URL check report — YYYY-MM-DD HH:MM UTC

Drafts checked: article-1-numba-discourse-reply.md, article-2-duckdb-news.md, article-3-hn-essay.md

Total URLs: <N>
- 200 OK: <X>
- 301 redirect (followed): <Y>
- Replaced (originally dead): <Z>

## Replacements
| Original | Replacement | Reason |
| --- | --- | --- |
| <url> | <new url> | <404 / timeout / rotted> |
```

- [ ] **Step 5: Commit**

```bash
git -C /home/erik/projects/numbduck add docs/plans/articles/url-check-report.md docs/plans/articles/article-*.md
git -C /home/erik/projects/numbduck commit -m "docs: verify URLs in drafts, replace any rot"
```

---

## Task 5: Cross-article consistency review

**Goal:** Read all three drafts as a set; fix consistency issues (voice, numbers, term usage, framing of shared facts).

**Files:**
- Modify: any of `docs/plans/articles/article-{1,2,3}-*.md`

**Acceptance Criteria:**
- [ ] All numbers cited across the three articles match `benchmarks-snapshot.md` exactly
- [ ] bnmoch3 is framed respectfully in all three (no snark, no "they got it wrong")
- [ ] UDAF "impossible→possible" framing appears in Articles 2 and 3 (and the one-sentence aside in Article 1)
- [ ] Team voice consistent — `grep -nE '\bI\b|\bmy\b' docs/plans/articles/article-*.md` returns empty
- [ ] Cross-article links (if any included) are correct
- [ ] No duplicated language between drafts (each has its own voice within team-voice constraint)

**Verify:** Manual re-read of all three drafts back-to-back; final grep returns nothing flagged.

**Steps:**

- [ ] **Step 1: Read all three drafts sequentially**

Read article-1, article-2, article-3 in order. As you read, note any inconsistency in a scratch file `/tmp/consistency-notes.md`.

- [ ] **Step 2: Numbers cross-check**

Diff cited numbers in each article against `benchmarks-snapshot.md`'s "Headline numbers" section. Any drift → fix.

- [ ] **Step 3: Voice audit**

```bash
grep -nE '\bI\b|\bmy\b|\bme\b' /home/erik/projects/numbduck/docs/plans/articles/article-*.md
```

Expected: empty. Any hit → replace with team voice ("we", "our", "the numbduck team", "us").

- [ ] **Step 4: bnmoch3 framing audit**

```bash
grep -A 3 'bnmoch3' /home/erik/projects/numbduck/docs/plans/articles/article-*.md
```

Each mention should be respectful — "right test of standard DuckDB UDFs" framing, not "got it wrong."

- [ ] **Step 5: Commit any fixes**

```bash
git -C /home/erik/projects/numbduck add docs/plans/articles/article-*.md
git -C /home/erik/projects/numbduck commit -m "docs: cross-article consistency pass"
```

---

## Task 6: Publish Article 1 — Numba discourse reply

**Goal:** User posts the reply to the existing [Fully JIT'ed DuckDB](https://numba.discourse.group/t/fully-jited-duckdb/3060) thread.

**Files:** None (external action).

**Acceptance Criteria:**
- [ ] Reply text matches `docs/plans/articles/article-1-numba-discourse-reply.md` exactly
- [ ] Posted in the existing thread (not a new thread)
- [ ] Reply URL captured in the article file as a comment at the top after posting
- [ ] Post is publicly visible

**Verify:** Visit https://numba.discourse.group/t/fully-jited-duckdb/3060 and confirm the reply is the most recent post.

**Steps:**

- [ ] **Step 1: Agent — surface the final draft**

Show the user the contents of `docs/plans/articles/article-1-numba-discourse-reply.md` as the proposed reply text.

- [ ] **Step 2: User — post the reply**

User logs into numba.discourse.group, navigates to the thread, pastes the reply text, posts. **Agent cannot do this step — only the user has the credentials.**

- [ ] **Step 3: User — share the new reply URL**

User pastes the reply URL back into the session.

- [ ] **Step 4: Agent — record the URL at the top of the draft file**

```markdown
<!-- Posted at <REPLY_URL> on YYYY-MM-DD -->
```

- [ ] **Step 5: Commit**

```bash
git -C /home/erik/projects/numbduck add docs/plans/articles/article-1-numba-discourse-reply.md
git -C /home/erik/projects/numbduck commit -m "docs: article 1 published — record reply URL"
```

---

## Task 7: Publish Article 2 — DuckDB news / discourse

**Goal:** Pitch Article 2 to DuckDB Labs for publication at [duckdb.org/news/](https://duckdb.org/news/). Fall back to DuckDB Discourse if pitch is declined or unanswered after 2 weeks.

**Files:** None (external action). May modify article in response to editor feedback.

**Acceptance Criteria:**
- [ ] Pitch sent to DuckDB Labs (email or appropriate submission channel)
- [ ] Response received within 2 weeks; outcome recorded
- [ ] If accepted: article published; URL captured
- [ ] If declined or no response: fallback (DuckDB Discourse "Show & Tell" or equivalent) executed; URL captured

**Verify:** Final publication URL is publicly accessible.

**Steps:**

- [ ] **Step 1: Agent — identify the submission channel**

```bash
curl -sL https://duckdb.org/community.html 2>&1 | grep -iE 'blog|news|submit|pitch|contact' | head -20
```

Or check the existing community-contributed posts ([Ben Fleis's Delta post](https://duckdb.org/2026/05/07/), [Pedro Holanda's streaming post](https://duckdb.org/2026/04/02/data-inlining-in-ducklake.html)) to identify the contribution path. May involve emailing DuckDB Labs or opening a discussion on [duckdb/duckdb](https://github.com/duckdb/duckdb) or [duckdb/blog](https://github.com/duckdb/blog) (if such a repo exists).

- [ ] **Step 2: Agent — draft the pitch email/post**

~150-250 words pitch including: who the team is, what the article covers, why it fits duckdb.org/news/, link to Discussion #4797 as historical context, link to the rendered draft (could be a Gist if you don't want to publish-then-pitch).

- [ ] **Step 3: User — send the pitch**

User sends the pitch. Agent cannot send on user's behalf.

- [ ] **Step 4: Wait for response (up to 2 weeks)**

Agent does not poll. User reports back when response arrives.

- [ ] **Step 5a (if accepted): Agent — collaborate on edits**

If DuckDB Labs requests edits, apply them to the article draft. Re-commit. User pushes final version to DuckDB Labs.

- [ ] **Step 5b (if declined or 2 weeks silent): Agent — prepare fallback**

User posts to DuckDB Discourse under appropriate category. Agent prepares any framing adjustments needed for that venue.

- [ ] **Step 6: Agent — record final publication URL at top of draft**

```markdown
<!-- Published at <FINAL_URL> on YYYY-MM-DD -->
```

- [ ] **Step 7: Commit**

```bash
git -C /home/erik/projects/numbduck add docs/plans/articles/article-2-duckdb-news.md
git -C /home/erik/projects/numbduck commit -m "docs: article 2 published — record URL"
```

---

## Task 8: Publish Article 3 — HN essay

**Goal:** Host Article 3 at a stable URL; submit to Hacker News.

**Files:** None in this repo (the hosted version may live elsewhere).

**Acceptance Criteria:**
- [ ] Article hosted at a stable, publicly accessible URL (location decided per spec deferred-decision: numbduck repo `docs/`, GitHub Pages, dev.to, Medium, or team blog)
- [ ] HN submission made with final title (one of the spec's title candidates, or a user-finalized variant)
- [ ] Submission URL captured

**Verify:** HN submission visible at the captured URL; article hosted URL returns 200.

**Steps:**

- [ ] **Step 1: User — decide hosting location**

User picks from the spec's candidates: numbduck repo `docs/` (rendered on GitHub Pages), dev.to under a team handle, Medium, team blog. Agent can set up GitHub Pages or convert markdown to dev.to format on request.

- [ ] **Step 2: Agent — prepare hosted version**

Convert the markdown draft to the chosen host's format (if needed). For GitHub Pages: configure Jekyll/minimal theme, place the article in `_posts/` or `docs/`. For dev.to: prepare frontmatter. For Medium: paste-ready markdown.

- [ ] **Step 3: User — publish to host**

User publishes. Agent cannot do this — user holds credentials.

- [ ] **Step 4: User — submit to HN**

User logs into [news.ycombinator.com/submit](https://news.ycombinator.com/submit), submits the hosted URL with final title. Best window: US-Eastern weekday morning per spec.

- [ ] **Step 5: Agent — record URLs at top of draft**

```markdown
<!-- Hosted at <HOST_URL> on YYYY-MM-DD -->
<!-- Submitted to HN at <HN_URL> on YYYY-MM-DD -->
```

- [ ] **Step 6: Commit**

```bash
git -C /home/erik/projects/numbduck add docs/plans/articles/article-3-hn-essay.md
git -C /home/erik/projects/numbduck commit -m "docs: article 3 published — record URLs"
```

- [ ] **Step 7: Engage in HN comments for ~6 hours post-submission**

Per spec: technical, not defensive. Agent can help draft replies but user posts them.

---

## Out of scope (defer to follow-on plan)

- Amplification: Discord posts, GitHub Issues comments ([duckdb-python#404](https://github.com/duckdb/duckdb-python/issues/404), [duckdb#5117](https://github.com/duckdb/duckdb/discussions/5117), [duckdb#15906](https://github.com/duckdb/duckdb/discussions/15906), [duckdb#3658](https://github.com/duckdb/duckdb/discussions/3658)), reddit r/dataengineering, lobste.rs, social media. Separate plan after anchors published.
- Long-term maintenance of published articles (broken-link updates, follow-on Q&A).
- Translation / cross-posting beyond the 3 anchor channels.

---

## Self-review checklist

- [ ] Every section of the design spec maps to at least one task
- [ ] No "TBD" / "TODO" / "fill in later" in any task description
- [ ] File paths are absolute (`/home/erik/projects/numbduck/...`) or repo-relative with the repo root named
- [ ] Verify commands are runnable as written (no `<placeholder>` brackets)
- [ ] Each commit message follows the project's recent style (verb-first, no Co-Authored-By, no AI attribution)
- [ ] User-action tasks (6, 7, 8) clearly state "agent cannot do this — user must execute"
