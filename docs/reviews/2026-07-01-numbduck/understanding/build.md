# numbduck subsystem: Packaging, CI, versioning

Scope: how numbduck is packaged (`pyproject.toml`), how versions are stamped, and
what each GitHub Actions workflow does. Reviewed at HEAD on branch
`review/numbduck-2026-06-29` (current `@proxy` tree). All `file:line` cites are to
the tree as it stands now.

---

## 1. Packaging (`pyproject.toml`)

Full file is 31 lines.

- **Build backend**: `setuptools.build_meta`, pinned build deps
  `setuptools~=75.3.0`, `wheel~=0.45.0` (`pyproject.toml:1-3`).
- **Package discovery**: `where = ["."]`, `include = ["numbduck*"]`
  (`pyproject.toml:5-7`) — only the `numbduck` package is shipped; `test/`,
  `examples/`, `docs/` are excluded from the wheel.
- **Runtime dependencies** (`pyproject.toml:11-14`):
  - `duckdb>=1.3.2,<1.6`
  - `numbox>=0.5.13`
- **Version is dynamic** (`pyproject.toml:15`) and resolved via
  `version = {attr = "numbduck.__version__"}` (`pyproject.toml:29-30`). setuptools
  imports the `numbduck` package and reads its `__version__` attribute at build
  time.
- **`requires-python = ">=3.10"`** (`pyproject.toml:16`).
- **pytest config** (`pyproject.toml:24-27`): registers a single marker
  `benchmark` ("slow benchmarks, run only locally (deselected in CI)").
  Note: the marker is *declared* but the CI `pytest` invocation
  (`numbduck_ci.yml:79`) does **not** pass `-m "not benchmark"`; deselection
  relies on the benchmark tests themselves opting out (e.g. env-gated), not on the
  marker filter. Worth a glance in the defect pass if any `@pytest.mark.benchmark`
  test is expensive.

### 1a. Dependency-pin rationale

- **`numbox>=0.5.13`** — numbduck's `ducklib.py` now imports `@proxy` /
  `@proxy_if_available` from `numbox.core.proxy.proxy` and `_call_lib_func` /
  `_call_lib_func_byval` from `numbox.core.bindings.call`. The `proxy` module
  exists in the sibling checkout at
  `/home/erik/projects/numbox/numbox/core/proxy/proxy.py`, i.e. the `@proxy`
  decorator is a relatively new numbox surface; `>=0.5.13` is the floor that
  guarantees it. There is **no upper cap** on numbox — any future numbox that
  renames/moves `proxy` or `call` would break numbduck silently at import.
- **`duckdb>=1.3.2,<1.6`** — floor is the oldest C API numbduck targets (the
  CLAUDE.md "Adding a New Binding" section links `duckdb.h` at `v1.3.2`). The
  `<1.6` cap is a hard exclusion of DuckDB 1.6+, which has not been validated.
  The version-gated wrappers (`@proxy_if_available(duckdb_lib, sig, ...)`) exist
  precisely because some C API symbols only appear in newer DuckDB builds; the
  spread of duckdb versions in CI (1.3.2 / 1.4.0 / 1.5.1) is what exercises that
  gating. **This is load-bearing**: the `duckdb-version` matrix axis is the only
  thing that proves `proxy_if_available` correctly degrades on the oldest pinned
  DuckDB and correctly binds on the newest.

### 1b. In-repo version is empty (stamped only in CI)

`numbduck/__init__.py` in the tree is literally `__version__ = ""` (one line, no
newline). Consequences:

- A local `pip install -e .` or `python -m build` **outside CI** produces an
  empty/degenerate version string (setuptools reads `""`). The workflows below
  overwrite `__init__.py` *before* building, so CI/release wheels get a real
  version, but a hand-built wheel would not.
- `test/test_init.py` exists and presumably asserts something about
  `__version__`; if it asserts non-empty it would fail on a clean checkout. Flag
  for the defect pass (not audited here).

---

## 2. Version stamping flow (the `github.ref_name -> shell -> __init__.py` chain)

Both `numbduck_ci.yml` and `release.yml` use the same three-step idiom:

1. **Determine version** (`numbduck_ci.yml:51-60`, `release.yml:21-29`):
   ```bash
   if [ "${{ github.ref_type }}" == "tag" ]; then
     echo "VERSION=${{ github.ref_name }}" >> $GITHUB_ENV
   else
     BUILD_VERSION="0.1.${{ github.run_number }}"   # release.yml uses 0.0.<run_number>
     echo "VERSION=${BUILD_VERSION}" >> $GITHUB_ENV
   fi
   ```
   On a tag build, `VERSION` = the tag name (`github.ref_name`); otherwise a
   synthetic `0.1.<run_number>` (CI) / `0.0.<run_number>` (release).
2. **Write version to `__init__.py`** (`numbduck_ci.yml:61-63`,
   `release.yml:30-31`):
   ```bash
   echo "__version__ = '${{ env.VERSION }}'" > numbduck/__init__.py
   ```
3. **Build** reads it via the `attr = "numbduck.__version__"` dynamic-version
   hook, so the wheel metadata `Version:` = the stamped value.

Data flow: **GitHub ref context → runner expression expansion → shell → GITHUB_ENV
→ echo into `numbduck/__init__.py` → setuptools import → wheel metadata.**

### 2a. `git clean -Xfd` does NOT wipe the stamped version

Both build steps run `git clean -Xfd` immediately before `python -m build`
(`numbduck_ci.yml:85`, `release.yml:36`). `-X` only removes files matched by
`.gitignore`. `numbduck/__init__.py` is a *tracked* file (confirmed:
`git ls-files` lists it), and `.gitignore` ignores `*.py[codz]` (i.e. `.pyc`
etc.), **not** `.py`. So the just-written version survives the clean. Not a bug,
but a subtle ordering dependency worth stating: the stamp step precedes
`git clean` and only survives because the file is tracked.

### 2b. Fragile: expression injection via `github.ref_name`

`${{ github.ref_name }}` is expanded by the runner directly into a `run:` shell
line (`numbduck_ci.yml:56` and the `echo "__version__ = '...'"` at line 63; same
in `release.yml:25,31`). On a **tag** build the tag name is interpolated
unquoted-through-expansion into a shell command; a crafted tag/branch name
containing shell metacharacters (backticks, `$(...)`, or a `'`) is a classic
Actions script-injection vector. `github.run_number` is an integer and safe.
Exploitability is limited (creating tags needs push access), but it is a real
sharp edge — note, don't audit.

### 2c. release.yml `else` branch is effectively dead but inconsistent

`release.yml` triggers only on `release: published` (`release.yml:3-5`). A
published GitHub release always targets a tag, so `github.ref_type == "tag"` and
the `else` branch (`0.0.<run_number>`) is not expected to fire. Two loose ends:
- The synthetic fallback minor differs between workflows: CI uses `0.1.`, release
  uses `0.0.` — cosmetic drift, not correctness.
- If a release were ever cut from a non-tag ref the version scheme would silently
  diverge.

---

## 3. `numbduck_ci.yml` (main matrix)

- **Triggers** (`numbduck_ci.yml:3-7`): `push` on `**` (every branch) and
  `pull_request` targeting `main`.
- **Concurrency** (`9-11`): grouped by `workflow-ref`, `cancel-in-progress: true`
  — a new push to the same ref cancels the older run.
- **Permissions** (`13-14`): `contents: read` only.

### 3a. The matrix (`numbduck_ci.yml:19-43`)

Axes:
- `python-version`: 3.10, 3.11, 3.12, 3.13, 3.14 (`:21`)
- `arch` (used as `runs-on`): `ubuntu-latest`, `ubuntu-24.04-arm`,
  `windows-latest` (`:22`)
- `numba-version`: 0.60.0, 0.64.0 (`:23`)
- `duckdb-version`: 1.3.2, 1.4.0, 1.5.1 (`:24`)

Unconstrained product = 5·3·2·3 = **90 jobs**. Excludes (`:25-42`):
- **numba 0.60.0 does not support Python 3.13** → exclude 3.13 + 0.60.0
  (`:27-28`).
- **numba 0.60.0 does not support Python 3.14** → exclude 3.14 + 0.60.0
  (`:29-31`). (This is the "py3.14 × numba0.60.0 exclude" called out in the task.)
- **Windows cost trimming**: exclude Windows for py3.10 (`:33-34`), py3.12
  (`:35-36`), py3.13 (`:37-38`); exclude Windows + duckdb 1.3.2 (`:39-40`) and
  Windows + duckdb 1.4.0 (`:41-42`). Net: Windows keeps only **py3.11 & py3.14 ×
  duckdb 1.5.1** (comment on `:32` says "3.11 + latest duckdb", but 3.14 also
  survives because no exclude removes it).

Resulting job count:
- **Linux** (2 arches): per arch, numba 0.64.0 covers all 5 pythons × 3 duckdb =
  15; numba 0.60.0 covers pythons {3.10,3.11,3.12} × 3 duckdb = 9 → 24/arch → **48
  Linux jobs**.
- **Windows**: py3.11 × numba{0.60.0,0.64.0} × duckdb 1.5.1 = 2; py3.14 ×
  numba{0.64.0} × duckdb 1.5.1 = 1 → **3 Windows jobs**.
- **Total ≈ 51 jobs.**

Notable coverage gaps (note only): macOS is **not** in the matrix at all (the
`ubuntu-24.04-arm` runner is the only ARM coverage); Windows numba-0.60.0 is only
exercised on py3.11.

### 3b. Steps (`numbduck_ci.yml:45-87`)

1. `actions/checkout@v6`, `actions/setup-python@v6` (`:46-50`).
2. Determine version + write `__init__.py` (§2 above).
3. **Install deps** (`:64-70`): upgrade pip; `pip install flake8 pytest`; then
   pin `numba==<matrix>` and `duckdb==<matrix>`; then `pip install -e .`.
   **Ordering subtlety**: `pip install -e .` runs *after* the explicit numba /
   duckdb pins, and numbduck's own deps are `duckdb>=1.3.2,<1.6` +
   `numbox>=0.5.13` (no numba pin in `pyproject.toml`). numbox is pulled
   transitively by `pip install -e .`; if numbox's own dependency resolution
   demanded a numba newer than the matrix pin, pip could upgrade numba out from
   under the matrix. Worth a glance — the matrix's whole point is testing specific
   numba versions.
4. **Lint** (`:71-76`): two flake8 passes — a hard failure pass
   (`--select=E9,F63,F7,F82`) and an advisory `--exit-zero` pass
   (`--max-complexity=10 --max-line-length=127`). The hard pass excludes
   `test/core/random_image_ref.py` (`:74`) — **that path does not exist** in the
   tree (no `test/core/` dir). It's a harmless stale copy-paste leftover (excluding
   a nonexistent path is a no-op), but a cleanup candidate.
5. **Test** (`:77-79`): bare `pytest` (no `-m` marker filter, no `--durations`).
6. **Build wheel** (`:80-86`): install `build==1.2.2.post1 wheel==0.45.0`,
   `git clean -Xfd`, `python -m build --wheel`. The wheel is built but **not
   uploaded** — CI proves the build works; it does not publish.

---

## 4. `release.yml` (the NEW release/publish workflow)

- **Trigger** (`release.yml:3-5`): `release: published` only.
- Two jobs:
  - **`release-build`** (`:11-43`): checkout@v4, setup-python@v5 (**pinned to
    3.12**, `:19`), Determine version + write `__init__.py` (§2, `0.0.` fallback),
    `python -m build --wheel` after `git clean -Xfd` (`:33-37`), then
    `upload-artifact@v4` as `release-dists` (`:39-43`). **Wheel only — no sdist**
    is built or published; numbduck is pure-Python so the wheel is
    `py3-none-any`, but PyPI will have no source distribution.
  - **`pypi-publish`** (`:45-66`): `needs: release-build`; grants
    `id-token: write` (`:51-53`) and binds `environment: pypi` (`:54-56`) for
    **PyPI Trusted Publishing (OIDC)** — no API token stored. Downloads the
    `release-dists` artifact (`:59-63`) and publishes via
    `pypa/gh-action-pypi-publish@release/v1` (`:65-66`).

Boundary note: the release build stamps version from the **tag name**
(`github.ref_name`), so the git tag *is* the source of truth for the published
PyPI version. There is no consistency check that the tag matches any in-repo
value (the in-repo value is empty by design).

Action-version drift: release.yml pins `checkout@v4` / `setup-python@v5` /
`upload-artifact@v4`, while the other workflows use `@v6`/`@v7`. Cosmetic, but a
sync candidate.

---

## 5. `doc-codeblock-flake8.yml` + `extract_codeblocks.py`

- **Workflow** (`doc-codeblock-flake8.yml`): triggers on every `pull_request`
  and `push` to `main` (`:3-6`); setup-python 3.12; `pip install flake8`
  (`:14-18`). The lint step (`:20-35`) collects a `paths` array from
  `docs README.md README.rst` (whichever exist, `:24-26`), skips cleanly if none
  exist (`:27-30`), hard-errors if `.github/scripts/extract_codeblocks.py` is
  missing (`:31-34`), then runs the extractor over the collected paths (`:35`).
- **Extractor** (`.github/scripts/extract_codeblocks.py`, 172 lines): pulls every
  `.. code-block:: python` / `.. code:: python` block from `.rst`
  (`extract_rst_blocks`, `:27-77`) and every ```` ```python ```` fence from `.md`
  (`extract_md_blocks`, `:80-99`), dedents each, and pipes it to flake8 via stdin
  with `--stdin-display-name=<path>:<line>` (`lint_block`, `:130-141`) so
  violations map back to the doc source line. Exit code = worst flake8 rc seen
  (`main`, `:144-167`). Default `--max-line-length=120` (`:147`) and
  `--extend-ignore=E302,E303,E305,W292,W391` (`DOC_SNIPPET_IGNORE`, `:127`) so
  short snippets aren't dinged for blank-line/EOF rules.
- **`SKIP_DIRS`** (`extract_codeblocks.py:102-103`):
  `{'venv', '.venv', 'venv313', '_build', '.doctrees', 'node_modules', '.git',
  '__pycache__', 'plans', 'reviews'}`. `_walk` (`:106-110`) drops any file whose
  path contains one of these parts. **`reviews` (and `plans`) are skipped** — so
  the code blocks in *this very review note* under `docs/reviews/...` are NOT
  linted by this job. That is intentional: review/plan markdown is scratch and
  need not be flake8-clean. Directory args are walked recursively via
  `iter_files`/`_walk` (`:113-124`); explicitly-passed file paths bypass
  `SKIP_DIRS` (only `_walk` applies the filter), but the workflow only passes
  `docs` (a dir) plus top-level READMEs, so `reviews`/`plans` under `docs/` are
  reliably excluded.

---

## 6. `link-check.yml` (lychee)

- **Triggers** (`link-check.yml:3-7`): `pull_request`, weekly `cron` (Mon
  06:17), and `workflow_dispatch`.
- **Scope** (`:20-41`): on PRs, diffs `origin/<base>...HEAD` for added/modified
  (`--diff-filter=AM`) `.rst/.md/.py` files **outside `.github/`** (`:23-36`); on
  non-PR events, finds all `.rst/.md/.py` excluding `.git`, `venv`, and `.github`
  (`:37-41`). Skips the lychee step entirely if the file list is empty
  (`:44`, `if: steps.scope.outputs.files != ''`).
- **lychee** (`:43-60`): `lycheeverse/lychee-action@v2`, accepts
  `200,206,429`, excludes `mailto:`, fails the job on broken links (`fail: true`),
  writes `lychee-report.md` (`:59-60`); report uploaded on `always()` (`:62-68`).
- **Relevant to this review**: unlike doc-codeblock-flake8, link-check has **no
  `reviews`/`plans` exclusion**. On a PR that *adds* markdown under
  `docs/reviews/...`, those `.md` files match the `AM` diff filter and their links
  **would be link-checked**. Per project policy review notes are normally excluded
  from PRs, so in practice they won't be pushed — but if they were, any dead link
  in a review note fails CI.

---

## 7. `haversine_bench.yml` (manual benchmark)

`workflow_dispatch` only (`haversine_bench.yml:4`). Matrix `os: [ubuntu-latest,
ubuntu-24.04-arm]`, `fail-fast: false` (`:11-15`). Stamps a hardcoded
`__version__ = '0.0.1'` (`:22-23`), installs a **fully pinned** stack
(`numpy==1.26.4 pyarrow==14.0.0 numba==0.64.0 duckdb==1.5.1`, `:26-27`) plus
`pip install -e .`, then runs `NUMBDUCK_BENCH_BIG=1 python examples/haversine.py`
(`:29-30`). This is the only workflow that pins `numpy`/`pyarrow`, and the only
one that runs an `examples/` script rather than the test suite.

---

## 8. Invariants & fragile assumptions (for the defect pass)

Load-bearing invariants:
- **Version is stamped, never committed**: `numbduck/__init__.py` is `""` in-repo;
  every build workflow rewrites it before `python -m build`. Any new build path
  that skips the stamp step ships an empty version.
- **`duckdb-version` matrix axis is the correctness proof for
  `proxy_if_available`**: dropping the oldest (1.3.2) or newest (1.5.1) duckdb from
  the matrix would stop exercising the version-gated symbol path.
- **PyPI version == git tag name** (via `github.ref_name`), with no cross-check
  against any in-repo value.

Fragile / risky (noted, not audited):
1. **Actions script-injection surface** via `${{ github.ref_name }}` interpolated
   into `run:` shell on tag builds (`numbduck_ci.yml:56,63`; `release.yml:25,31`).
2. **Empty in-repo `__version__`** breaks any non-CI build / possibly
   `test/test_init.py` on a clean checkout (`numbduck/__init__.py:1`).
3. **No numbox upper cap** (`pyproject.toml:13`): numbduck imports concrete
   internals (`numbox.core.proxy.proxy`, `numbox.core.bindings.call`); a future
   numbox reshuffle breaks import with no version guard.
4. **numba pin can be clobbered by `pip install -e .`** if numbox's transitive
   resolution wants a newer numba than the matrix pin (`numbduck_ci.yml:68-70`).
5. **release.yml only ships a wheel, no sdist** (`release.yml:37`); action
   versions lag the other workflows.
6. **Stale flake8 exclude** of nonexistent `test/core/random_image_ref.py`
   (`numbduck_ci.yml:74`).
7. **CLAUDE.md is stale vs the current tree**: it documents the old `@cres` +
   `_call_lib_func` pattern (CLAUDE.md `:22,31-37`) and pins `numbox~=0.5.6`
   (`:16`), while the tree uses `@proxy`/`@proxy_if_available` and
   `numbox>=0.5.13` (`pyproject.toml:13`). Not a build defect, but the packaging
   doc-of-record disagrees with reality — relevant when reasoning about intended
   pins.
8. **benchmark marker declared but not filtered in CI** (`pyproject.toml:26` vs
   `numbduck_ci.yml:79`); no `--durations` on the pytest step either.
