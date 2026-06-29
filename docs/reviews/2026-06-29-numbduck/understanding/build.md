# numbduck subsystem note: Packaging, CI, versioning

Scope: `pyproject.toml`, the four GitHub Actions workflows, the doc-codeblock
extractor, and the packaging/versioning claims in `CLAUDE.md`. This is a
build/CI subsystem; there is no C/JIT/Python runtime boundary here, so the
"boundaries / invariants" framing below is applied to the build pipeline
(version injection, dependency resolution, matrix coverage) rather than to
machine code.

## 1. Packaging (`pyproject.toml`)

- Build backend is setuptools: `requires = ["setuptools~=75.3.0", "wheel~=0.45.0"]`,
  `build-backend = "setuptools.build_meta"` (`pyproject.toml:1-3`).
- Package discovery: `where = ["."]`, `include = ["numbduck*"]`
  (`pyproject.toml:5-7`). Only the `numbduck` package ships; `test/`,
  `examples/`, `docs/` are excluded from the wheel.
- Runtime dependencies (`pyproject.toml:11-14`):
  - `duckdb>=1.3.2,<1.6`
  - `numbox~=0.5.11`
- `requires-python = ">=3.10"` (`pyproject.toml:16`).
- License from file, keywords `duckdb/numba/numpy`, author "NumbDuck GitHub
  Repository Contributors", README is `README.md` (`pyproject.toml:17-22`).
- `dynamic = ["version"]` (`pyproject.toml:15`) with the version resolved from
  an attribute: `version = {attr = "numbduck.__version__"}`
  (`pyproject.toml:29-30`).
- Pytest config registers one marker, `benchmark`, described as "slow
  benchmarks, run only locally (deselected in CI)" (`pyproject.toml:24-27`).
  Note: the marker is *declared* but nothing in `pyproject.toml` deselects it;
  `numbduck_ci.yml` runs a bare `pytest` (`numbduck_ci.yml:76`) with no
  `-m "not benchmark"`. So benchmark tests are deselected only if individual
  tests/conftest do it, or if no test actually carries the marker. Flagged as a
  fragile assumption below.

### Dependency pin meanings

- `numbox~=0.5.11` is a PEP 440 compatible-release pin: equivalent to
  `>=0.5.11, ==0.5.*`, i.e. `>=0.5.11, <0.6.0`. It admits 0.5.11 and later
  0.5.x patch/minor releases but forbids 0.6.0.
- `duckdb>=1.3.2,<1.6` is an explicit floor/ceiling range admitting the entire
  1.3, 1.4, and 1.5 lines but excluding 1.6. The CI matrix exercises three
  concrete points in that range (1.3.2, 1.4.0, 1.5.1) but not the full span.

### DISCREPANCY: documented numbox pin is stale

`CLAUDE.md` states the numbox pin as `numbox~=0.5.6` in two places
(`CLAUDE.md:16` "Key dependencies" and the task brief), but the actual,
load-bearing pin in `pyproject.toml:13` is `numbox~=0.5.11`. The resolver uses
`pyproject.toml`, so the build floor is 0.5.11. Any downstream review reasoning
from the 0.5.6 figure is reasoning from stale documentation. (`~=0.5.6` would
have allowed 0.5.6–0.5.x; `~=0.5.11` raises the floor to 0.5.11.)

## 2. Versioning mechanism

The version is computed dynamically and injected by CI, never stored in the
repo:

- Source of truth at build time is the module attribute `numbduck.__version__`
  (`pyproject.toml:30`).
- The committed `numbduck/__init__.py` contains `__version__ = ""` (empty
  string). A local `pip install -e .` / `python -m build` therefore resolves the
  version to the empty string unless `__init__.py` is overwritten first. Flagged
  below.
- CI overwrites `__init__.py` before building. In `numbduck_ci.yml`:
  - `Determine version` (`numbduck_ci.yml:48-57`): if the ref is a tag, the
    version is the tag name (`github.ref_name`); otherwise it is
    `0.1.${{ github.run_number }}` (`numbduck_ci.yml:55`). The result is written
    to `$GITHUB_ENV` as `VERSION`.
  - `Write version to __init__.py` (`numbduck_ci.yml:58-60`):
    `echo "__version__ = '${VERSION}'" > numbduck/__init__.py`, clobbering the
    empty string. The subsequent editable install and wheel build then read this
    value.
- The benchmark workflow hard-codes a placeholder: it writes
  `__version__ = '0.0.1'` (`haversine_bench.yml:22-23`) purely so the editable
  install succeeds; it does not build a wheel.

Control flow for a tagged release vs. a branch push diverges only at
`numbduck_ci.yml:52`. There is no tag-trigger filter and no publish/upload step
in any workflow — `numbduck_ci.yml` builds the wheel (`--wheel`,
`numbduck_ci.yml:80-83`) but nothing uploads it as an artifact or to PyPI, so
"tagged" versioning is computed but never released by these workflows.

## 3. Main CI matrix (`numbduck_ci.yml`)

Triggers: push on every branch (`branches: ["**"]`) and PR into `main`
(`numbduck_ci.yml:3-7`). Concurrency group keyed on workflow+ref with
`cancel-in-progress: true` (`numbduck_ci.yml:9-11`), so a new push to a branch
cancels its own in-flight run. Permissions are `contents: read`
(`numbduck_ci.yml:13-14`).

Matrix dimensions (`numbduck_ci.yml:19-24`):
- `python-version`: 3.10, 3.11, 3.12, 3.13
- `arch` (used as the `runs-on` label, `numbduck_ci.yml:40`):
  `ubuntu-latest`, `ubuntu-24.04-arm`, `windows-latest`
- `numba-version`: 0.60.0, 0.64.0
- `duckdb-version`: 1.3.2, 1.4.0, 1.5.1

Full cross product is 4×3×2×3 = 72 jobs before exclusions.

Exclusions (`numbduck_ci.yml:25-39`):
- Python 3.13 × numba 0.60.0 — "numba 0.60.0 does not support Python 3.13"
  (drops 1 python×numba combo across all arch/duckdb = 9 jobs).
- Windows is restricted to a thin slice: Python 3.10, 3.12, 3.13 are excluded on
  `windows-latest`, leaving only Python 3.11; and duckdb 1.3.2 and 1.4.0 are
  excluded on Windows, leaving only duckdb 1.5.1. Comment: "Windows only on
  Python 3.11 + latest duckdb to reduce billable minutes"
  (`numbduck_ci.yml:29-39`). So Windows runs only {3.11} × {0.60.0, 0.64.0} ×
  {1.5.1} = 2 jobs.
- Net effect: Linux x86-64 and Linux ARM64 get broad coverage; Windows is a
  smoke test of one Python and the newest duckdb.

Steps (`numbduck_ci.yml:42-83`):
1. `actions/checkout@v6`, `actions/setup-python@v6` pinned to matrix Python.
2. Determine + inject version (see §2).
3. Install deps (`numbduck_ci.yml:61-67`): upgrade pip; install `flake8 pytest`;
   then **pin numba and duckdb to the matrix values** via
   `pip install "numba==${matrix.numba-version}"` and
   `"duckdb==${matrix.duckdb-version}"`; then `pip install -e .`. Because numba
   and duckdb are installed *before* the editable install, the `==` pins win and
   the editable install must be satisfiable against them — i.e. the matrix
   duckdb pins (1.3.2/1.4.0/1.5.1) must all satisfy `duckdb>=1.3.2,<1.6` (they
   do). numbox is *not* pinned here, so `pip install -e .` pulls whatever
   satisfies `numbox~=0.5.11` (latest 0.5.x at run time) — an unpinned,
   time-varying input. Flagged below.
4. flake8 (`numbduck_ci.yml:68-73`): a hard pass selecting only
   `E9,F63,F7,F82` (syntax errors / undefined names) that fails the build, with
   `random_image_ref.py` excluded; then a second `--exit-zero` advisory pass at
   `--max-complexity=10 --max-line-length=127`. Only the first pass gates.
5. `pytest` (bare, `numbduck_ci.yml:74-76`).
6. Build deps `build==1.2.2.post1 wheel==0.45.0` (`numbduck_ci.yml:77-79`).
7. `git clean -Xfd` then `python -m build --wheel` (`numbduck_ci.yml:80-83`).
   `git clean -Xfd` removes git-ignored files (build artifacts, caches) before
   building; it does *not* touch the just-written `numbduck/__init__.py` unless
   that file is git-ignored (it is tracked, so it survives — the injected
   version is preserved).

Supported-Python invariant: `requires-python = ">=3.10"` in packaging vs. the CI
matrix {3.10–3.13}. The lower bound matches; the upper end is exercised through
3.13. numba 0.60.0 caps the Python upper end at 3.12 (hence the 3.13×0.60.0
exclusion), so 3.13 is only validated against numba 0.64.0.

## 4. Doc-codeblock lint (`doc-codeblock-flake8.yml` + `extract_codeblocks.py`)

Workflow (`doc-codeblock-flake8.yml`): runs on every PR and on push to `main`
(`doc-codeblock-flake8.yml:3-6`). Single Ubuntu/Python-3.12 job
(`doc-codeblock-flake8.yml:10-16`), installs `flake8`
(`doc-codeblock-flake8.yml:18`). The lint step (`doc-codeblock-flake8.yml:20-35`)
builds a path list from `docs README.md README.rst` that exist
(`doc-codeblock-flake8.yml:23-26`), exits 0 if none exist, errors if the
extractor script is missing (`doc-codeblock-flake8.yml:31-34`), else runs
`python .github/scripts/extract_codeblocks.py "${paths[@]}"`.

Extractor (`extract_codeblocks.py`): pulls every Python code block out of `.rst`
and `.md` docs and lints each block independently with flake8 over stdin so
violations map back to `path:line`.

- Control flow: `main` (`extract_codeblocks.py:143-166`) parses paths +
  `--max-line-length` (default 120, `:146`) + `--extend-ignore` (default
  `E302,E303,E305,W292,W391`, `:126,:147`) + `--flake8` exe. It iterates files
  (`iter_files`, `:112-123`), extracts blocks per kind, lints each, and returns
  the worst (max) flake8 return code seen (`:153-166`). If no block is found at
  all it prints a notice to stderr but still returns 0 (`:164-166`) — so a docs
  set with zero python blocks passes.
- File discovery (`iter_files`/`_walk`, `:105-123`): a file is linted only if
  its suffix is `.rst`/`.md`; a directory is `rglob`'d for both. `_walk` skips
  any path containing a `SKIP_DIRS` part (`:102`):
  `venv .venv venv313 _build .doctrees node_modules .git __pycache__ plans`.
  `plans` is skipped so internal `docs/plans/` snippets are never linted.
- rst extraction (`extract_rst_blocks`, `:27-77`): matches
  `.. code-block:: python` / `.. code:: python` (`RST_DIRECTIVE`, `:23`),
  consumes option lines (`:38-47`, indented lines starting with `:`), then
  captures the indented body until the indentation drops below the body indent
  (`:48-72`), trims trailing blank lines, and `textwrap.dedent`s the block
  (`:73-76`). It yields `(body_start_line, code)`.
- md extraction (`extract_md_blocks`, `:80-99`): matches a ` ```python ` fence
  (`MD_FENCE`, `:24`), captures until a closing ` ``` ` line, dedents.
- Linting (`lint_block`, `:129-140`): one flake8 subprocess per block, fed the
  block on stdin, with `--max-line-length`, `--extend-ignore`, and
  `--stdin-display-name=<path>:<line>`. The per-block invocation means each
  snippet is linted in isolation — cross-block name references (a name defined in
  an earlier block, used in a later one) will trip `F821 undefined name`. This is
  an intentional consequence of the block-isolation design, not a bug, but it is
  a sharp edge for docs authors. The `extend-ignore` default suppresses
  blank-line/EOF rules (E302/E303/E305/W292/W391) that don't apply to short
  snippets.

Boundary note: this workflow's flake8 config (max-line-length 120,
extend-ignore of blank-line rules) is *independent* of the main CI flake8 config
(max-line-length 127, select E9/F63/F7/F82). Doc snippets and source are linted
under different rule sets.

## 5. Link check (`link-check.yml`)

Triggers: PR, weekly cron `17 6 * * 1` (Mondays), and `workflow_dispatch`
(`link-check.yml:3-7`). Permissions `contents: read` + `issues: write`
(`link-check.yml:11-14`).

Scope selection (`link-check.yml:20-41`):
- On PR: diffs `origin/<base_ref>...HEAD` for added/modified (`--diff-filter=AM`)
  files matching `\.(rst|md|py)$`, excluding `^\.github/`
  (`link-check.yml:23-36`). Empty result → skip.
- Otherwise (cron / dispatch): `find` for all `.rst/.md/.py` excluding
  `.git`, `venv`, nested `venv*`, and `.github` (`link-check.yml:37-41`).

Lychee step (`lycheeverse/lychee-action@v2`, `link-check.yml:43-60`) runs only
if files were selected. Args: no-progress, max-concurrency 4, max-retries 2,
retry-wait 5s, timeout 20s, `--accept 200,206,429`, exclude `mailto:`, exclude
paths `.git`/`venv`, `fail: true`. Report uploaded as `lychee-report` artifact
on `always()` (`link-check.yml:62-68`). Accepting 429 means rate-limited URLs
don't fail the build (a deliberate noise-reduction choice). `.py` files are
in-scope, so URLs in docstrings/comments are checked.

## 6. Haversine benchmark (`haversine_bench.yml`)

Manual only (`workflow_dispatch`, `haversine_bench.yml:3-4`), `contents: read`.
Matrix over `ubuntu-latest` and `ubuntu-24.04-arm` (no Windows/macOS),
`fail-fast: false` (`haversine_bench.yml:10-15`). Python 3.12. Writes
placeholder version `0.0.1` (`:22-23`), then **fully pins** the scientific
stack: `numpy==1.26.4 pyarrow==14.0.0 numba==0.64.0 duckdb==1.5.1` (`:27`), then
`pip install -e .`. Runs `NUMBDUCK_BENCH_BIG=1 python examples/haversine.py`
(`:29-30`). This is the only place numpy/pyarrow versions are pinned anywhere in
the build config; the main CI does not pin numpy/pyarrow at all.

## 7. Invariants and fragile assumptions (for the defect review)

These are flagged, not audited.

1. **Documented numbox pin is stale (0.5.6 vs actual 0.5.11).** `pyproject.toml:13`
   pins `numbox~=0.5.11`; `CLAUDE.md:16` and the review brief say `~=0.5.6`. The
   build floor is 0.5.11. (`pyproject.toml:13` vs `CLAUDE.md:16`.)
2. **numbox is unpinned in CI.** Main CI pins numba and duckdb to matrix values
   but lets `pip install -e .` resolve numbox to whatever latest 0.5.x exists at
   run time (`numbduck_ci.yml:61-67`). A new numbox 0.5.x release can change CI
   behavior with no repo change — non-reproducible input. Same for the benchmark
   except numpy/pyarrow there are pinned.
3. **Committed `__version__` is the empty string.** `numbduck/__init__.py` is
   `__version__ = ""`. A local non-CI build (`python -m build`) yields an empty
   version because the dynamic `attr` reads it (`pyproject.toml:30`). Only CI's
   overwrite step (`numbduck_ci.yml:58-60`) produces a meaningful version. A
   contributor building locally gets a malformed/empty version silently.
4. **`benchmark` marker is declared but not deselected by config.** Bare
   `pytest` in CI (`numbduck_ci.yml:76`) has no `-m "not benchmark"`, and
   `pyproject.toml:24-27` only registers the marker. If any test carries
   `@pytest.mark.benchmark` and relies on config-level deselection, it would run
   in CI. Verify whether conftest or addopts handles this (not in the files in
   scope).
5. **Wheel is built but never published/uploaded.** Tag-based version is computed
   (`numbduck_ci.yml:52-53`) but no workflow uploads the wheel as an artifact or
   to an index, so the "tag → release version" path is dead-ended in these
   files.
6. **Doc snippets are linted in isolation.** `lint_block` runs flake8 per block
   (`extract_codeblocks.py:129-140`), so any doc python block that references a
   name defined in a previous block will fail `F821`. Sharp edge, by design.
7. **Windows coverage is a single slice.** Only Python 3.11 + duckdb 1.5.1 on
   Windows (`numbduck_ci.yml:29-39`); Windows-specific ABI paths (the
   struct-by-value Windows branches described in `CLAUDE.md:43-53`) are exercised
   under exactly one Python/duckdb/{2 numba} combination.
8. **Two different flake8 rule sets** govern source (`numbduck_ci.yml:68-73`,
   max-line-length 127, select E9/F63/F7/F82) vs. doc snippets
   (`extract_codeblocks.py`, max-line-length 120, extend-ignore blank-line
   rules). A line legal in source can be flagged in a doc block and vice versa.
9. **PR link-check base diff assumes a fetchable base ref.** `link-check.yml:24`
   does `git fetch origin <base_ref> --depth=1`; the diff (`:25`) uses a
   three-dot range. Fine for normal PRs; a force-push or unusual base could skew
   the changed-file set.
