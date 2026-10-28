import inspect
import json
import os
import warnings

from numba.core.options import DefaultOptions
from numbox.core.configurations import cache_remedy, check_cache_location, is_a_cache_error


# njit's accepted compilation options. Take the target-level options straight
# from numba (``DefaultOptions`` carries one attribute per njit option) so this
# stays in sync automatically, and add the dispatcher-level ``cache`` flag
# handled by numba.core.decorators.jit. Any other key reaches njit as an
# unrecognized kwarg and aborts ducklib import with a numba error that never
# names the env var, so unknown keys are rejected here instead.
_ALLOWED_JIT_OPTIONS = frozenset(
    name for name in vars(DefaultOptions) if not name.startswith("__")
) | {"cache"}

# nogil lets a binding called from Python release the GIL for the length of
# the C call. DuckDB runs a query on its worker threads, and a worker that needs
# the GIL (a Python UDF on the same connection, a numba @cfunc reporting an
# exception it swallowed) would otherwise wait for it forever while the calling
# thread waits for the workers.
_DEFAULT_JIT_OPTIONS = {"cache": True, "nogil": True}


def _changes_compile_options(options):
    return {k: v for k, v in options.items() if k != "cache"} != {
        k: v for k, v in _DEFAULT_JIT_OPTIONS.items() if k != "cache"}


def get_jit_options():
    """The jit options every binding and buffer allocator is compiled with.

    The defaults are ``{"cache": true, "nogil": true}``. NUMBDUCK_JIT_OPTIONS, a
    JSON object, overrides individual defaults and adds other njit options,
    e.g. export NUMBDUCK_JIT_OPTIONS='{"cache": false}' turns the disk cache
    off and keeps nogil.

    Only the default compile options use numba's disk cache. Its key holds no
    compile options, so an object compiled under one set would be served to a
    process running another, and one process under non-default options would
    poison the cache for every later default one. Any other option turns the
    cache off, with a RuntimeWarning when the cache was asked for explicitly.
    """
    as_str = os.environ.get("NUMBDUCK_JIT_OPTIONS")
    if as_str is None:
        return dict(_DEFAULT_JIT_OPTIONS)
    try:
        as_json = json.loads(as_str)
    except json.JSONDecodeError:
        raise ValueError("NUMBDUCK_JIT_OPTIONS must be valid JSON")
    if not isinstance(as_json, dict):
        raise ValueError('NUMBDUCK_JIT_OPTIONS must be a JSON object, e.g. {"cache": false}')
    unknown = set(as_json) - _ALLOWED_JIT_OPTIONS
    if unknown:
        raise ValueError(
            f"NUMBDUCK_JIT_OPTIONS contains unknown jit option(s) {sorted(unknown)}; "
            f"allowed keys are {sorted(_ALLOWED_JIT_OPTIONS)}"
        )
    options = {**_DEFAULT_JIT_OPTIONS, **as_json}
    if options.get("cache") and _changes_compile_options(options):
        if as_json.get("cache"):
            warnings.warn(
                "NUMBDUCK_JIT_OPTIONS asks for the disk cache together with non-default compile options; "
                "numbduck compiles without the cache, because numba's cache key holds no compile options and "
                "an object compiled under one set would be served to a process running another.",
                RuntimeWarning,
                stacklevel=2,
            )
        options["cache"] = False
    return options


def _cache_probe():
    """A function of this module that is never compiled.

    The file that holds its code is the one numba looks up for the package.
    """


# The bound on the name of a file numba writes for a function of numbduck's, numbox's own bound for its files. The
# name holds the module's stem, the function's qualified name, numba's line number, the interpreter tag and an index
# number, and numba writes the file under a temporary name 21 bytes longer. numbox's proxy gives a binding's body a
# qualified name with two leading underscores. The longest name is 96 bytes, so a location within the 32-byte
# margin of the path limit turns caching off though its names would fit. The margin keeps a longer binding name
# from overflowing a location that passed the check. A test holds every function of the package under the bound.
LONGEST_CACHE_FILE_NAME = 128


def uncached_where_no_cache_can_be_written(options):
    """Return ``options``, with ``cache`` off after one warning where numba can write no cache for numbduck.

    numba sets a cached function up when it is decorated, and raises ``RuntimeError`` at that point when no cache
    location can be written. It has none on a read-only install whose user cache directory cannot be written
    either, and none under an import from an ``.egg``, ``.whl`` or ``.pyz`` archive, which Spark's ``--py-files``
    ships. For a ``.zip`` archive, numba takes the user's cache directory without checking that it can be written,
    and the first write raises ``OSError`` instead, at the first call of a buffer allocator, which compiles lazily.
    From numba 0.68 on, a ``.zip`` member shipped as ``.pyc`` alone raises ``KeyError`` instead, when numba reads
    its source from the archive for the stamp. None of these errors names a remedy.

    Every binding and buffer allocator takes its options from the one ``jit_options``, and all of them are in this
    module's directory. The check is therefore made here, once, for this module's file, with numbox's
    ``check_cache_location``. That function runs the cache set-up that numba runs at decoration and the writability
    check that numba runs at the first save, and compiles nothing. Where numba can cache no function of the file,
    the options are returned with ``cache`` off, and one warning names numbox's remedy for the placement. The
    remedy is to set ``NUMBA_CACHE_DIR`` for a source file on disk, to make the user's cache directory writable
    for a ``.zip``, and to put the source files on disk for any other archive.
    ``NUMBDUCK_JIT_OPTIONS='{"cache": false}'`` turns caching off and silences the warning. An error that is not a
    cache error, as numbox's ``is_a_cache_error`` decides, is raised as it was.
    """
    if not options.get("cache"):
        return options
    py_file = inspect.getfile(_cache_probe)
    try:
        check_cache_location(py_file, LONGEST_CACHE_FILE_NAME)
    except (RuntimeError, OSError, KeyError, ValueError) as error:
        if not is_a_cache_error(error):
            raise
        failure = error
    else:
        return options
    silence = "NUMBDUCK_JIT_OPTIONS='{\"cache\": false}' to turn caching off and silence this warning"
    remedy = cache_remedy(py_file, failure, silence, package="numbduck")
    warnings.warn(
        f"numba cannot cache numbduck here ({failure}); it compiles without a cache. {remedy}",
        RuntimeWarning, stacklevel=2,
    )
    return {**options, "cache": False}


jit_options = uncached_where_no_cache_can_be_written(get_jit_options())
