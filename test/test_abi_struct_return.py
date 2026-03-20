"""
Prove that Windows x64 and System V x86-64 need different LLVM IR for
functions that pass or return structs by value.

DuckDB's C API has functions like duckdb_get_hugeint that return a
16-byte struct (duckdb_hugeint = {uint64_t lower, int64_t upper}).
The compiled libduckdb uses the platform's native ABI:

  System V x86-64 (Linux/macOS):
    - 16-byte structs return in RAX+RDX register pair
    - 16-byte struct args pass in RDI+RSI register pair

  Windows x64:
    - >8-byte struct returns use sret (hidden first pointer arg)
    - >8-byte struct args pass by hidden pointer

LLVM's JIT does NOT lower struct ABI automatically -- that's a frontend
responsibility.  So we must emit the right IR per platform.  These tests
call the same DuckDB function with both conventions and verify that only
the platform-correct one returns the right answer.

The "wrong convention" tests run in a subprocess because an ABI mismatch
typically causes a segfault (signal 11 / exit code -11 on Linux, access
violation on Windows).

References:
  - https://learn.microsoft.com/en-us/cpp/build/x64-calling-convention
  - https://refspecs.linuxbase.org/elf/x86_64-abi-0.99.pdf (section 3.2.3)
  - https://github.com/numba/llvmlite/issues/300
  - https://github.com/llvm/llvm-project/issues/85417
"""
import subprocess
import sys
import textwrap

import numpy
import pytest
from llvmlite import ir
from llvmlite.ir import FunctionType, VoidType
from numba import njit
from numba.core.cgutils import get_or_insert_function
from numba.extending import intrinsic

from numbduck.ducklib import (
    duckdb_create_hugeint,
    duckdb_destroy_value,
    duckdb_get_hugeint,
    duckdb_hugeint_ty,
    signatures,
)

_is_win = sys.platform == "win32"


# ── Intrinsics: same function, two calling conventions ───────────────

@intrinsic(prefer_literal=True)
def _get_hugeint_direct_return(typingctx, val_p_ty):
    """Emit a direct struct return -- the System V convention."""
    sig = signatures["duckdb_get_hugeint"]

    def codegen(context, builder, signature, arguments):
        val_p = arguments[0]
        ret_ll_ty = context.get_value_type(duckdb_hugeint_ty)
        func_ty_ll = FunctionType(ret_ll_ty, [val_p.type])
        func_p = get_or_insert_function(
            builder.module, func_ty_ll, "duckdb_get_hugeint")
        return builder.call(func_p, [val_p])

    return sig.return_type(val_p_ty), codegen


@intrinsic(prefer_literal=True)
def _get_hugeint_sret(typingctx, val_p_ty):
    """Emit an sret call -- the Windows x64 convention."""
    sig = signatures["duckdb_get_hugeint"]

    def codegen(context, builder, signature, arguments):
        val_p = arguments[0]
        ret_ll_ty = context.get_value_type(duckdb_hugeint_ty)
        func_ty_ll = FunctionType(
            VoidType(),
            [ret_ll_ty.as_pointer(), val_p.type],
        )
        func_p = get_or_insert_function(
            builder.module, func_ty_ll, "duckdb_get_hugeint")
        func_p.args[0].add_attribute("sret")
        sret_p = builder.alloca(ret_ll_ty)
        builder.call(func_p, [sret_p, val_p])
        return builder.load(sret_p)

    return sig.return_type(val_p_ty), codegen


# ── Helpers ──────────────────────────────────────────────────────────

def _destroy(val_p):
    """duckdb_destroy_value needs a pointer-to-handle."""
    buf = numpy.zeros(1, dtype=numpy.intp)
    buf[0] = val_p
    duckdb_destroy_value(buf.ctypes.data)


LO, HI = numpy.uint64(0xDEADBEEFCAFEBABE), numpy.int64(0x1234567890ABCDEF)


# ── JIT wrappers ─────────────────────────────────────────────────────

@njit
def _roundtrip_correct(lo_in, hi_in):
    val_p = duckdb_create_hugeint((lo_in, hi_in))
    result = duckdb_get_hugeint(val_p)
    return val_p, result[0], result[1]


@njit
def _roundtrip_direct(lo_in, hi_in):
    val_p = duckdb_create_hugeint((lo_in, hi_in))
    result = _get_hugeint_direct_return(val_p)
    return val_p, result[0], result[1]


@njit
def _roundtrip_sret(lo_in, hi_in):
    val_p = duckdb_create_hugeint((lo_in, hi_in))
    result = _get_hugeint_sret(val_p)
    return val_p, result[0], result[1]


# ── Subprocess helper for "wrong convention" tests ───────────────────

def _run_wrong_convention_in_subprocess(method):
    """Run a roundtrip with the wrong convention in a subprocess.

    Returns (exit_code, stdout).  A segfault gives exit code -11 on
    Linux or 0xC0000005 on Windows.
    """
    script = textwrap.dedent(f"""\
        import numpy
        from test.test_abi_struct_return import (
            _roundtrip_{method}, _destroy, LO, HI,
        )
        try:
            val_p, lo, hi = _roundtrip_{method}(LO, HI)
            _destroy(val_p)
            correct = (lo == LO and hi == HI)
            print(f"lo=0x{{lo:x}} hi=0x{{hi:x}} correct={{correct}}")
        except Exception as e:
            print(f"exception: {{e}}")
    """)
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True, text=True, timeout=120,
    )
    return result.returncode, result.stdout.strip(), result.stderr.strip()


# ── Tests: correct convention ────────────────────────────────────────

def test_platform_aware_binding_always_works():
    """numbduck's _call_lib_func_struct_out picks the right IR per platform."""
    val_p, lo, hi = _roundtrip_correct(LO, HI)
    _destroy(val_p)
    assert lo == LO
    assert hi == HI


@pytest.mark.skipif(not _is_win, reason="sret is the correct convention on Windows")
def test_sret_correct_on_windows():
    """On Windows x64, sret returns the correct values."""
    val_p, lo, hi = _roundtrip_sret(LO, HI)
    _destroy(val_p)
    assert lo == LO
    assert hi == HI


@pytest.mark.skipif(_is_win, reason="direct return is the correct convention on System V")
def test_direct_return_correct_on_sysv():
    """On System V, direct struct return in RAX+RDX works."""
    val_p, lo, hi = _roundtrip_direct(LO, HI)
    _destroy(val_p)
    assert lo == LO
    assert hi == HI


# ── Tests: wrong convention (subprocess-isolated) ────────────────────

@pytest.mark.skipif(_is_win, reason="sret is correct on Windows, not wrong")
def test_sret_crashes_or_gives_garbage_on_sysv():
    """On System V, calling with sret (Windows convention) segfaults.

    The compiled libduckdb expects duckdb_get_hugeint(val_p) with:
      RDI = val_p
    But sret IR emits:
      RDI = hidden sret pointer  (function misreads as val_p)
      RSI = val_p               (function ignores -- only 1 param)
    The function dereferences a stack pointer as a duckdb_value handle,
    causing a segfault.
    """
    rc, stdout, stderr = _run_wrong_convention_in_subprocess("sret")
    if rc < 0:
        # Negative exit code = killed by signal (e.g. -11 = SIGSEGV)
        return  # segfault proves the ABI mismatch
    if "correct=False" in stdout:
        return  # garbage values prove the ABI mismatch
    assert "correct=True" not in stdout, (
        f"sret unexpectedly returned correct values on System V.\n"
        f"exit={rc} stdout={stdout!r}"
    )


@pytest.mark.skipif(not _is_win, reason="direct return is correct on System V, not wrong")
def test_direct_return_crashes_or_gives_garbage_on_windows():
    """On Windows x64, calling with direct struct return segfaults.

    The compiled libduckdb uses sret for duckdb_get_hugeint:
      RCX = hidden sret pointer (caller provides)
      RDX = val_p
    But direct-return IR emits:
      RCX = val_p  (function misreads as sret pointer and writes there)
    The function writes 16 bytes through val_p -- a duckdb_value handle,
    not a writable buffer -- causing an access violation.
    """
    rc, stdout, stderr = _run_wrong_convention_in_subprocess("direct")
    if rc != 0 and "correct=True" not in stdout:
        return  # crash or error proves the ABI mismatch
    if "correct=False" in stdout:
        return  # garbage values prove the ABI mismatch
    assert "correct=True" not in stdout, (
        f"direct return unexpectedly returned correct values on Windows.\n"
        f"exit={rc} stdout={stdout!r}"
    )
