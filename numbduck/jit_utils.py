# These utilities are not DuckDB-specific and may be better moved to numbox
# in the future.
from numba import njit
from numba.core import errors
from numba.core.extending import intrinsic
from numba.core.types import int32, int64, float64, intp, CPointer
import llvmlite.ir as llvmir


@njit
def array_data_p(arr):
    """Return the data pointer of a numpy array as intp.

    Wraps arr.ctypes.data (which returns uint64 in numba) with an intp cast
    so the result matches the signed-integer pointer convention used by numbox
    binding signatures.

    Callable from both Python and @njit context.
    """
    return intp(arr.ctypes.data)


@intrinsic
def i32_ptr(typingctx, ptr_ty):
    """Cast an intp to CPointer(int32) for use with numba.carray."""
    if ptr_ty != intp:
        raise errors.TypingError(f"i32_ptr expects intp, got {ptr_ty}")

    def codegen(context, builder, signature, args):
        return builder.inttoptr(args[0], llvmir.IntType(32).as_pointer())
    return CPointer(int32)(intp,), codegen


@intrinsic
def i64_ptr(typingctx, ptr_ty):
    """Cast an intp to CPointer(int64) for use with numba.carray."""
    if ptr_ty != intp:
        raise errors.TypingError(f"i64_ptr expects intp, got {ptr_ty}")

    def codegen(context, builder, signature, args):
        return builder.inttoptr(args[0], llvmir.IntType(64).as_pointer())
    return CPointer(int64)(intp,), codegen


@intrinsic
def f64_ptr(typingctx, ptr_ty):
    """Cast an intp to CPointer(float64) for use with numba.carray."""
    if ptr_ty != intp:
        raise errors.TypingError(f"f64_ptr expects intp, got {ptr_ty}")

    def codegen(context, builder, signature, args):
        return builder.inttoptr(args[0], llvmir.DoubleType().as_pointer())
    return CPointer(float64)(intp,), codegen
