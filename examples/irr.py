"""IRR (Internal Rate of Return) UDAF — how to build a DuckDB aggregate function.

Demonstrates the full DuckDB aggregate lifecycle using numbduck:
  1. Define aggregate state as a numba structref (via numbox make_structref)
  2. Write @njit callbacks for init/update/combine/finalize/destroy
  3. Register the aggregate with DuckDB's C API
  4. Run a SQL query that invokes the UDAF

The IRR UDAF finds the monthly discount rate r such that:
    -investment - target_npv + sum(cashflows[i] / (1 + r) ^ periods[i]) = 0

SQL usage:
    SELECT irr(cashflow, period, investment, target_npv) FROM monthly_data;

See test/test_ducklib.md for a detailed explanation of the structref bridge
intrinsics and the removerefctpass interaction.
"""
import ctypes
import math
import sys

import duckdb
import numpy
from numba import cfunc, carray, njit
from numba import types as nb_types
from numba.core import cgutils
from numba.experimental import structref
from numba.extending import intrinsic
import llvmlite.ir as llir
from numba.typed import List as typed_list

from numbox.utils.highlevel import make_structref
from numbox.utils.lowlevel import _cast_int_to_void_p, get_unicode_data_p

from numbduck import ducklib
from numbduck.pybridge import extract_connection_ptr


# ---- NRT <-> DuckDB bridge intrinsics ----
#
# These let an NRT-managed structref round-trip through DuckDB's
# aggregate state slot (a raw void*) without breaking reference counts.
# See test/test_ducklib.md for the full explanation.

_MI_TY = nb_types.MemInfoPointer(nb_types.voidptr)


@intrinsic
def _export_meminfo(typingctx, struct_ty):
    sig = nb_types.intp(struct_ty)

    def codegen(context, builder, signature, args):
        struct_val = args[0]
        _, meminfo_p = context.nrt.get_meminfos(
            builder, struct_ty, struct_val)[0]
        context.nrt.incref(builder, _MI_TY, meminfo_p)
        return builder.ptrtoint(meminfo_p, cgutils.intp_t)
    return sig, codegen


@njit
def export_meminfo(s):
    return _export_meminfo(s)


@intrinsic
def _borrow_structref(typingctx, struct_type_ref, p_ty):
    inst_type = struct_type_ref.instance_type
    sig = inst_type(struct_type_ref, p_ty)

    def codegen(context, builder, signature, args):
        p_val = args[1]
        mi_ll_ty = context.get_value_type(_MI_TY)
        meminfo = builder.inttoptr(p_val, mi_ll_ty)
        context.nrt.incref(builder, _MI_TY, meminfo)
        st = cgutils.create_struct_proxy(inst_type)(context, builder)
        st.meminfo = meminfo
        return st._getvalue()
    return sig, codegen


@njit
def borrow_structref(struct_type, p):
    return _borrow_structref(struct_type, p)


@intrinsic
def _release_meminfo(typingctx, p_ty):
    sig = nb_types.void(p_ty)

    def codegen(context, builder, signature, args):
        ptr_ty = llir.IntType(8).as_pointer()
        fnty = llir.FunctionType(llir.VoidType(), [ptr_ty])
        fn = cgutils.get_or_insert_function(
            builder.module, fnty, "NRT_MemInfo_release")
        meminfo = builder.inttoptr(args[0], ptr_ty)
        builder.call(fn, [meminfo])
    return sig, codegen


@njit
def release_meminfo(p):
    _release_meminfo(p)


# ---- IRR state structref ----

@structref.register
class IRRStateType(nb_types.StructRef):
    def preprocess_fields(self, fields):
        return tuple((n, nb_types.unliteral(t)) for n, t in fields)


IRRState = make_structref(
    "IRRState",
    {
        "cashflows": nb_types.ListType(nb_types.float64),
        "periods": nb_types.ListType(nb_types.float64),
        "investment": nb_types.float64,
        "target_npv": nb_types.float64,
        "initialized": nb_types.int64,
    },
    IRRStateType,
)

irr_state_type = IRRStateType([
    ("cashflows", nb_types.ListType(nb_types.float64)),
    ("periods", nb_types.ListType(nb_types.float64)),
    ("investment", nb_types.float64),
    ("target_npv", nb_types.float64),
    ("initialized", nb_types.int64),
])
