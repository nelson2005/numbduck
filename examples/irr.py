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


# ---- Bisection solver ----

@njit
def irr_bisect(cashflows, periods, n, investment, target_npv):
    r_lo = -0.99
    r_hi = 10.0
    for _ in range(100):
        r_mid = (r_lo + r_hi) / 2.0
        npv = -investment - target_npv
        for i in range(n):
            npv += cashflows[i] / (1.0 + r_mid) ** periods[i]
        if abs(npv) < 1e-9:
            return r_mid
        if npv > 0.0:
            r_lo = r_mid
        else:
            r_hi = r_mid
    return math.nan


# ---- DuckDB aggregate callbacks ----
#
# DuckDB calls these in order: state_size -> init -> update (per chunk) ->
# combine (parallel merge) -> finalize -> destroy.
# Each receives raw pointers; we use the bridge intrinsics to
# reconstruct the structref from the state slot.

@njit
def _irr_state_size_impl(info):
    return numpy.uint64(8)


@cfunc(nb_types.uint64(nb_types.intp))
def _irr_state_size_cb(info):
    return _irr_state_size_impl(info)


@njit
def _irr_init_impl(info, state):
    cfs = typed_list.empty_list(nb_types.float64)
    pds = typed_list.empty_list(nb_types.float64)
    s = IRRState(cfs, pds, 0.0, 0.0, 0)
    p = export_meminfo(s)
    slot = carray(_cast_int_to_void_p(state), (1,), dtype=numpy.intp)
    slot[0] = p


@cfunc(nb_types.void(nb_types.intp, nb_types.intp))
def _irr_init_cb(info, state):
    _irr_init_impl(info, state)


@njit
def _irr_update_impl(info, chunk, states):
    n = ducklib.duckdb_data_chunk_get_size(chunk)
    vec_cf = ducklib.duckdb_data_chunk_get_vector(chunk, 0)
    vec_pd = ducklib.duckdb_data_chunk_get_vector(chunk, 1)
    vec_inv = ducklib.duckdb_data_chunk_get_vector(chunk, 2)
    vec_npv = ducklib.duckdb_data_chunk_get_vector(chunk, 3)
    cf_data = carray(
        _cast_int_to_void_p(ducklib.duckdb_vector_get_data(vec_cf)),
        (n,), dtype=numpy.float64)
    pd_data = carray(
        _cast_int_to_void_p(ducklib.duckdb_vector_get_data(vec_pd)),
        (n,), dtype=numpy.float64)
    inv_data = carray(
        _cast_int_to_void_p(ducklib.duckdb_vector_get_data(vec_inv)),
        (n,), dtype=numpy.float64)
    npv_data = carray(
        _cast_int_to_void_p(ducklib.duckdb_vector_get_data(vec_npv)),
        (n,), dtype=numpy.float64)
    state_slots = carray(
        _cast_int_to_void_p(states), (n,), dtype=numpy.intp)
    for i in range(n):
        slot = carray(
            _cast_int_to_void_p(state_slots[i]), (1,), dtype=numpy.intp)
        s = borrow_structref(irr_state_type, slot[0])
        s.cashflows.append(cf_data[i])
        s.periods.append(pd_data[i])
        if s.initialized == 0:
            s.investment = inv_data[i]
            s.target_npv = npv_data[i]
            s.initialized = 1


@cfunc(nb_types.void(nb_types.intp, nb_types.intp, nb_types.intp))
def _irr_update_cb(info, chunk, states):
    _irr_update_impl(info, chunk, states)


@njit
def _irr_combine_impl(info, source, target, count):
    src_slots = carray(
        _cast_int_to_void_p(source), (count,), dtype=numpy.intp)
    tgt_slots = carray(
        _cast_int_to_void_p(target), (count,), dtype=numpy.intp)
    for i in range(count):
        src_slot = carray(
            _cast_int_to_void_p(src_slots[i]), (1,), dtype=numpy.intp)
        tgt_slot = carray(
            _cast_int_to_void_p(tgt_slots[i]), (1,), dtype=numpy.intp)
        src = borrow_structref(irr_state_type, src_slot[0])
        tgt = borrow_structref(irr_state_type, tgt_slot[0])
        for j in range(len(src.cashflows)):
            tgt.cashflows.append(src.cashflows[j])
            tgt.periods.append(src.periods[j])
        if tgt.initialized == 0 and src.initialized == 1:
            tgt.investment = src.investment
            tgt.target_npv = src.target_npv
            tgt.initialized = 1


@cfunc(nb_types.void(nb_types.intp, nb_types.intp,
                     nb_types.intp, nb_types.uint64))
def _irr_combine_cb(info, source, target, count):
    _irr_combine_impl(info, source, target, count)


@njit
def _irr_finalize_impl(info, source, result, count, offset):
    out_data = ducklib.duckdb_vector_get_data(result)
    src_slots = carray(
        _cast_int_to_void_p(source), (count,), dtype=numpy.intp)
    out_vals = carray(
        _cast_int_to_void_p(out_data), (offset + count,),
        dtype=numpy.float64)
    for i in range(count):
        src_slot = carray(
            _cast_int_to_void_p(src_slots[i]), (1,), dtype=numpy.intp)
        s = borrow_structref(irr_state_type, src_slot[0])
        n = len(s.cashflows)
        if n == 0:
            out_vals[offset + i] = math.nan
            continue
        # sort by period (simple insertion sort — N is small)
        for j in range(1, n):
            key_cf = s.cashflows[j]
            key_pd = s.periods[j]
            k = j - 1
            while k >= 0 and s.periods[k] > key_pd:
                s.cashflows[k + 1] = s.cashflows[k]
                s.periods[k + 1] = s.periods[k]
                k -= 1
            s.cashflows[k + 1] = key_cf
            s.periods[k + 1] = key_pd
        out_vals[offset + i] = irr_bisect(
            s.cashflows, s.periods, n, s.investment, s.target_npv)


@cfunc(nb_types.void(nb_types.intp, nb_types.intp, nb_types.intp,
                     nb_types.uint64, nb_types.uint64))
def _irr_finalize_cb(info, source, result, count, offset):
    _irr_finalize_impl(info, source, result, count, offset)


@njit
def _irr_destroy_impl(states, count):
    state_slots = carray(
        _cast_int_to_void_p(states), (count,), dtype=numpy.intp)
    for i in range(count):
        slot = carray(
            _cast_int_to_void_p(state_slots[i]), (1,), dtype=numpy.intp)
        release_meminfo(slot[0])


@cfunc(nb_types.void(nb_types.intp, nb_types.uint64))
def _irr_destroy_cb(states, count):
    _irr_destroy_impl(states, count)
