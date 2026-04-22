import operator

import numpy
from numba import njit, types as nb_types
from numba.experimental import structref
from numba.extending import overload
from numbox.utils.highlevel import make_structref


class VectorType(nb_types.StructRef):
    def preprocess_fields(self, fields):
        return tuple((n, nb_types.unliteral(t)) for n, t in fields)


_vector_cache = {}


def make_vector(elem_type):
    key = elem_type.key
    if key in _vector_cache:
        return _vector_cache[key]

    type_cls = type(
        f"Vector_{elem_type.name}_Type",
        (VectorType,),
        {},
    )
    structref.register(type_cls)

    fields = {
        "buf": nb_types.Array(elem_type, 1, 'C'),
        "size": nb_types.int64,
    }

    proxy_cls = make_structref(
        f"Vector_{elem_type.name}",
        fields,
        type_cls,
    )

    np_dtype = numpy.dtype(str(elem_type))

    @njit
    def create(capacity):
        return proxy_cls(numpy.empty(capacity, dtype=np_dtype), 0)

    type_inst = type_cls([
        ("buf", nb_types.Array(elem_type, 1, 'C')),
        ("size", nb_types.int64),
    ])

    result = (create, type_inst)
    _vector_cache[key] = result
    return result


@overload(len)
def _vector_len(v):
    if isinstance(v, VectorType):
        def impl(v):
            return v.size
        return impl


@overload(operator.getitem)
def _vector_getitem(v, i):
    if isinstance(v, VectorType):
        def impl(v, i):
            return v.buf[i]
        return impl


@overload(operator.setitem)
def _vector_setitem(v, i, val):
    if isinstance(v, VectorType):
        def impl(v, i, val):
            v.buf[i] = val
        return impl


@njit
def vector_push(v, val):
    if v.size == v.buf.shape[0]:
        new_buf = numpy.empty(v.buf.shape[0] * 2, v.buf.dtype)
        new_buf[:v.size] = v.buf[:v.size]
        v.buf = new_buf
    v.buf[v.size] = val
    v.size += 1


@njit
def vector_extend(dst, src):
    needed = dst.size + src.size
    cap = dst.buf.shape[0]
    if needed > cap:
        while cap < needed:
            cap *= 2
        new_buf = numpy.empty(cap, dst.buf.dtype)
        new_buf[:dst.size] = dst.buf[:dst.size]
        dst.buf = new_buf
    dst.buf[dst.size:dst.size + src.size] = src.buf[:src.size]
    dst.size += src.size
