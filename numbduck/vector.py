import operator

import numpy
from numba import types as nb_types
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
        jit_options={"cache": False},
    )

    type_inst = type_cls([
        ("buf", nb_types.Array(elem_type, 1, 'C')),
        ("size", nb_types.int64),
    ])

    result = (proxy_cls, type_inst)
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
