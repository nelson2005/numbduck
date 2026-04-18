import numpy
from numba import njit, types as nb_types

from numbduck.vector import make_vector


def test_construction_and_len():
    Float64Vec, _ = make_vector(nb_types.float64)

    @njit
    def go():
        v = Float64Vec(numpy.empty(8, dtype=numpy.float64), 0)
        return len(v), v.buf.shape[0]

    size, cap = go()
    assert size == 0
    assert cap == 8


def test_getitem_setitem():
    Float64Vec, _ = make_vector(nb_types.float64)

    @njit
    def go():
        v = Float64Vec(numpy.empty(4, dtype=numpy.float64), 0)
        v.buf[0] = 10.0
        v.buf[1] = 20.0
        v.size = 2
        v[1] = 99.0
        return v[0], v[1], len(v)

    a, b, n = go()
    assert a == 10.0
    assert b == 99.0
    assert n == 2


def test_factory_caching():
    r1 = make_vector(nb_types.float64)
    r2 = make_vector(nb_types.float64)
    assert r1[0] is r2[0]
    assert r1[1] is r2[1]
