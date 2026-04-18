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


from numbduck.vector import vector_push, vector_extend


def test_vector_push():
    Float64Vec, _ = make_vector(nb_types.float64)

    @njit
    def go():
        v = Float64Vec(numpy.empty(4, dtype=numpy.float64), 0)
        vector_push(v, 1.0)
        vector_push(v, 2.0)
        vector_push(v, 3.0)
        return v[0], v[1], v[2], len(v), v.buf.shape[0]

    a, b, c, n, cap = go()
    assert (a, b, c) == (1.0, 2.0, 3.0)
    assert n == 3
    assert cap == 4


def test_vector_push_growth():
    Float64Vec, _ = make_vector(nb_types.float64)

    @njit
    def go():
        v = Float64Vec(numpy.empty(2, dtype=numpy.float64), 0)
        for i in range(5):
            vector_push(v, float(i * 10))
        return v[0], v[1], v[2], v[3], v[4], len(v), v.buf.shape[0]

    vals = go()
    assert vals[:5] == (0.0, 10.0, 20.0, 30.0, 40.0)
    assert vals[5] == 5
    assert vals[6] == 8


def test_vector_extend():
    Float64Vec, _ = make_vector(nb_types.float64)

    @njit
    def go():
        a = Float64Vec(numpy.empty(4, dtype=numpy.float64), 0)
        b = Float64Vec(numpy.empty(4, dtype=numpy.float64), 0)
        vector_push(a, 1.0)
        vector_push(a, 2.0)
        vector_push(b, 3.0)
        vector_push(b, 4.0)
        vector_push(b, 5.0)
        vector_extend(a, b)
        return a[0], a[1], a[2], a[3], a[4], len(a), a.buf.shape[0]

    vals = go()
    assert vals[:5] == (1.0, 2.0, 3.0, 4.0, 5.0)
    assert vals[5] == 5
    assert vals[6] == 8


def test_vector_extend_no_growth():
    Float64Vec, _ = make_vector(nb_types.float64)

    @njit
    def go():
        a = Float64Vec(numpy.empty(8, dtype=numpy.float64), 0)
        b = Float64Vec(numpy.empty(4, dtype=numpy.float64), 0)
        vector_push(a, 1.0)
        vector_push(b, 2.0)
        vector_push(b, 3.0)
        vector_extend(a, b)
        return a[0], a[1], a[2], len(a), a.buf.shape[0]

    vals = go()
    assert vals[:3] == (1.0, 2.0, 3.0)
    assert vals[3] == 3
    assert vals[4] == 8


def test_multi_dtype_int64():
    Int64Vec, _ = make_vector(nb_types.int64)

    @njit
    def go():
        v = Int64Vec(numpy.empty(2, dtype=numpy.int64), 0)
        vector_push(v, 100)
        vector_push(v, 200)
        vector_push(v, 300)
        return v[0], v[1], v[2], len(v), v.buf.shape[0]

    vals = go()
    assert vals[:3] == (100, 200, 300)
    assert vals[3] == 3
    assert vals[4] == 4
