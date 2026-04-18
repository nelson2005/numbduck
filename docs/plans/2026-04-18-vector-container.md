# Vector Container — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers-extended-cc:subagent-driven-development (recommended) or superpowers-extended-cc:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create `numbduck/vector.py` — a generic, growable, contiguous-storage container backed by numpy arrays, then update `examples/irr.py` to use it instead of `numba.typed.List`.

**Architecture:** Factory function `make_vector(elem_type)` creates concrete structref types per dtype. A shared `VectorType` base class enables generic operator overloads (`len`, `[]`). Free `@njit` functions `vector_push` and `vector_extend` handle growth via geometric doubling. The vector is NRT-managed — buffer reallocation is safe because `vector_push`/`vector_extend` take structref args, preventing `removerefctpass` from stripping refcount operations.

**Tech Stack:** Python 3.10+, numba (`@njit`, structref, `@overload`), numbox (`make_structref`), numpy.

**Spec:** `docs/specs/2026-04-18-vector-container-design.md`

---

### Task 1: Vector factory and basic operations

**Goal:** Create `numbduck/vector.py` with `VectorType` base, `make_vector` factory, and operator overloads for `len`, `__getitem__`, `__setitem__`. Create `test/test_vector.py` with unit tests.

**Files:**
- Create: `numbduck/vector.py`
- Create: `test/test_vector.py`

**Acceptance Criteria:**
- [ ] `make_vector(nb_types.float64)` returns `(ProxyClass, type_instance)` pair
- [ ] Multiple calls with the same `elem_type` return the same cached result
- [ ] `len(v)` returns `v.size` inside `@njit`
- [ ] `v[i]` and `v[i] = x` work inside `@njit`

**Verify:** `rm -rf __pycache__ test/__pycache__ numbduck/__pycache__ ~/.cache/numba && ./venv/bin/python -m pytest test/test_vector.py -v --durations=20` → all tests pass

**Steps:**

- [ ] **Step 1: Write `test/test_vector.py` with construction and operator tests**

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `rm -rf __pycache__ test/__pycache__ numbduck/__pycache__ ~/.cache/numba && ./venv/bin/python -m pytest test/test_vector.py -v --durations=20`

Expected: `ModuleNotFoundError: No module named 'numbduck.vector'`

- [ ] **Step 3: Create `numbduck/vector.py` with factory and overloads**

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `rm -rf __pycache__ test/__pycache__ numbduck/__pycache__ ~/.cache/numba && ./venv/bin/python -m pytest test/test_vector.py -v --durations=20`

Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add numbduck/vector.py test/test_vector.py
git commit -m "Add vector container with factory and basic operations"
```

---

### Task 2: Growth operations

**Goal:** Add `vector_push` and `vector_extend` free functions with geometric doubling. Add tests including multi-dtype coverage.

**Files:**
- Modify: `numbduck/vector.py` (add `vector_push`, `vector_extend`)
- Modify: `test/test_vector.py` (add growth and multi-dtype tests)

**Acceptance Criteria:**
- [ ] `vector_push` appends one element, doubling capacity when full
- [ ] `vector_extend` bulk-appends from another vector, growing as needed
- [ ] Both work with `int64` vectors (not just `float64`)
- [ ] All elements are preserved across buffer reallocation

**Verify:** `rm -rf __pycache__ test/__pycache__ numbduck/__pycache__ ~/.cache/numba && ./venv/bin/python -m pytest test/test_vector.py -v --durations=20` → all tests pass

**Steps:**

- [ ] **Step 1: Add growth and multi-dtype tests to `test/test_vector.py`**

Append to `test/test_vector.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify new tests fail**

Run: `rm -rf __pycache__ test/__pycache__ numbduck/__pycache__ ~/.cache/numba && ./venv/bin/python -m pytest test/test_vector.py::test_vector_push -v --durations=20`

Expected: `ImportError: cannot import name 'vector_push' from 'numbduck.vector'`

- [ ] **Step 3: Add `vector_push` and `vector_extend` to `numbduck/vector.py`**

Append to `numbduck/vector.py` after the overload definitions:

```python
from numba import njit


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
```

Note: move the `from numba import njit` to the top-level imports (combine with the existing imports section — `njit` is not currently imported in `vector.py`).

- [ ] **Step 4: Run all vector tests**

Run: `rm -rf __pycache__ test/__pycache__ numbduck/__pycache__ ~/.cache/numba && ./venv/bin/python -m pytest test/test_vector.py -v --durations=20`

Expected: 8 passed

- [ ] **Step 5: Commit**

```bash
git add numbduck/vector.py test/test_vector.py
git commit -m "Add vector_push and vector_extend with growth"
```

---

### Task 3: IRR integration

**Goal:** Update `examples/irr.py` to use `Float64Vector` from `numbduck.vector` instead of `numba.typed.List`. Verify the example still passes all checks including NRT leak detection.

**Files:**
- Modify: `examples/irr.py`

**Acceptance Criteria:**
- [ ] `typed.List` imports removed, replaced with `numbduck.vector` imports
- [ ] IRRState uses `float64_vec_type` for cashflows/periods fields
- [ ] `_irr_init_impl` creates `Float64Vector` instances
- [ ] `_irr_update_impl` uses `vector_push`
- [ ] `_irr_combine_impl` uses `vector_extend`
- [ ] NPV cross-checks pass for both single-group and multi-group tests
- [ ] NRT leak check passes (alloc == free)
- [ ] Full test suite still passes

**Verify:**
1. `rm -rf __pycache__ examples/__pycache__ numbduck/__pycache__ ~/.cache/numba && ./venv/bin/python examples/irr.py` → `All checks passed.`
2. `rm -rf __pycache__ test/__pycache__ numbduck/__pycache__ ~/.cache/numba && ./venv/bin/python -m pytest test/ -v --durations=20` → all tests pass

**Steps:**

- [ ] **Step 1: Update imports in `examples/irr.py`**

Remove:
```python
from numba.typed import List as typed_list
```

Add:
```python
from numbduck.vector import make_vector, vector_push, vector_extend
```

Add after the existing `from numbduck` imports:
```python
Float64Vector, float64_vec_type = make_vector(nb_types.float64)
```

- [ ] **Step 2: Update `irr_state_type` and `IRRState` field types**

Replace both `nb_types.ListType(nb_types.float64)` occurrences with `float64_vec_type`:

In `IRRState = make_structref(...)`:
```python
IRRState = make_structref(
    "IRRState",
    {
        "cashflows": float64_vec_type,
        "periods": float64_vec_type,
        "investment": nb_types.float64,
        "target_npv": nb_types.float64,
        "initialized": nb_types.int64,
    },
    IRRStateType,
)
```

In `irr_state_type = IRRStateType([...])`:
```python
irr_state_type = IRRStateType([
    ("cashflows", float64_vec_type),
    ("periods", float64_vec_type),
    ("investment", nb_types.float64),
    ("target_npv", nb_types.float64),
    ("initialized", nb_types.int64),
])
```

- [ ] **Step 3: Update `_irr_init_impl`**

Replace:
```python
    cfs = typed_list.empty_list(nb_types.float64)
    pds = typed_list.empty_list(nb_types.float64)
    s = IRRState(cfs, pds, 0.0, 0.0, 0)
```

With:
```python
    cfs = Float64Vector(numpy.empty(8, dtype=numpy.float64), 0)
    pds = Float64Vector(numpy.empty(8, dtype=numpy.float64), 0)
    s = IRRState(cfs, pds, 0.0, 0.0, 0)
```

- [ ] **Step 4: Update `_irr_update_impl`**

Replace:
```python
        s.cashflows.append(cf_data[i])
        s.periods.append(pd_data[i])
```

With:
```python
        vector_push(s.cashflows, cf_data[i])
        vector_push(s.periods, pd_data[i])
```

- [ ] **Step 5: Update `_irr_combine_impl`**

Replace:
```python
        tgt.cashflows.extend(src.cashflows)
        tgt.periods.extend(src.periods)
```

With:
```python
        vector_extend(tgt.cashflows, src.cashflows)
        vector_extend(tgt.periods, src.periods)
```

- [ ] **Step 6: Verify the example runs**

Run: `rm -rf __pycache__ examples/__pycache__ numbduck/__pycache__ ~/.cache/numba && ./venv/bin/python examples/irr.py`

Expected: `All checks passed.`

- [ ] **Step 7: Verify the full test suite**

Run: `rm -rf __pycache__ test/__pycache__ numbduck/__pycache__ ~/.cache/numba && ./venv/bin/python -m pytest test/ -v --durations=20`

Expected: all tests pass (test_vector.py + test_ducklib.py + test_init.py)

- [ ] **Step 8: Commit**

```bash
git add examples/irr.py
git commit -m "Replace typed.List with vector container in IRR example"
```
