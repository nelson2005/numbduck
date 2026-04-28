"""IRR aggregate state structref — defined in its own module so the
StructRef class identity is stable across processes.

Defining the structref class in ``__main__`` (i.e. inside ``irr.py``
when run as a script) gives it a fresh class identity per process,
which fails type inference on a warm numba cache:
    No conversion from numba.IRRStateType(...) to numba.IRRStateType(...)
"""
from numba import types as nb_types
from numba.experimental import structref
from numbox.core.vector.vector import make_vector
from numbox.utils.highlevel import make_structref


Float64Vector, float64_vec_type = make_vector(nb_types.float64)


@structref.register
class IRRStateType(nb_types.StructRef):
    pass


_irr_state_fields = [
    ("cashflows", float64_vec_type),
    ("periods", float64_vec_type),
    ("investment", nb_types.float64),
    ("target_npv", nb_types.float64),
    ("initialized", nb_types.int64),
]
IRRState = make_structref("IRRState", dict(_irr_state_fields), IRRStateType)
irr_state_type = IRRStateType(_irr_state_fields)
