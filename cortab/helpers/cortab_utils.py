"""Basic utilities and helpers for CorTab."""

import math
import functools


def skip_nan(f):
    """Skip NaN/empty values of object_field."""
    @functools.wraps(f)
    def _wrapper(subject_field, object_field, store):
        try:
            if math.isnan(object_field):
                return None

        except TypeError:
            pass

        return f(subject_field, object_field, store)

    return _wrapper
