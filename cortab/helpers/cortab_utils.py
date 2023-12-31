"""Basic utilities and helpers for CorTab."""

import functools
import hashlib
import inspect
import math
import operator

from types import SimpleNamespace
from typing import Callable
from uuid import uuid4

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDFS




def remove_nan(graph: Graph) -> Graph:
    """Remove triples with a NaN object from graph."""
    for triple in graph:
        *_, triple_object = triple

        if triple_object == Literal(math.nan):
            graph.remove(triple)

    return graph


def nan_handler(f: Callable):
    """Check for NaN kwargs in a decorated function.

    Note that checking is done for a function's /signature/,
    i.e. at function definition time.

    Only kwargs are checked; if a kwarg is NaN, an empty list is returned;
    else the decorated function runs.

    Intended (but certainly not sole) usage is in decoratoed generator functions:

    @nan_handler
    def some_generator(x="some value"):
        yield from x

    @nan_handler
    def another_generator(x=math.nan):
        yield from x

    print(list(some_generator()))
    print(list(another_generator()))
    """
    @functools.wraps(f)
    def _wrapper(*args, **kwargs):

        for parameter in inspect.signature(f).parameters.values():
            if parameter.default is not inspect._empty:
                try:
                    if math.isnan(parameter.default):
                        return []
                except TypeError:
                    pass

        return f(*args, **kwargs)
    return _wrapper


def genhash(input: str,
            length: int = 10,
            hash_function: Callable = hashlib.sha256) -> str:
    """Generate a truncated URL-safe string hash."""
    _hash = hash_function(input.encode('utf-8')).hexdigest()
    return _hash[:length]


def vocabs_lookup(vocab: Graph,
                  value: str | Literal,
                  predicate: URIRef = RDFS.label) -> URIRef:
    """Reverse URI lookup from vocabs."""
    value = Literal(value) if isinstance(value, str) else value

    return next(
        vocab.subjects(
            RDFS.label,
            value
        )
    )


## deprecated
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


def mkuri(
        hash_value: str | None = None,
        length: int | None = 10,
        hash_function: Callable = hashlib.sha256
) -> URIRef:
    """Create a CLSCor entity URI.

    If a hash value is give, the path is generated using
    a hash function, else the path is generated using a uuid4.
    """
    _base_uri: str = "https://clscor.io/entity/"
    _path: str = (
        str(uuid4()) if hash_value is None
        else genhash(
                hash_value,
                length=length,
                hash_function=hash_function
        )
    )

    return URIRef(f"{_base_uri}{_path[:length]}")


def uri_ns(*names: str | tuple[str, str]) -> SimpleNamespace:
    """Generate a Namespace mapping for names and computed URIs."""
    def _uris():
        for name in names:
            match name:
                case str():
                    yield name, mkuri()
                case tuple():
                    yield name[0], mkuri(name[1])
                case _:
                    raise Exception(
                        "Args must be of type str | tuple[str, str]."
                    )

    return SimpleNamespace(**dict(_uris()))
