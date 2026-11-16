"""Load the `[transform]` section's code into a function the loop calls on
every batch.

The code runs once, at load, as a module of its own; it must define
`transform(batch: pa.Table) -> pa.Table`. See the README's *Transform*.
"""

import sys
import types
from collections.abc import Callable

import pyarrow as pa
from loguru import logger

from tkati_node_el.settings import TransformSettings

type Transform = Callable[[pa.Table], pa.Table]

# Tracebacks and syntax errors from the code name this as its file.
_FILENAME = "<settings: transform.code>"


def load_transform(settings: TransformSettings) -> Transform:
    """Run `settings.code` and return its `transform`, wrapped to check what it
    returns. Anything wrong with the code raises here, at startup."""
    module = types.ModuleType("tkati_transform")
    module.__file__ = _FILENAME
    # Lets module-introspecting code in it (`dataclasses`, pickling) find it.
    sys.modules[module.__name__] = module
    exec(compile(settings.code, _FILENAME, "exec"), module.__dict__)
    fn = module.__dict__.get("transform")
    if not callable(fn):
        raise TypeError("transform.code must define a function `transform(batch)`")
    return _checked(fn)


def _checked(fn: Callable[[pa.Table], object]) -> Transform:
    """`fn`, raising `TypeError` on a non-table result, and logging the output
    schema on the first batch and whenever it changes."""
    last_schema: pa.Schema | None = None

    def transform(batch: pa.Table) -> pa.Table:
        nonlocal last_schema
        output = fn(batch)
        if not isinstance(output, pa.Table):
            raise TypeError(
                f"transform() must return a pyarrow.Table, got {type(output).__name__}"
            )
        schema = output.schema
        if last_schema is None:
            logger.info(f"transform output schema:\n{schema}")
        elif not schema.equals(last_schema):
            # Almost always a bug, e.g. a column inferred as `null` when a
            # batch has no values for it; the output will see it next.
            logger.warning(
                f"transform output schema changed from:\n{last_schema}\nto:\n{schema}"
            )
        last_schema = schema
        return output

    return transform
