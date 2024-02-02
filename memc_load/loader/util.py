import os
from itertools import chain, islice
from pathlib import Path


def dot_rename(path: Path) -> None:
    """Prepends dot to filename"""
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, '.' + fn))


def chunks_iterator(iterable, size):
    iterator = iter(iterable)
    for first in iterator:
        yield chain([first], islice(iterator, size - 1))
