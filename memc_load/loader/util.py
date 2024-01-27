import os
from pathlib import Path


def dot_rename(path: Path) -> None:
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, '.' + fn))
