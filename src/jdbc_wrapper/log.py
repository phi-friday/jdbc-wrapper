from __future__ import annotations

import sys

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


def dummy() -> None:  # noqa: D103
    tomllib.loads("")
