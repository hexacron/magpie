"""magpie - self-hosted capture of public X/Twitter posts into hashed evidence packages.

Kept deliberately light: importing the package must not pull in fastapi,
playwright or httpx. Import the submodule you need.
"""

from __future__ import annotations

from .models import TOOL_NAME, TOOL_VERSION

__all__ = ["TOOL_NAME", "TOOL_VERSION", "__version__"]

__version__ = TOOL_VERSION
