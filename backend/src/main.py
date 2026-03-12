"""Main entry point for the IJM backend.

The actual application is built in ``app.py``.  This module re-exports
the ``app`` instance so that ``uvicorn src.main:app`` continues to work.
"""

from src.app import app  # noqa: F401
