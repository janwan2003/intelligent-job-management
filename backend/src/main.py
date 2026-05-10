"""Main entry point for the IJM backend.

The actual application is built in ``app.py``.  This module re-exports
the ``app`` instance so that ``uvicorn src.main:app`` continues to work.
"""

import logging
import os

# Ensure ``src.*`` loggers emit at INFO by default.  Without this, uvicorn's
# default config attaches handlers only to its own loggers, so our
# ``logger.info(...)`` calls in ``src.app`` (e.g. "Optimizer preempting job",
# "Waiting for N preempted container(s) to stop") are silently dropped — which
# made diagnosing the auto-preempt timing impossible without running pdb.
_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=_LEVEL,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

from src.app import app  # noqa: E402, F401
