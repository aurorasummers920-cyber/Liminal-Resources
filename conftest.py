"""
conftest.py — Patches module-level side effects in reconcile_1997 so tests
can import it without needing /var/log write access.
"""
import os
import sys
import tempfile
from unittest import mock

# Patch the LOG_FILE to a temp location before reconcile_1997 is imported
_tmp_log = os.path.join(tempfile.gettempdir(), "test_app.log")
os.environ.setdefault("LOG_FILE_OVERRIDE", _tmp_log)

# We need to patch the module-level constant before import.
# The simplest way: create the file path early so the RotatingFileHandler succeeds.
# But LOG_FILE is set before we can override it, so we mock the handler instead.

import logging.handlers as _lh

_orig_rfh_init = _lh.RotatingFileHandler.__init__


def _patched_rfh_init(self, filename, *args, **kwargs):
    """Redirect /var/log/app.log to a temp file for tests."""
    if filename == "/var/log/app.log":
        filename = _tmp_log
    _orig_rfh_init(self, filename, *args, **kwargs)


_lh.RotatingFileHandler.__init__ = _patched_rfh_init
