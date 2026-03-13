"""Integration tests for the epmt shell (IPython) command.

Translated from 011-shell.bats.
"""
import pytest

from conftest import run_cmd


def _ipython_available():
    """Return True if IPython can be imported."""
    try:
        import IPython  # noqa: F401  # pylint: disable=import-outside-toplevel,unused-import
        return True
    except ImportError:
        return False


class TestShell:
    @pytest.mark.skipif(not _ipython_available(), reason="IPython not installed")
    def test_epmt_shell(self):
        """epmt shell should start an IPython session."""
        r = run_cmd("epmt shell", input="")
        output = r.stdout + r.stderr
        assert "IPython" in output
        assert "In [1]:" in output
