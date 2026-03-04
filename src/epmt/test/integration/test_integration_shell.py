"""Integration tests for the epmt shell (IPython) command.

Translated from 011-shell.bats.
"""
import pytest

from conftest import run_cmd


class TestShell:
    def test_epmt_shell(self):
        """epmt shell should start an IPython session."""
        r = run_cmd("epmt shell", input="")
        output = r.stdout + r.stderr
        assert "IPython" in output
        assert "In [1]:" in output
