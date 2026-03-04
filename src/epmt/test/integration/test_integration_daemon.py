"""Integration tests for the epmt daemon (start/stop/status).

Translated from 010-daemon.bats.
"""
import os
import time
import pytest

from conftest import run_cmd, epmt_setting, epmt_python_setting


def _stop_daemon():
    """Try to stop the daemon, retrying for a few seconds."""
    for _ in range(5):
        r = run_cmd("epmt daemon --stop")
        if r.returncode == 0:
            return
        time.sleep(1)


@pytest.fixture(autouse=True)
def setup_and_teardown():
    """Ensure no daemon is running before/after tests."""
    yield
    _stop_daemon()


class TestDaemon:
    def test_no_daemon_running(self):
        """Initially no daemon should be running."""
        unprocessed = epmt_python_setting(
            "from epmt import epmt_query as eq; print(eq.get_unprocessed_jobs())"
        )
        assert unprocessed == "[]"
        r = run_cmd("epmt daemon")
        assert "EPMT daemon not running" in r.stdout + r.stderr

    def test_start_epmt_daemon(self):
        """Start the daemon and verify it is running."""
        unprocessed = epmt_python_setting(
            "from epmt import epmt_query as eq; print(eq.get_unprocessed_jobs())"
        )
        assert unprocessed == "[]"
        run_cmd("epmt -v daemon --start")
        r = run_cmd("epmt daemon")
        assert "EPMT daemon running PID" in r.stdout + r.stderr
        logfile = epmt_setting("logfile")
        time.sleep(1)
        if logfile:
            r = run_cmd(f"grep 'starting daemon loop' {logfile}")
            assert r.returncode == 0

    def test_stop_epmt_daemon(self):
        """Stop a running daemon."""
        unprocessed = epmt_python_setting(
            "from epmt import epmt_query as eq; print(eq.get_unprocessed_jobs())"
        )
        assert unprocessed == "[]"
        # Ensure daemon is running first
        run_cmd("epmt -v daemon --start")
        r = run_cmd("epmt daemon")
        assert "EPMT daemon running PID" in r.stdout + r.stderr
        # Stop it
        r = run_cmd("epmt daemon --stop")
        assert "Sending signal to EPMT daemon pid" in r.stdout + r.stderr
        logfile = epmt_setting("logfile")
        if logfile and os.path.exists(logfile):
            os.remove(logfile)
