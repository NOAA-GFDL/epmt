"""Integration tests for escape character handling in epmt workloads.

Translated from 002-escape.bats.
"""
import os
import platform
import pytest

from conftest import run_cmd, epmt_setting


@pytest.fixture(autouse=True, scope="class")
def setup_and_teardown():
    """Setup and teardown matching the original bats setup()/teardown()."""
    jobs_in_module = "12340"
    run_cmd(f"epmt delete {jobs_in_module}")
    yield
    run_cmd(f"epmt delete {jobs_in_module}")


class TestEscape:
    def test_epmt_start_run_stop_submit_with_escape_char(self):
        """Start/run/stop/submit cycle with escape characters in workload."""
        papiex_path = epmt_setting("install_prefix")

        # Run the workload via an external bash script to avoid
        # Python/bash quoting issues with escape characters.
        script_dir = os.path.dirname(os.path.abspath(__file__))
        script = os.path.join(script_dir, "epmt-escape-workload.sh")
        r = run_cmd(f"bash {script}")
        # On Linux with papiex, we expect 18 processes; otherwise 0
        if platform.system() == "Linux" and papiex_path and os.path.isfile(
            f"{papiex_path}/lib/libpapiex.so"
        ):
            assert "Imported successfully - job: 12340 processes: 18" in r.stdout
        else:
            assert "Imported successfully - job: 12340 processes: 0" in r.stdout

    def test_check_for_job_with_escape_char_if_persistent_db(self):
        """Check escape chars in persistent DB (postgres only)."""
        db_params_result = run_cmd("epmt -h | grep db_params: | cut -f2- -d:")
        db_params = db_params_result.stdout.strip()
        if "postgres" not in db_params:
            pytest.skip("Test requires postgres DB")
        r = run_cmd("epmt list 12340")
        assert "['12340']" in r.stdout
        r = run_cmd("epmt dump 12340")
        assert r.returncode == 0
