"""Integration tests for escape character handling in epmt workloads.

Translated from 002-escape.bats.
"""
import os
import platform
import pytest

from conftest import run_cmd, epmt_setting


@pytest.fixture(autouse=True)
def setup_and_teardown():
    """Setup and teardown matching the original bats setup()/teardown()."""
    jobs_in_module = "12340"
    run_cmd(f"epmt delete {jobs_in_module}")
    yield
    run_cmd(f"epmt delete {jobs_in_module}")


def _workload_script():
    """Return the inline bash workload with escape characters."""
    return r"""
cut -d\" -f2 < /dev/null
/bin/echo '\\\'
/bin/echo \ b
/bin/echo \\
/bin/echo ,
/bin/echo \'
/bin/echo -e "\tHello"
/bin/echo -e "\tThereU\nR"
/bin/echo -e \\\a
/bin/echo -e "\a"
/bin/echo -e \\
/bin/echo -e 'some test \b and more text'
/bin/echo \b
/bin/echo \\b
/bin/echo '\b'
/bin/echo -e '\. some text'
/bin/echo -e 'try\.some more text'
sed 's/^\.//' < /dev/null
"""


class TestEscape:
    def test_epmt_start_run_stop_submit_with_escape_char(self):
        """Start/run/stop/submit cycle with escape characters in workload."""
        db_params = run_cmd("epmt -h | grep db_params: | cut -f2- -d:")
        papiex_path = epmt_setting("install_prefix")

        env = {"SLURM_JOB_ID": "12340", "SLURM_JOB_NAME": "12340_name"}
        run_cmd("epmt start", env=env, check=True)
        # Source the environment
        source_result = run_cmd("epmt source", env=env)
        # Build a script that sources the env, runs the workload, then cleans up
        script = f"""
export SLURM_JOB_ID=12340
export SLURM_JOB_NAME=12340_name
epmt start
eval `epmt source`
{_workload_script()}
epmt_uninstrument
epmt stop
epmt submit --remove
"""
        r = run_cmd(f"bash -c {repr(script)}")
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
