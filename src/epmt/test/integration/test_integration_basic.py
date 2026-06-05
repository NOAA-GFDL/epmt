"""Integration tests for basic epmt operations (version, start, stop, submit).

Translated from 001-basic.bats.
"""
import os
import re
import shutil
import subprocess
import tempfile
import pytest

from conftest import run_cmd, epmt_python_setting


@pytest.fixture(autouse=True)
def setup_and_teardown(resource_path, epmt_output_prefix):
    """Setup and teardown matching the original bats setup()/teardown()."""
    jobs_in_module = "692500 804280 685000"
    # Clean output prefix dirs used by start/stop tests
    for d in ["1", "2"]:
        p = os.path.join(epmt_output_prefix, d)
        if os.path.isdir(p):
            shutil.rmtree(p)
    yield
    # Teardown: remove jobs and clean up
    run_cmd(f"epmt dump {jobs_in_module}")
    run_cmd(f"epmt delete {jobs_in_module}")
    for d in ["1", "2"]:
        p = os.path.join(epmt_output_prefix, d)
        if os.path.isdir(p):
            shutil.rmtree(p)


class TestBasic:
    def test_epmt_version(self):
        """epmt -V should print version in format 'EPMT X.Y.Z' or 'EPMT X.Y.Z.post'."""
        result = run_cmd("epmt -V")
        assert re.match(r"^EPMT \d+\.\d+\.\d+(\.post)?$", result.stdout.strip()), (
            f"Unexpected version output: {result.stdout!r}"
        )

    def test_epmt_start(self, epmt_output_prefix):
        """epmt start with SLURM_JOB_ID=1."""
        env = {"SLURM_JOB_ID": "1"}
        # First start with -e should succeed
        r = run_cmd("epmt start -e", env=env)
        assert r.returncode == 0, f"epmt start -e failed: {r.stderr}"
        # Second start with -e should fail (already started)
        r = run_cmd("epmt start -e", env=env)
        assert r.returncode != 0, "epmt start -e should fail on duplicate"
        # Start without -e should succeed (no error on existing)
        r = run_cmd("epmt start", env=env)
        assert r.returncode == 0, f"epmt start failed: {r.stderr}"

    def test_epmt_stop(self, epmt_output_prefix):
        """epmt stop with SLURM_JOB_ID=2."""
        env = {"SLURM_JOB_ID": "2"}
        r = run_cmd("epmt start -e", env=env)
        assert r.returncode == 0, f"epmt start -e failed: {r.stderr}"
        r = run_cmd("epmt stop -e", env=env)
        assert r.returncode == 0, f"epmt stop -e failed: {r.stderr}"
        # Second stop with -e should fail
        r = run_cmd("epmt stop -e", env=env)
        assert r.returncode != 0, "epmt stop -e should fail on duplicate"
        # Stop without -e should succeed
        r = run_cmd("epmt stop", env=env)
        assert r.returncode == 0, f"epmt stop failed: {r.stderr}"

    def test_epmt_submit(self, resource_path):
        """epmt submit a tgz file."""
        r = run_cmd(f"epmt submit {resource_path}/test/data/submit/692500.tgz")
        assert r.returncode == 0, f"epmt submit failed: {r.stderr}"
        assert "Imported successfully - job: 692500 processes: 6486" in r.stdout

    def test_epmt_submit_dir(self, resource_path):
        """epmt submit from an extracted directory."""
        tmp_job_dir = tempfile.mkdtemp()
        try:
            run_cmd(f"tar zxvf {resource_path}/test/data/submit/804280.tgz -C {tmp_job_dir}")
            r = run_cmd(f"epmt submit {tmp_job_dir}/")
            assert r.returncode == 0, f"epmt submit dir failed: {r.stderr}"
            assert "Imported successfully - job: 804280 processes: 6039" in r.stdout
        finally:
            shutil.rmtree(tmp_job_dir, ignore_errors=True)

    def test_epmt_submit_duplicate_error(self, resource_path):
        """epmt submit -e should fail on duplicate job."""
        tgz = f"{resource_path}/test/data/submit/692500.tgz"
        r = run_cmd(f"epmt -v submit -e {tgz} {tgz}")
        assert r.returncode != 0, "epmt submit -e should fail on duplicate"
        assert "job 692500 is already in database" in r.stdout + r.stderr
