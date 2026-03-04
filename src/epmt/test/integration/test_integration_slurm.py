"""Integration tests for SLURM integration (sbatch/srun prolog/epilog).

Translated from 025-slurm.bats.

NOTE: These tests require a SLURM cluster with sinfo/srun/sbatch available.
They are automatically skipped if SLURM commands are not found.
"""
import os
import time
import shutil
import pytest

from conftest import run_cmd, epmt_setting


def _slurm_available():
    """Check if SLURM commands are available."""
    for cmd in ("sinfo", "srun", "sbatch"):
        r = run_cmd(f"command -v {cmd}")
        if r.returncode != 0:
            return False
    return True


def _verify_staged_file(stage_dest):
    """Verify that a staged tgz file was created and has expected contents."""
    wait_seconds = 10
    tgz = None
    for _ in range(wait_seconds):
        # List tgz files sorted by time
        r = run_cmd(f"ls -t {stage_dest}/*.tgz 2>/dev/null")
        if r.returncode == 0 and r.stdout.strip():
            tgz = r.stdout.strip().split("\n")[0]
            break
        time.sleep(1)
    assert tgz and os.path.getsize(tgz) > 0, "No staged tgz file found"
    # Verify contents
    r = run_cmd(f"tar tf {tgz}")
    assert "job_metadata" in r.stdout
    output = r.stdout
    has_csv = "collated-papiex-" in output and ".csv" in output
    has_tsv = "papiex.tsv" in output and "papiex-header.tsv" in output
    assert has_csv or has_tsv, f"Expected papiex data in tarball but got: {output}"
    # Cleanup
    os.remove(tgz)


@pytest.fixture(autouse=True)
def setup_and_teardown():
    """Skip if no SLURM, set up test scripts."""
    if not _slurm_available():
        pytest.skip("SLURM not available")

    stage_dest = epmt_setting("stage_command_dest")
    assert stage_dest and os.path.isdir(stage_dest)

    resource_path_result = run_cmd("dirname `command -v epmt`")
    resource_path = os.path.join(resource_path_result.stdout.strip(), "..")
    assert os.path.isdir(resource_path)

    # Create test scripts
    for shell, shebang in [
        ("tcsh", "#!/bin/tcsh"),
        ("csh", "#!/bin/csh"),
        ("bash", "#!/bin/bash"),
        ("sh", "#!/bin/sh"),
    ]:
        script = f"/tmp/sleeptest.{shell}"
        with open(script, "w") as f:
            f.write(f"{shebang}\nsleep 1\n")
        os.chmod(script, 0o755)

    yield {"stage_dest": stage_dest, "resource_path": resource_path}

    # Cleanup
    for shell in ("tcsh", "csh", "bash", "sh"):
        script = f"/tmp/sleeptest.{shell}"
        if os.path.exists(script):
            os.remove(script)


class TestSlurm:
    def test_sbatch_epmt_example_tcsh(self, setup_and_teardown):
        ctx = setup_and_teardown
        r = run_cmd(f"sbatch {ctx['resource_path']}/examples/epmt-example.tcsh")
        assert r.returncode == 0
        _verify_staged_file(ctx["stage_dest"])

    def test_sbatch_epmt_example_csh(self, setup_and_teardown):
        ctx = setup_and_teardown
        r = run_cmd(f"sbatch {ctx['resource_path']}/examples/epmt-example.csh")
        assert r.returncode == 0
        _verify_staged_file(ctx["stage_dest"])

    def test_sbatch_epmt_example_bash(self, setup_and_teardown):
        ctx = setup_and_teardown
        r = run_cmd(f"sbatch {ctx['resource_path']}/examples/epmt-example.bash")
        assert r.returncode == 0
        _verify_staged_file(ctx["stage_dest"])

    def test_sbatch_epmt_example_sh(self, setup_and_teardown):
        ctx = setup_and_teardown
        r = run_cmd(f"sbatch {ctx['resource_path']}/examples/epmt-example.sh")
        assert r.returncode == 0
        _verify_staged_file(ctx["stage_dest"])

    def test_srun_prolog_epilog_inline(self, setup_and_teardown):
        ctx = setup_and_teardown
        rp = ctx["resource_path"]
        r = run_cmd(
            f'srun -n1 --task-prolog="{rp}/slurm/slurm_task_prolog_epmt.sh" '
            f'--task-epilog="{rp}/slurm/slurm_task_epilog_epmt.sh" sleep 1'
        )
        assert r.returncode == 0
        _verify_staged_file(ctx["stage_dest"])

    def test_srun_prolog_epilog_tcsh(self, setup_and_teardown):
        ctx = setup_and_teardown
        rp = ctx["resource_path"]
        r = run_cmd(
            f'srun -n1 --task-prolog="{rp}/slurm/slurm_task_prolog_epmt.sh" '
            f'--task-epilog="{rp}/slurm/slurm_task_epilog_epmt.sh" /tmp/sleeptest.tcsh'
        )
        assert r.returncode == 0
        _verify_staged_file(ctx["stage_dest"])

    def test_srun_prolog_epilog_csh(self, setup_and_teardown):
        ctx = setup_and_teardown
        rp = ctx["resource_path"]
        r = run_cmd(
            f'srun -n1 --task-prolog="{rp}/slurm/slurm_task_prolog_epmt.sh" '
            f'--task-epilog="{rp}/slurm/slurm_task_epilog_epmt.sh" /tmp/sleeptest.csh'
        )
        assert r.returncode == 0
        _verify_staged_file(ctx["stage_dest"])

    def test_srun_prolog_epilog_bash(self, setup_and_teardown):
        ctx = setup_and_teardown
        rp = ctx["resource_path"]
        r = run_cmd(
            f'srun -n1 --task-prolog="{rp}/slurm/slurm_task_prolog_epmt.sh" '
            f'--task-epilog="{rp}/slurm/slurm_task_epilog_epmt.sh" /tmp/sleeptest.bash'
        )
        assert r.returncode == 0
        _verify_staged_file(ctx["stage_dest"])

    def test_srun_prolog_epilog_sh(self, setup_and_teardown):
        ctx = setup_and_teardown
        rp = ctx["resource_path"]
        r = run_cmd(
            f'srun -n1 --task-prolog="{rp}/slurm/slurm_task_prolog_epmt.sh" '
            f'--task-epilog="{rp}/slurm/slurm_task_epilog_epmt.sh" /tmp/sleeptest.sh'
        )
        assert r.returncode == 0
        _verify_staged_file(ctx["stage_dest"])
