"""Integration tests for SLURM integration (sbatch/srun prolog/epilog).

Translated from 025-slurm.bats.

NOTE: These tests require a SLURM cluster with sinfo/srun/sbatch available
and the SLURM controller running.  They are automatically skipped if SLURM
commands are not found or the controller is unreachable.
"""
import os
import time
import pytest

from conftest import run_cmd, epmt_setting


def _slurm_available():
    """Check if SLURM commands are available and the controller is reachable.

    The Docker release image has SLURM binaries installed (from the
    slurm-cluster base image) but does not necessarily start the SLURM
    daemons.  We therefore verify both that the commands exist *and* that
    ``sinfo`` can actually contact the controller.
    """
    for cmd in ("sinfo", "srun", "sbatch"):
        r = run_cmd(f"command -v {cmd}")
        if r.returncode != 0:
            return False
    # Verify the SLURM controller is reachable (not just installed)
    r = run_cmd("sinfo -N --noheader")
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


def _find_examples_dir():
    """Find directory containing epmt-example.{sh,bash,csh,tcsh} scripts.

    Checks the pip-installed package path first, then the source tree.
    """
    # Pip-installed package: epmt/test/shell/
    try:
        import epmt as _epmt  # pylint: disable=import-outside-toplevel
        candidate = os.path.join(os.path.dirname(_epmt.__file__), "test", "shell")
        if os.path.isdir(candidate) and os.path.exists(
            os.path.join(candidate, "epmt-example.sh")
        ):
            return candidate
    except ImportError:
        pass

    # Source tree: src/epmt/test/shell/
    candidate = os.path.join(os.getcwd(), "src", "epmt", "test", "shell")
    if os.path.isdir(candidate):
        return candidate

    return None


def _create_slurm_prolog(path):
    """Create a working SLURM task prolog script that discovers epmt via PATH.

    Mirrors utils/SLURM/slurm_task_prolog_epmt.sh but uses ``command -v epmt``
    instead of a hardcoded install path so it works in pip-installed environments.
    """
    with open(path, "w", encoding="utf-8") as f:
        f.write(
            '#!/bin/bash\n'
            'err_report() { echo "print $0: Error at line $1"; exit 0; }\n'
            "trap 'err_report $LINENO' ERR\n"
            'EPMT=$(command -v epmt)\n'
            'if [[ -f "$EPMT" ]] && [[ -x "$EPMT" ]]; then\n'
            '    if [[ ! -z "$SLURM_LOCALID" ]] && '
            '[[ "$SLURM_LOCALID" == "0" ]]; then\n'
            '        $EPMT start\n'
            '    fi\n'
            '    $EPMT source --slurm\n'
            'fi\n'
        )
    os.chmod(path, 0o755)


def _create_slurm_epilog(path):
    """Create a working SLURM task epilog script that discovers epmt via PATH.

    Mirrors utils/SLURM/slurm_task_epilog_epmt.sh but uses ``command -v epmt``
    instead of a hardcoded install path so it works in pip-installed environments.
    """
    with open(path, "w", encoding="utf-8") as f:
        f.write(
            '#!/bin/bash\n'
            'err_report() { echo "$0: Error at line $1"; exit 0; }\n'
            "trap 'err_report $LINENO' ERR\n"
            'EPMT=$(command -v epmt)\n'
            'if [[ -f "$EPMT" ]] && [[ -x "$EPMT" ]]; then\n'
            '    if [[ ! -z "$SLURM_LOCALID" ]] && '
            '[[ "$SLURM_LOCALID" == "0" ]]; then\n'
            '        $EPMT stop\n'
            '        $EPMT stage\n'
            '    fi\n'
            'fi\n'
        )
    os.chmod(path, 0o755)


def _find_resource_path(tmp_path):
    """Find or build a resource path with examples/ and slurm/ subdirs.

    In a full release install ``dirname(which epmt)/..`` contains both
    ``examples/`` and ``slurm/``.  In a pip install (e.g. Docker release
    image) those directories do not exist next to the ``epmt`` binary, so
    we construct a temporary resource directory instead:

    * ``examples/`` is symlinked from the pip-installed package
      (``epmt/test/shell/``) which is included via package-data.
    * ``slurm/`` is populated with working prolog/epilog scripts that
      discover ``epmt`` via PATH rather than a hardcoded install path.
    """
    # 1. Full release install: dirname(which epmt)/..
    r = run_cmd("command -v epmt")
    if r.returncode == 0:
        candidate = os.path.normpath(
            os.path.join(os.path.dirname(r.stdout.strip()), "..")
        )
        if (os.path.isdir(os.path.join(candidate, "examples"))
                and os.path.isdir(os.path.join(candidate, "slurm"))):
            return candidate

    # 2. Construct from pip-installed package
    resource_dir = str(tmp_path / "resource")
    os.makedirs(resource_dir, exist_ok=True)

    # Symlink examples from the pip package (epmt/test/shell/)
    examples_src = _find_examples_dir()
    if examples_src:
        os.symlink(examples_src, os.path.join(resource_dir, "examples"))

    # Create working slurm prolog/epilog scripts
    slurm_dir = os.path.join(resource_dir, "slurm")
    os.makedirs(slurm_dir, exist_ok=True)
    _create_slurm_prolog(os.path.join(slurm_dir, "slurm_task_prolog_epmt.sh"))
    _create_slurm_epilog(os.path.join(slurm_dir, "slurm_task_epilog_epmt.sh"))

    return resource_dir


@pytest.fixture(autouse=True)
def setup_and_teardown(tmp_path):
    """Skip if no SLURM, set up test scripts."""
    if not _slurm_available():
        pytest.skip("SLURM not available")

    stage_dest = epmt_setting("stage_command_dest")
    assert stage_dest and os.path.isdir(stage_dest)

    resource_path = _find_resource_path(tmp_path)
    assert os.path.isdir(os.path.join(resource_path, "examples")), \
        "Could not find examples directory"
    assert os.path.isdir(os.path.join(resource_path, "slurm")), \
        "Could not find slurm scripts directory"

    # Create test scripts
    for shell, shebang in [
        ("tcsh", "#!/bin/tcsh"),
        ("csh", "#!/bin/csh"),
        ("bash", "#!/bin/bash"),
        ("sh", "#!/bin/sh"),
    ]:
        script = f"/tmp/sleeptest.{shell}"
        with open(script, "w", encoding="utf-8") as f:
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
