"""
Shared fixtures and helpers for EPMT integration tests.

These tests exercise the epmt CLI by invoking it as a subprocess,
mirroring what the original bats-based integration tests did.
"""
import os
import subprocess
import shutil
import pytest


def run_cmd(cmd, env=None, check=False, input=None):
    """Run a shell command and return a CompletedProcess.

    Parameters
    ----------
    cmd : str
        Shell command string to execute.
    env : dict, optional
        Environment variables (merged with os.environ).
    check : bool
        If True, raise on non-zero exit code.
    input : str, optional
        Stdin input to pass to the process.

    Returns
    -------
    subprocess.CompletedProcess
    """
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    return subprocess.run(
        cmd,
        shell=True,
        capture_output=True,
        text=True,
        env=merged_env,
        check=check,
        input=input,
    )


def epmt_setting(key):
    """Retrieve an epmt setting by parsing ``epmt -h`` output.

    The ``epmt -h`` output contains lines like ``key:value``.
    """
    result = run_cmd("epmt -h")
    for line in result.stdout.splitlines():
        if line.strip().startswith(key + ":"):
            return line.split(":", 1)[1].strip()
    return None


def epmt_python_setting(expr):
    """Evaluate a Python expression that reads an epmt setting."""
    result = run_cmd(f"python3 -c '{expr}'")
    return result.stdout.strip()


@pytest.fixture
def resource_path():
    """Return the path to src/epmt (used to locate test data)."""
    path = os.path.join(os.getcwd(), "src", "epmt")
    assert os.path.isdir(path), f"resource_path {path} does not exist"
    return path


@pytest.fixture
def epmt_output_prefix():
    """Return the epmt_output_prefix/<user> directory."""
    prefix = epmt_python_setting(
        "import epmt.epmt_settings as settings; print(settings.epmt_output_prefix);"
    )
    assert prefix, "epmt_output_prefix is empty"
    return os.path.join(prefix, os.environ.get("USER", "root"))


@pytest.fixture
def stage_dest():
    """Return the stage_command_dest directory."""
    dest = epmt_setting("stage_command_dest")
    assert dest, "stage_command_dest is empty"
    assert os.path.isdir(dest), f"stage_command_dest {dest} does not exist"
    return dest
