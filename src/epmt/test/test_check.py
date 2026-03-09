"""Tests for epmt_check verify functions.

Each test maps to one verification function called by epmt_check().
Tests assert a definite expected result; pytest.skip is used when
environment preconditions (e.g. papiex libs installed) are not met.

The sole exception is test_verify_papiex_options — see its inline
comment for why it accepts either True or False.
"""

import os
import pytest

from epmt.epmtlib import capture
import epmt.epmt_settings as settings
from epmt.orm import setup_db


def _papiex_libs_present():
    """Return True if the papiex shared libs are installed under settings.install_prefix."""
    return all(
        os.path.exists(settings.install_prefix + lib)
        for lib in ["/lib/libpapiex.so", "/lib/libmonitor.so"]
    )


@pytest.fixture(autouse=True, scope="module")
def init_db():
    """Initialize the in-memory database once for all tests in this module."""
    setup_db(settings)


def test_verify_db_params():
    from epmt.epmt_cmds import verify_db_params
    with capture() as (out, err):
        result = verify_db_params()
    assert result is True


def test_verify_install_prefix():
    if not _papiex_libs_present():
        pytest.skip("papiex libs not installed")
    from epmt.epmt_cmds import verify_install_prefix
    with capture() as (out, err):
        result = verify_install_prefix()
    assert result is True


def test_verify_epmt_output_prefix():
    from epmt.epmt_cmds import verify_epmt_output_prefix
    with capture() as (out, err):
        result = verify_epmt_output_prefix()
    assert result is True


def test_verify_perf():
    from epmt.epmt_cmds import verify_perf
    with capture() as (out, err):
        result = verify_perf()
    assert result is True


def test_verify_papiex_options():
    # verify_papiex_options checks that PAPI's perf_event component is active
    # and that configured events can be resolved via papi_command_line.
    # This requires hardware counter access via perf_event_open(), which is
    # restricted in VM/container environments (e.g. GitHub Actions VMs) even
    # with --privileged and perf_event_paranoid=2.  On bare-metal HPC (PPAN),
    # this check passes.  Because the result depends on the hypervisor rather
    # than the software installation, we accept either True or False here.
    # This is the ONLY test with such flexibility — the corresponding failure
    # is guarded in epmt_check() and does not affect its return value.
    if not _papiex_libs_present():
        pytest.skip("papiex libs not installed")
    from epmt.epmt_cmds import verify_papiex_options
    with capture() as (out, err):
        result = verify_papiex_options()
    assert isinstance(result, bool)


def test_verify_stage_command():
    from epmt.epmt_cmds import verify_stage_command
    with capture() as (out, err):
        result = verify_stage_command()
    assert result is True


def test_verify_papiex(monkeypatch):
    if not _papiex_libs_present():
        pytest.skip("papiex libs not installed")
    from epmt.epmt_cmds import verify_papiex
    # The CLI (epmt check) sets SLURM_JOB_ID='1' so that verify_papiex can
    # call epmt_run without a real SLURM/PBS job context (see epmt_cmds.py
    # line 1786).  Mirror that here.
    monkeypatch.setenv('SLURM_JOB_ID', '1')
    with capture() as (out, err):
        result = verify_papiex()
    assert result is True


def test_epmt_check(monkeypatch):
    if not _papiex_libs_present():
        pytest.skip("papiex libs not installed")
    from epmt.epmt_cmds import epmt_check
    # The CLI (epmt check) sets SLURM_JOB_ID='1' before calling epmt_check()
    # (see epmt_cmds.py line 1786).  Mirror that here so verify_papiex can
    # succeed without a real job context.
    monkeypatch.setenv('SLURM_JOB_ID', '1')
    with capture() as (out, err):
        result = epmt_check()
    assert result is True
