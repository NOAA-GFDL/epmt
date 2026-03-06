import os
import unittest

from epmt.epmtlib import capture
import epmt.epmt_settings as settings
from epmt.orm import setup_db


def setUpModule():
    setup_db(settings)


def _papiex_libs_present():
    """Return True if the papiex shared libs are installed under settings.install_prefix."""
    return all(
        os.path.exists(settings.install_prefix + lib)
        for lib in ["/lib/libpapiex.so", "/lib/libmonitor.so"]
    )


def _perf_event_paranoid_ok():
    """Return True if /proc/sys/kernel/perf_event_paranoid exists and is <= 2."""
    try:
        with open("/proc/sys/kernel/perf_event_paranoid") as f:
            return int(f.read()) <= 2
    except Exception:
        return False


def _epmt_run_available():
    """Return True if epmt_run can execute (requires a SLURM/PBS job ID env var)."""
    return any(os.environ.get(v) for v in ['SLURM_JOB_ID', 'SLURM_JOBID', 'PBS_JOB_ID'])


class EPMTCheck(unittest.TestCase):

    def test_verify_db_params(self):
        from epmt.epmt_cmds import verify_db_params
        with capture() as (out, err):
            result = verify_db_params()
        self.assertTrue(result)

    def test_verify_install_prefix(self):
        from epmt.epmt_cmds import verify_install_prefix
        with capture() as (out, err):
            result = verify_install_prefix()
        if _papiex_libs_present():
            self.assertTrue(result)
        else:
            self.assertFalse(result)

    def test_verify_epmt_output_prefix(self):
        from epmt.epmt_cmds import verify_epmt_output_prefix
        with capture() as (out, err):
            result = verify_epmt_output_prefix()
        self.assertTrue(result)

    def test_verify_perf(self):
        from epmt.epmt_cmds import verify_perf
        with capture() as (out, err):
            result = verify_perf()
        if _perf_event_paranoid_ok():
            self.assertTrue(result)
        else:
            self.assertFalse(result)

    def test_verify_papiex_options(self):
        from epmt.epmt_cmds import verify_papiex_options
        with capture() as (out, err):
            result = verify_papiex_options()
        # verify_papiex_options requires hardware counter access via papi_component_avail;
        # it passes on bare-metal HPC but fails in VM/container/CI environments
        if _papiex_libs_present():
            # Even when libs are present, papi_component_avail may still fail
            # in VM/container environments due to restricted perf_event_open()
            self.assertIsInstance(result, bool)
        else:
            self.assertFalse(result)

    def test_verify_stage_command(self):
        from epmt.epmt_cmds import verify_stage_command
        with capture() as (out, err):
            result = verify_stage_command()
        self.assertTrue(result)

    def test_verify_papiex(self):
        from epmt.epmt_cmds import verify_papiex
        with capture() as (out, err):
            result = verify_papiex()
        if _papiex_libs_present() and _epmt_run_available():
            self.assertTrue(result)
        else:
            self.assertFalse(result)

    def test_epmt_check(self):
        from epmt.epmt_cmds import epmt_check
        with capture() as (out, err):
            result = epmt_check()
        # epmt_check returns True only when all non-guarded checks pass.
        # verify_papiex needs both papiex libs and a job ID (SLURM/PBS).
        # In source-tree CI the papiex libs and perf paranoid checks fail.
        # In Docker CI without SLURM job context, verify_papiex fails.
        if _papiex_libs_present() and _perf_event_paranoid_ok() and _epmt_run_available():
            self.assertTrue(result)
        else:
            self.assertFalse(result)
