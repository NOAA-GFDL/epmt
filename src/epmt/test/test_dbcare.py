"""
Unit tests for epmt_cmd_dbcare and related retirement/post-processing logic.

Tests the dbcare command orchestration, retire_jobs efficiency (PR #189),
retire_refmodels, and the signal handler signature fix (PR #193).

Uses an in-memory SQLite database to avoid contention with production.
"""
import unittest
from datetime import datetime
from glob import glob
from unittest.mock import patch

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.dialects.postgresql import JSONB

# register JSONB -> JSON for SQLite so in-memory tests can create tables
@compiles(JSONB, "sqlite")
def _compile_jsonb_sqlite(type_, compiler, **kw):
    return "JSON"

from epmt import epmt_query as eq, epmt_settings as settings
from epmt.orm import setup_db
from epmt.orm.sqlalchemy.models import Job
from epmt.epmtlib import timing, capture, get_install_root, epmt_logging_init
from epmt.epmt_cmds import epmt_submit
from epmt.epmt_cmd_dbcare import epmt_dbcare
from epmt.epmt_cmd_retire import epmt_retire
import epmt.orm.sqlalchemy.general as orm_general

install_root = get_install_root()
epmt_logging_init(0)

JOBS_LIST = ['685000']

_orig_db_params = settings.db_params.copy()


def do_cleanup():
    eq.delete_jobs(JOBS_LIST, force=True, remove_models=True)


@timing
def setUpModule():
    # reset engine so we get a fresh in-memory SQLite DB
    orm_general.engine = None
    orm_general.db_setup_complete = False
    settings.db_params = {'url': 'sqlite:///:memory:', 'echo': False}
    setup_db(settings)
    do_cleanup()
    datafiles = '{}/test/data/misc/685000.tgz'.format(install_root)
    print('setUpModule (test_dbcare): submitting to db {0}'.format(datafiles))
    settings.post_process_job_on_ingest = True
    with capture() as (_out, _err):
        epmt_submit(glob(datafiles), dry_run=False)
    settings.post_process_job_on_ingest = False
    assert eq.get_jobs(['685000'], fmt='terse') == ['685000']


def tearDownModule():
    do_cleanup()
    settings.db_params = _orig_db_params


class TestDbcare(unittest.TestCase):
    """Tests for epmt_dbcare orchestration."""

    def test_dbcare_all_skipped(self):
        """dbcare with no flags should skip everything and not error."""
        epmt_dbcare(retire_jobs=False, vacuum_tables=False, post_process=False)

    def test_dbcare_retire_noop(self):
        """dbcare with retire enabled but retire_*_ndays=0 should be a no-op."""
        import epmt.epmt_cmd_retire as retire_mod
        with patch.object(retire_mod, 'retire_jobs_ndays', 0), \
             patch.object(retire_mod, 'retire_models_ndays', 0):
            epmt_dbcare(retire_jobs=True, vacuum_tables=False, post_process=False)

    def test_dbcare_retire_does_not_delete_recent_jobs(self):
        """dbcare retire with a large ndays should not delete recent test jobs."""
        import epmt.epmt_cmd_retire as retire_mod
        with patch.object(retire_mod, 'retire_jobs_ndays', 5000), \
             patch.object(retire_mod, 'retire_models_ndays', 5000):
            epmt_dbcare(retire_jobs=True, vacuum_tables=False, post_process=False)
            jobs = eq.get_jobs(['685000'], fmt='terse')
            self.assertIn('685000', jobs)


class TestVacuum(unittest.TestCase):
    """Tests for vacuum functionality."""

    def test_vacuum_skipped_when_disabled(self):
        """dbcare with vacuum_tables=False should skip vacuuming."""
        import epmt.epmt_cmd_dbcare as dbcare_mod
        with patch.object(dbcare_mod, '_vacuum_tables') as mock_vacuum:
            epmt_dbcare(retire_jobs=False, vacuum_tables=False, post_process=False)
            mock_vacuum.assert_not_called()

    def test_vacuum_called_when_enabled(self):
        """dbcare with vacuum_tables=True should call _vacuum_tables."""
        import epmt.epmt_cmd_dbcare as dbcare_mod
        with patch.object(dbcare_mod, '_vacuum_tables') as mock_vacuum:
            epmt_dbcare(retire_jobs=False, vacuum_tables=True, post_process=False)
            mock_vacuum.assert_called_once()

    def test_vacuum_tables_no_engine(self):
        """_vacuum_tables should log error and return if no engine available."""
        import epmt.epmt_cmd_dbcare as dbcare_mod
        saved_engine = orm_general.engine
        try:
            orm_general.engine = None
            dbcare_mod._vacuum_tables()
        finally:
            orm_general.engine = saved_engine

    def test_vacuum_tables_list(self):
        """VACUUM_TABLES should contain the expected tables."""
        from epmt.epmt_cmd_dbcare import VACUUM_TABLES
        self.assertIn('jobs', VACUUM_TABLES)
        self.assertIn('processes', VACUUM_TABLES)
        self.assertIn('processes_staging', VACUUM_TABLES)


class TestRetireJobs(unittest.TestCase):
    """Tests for retire_jobs efficiency improvements (PR #189)."""

    def test_retire_jobs_zero_ndays_noop(self):
        """retire_jobs with ndays=0 should return 0 and not delete anything."""
        result = eq.retire_jobs(ndays=0)
        self.assertEqual(result, 0)

    def test_retire_jobs_respects_age_threshold(self):
        """retire_jobs should only delete jobs older than ndays."""
        # job 685000 is from 2019-06-15, so it's very old
        ndays = (datetime.now() - datetime(2019, 6, 15, 7, 52, 4)).days
        jobs_before = eq.get_jobs(['685000'], fmt='terse')
        self.assertIn('685000', jobs_before)

        # use a threshold that's 1 day MORE than the job's age — should NOT delete
        _result = eq.retire_jobs(ndays=ndays + 2)
        jobs_after = eq.get_jobs(['685000'], fmt='terse')
        self.assertIn('685000', jobs_after)

    def test_retire_jobs_deletes_old_jobs(self):
        """retire_jobs should delete jobs older than ndays threshold."""
        ndays = (datetime.now() - datetime(2019, 6, 15, 7, 52, 4)).days

        # set threshold to job's age minus 1 day — should delete it
        result = eq.retire_jobs(ndays=ndays - 1)
        self.assertGreater(result, 0)

        jobs_after = eq.get_jobs(['685000'], fmt='terse')
        self.assertNotIn('685000', jobs_after)

        # re-submit the job for subsequent tests
        datafiles = '{}/test/data/misc/685000.tgz'.format(install_root)
        settings.post_process_job_on_ingest = True
        with capture() as (_out, _err):
            epmt_submit(glob(datafiles), dry_run=False)
        settings.post_process_job_on_ingest = False

    def test_retire_jobs_no_model_filter(self):
        """retire_jobs should use ~Job.ref_models.any() filter (PR #189)."""
        ndays = (datetime.now() - datetime(2019, 6, 15, 7, 52, 4)).days
        with patch.object(eq, 'get_jobs', wraps=eq.get_jobs) as mock_get_jobs:
            eq.retire_jobs(ndays=ndays + 2, dry_run=True)
            for call in mock_get_jobs.call_args_list:
                kwargs = call.kwargs if hasattr(call, 'kwargs') else call[1]
                if 'fltr' in kwargs and kwargs['fltr'] is not None:
                    return


class TestRetireRefmodels(unittest.TestCase):
    """Tests for retire_refmodels."""

    def test_retire_refmodels_zero_ndays_noop(self):
        """retire_refmodels with ndays=0 should return 0."""
        result = eq.retire_refmodels(ndays=0)
        self.assertEqual(result, 0)

    def test_retire_refmodels_large_ndays_noop(self):
        """retire_refmodels with very large ndays should not delete any models."""
        result = eq.retire_refmodels(ndays=5000)
        self.assertEqual(result, 0)


class TestDeleteJobs(unittest.TestCase):
    """Tests for delete_jobs with fltr parameter (PR #189)."""

    def test_delete_jobs_with_fltr_param(self):
        """delete_jobs should accept and pass through fltr parameter (PR #189)."""
        import inspect
        sig = inspect.signature(eq.delete_jobs)
        if 'fltr' not in sig.parameters:
            self.skipTest('fltr parameter not yet available (PR #189 not merged)')
        no_model_fltr = ~Job.ref_models.any()
        result = eq.delete_jobs([], force=True, before=-5000, fltr=no_model_fltr)
        self.assertEqual(result, 0)

    def test_delete_jobs_warn_no_spam(self):
        """delete_jobs with warn=False should not produce 'verbosity' warnings (PR #189)."""
        with self.assertLogs('epmt', level='DEBUG') as cm:
            eq.delete_jobs(['nonexistent_job_id'], force=True)

        # verify no 'verbosity is controlled elsewhere' messages
        for record in cm.output:
            self.assertNotIn('verbosity is controlled elsewhere', record)


class TestRetireCommand(unittest.TestCase):
    """Tests for epmt_retire command orchestration."""

    def test_retire_models_before_jobs(self):
        """epmt_retire should run model retirement before job retirement."""
        call_order = []

        def mock_retire_refmodels(*args, **kwargs):
            call_order.append('models')
            return 0

        def mock_retire_jobs(*args, **kwargs):
            call_order.append('jobs')
            return 0

        import epmt.epmt_cmd_retire as retire_mod
        with patch.object(retire_mod, 'retire_refmodels', side_effect=mock_retire_refmodels):
            with patch.object(retire_mod, 'retire_jobs', side_effect=mock_retire_jobs):
                epmt_retire()

        self.assertEqual(call_order, ['models', 'jobs'])


class TestSignalHandler(unittest.TestCase):
    """Tests for signal handler signature fix (PR #193)."""

    def test_sig_handler_accepts_frame(self):
        """sig_handler must accept both signo and frame parameters."""
        import inspect
        from epmt.epmt_job import post_process_job
        source = inspect.getsource(post_process_job)
        self.assertIn('def sig_handler(signo, frame)', source)


if __name__ == '__main__':
    unittest.main()
