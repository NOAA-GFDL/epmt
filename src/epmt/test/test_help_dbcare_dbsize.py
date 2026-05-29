"""
Unit tests for epmt_cmd_help, epmt_cmd_dbcare (post_process), and orm_db_size.

Covers previously untested lines in:
  - epmt_cmd_help.py (epmt_help_api with and without function arguments)
  - epmt_cmd_dbcare.py (post_process path)
  - epmt/orm/__init__.py (orm_db_size on non-postgres provider)
"""
import unittest
from unittest.mock import patch, MagicMock
from io import StringIO

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.dialects.postgresql import JSONB

# register JSONB -> JSON for SQLite so in-memory tests can create tables
@compiles(JSONB, "sqlite")
def _compile_jsonb_sqlite(type_, compiler, **kw):
    return "JSON"

from epmt import epmt_settings as settings
from epmt.orm import setup_db, orm_db_size
from epmt.epmtlib import capture, get_install_root, epmt_logging_init
import epmt.orm.sqlalchemy.general as orm_general

install_root = get_install_root()
epmt_logging_init(0)

_orig_db_params = settings.db_params.copy()


def setUpModule():
    # reset engine so we get a fresh in-memory SQLite DB
    orm_general.engine = None
    orm_general.db_setup_complete = False
    settings.db_params = {'url': 'sqlite:///:memory:', 'echo': False}
    setup_db(settings)


def tearDownModule():
    settings.db_params = _orig_db_params


class TestHelpApi(unittest.TestCase):
    """Test epmt_help_api covers most lines in epmt_cmd_help.py."""

    def test_help_api_no_args_prints_module_docs(self):
        """epmt_help_api() with no funcs should print module docs and index."""
        from epmt.epmt_cmd_help import epmt_help_api

        with capture() as (out, err):
            epmt_help_api(funcs=[])

        output = out.getvalue()
        # Should have printed documentation from the query and outliers modules
        self.assertTrue(len(output) > 0, "help output should not be empty")
        # The module docs should contain function listings
        self.assertIn('get_jobs', output)

    def test_help_api_with_known_function(self):
        """epmt_help_api(['get_jobs']) should print function signature and docstring."""
        from epmt.epmt_cmd_help import epmt_help_api

        with capture() as (out, err):
            epmt_help_api(funcs=['get_jobs'])

        output = out.getvalue()
        self.assertIn('get_jobs', output)
        self.assertIn('from', output)
        self.assertIn('import', output)

    def test_help_api_with_unknown_function(self):
        """epmt_help_api with unknown function prints error to stderr."""
        from epmt.epmt_cmd_help import epmt_help_api
        import sys
        from io import StringIO

        # epmt_cmd_help imports stderr at module level, so we patch it directly
        with patch('epmt.epmt_cmd_help.stderr', new_callable=StringIO) as mock_err:
            with capture() as (out, err):
                epmt_help_api(funcs=['nonexistent_function_xyz'])

        err_output = mock_err.getvalue()
        self.assertIn('Could not find function', err_output)


class TestDbcarePostProcess(unittest.TestCase):
    """Test the post_process path of epmt_dbcare."""

    def test_dbcare_post_process_zero_unprocessed(self):
        """dbcare with post_process=True should raise ValueError when 0 unprocessed jobs."""
        from epmt.epmt_cmd_dbcare import epmt_dbcare
        from epmt.orm.sqlalchemy import orm_raw_sql

        # Mock orm_raw_sql to return 0 unprocessed jobs
        with patch('epmt.epmt_cmd_dbcare.orm_raw_sql') as mock_sql:
            # First call is the count query, returns 0
            mock_sql.return_value = [[0]]
            with self.assertRaises(ValueError) as ctx:
                epmt_dbcare(retire_jobs=False, vacuum_tables=False, post_process=True)
            self.assertIn('nothing to do', str(ctx.exception))


class TestOrmDbSize(unittest.TestCase):
    """Test orm_db_size on non-postgres (sqlite) returns False."""

    def test_orm_db_size_unsupported_provider(self):
        """orm_db_size should return False on sqlite with a warning about unsupported provider."""
        result = orm_db_size()
        self.assertFalse(result)

    def test_orm_db_size_with_specific_findwhat(self):
        """orm_db_size with specific findwhat list should still return False on sqlite."""
        result = orm_db_size(findwhat=['database'], usejson=False, usebytes=True)
        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()
