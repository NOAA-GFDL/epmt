'''
tests of epmt's database migration functionality
'''

import unittest
from os import path, getcwd, chdir, remove

from epmt import epmt_settings as settings
from epmt.orm import setup_db, orm_in_memory
from epmt.epmtlib import capture, get_install_root

def setUpModule():
    setup_db(settings)


MIGRATION_HEAD = '4ae9a1cac540'


class EPMTDBMigration(unittest.TestCase):
    @unittest.skipUnless((settings.orm == 'sqlalchemy') and not (orm_in_memory()),
                         'requires sqlalchemy with persistent backend')
    def test_baseline_migration(self):
        from epmt.orm import get_db_schema_version
        self.assertEqual(get_db_schema_version(), MIGRATION_HEAD)

    @unittest.skipUnless((settings.orm == 'sqlalchemy') and not (orm_in_memory()),
                         'requires sqlalchemy with persistent backend')
    def test_create_and_apply_migration(self):
        import alembic.config
        rev_id = 'deadbeef'
        # alembic.ini lives in the epmt install root; chdir there so
        # alembic can find its config and script_location.
        saved_cwd = getcwd()
        install_dir = get_install_root()
        chdir(install_dir)
        try:
            migration_file = path.join(
                'epmt_migrations', 'versions',
                f'{rev_id}_add_active_column_to_users_table.py')
            with capture() as (out, _err):
                alembic.config.main(argv=["revision", "--rev-id", rev_id, "-m", "add active column to users table"])
            s = out.getvalue()
            self.assertRegex(s, f'.*{rev_id}_add_active_column_to_users_table.py .* done')
            self.assertTrue(path.isfile(migration_file))
            from epmt.orm import migrate_db, get_db_schema_version
            with capture() as (out, _err):
                migrate_db()
            self.assertEqual(get_db_schema_version(), 'deadbeef')
            with capture() as (out, _err):
                alembic.config.main(argv=['downgrade', MIGRATION_HEAD])
            self.assertEqual(get_db_schema_version(), MIGRATION_HEAD)
            remove(migration_file)
        finally:
            chdir(saved_cwd)


if __name__ == '__main__':
    unittest.main()
