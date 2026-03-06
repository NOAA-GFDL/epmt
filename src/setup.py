from setuptools import setup
import sysconfig

_site_packages_relative = sysconfig.get_path('purelib').replace(sysconfig.get_path('data') + '/', '')

setup(name="epmt",
      version="4.11.0",
      url="https://some.where",
      author_email="some@where.com",
      packages=['epmt', 'epmt.orm', 'epmt.orm.sqlalchemy', 'epmt.test'],
      package_data={'epmt':
                    ['alembic.ini',
                     'preset_settings/*.py',

                     'epmt_migrations/README',
                     'epmt_migrations/env.py',
                     'epmt_migrations/script.py.mako',
                     'epmt_migrations/docker-entrypoint-initdb.d/init-user-db.sh',
                     'epmt_migrations/versions/*',

                             'test/run',
                             'test/test_run.sh',
                             'test/test_source.csh',

                             'test/data/corrupted_csv/*',
                             'test/data/csv/*',
                             'test/data/daemon/627919.tgz',
                             'test/data/daemon/ingest/*',
                             'test/data/misc/*',
                             'test/data/outliers/*',
                             'test/data/outliers_nb/*',
                             'test/data/query/*',
                             'test/data/query_notebook/*',
                             'test/data/submit/692500.tgz',
                             'test/data/submit/804268.tgz',
                             'test/data/submit/804280.tgz',
                             'test/data/submit/3455/*',
                             'test/data/tsv/collated-tsv-2220.tgz',
                             'test/data/tsv/12340/*',

                             'test/integration/conftest.py',
                             'test/integration/test_integration_*.py',
                             'test/integration/epmt-annotate.sh',
                             'test/integration/epmt-escape-workload.sh',

                             'test/shell/*',
                      ]
                    },

# (Lines 88-92 removed)
      
      data_files=[(f'{_site_packages_relative}/epmt/lib',
                   [
#                    'papiex-epmt-install/lib/libmonitor_wrap.a',
                    'papiex-epmt-install/lib/libmonitor.so',
                    'papiex-epmt-install/lib/libmonitor.so.0',                         
                    'papiex-epmt-install/lib/libmonitor.so.0.0.0',
#                    'papiex-epmt-install/lib/libpapi.a',
#                    'papiex-epmt-install/lib/libpapi.so',
#                    'papiex-epmt-install/lib/libpapi.so.5',                    
#                    'papiex-epmt-install/lib/libpapi.so.5.7.0',
#                    'papiex-epmt-install/lib/libpapi.so.5.7.0.0',                    
#                    'papiex-epmt-install/lib/libpfm.a',
#                    'papiex-epmt-install/lib/libpfm.so',
#                    'papiex-epmt-install/lib/libpfm.so.4',
#                    'papiex-epmt-install/lib/libpfm.so.4.10.1',
                    'papiex-epmt-install/lib/libpapiex.so',
                    'papiex-epmt-install/lib/libpapiex.so.2',                    
#                    'papiex-epmt-install/lib/libpapiex.so.2.3.14',


                   ] ),
                 ],
      scripts=['scripts/epmt'],
      )
