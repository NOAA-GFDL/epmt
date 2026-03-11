# Copilot Instructions for EPMT

## Project Overview
EPMT (Experiment Performance Monitoring Tool) is a Python 3.9+ package for
tracking and analyzing HPC job performance data.  It supports SQLite
(in-memory) and PostgreSQL backends via SQLAlchemy.

## Repository Layout
- `src/epmt/` — main package source (installed via pip)
- `src/epmt/test/` — unit tests (unittest-based, run with pytest)
- `src/epmt/test/integration/` — integration tests (pytest-based, invoke epmt CLI as subprocess)
- `src/epmt/orm/sqlalchemy/` — SQLAlchemy ORM layer
- `src/epmt/epmt_migrations/` — Alembic migration scripts
- `preset_settings/` — template settings files for different DB backends
- `Dockerfiles/` — Docker images for CI and release
- `.github/workflows/` — CI/CD workflow definitions

## Settings System
Settings are loaded in `src/epmt/epmt_settings.py`:
1. `from epmt.epmt_default_settings import *` (defaults, including `sqlite:///:memory:`)
2. `from epmt.settings import *` (user overrides)

To switch to PostgreSQL, overwrite `src/epmt/settings.py` (or the installed
copy in site-packages) with a preset from `preset_settings/`.

## CI/CD Workflows
Both workflows use a matrix strategy with `db_backend: [sqlite, postgres]`
to test against both backends:

### `build_and_test_epmt.yml`
- Runs on `rockylinux:8` container with Python/SQLite built from source
- PostgreSQL provided via GitHub Actions `services:` block
- The `Configure epmt to use PostgreSQL` step only runs for the postgres matrix entry

### `docker_build_test.yml`
- Builds Docker images (slurm-cluster → epmt-build → test-release)
- Postgres container started manually on `epmt-test-net` Docker network
- SLURM daemons started inside the test container for integration tests

### Caching
- Python/SQLite builds cached by version
- Docker images cached by content hash of `requirements.txt.py3`
- Docker caches saved only on pushes to `main`, not PRs
- `continue-on-error: true` steps mask real failures;
  check actual job logs for `##[error]` to determine pass/fail

## Testing Notes

### Database Backend Differences
When writing tests that work with both SQLite and PostgreSQL:

- **Row ordering is non-deterministic** with PostgreSQL.  Always use
  `sorted()` or explicit `ORDER BY` when comparing lists of rows.
  (Example: `test_procs_convert`, `test_pca_trained_model`)

- **Alembic migrations** only run for persistent backends (not in-memory
  SQLite).  Tests that call `alembic.config.main()` must `chdir` to
  `get_install_root()` first so that `alembic.ini` is found.

- **Persistent DB means data persists between CLI invocations.**  Fixtures
  that clean up test data should use `scope="class"` or `scope="module"`
  if later tests in the same class need data created by earlier tests.

- The `Session` object lives in `epmt.orm.sqlalchemy.general.Session`.
  Import it explicitly when tests need direct session operations.

### Docker Test Context
In `docker_build_test`, tests run from `/usr/lib/python3.9/site-packages`
(not the source tree).  Paths that work in `build_and_test_epmt` (source
tree at `src/epmt/`) may not resolve in Docker.  Use `epmt.__file__` or
`get_install_root()` for package-relative paths.

### Integration Tests
Integration tests in `src/epmt/test/integration/` use `conftest.py` helpers:
- `run_cmd(cmd)` — runs shell command, returns `CompletedProcess`
- `epmt_setting(key)` — parses `epmt -h` output for a setting value
- `resource_path` fixture — resolves to epmt package dir in both layouts

### SLURM Tests
SLURM integration tests require running daemons (slurmctld, slurmd, munged).
The Docker release image has SLURM binaries but does NOT start daemons
automatically.  `_slurm_available()` checks via `sinfo -N --noheader`, not
just `command -v`.

## Build Commands
```bash
make dist python-dist dist-test   # build sdist
python3 -m pip install src/dist/epmt-*.tar.gz  # install
TZ=UTC pytest -x -vv src/epmt/test/test_query.py  # run a specific test
pylint --rcfile pylintrc --fail-under 7.1 --ignore-paths src/epmt/ui src/epmt  # lint
```
