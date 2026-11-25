# **Experiment Process / Metadata Tool** (`epmt`)

`epmt` collects metadata and performance data about shell processes, down to individual threads in individual
processes. Currently, `epmt` is particularly specialized for interfacing with Slurm batch jobs associated with
earth modeling workflows, but is generalizable to other computational workflow contexts. It also offers
entrypoints to analyzing your data by interfacing with `jupyter` for easy access to
a notebook-style interface.

[![readthedocs](https://app.readthedocs.org/projects/epmt/badge/?version=latest&style=flat)](https://epmt.readthedocs.io/en/latest/)
[![codecov](https://codecov.io/gh/NOAA-GFDL/epmt/branch/main/graph/badge.svg)](https://codecov.io/gh/NOAA-GFDL/epmt)
[![pylint](https://img.shields.io/badge/pylint-%E2%89%A58.7-brightgreen)](https://github.com/NOAA-GFDL/epmt/actions/workflows/build_and_test_epmt.yml)
[![weekly_cache_builds](https://github.com/NOAA-GFDL/epmt/actions/workflows/weekly_cache_builds.yml/badge.svg)](https://github.com/NOAA-GFDL/epmt/actions/workflows/weekly_cache_builds.yml)
[![build_conda](https://github.com/NOAA-GFDL/epmt/actions/workflows/build_conda.yml/badge.svg)](https://github.com/NOAA-GFDL/epmt/actions/workflows/build_conda.yml?query=branch%3Amain)
[![markdown lint](https://github.com/NOAA-GFDL/epmt/actions/workflows/md_lint.yml/badge.svg)](https://github.com/NOAA-GFDL/epmt/actions/workflows/md_lint.yml)
[![spell check](https://github.com/NOAA-GFDL/epmt/actions/workflows/spell_check.yml/badge.svg)](https://github.com/NOAA-GFDL/epmt/actions/workflows/spell_check.yml)

<!-- markdownlint-disable MD013 -->
| Workflow                  | Python 3.10                                                                                                                                                                                                                 | Python 3.11                                                                                                                                                                                                                 |
| ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **create_test_conda_env** | [![3.10](https://github.com/NOAA-GFDL/epmt/actions/workflows/create_test_conda_env.yml/badge.svg)](https://github.com/NOAA-GFDL/epmt/actions/workflows/create_test_conda_env.yml?query=branch%3Amain+python-version%3A3.10) | [![3.11](https://github.com/NOAA-GFDL/epmt/actions/workflows/create_test_conda_env.yml/badge.svg)](https://github.com/NOAA-GFDL/epmt/actions/workflows/create_test_conda_env.yml?query=branch%3Amain+python-version%3A3.11) |

| Workflow                | SQLite                                                                                                                                                                                                                  | PostgreSQL                                                                                                                                                                                                                  |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **docker_build_test**   | [![sqlite](https://github.com/NOAA-GFDL/epmt/actions/workflows/docker_build_test.yml/badge.svg)](https://github.com/NOAA-GFDL/epmt/actions/workflows/docker_build_test.yml?query=branch%3Amain+db_backend%3Asqlite)     | [![postgres](https://github.com/NOAA-GFDL/epmt/actions/workflows/docker_build_test.yml/badge.svg)](https://github.com/NOAA-GFDL/epmt/actions/workflows/docker_build_test.yml?query=branch%3Amain+db_backend%3Apostgres)     |
| **build_and_test_epmt** | [![sqlite](https://github.com/NOAA-GFDL/epmt/actions/workflows/build_and_test_epmt.yml/badge.svg)](https://github.com/NOAA-GFDL/epmt/actions/workflows/build_and_test_epmt.yml?query=branch%3Amain+db_backend%3Asqlite) | [![postgres](https://github.com/NOAA-GFDL/epmt/actions/workflows/build_and_test_epmt.yml/badge.svg)](https://github.com/NOAA-GFDL/epmt/actions/workflows/build_and_test_epmt.yml?query=branch%3Amain+db_backend%3Apostgres) |
<!-- markdownlint-enable MD013 -->

## Installation

These are not-yet *fully* functional installations, as `epmt` was designed in an era where virtual environments
were not as ubiquitous as they are today. For full-featured build/installation approaches, consult the
`Makefile`, `.github/workflows`, and [`DEVELOPER.md`](./DEVELOPER.md)

### With `conda` (recommended)

The `conda` installation is currently favored as a quick-start for new users.

```bash
conda install noaa-gfdl::epmt
```

### From repo checkout

The following creates a whole `epmt` conda environment with `epmt` accessible via an editable `pip` installation.

```bash
git clone https://github.com/NOAA-GFDL/epmt.git
cd epmt
conda env create -f environment.yaml
conda activate epmt
```

If you already have an environment created that you wish to install `epmt`, and it's already activated:

```bash
git clone https://github.com/NOAA-GFDL/epmt.git
cd epmt
pip install .
```

### Verifying an Installation

The `check` command is a first-stop sanity-check of your `epmt` installation. Call it with

```bash
epmt check
```

Verify the version:

```bash
epmt -V
```

The Python distribution supplies the EPMT CLI and library, migrations, and test
fixtures. It does not bundle papiex native libraries or the separately maintained
epmt-dash application. Collection requires a working papiex installation;
`epmt check` checks collection capabilities as well as the Python installation.
Installing `epmt[ui]` installs UI dependencies, not the epmt-dash application.

### Building and validating Python distributions

From the repository root, using Python 3.10 or 3.11:

```bash
python -m pip install build twine
make python-dist-test
```

`make python-dist` builds an sdist and a wheel **from that sdist** into `dist/`,
checks their metadata and matching contents, and rejects staged native libraries,
UI code, and editor backups. Use an empty output directory when changing versions;
stale artifacts fail validation rather than being silently published.

`make python-dist-test` additionally installs the exact wheel in a temporary
environment outside the checkout, checks dependencies, versions and resources,
and runs focused installed-package tests. Each test module runs in a separate
process. The temporary environment is removed when validation ends.

To validate existing artifacts without rebuilding:

```bash
python utils/check_python_artifacts.py --install
```

These packaging checks do not replace collection, analytical compatibility,
PostgreSQL, or SLURM suites.

### Python publication

The `pip_publish` workflow validates builds on pull requests, pushes to `main`,
and tags. **Only a tag push can publish to PyPI.** Tag versions must match the
source and artifact versions after PEP 440 normalization.

Publication waits for artifact checks and clean-install tests on Python 3.10 and
3.11, then downloads and publishes the same validated sdist and wheel without
rebuilding. Only the publishing job has OIDC publishing permission.

Tag pushes still trigger publication automatically after these checks; this
change does not introduce a manual approval environment. Creating or pushing a
tag is a separate, human-approved release action, not part of local validation.
GitHub release assets are still uploaded manually. Use the validated sdist from
the workflow (or the identical PyPI file), not a newly rebuilt tarball.

## Quickstart: Watch `epmt` work

Try wrapping your commands with `epmt start` / `epmt stop`:

```bash
epmt start
epmt run ./compute_the_world --debug
epmt stop
epmt submit
```

Or use the **--auto** (`-a`) flag to automate the start/stop cycle:

```bash
epmt run -a -- ./compute_the_world --debug
epmt submit
```

## Further Documentation

For detailed information on configuration, data collection, SLURM integration, database submission, analysis,
performance metrics, debugging, and CI/CD, see [DEVELOPER.md](DEVELOPER.md).
