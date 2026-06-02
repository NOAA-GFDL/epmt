# **Experiment Process / Metadata Tool** (`epmt`)

`epmt` collects metadata and performance data about shell processes, down to individual threads in individual
processes. Currently, `epmt` is particularly specialized for interfacing with Slurm batch jobs associated with
earth modeling workflows, but is generalizable to other computational workflow contexts. It also offers
entrypoints to analyzing your data by interfacing with `jupyter` for easy access to
a notebook-style interface.

[![readthedocs](https://app.readthedocs.org/projects/epmt/badge/?version=latest&style=flat)](https://epmt.readthedocs.io/en/latest/)
[![codecov](https://codecov.io/gh/NOAA-GFDL/epmt/branch/main/graph/badge.svg)](https://codecov.io/gh/NOAA-GFDL/epmt)
[![pylint](https://img.shields.io/badge/pylint-%E2%89%A58.6-brightgreen)](https://github.com/NOAA-GFDL/epmt/actions/workflows/build_and_test_epmt.yml)
[![weekly_cache_builds](https://github.com/NOAA-GFDL/epmt/actions/workflows/weekly_cache_builds.yml/badge.svg)](https://github.com/NOAA-GFDL/epmt/actions/workflows/weekly_cache_builds.yml)
[![build_conda](https://github.com/NOAA-GFDL/epmt/actions/workflows/build_conda.yml/badge.svg)](https://github.com/NOAA-GFDL/epmt/actions/workflows/build_conda.yml?query=branch%3Amain)

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
pip install src/
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
epmt -a run ./compute_the_world --debug
epmt submit
```

## Further Documentation

For detailed information on configuration, data collection, SLURM integration, database submission, analysis,
performance metrics, debugging, and CI/CD, see [DEVELOPER.md](DEVELOPER.md).
