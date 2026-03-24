# EPMT

[![build_and_test_epmt](https://github.com/NOAA-GFDL/epmt/actions/workflows/build_and_test_epmt.yml/badge.svg)](https://github.com/NOAA-GFDL/epmt/actions/workflows/build_and_test_epmt.yml)
[![docker_build_test](https://github.com/NOAA-GFDL/epmt/actions/workflows/docker_build_test.yml/badge.svg)](https://github.com/NOAA-GFDL/epmt/actions/workflows/docker_build_test.yml)
[![codecov](https://codecov.io/gh/NOAA-GFDL/epmt/branch/main/graph/badge.svg)](https://codecov.io/gh/NOAA-GFDL/epmt)
[![pylint](https://img.shields.io/badge/pylint-%E2%89%A58.1-brightgreen)](https://github.com/NOAA-GFDL/epmt/actions/workflows/build_and_test_epmt.yml)

**Experiment Performance Management Tool (EPMT)** collects metadata and performance data about batch jobs, down to individual threads in individual processes. It is targeted at batch or ephemeral jobs, not daemon processes.

## Installation

### Conda (recommended)

```bash
conda install noaa-gfdl::epmt
```

### pip

```bash
pip install epmt
```

### Developer install (from source)

```bash
git clone https://github.com/NOAA-GFDL/epmt.git
cd epmt
conda env create -f environment.yaml
conda activate epmt
```

## Verifying Installation

Check your installation using the `epmt check` command:

```
$ epmt check
```

Verify the version:

```
$ epmt -V
```

## Quick Start

Instrument a job script by wrapping your commands with `epmt start` / `epmt stop`:

```bash
#!/bin/bash
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

For detailed information on configuration, data collection, SLURM integration, database submission, analysis, performance metrics, debugging, and CI/CD, see [DEVELOPER.md](DEVELOPER.md).
