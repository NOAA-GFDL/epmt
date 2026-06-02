# `epmt` Developer and Advanced Usage Guide

This document contains detailed information about `epmt` configuration, data collection,
database submission, analysis, metrics, debugging, and CI/CD infrastructure.

For installation and quick-start instructions, see [README.md](./README.md).

## Table of Contents

- [Configuration](#configuration)
  - [Environment Variables](#environment-variables)
  - [Getting Current Configuration Information](#getting-current-configuration-information)
- [The Three Modes of `epmt`](#the-three-modes-of-epmt)
  - [The First Mode, Data Collection](#the-first-mode-data-collection)
    - [Collecting Performance Data](#collecting-performance-data)
    - [Data Collection with SLURM epilog and prolog](#data-collection-with-slurm-epilog-and-prolog)
  - [The Second Mode, Data Submission](#the-second-mode-data-submission)
    - [Manual Submission Example](#manual-submission-example)
    - [Compressed Directory Submission Example](#compressed-directory-submission-exmple)
    - [Internal-batch Job Submission Example](#internal-batch-job-submission-example)
    - [Data From Current Session Submission Example](#data-from-current-session-submission-example)
  - [The Third Mode, Data Analysis and Visualization](#the-third-mode-data-analysis-and-visualization)
- [Debugging](#debugging)
- [Performance Metrics Data Dictionary](#performance-metrics-data-dictionary)
  - [Addition of new metrics](#addition-of-new-metrics)
- [CI/CD Workflows and Caching](#cicd-workflows-and-caching)
  - [Workflows](#workflows)
  - [Caches and Invalidation](#caches-and-invalidation)
  - [Cache Invalidation Gap vs. `make`](#cache-invalidation-gap-vs-make)
  - [Forcing a Rebuild](#forcing-a-rebuild)
  - [Weekly Pre-warming](#weekly-pre-warming)
- [Troubleshooting](#troubleshooting)
  - [Virtual Environments:](#virtual-environments)
  - [Common Error Messages](#common-error-messages)
    - [`version GLIBC_x.xx not found`](#version-glibc_xxx-not-found)

## Configuration

`epmt` houses default settings (`epmt_default_settings.py`), user settings (`settings.py`),
and a submodule (`epmt_settings.py`) that appends the user settings to the end of the
default settings, overriding them.

Custom user settings should only be included by developers who understand `epmt`'s requirements.
The default settings should first be evaluated, they look a little bit like the following:

```bash
$ cat src/epmt/epmt_default_settings.py
  ...
  papiex_options = "COLLATED_TSV"
  epmt_output_prefix = "/tmp/epmt/"
  input_pattern = "*-papiex*.[ct]sv"
  ...
  db_params = {
      'url': 'sqlite:///:memory:',
      'echo': False,
  }
  ...
```

### Environment Variables

The following variables replace, at run-time, the values in the `db_params` dictionary found in `settings.py`:

```text
EPMT_DB_PROVIDER
EPMT_DB_USER
EPMT_DB_PASSWORD
EPMT_DB_HOST
EPMT_DB_DBNAME
EPMT_DB_FILENAME
```

### Getting Current Configuration Information

You can examine all current settings by passing the `--help` option:

```bash
$ ./epmt --help
usage: epmt [-n] [-d] [-h] [-a] [--drop]
            [epmt_cmd] [epmt_cmd_args [epmt_cmd_args ...]]

positional arguments:
  epmt_cmd       start, run, stop, submit, dump
  epmt_cmd_args  Additional arguments, command dependent

optional arguments:
  -n, --dry-run  Don't touch the database
  -v, --verbose  Increase level of verbosity/debug
  -h, --help     Show this help message and exit
  -a, --auto     Do start/stop when running
  --drop         Drop all tables/data and recreate before importing

settings.py (overridden by below env. vars):
db_params               {'host': 'localhost', 'password': 'example', 'user': 'postgres', 'dbname': 'EPMT', 'provider': 'postgres'}
debug                   False                                                   
input_pattern           *-papiex-[0-9]*-[0-9]*.csv                              
install_prefix          ../papiex-oss/papiex-oss-install/                       
papiex_options          PERF_COUNT_SW_CPU_CLOCK                                 
epmt_output_prefix      /tmp/epmt/                                              

environment variables (overrides settings.py):
```

## The Three Modes of `epmt`

There are three modes to `epmt` usage; data collection, data submission, and data analysis. Each
has different dependencies:

- **Collection** requires Python 2.6 or higher, and compiled [`papiex`](https://github.com/noaa-gfdl/papiex)
libraries for process tagging
- **Submission** requires Python packages for SQL and database interactions (`sqlalchemy`, `sqlite`, `postgres`)
- **Analysis** requires `jupyter`, `iPython`, and additionally [`epmt-dash`](https://github.com/noaa-gfdl/epmt-dash)
and `plotly` for dashboard-style analytics

### The First Mode, Data Collection

#### Collecting Performance Data

Assuming you have `epmt` installed and in your path, let's modify a job file:

```bash
$ cat my_job.sh
#!/bin/bash
# Example job script for Torque or SLURM
./compute_the_world --debug 
```

This becomes:

```bash
$ cat my_job_epmt.sh
#!/bin/bash
# Example job script for Torque or SLURM
epmt start
epmt run ./compute_the_world --debug 
epmt stop
```

Or more succinctly by automating the start/stop cycle with the `--auto` or `-a` flag:

```bash
$ cat my_job_epmt2.sh
#!/bin/bash
# Example job script for Torque or SLURM
epmt -a run ./compute_the_world --debug
```

But usually we want to run more than one executable. We could have any number of run statements:

```bash
$ cat my_job_epmt3.sh
#!/bin/bash
# Example job script for Torque or SLURM
epmt start
epmt run ./initialize_the_world --random 
epmt run ./compute_the_world 
epmt run ./postprocess_the_world 
epmt stop
```

Let's skip all the markup and do it with only environment variables. `epmt`
provides the configuration to export to the environment through the `source`
command. This is intended for use in a job file and should be evaluated by the
running shell, be it `bash` or `csh`. `epmt source` prints the required
environment variables in Bash format unless either the `SHELL` or `_`
environment variable ends in `csh`.

**Note the unset of `LD_PRELOAD` before stop!** This prevents the data
collection routine from running on `epmt stop` itself.

```bash
$ cat my_job_bash.sh
#!/bin/bash
# Example job script for Torque or SLURM
 
### Preamble, collect job metadata and monitor all processes/threads  
epmt start
eval `epmt source`

./initialize_the_world --random 
./compute_the_world 
./postprocess_the_world 

# Postamble, disable monitoring and collect job metadata
unset LD_PRELOAD
epmt stop
```

Here's an example for `csh`, when run interactively,

```bash
$ /bin/csh
> epmt -j1 source
setenv PAPIEX_OPTIONS PERF_COUNT_SW_CPU_CLOCK;
setenv PAPIEX_OUTPUT /tmp/epmt/1/;
setenv LD_PRELOAD /Users/phil/Work/GFDL/epmt.git/../papiex-oss/papiex-oss-install/lib/libpapiex.so:/Users/phil/Work/GFDL/epmt.git/../papiex-oss/papiex-oss-install/lib/libpapi.so:/Users/phil/Work/GFDL/epmt.git/../papiex-oss/papiex-oss-install/lib/libpfm.so:/Users/phil/Work/GFDL/epmt.git/../papiex-oss/papiex-oss-install/lib/libmonitor.so
```

#### Data Collection with SLURM epilog and prolog

Using configured prolog and epilogs with SLURM tasks allows one to skip job
instrumentation entirely, except for job tags (`EPMT_JOB_TAGS`) and process
tags (`PAPIEX_TAGS`). These are configured in `slurm.conf` for jobs
submitted with `sbatch` but can be tested on the command line when using `srun`.

The above Csh job is equivalent to the below sequence using a prolog and
epilog, ***with the exception of the trailing submit statement.***

```bash
$ SLURM_TASK_SCRIPT_DIR=${EPMT_PREFIX}/epmt-install/slurm
$ srun -n1 \\
    --task-prolog=${SLURM_TASK_SCRIPT_DIR}/slurm_task_prolog_epmt.sh \\
    --task-epilog=${SLURM_TASK_SCRIPT_DIR}/slurm_task_epilog_epmt.sh
$ sleep 1
```

For this job to work using `sbatch`, make the following modifications in
`slurm.conf`, substituting the appropriate path for `$EPMT_PREFIX`:

```bash
SLURM_TASK_SCRIPT_DIR=${EPMT_PREFIX}/epmt-install/slurm
TaskProlog=${SLURM_TASK_SCRIPT_DIR}/slurm_task_prolog_epmt.sh
TaskEpilog=${SLURM_TASK_SCRIPT_DIR}/slurm_task_epilog_epmt.sh
```

If this fails, the `papiex` installation is likely either missing or
misconfigured in `settings.py`. The `-a` flag tells `epmt` to treat
this run as an entire job.

### The Second Mode, Data Submission

After collecting the data, jobs (groups of processes) are imported into the
database with the `submit` command. This command takes arguments in the form of
directories or tar files that must contain a `job_metadata` file.

Normal operation is to submit one or more directories:

```bash
epmt submit <dir1/> [...]
```

One can also submit a list of compressed tar files:

```bash
epmt submit <compressed_dir_file.*z> [...]
```

There is also a mode where the current environment is used to determine where to find the data.

```bash
epmt submit
```

#### Manual Submission Example

We can submit our previous job to the database defined in `settings.py` by
running the `epmt submit` command with the directory returned by `stage` (found
in the location set by `settings.py` `epmt_output_prefix`):

```bash
$ epmt -v submit /tmp/epmt/1/
INFO:epmt_cmds:submit_to_db(/tmp/epmt/1/,*-papiex-[0-9]*-[0-9]*.csv,False)
INFO:epmt_cmds:Unpickling from /tmp/epmt/1/job_metadata
INFO:epmt_cmds:1 files to submit
INFO:epmt_cmds:1 hosts found: ['linuxkit-025000000001-']
INFO:epmt_cmds:host linuxkit-025000000001-: 1 files to import
INFO:epmt_job:Binding to DB: {'filename': ':memory:', 'provider': 'sqlite'}
INFO:epmt_job:Generating mapping from schema...
INFO:epmt_job:Processing job id 1
INFO:epmt_job:Creating user root
INFO:epmt_job:Creating job 1
INFO:epmt_job:Creating host linuxkit-025000000001-
INFO:epmt_job:Creating metricname usertime
INFO:epmt_job:Creating metricname systemtime
INFO:epmt_job:Creating metricname rssmax
INFO:epmt_job:Creating metricname minflt
INFO:epmt_job:Creating metricname majflt
INFO:epmt_job:Creating metricname inblock
INFO:epmt_job:Creating metricname outblock
INFO:epmt_job:Creating metricname vol_ctxsw
INFO:epmt_job:Creating metricname invol_ctxsw
INFO:epmt_job:Creating metricname num_threads
INFO:epmt_job:Creating metricname starttime
INFO:epmt_job:Creating metricname processor
INFO:epmt_job:Creating metricname delayacct_blkio_time
INFO:epmt_job:Creating metricname guest_time
INFO:epmt_job:Creating metricname rchar
INFO:epmt_job:Creating metricname wchar
INFO:epmt_job:Creating metricname syscr
INFO:epmt_job:Creating metricname syscw
INFO:epmt_job:Creating metricname read_bytes
INFO:epmt_job:Creating metricname write_bytes
INFO:epmt_job:Creating metricname cancelled_write_bytes
INFO:epmt_job:Creating metricname time_oncpu
INFO:epmt_job:Creating metricname time_waiting
INFO:epmt_job:Creating metricname timeslices
INFO:epmt_job:Creating metricname rdtsc_duration
INFO:epmt_job:Creating metricname PERF_COUNT_SW_CPU_CLOCK
INFO:epmt_job:Adding 1 processes to job
INFO:epmt_job:Earliest process start: 2019-03-06 15:36:56.948350
INFO:epmt_job:Latest process end: 2019-03-06 15:37:06.996065
INFO:epmt_job:Computed duration of job: 10047715.000000 us, 0.17 m
INFO:epmt_job:Staged import of 1 processes, 1 threads
INFO:epmt_job:Staged import took 0:00:00.189151, 5.286781 processes per second
INFO:epmt_cmds:Committed job 1 to database: Job[u'1']
```

#### Compressed Directory Submission Example

This might happen at the end of the day via a cron job:

```bash
epmt submit <dir>/*tgz
```

#### Internal-batch Job Submission Example

These commands could be part of every users job, or in the batch systems configurable preambles/postambles.

```bash
$ cat my_job.sh
#!/bin/bash
# Example job script for Torque or SLURM
echo "$PBS_JOBID or $SLURM_JOBID"
epmt start
epmt run ./compute_the_world --debug 
epmt stop
epmt submit
```

The start/stop cycle can be removed with the `--auto` or `-a` flag, which performs start and stop for you:

```bash
epmt -a run ./debug_the_world --outliers
epmt submit
```

#### Data From Current Session Submission Example

If not inside of a batch environment, `epmt` will *attempt to fake-and-bake a job id*. This is useful
when performing interactive runs. You may not be able to submit these jobs to a shared database due to
constraints on job ID uniqueness, since the session ID is not guaranteed to be unique across reboots,
much less other systems. However, this use case is perfectly acceptable when using a private database:

```bash
$ epmt start
WARNING:epmt_cmds:JOB_ID unset: Using session id 6948 as JOB_ID
WARNING:epmt_cmds:JOB_NAME unset: Using job id 6948 as JOB_NAME
WARNING:epmt_cmds:JOB_SCRIPTNAME unset: Using process name 6948 as JOB_SCRIPTNAME
WARNING:epmt_cmds:JOB_USER unset: Using username phil as JOB_USE$ epmt run ./debug_the_world --outliers
$ epmt stop
$ epmt submit
```

### The Third Mode, Data Analysis and Visualization

`epmt` uses an `IPython` notebook data analytics environment. Starting the
Jupyter notebook is easy from the `epmt notebook` command:

```bash
$ epmt notebook
[I 15:39:24.236 NotebookApp] Serving notebooks from local directory: /home/chris/Documents/playground/MM/build/epmt
[I 15:39:24.236 NotebookApp] The Jupyter Notebook is running at:
[I 15:39:24.236 NotebookApp] http://localhost:8888/?token=9c7529e19e12cb8121d66ff471e96fdd3056f6acc4480274
[I 15:39:24.236 NotebookApp]  or http://127.0.0.1:8888/?token=9c7529e19e12cb8121d66ff471e96fdd3056f6acc4480274
[I 15:39:24.236 NotebookApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
[C 15:39:24.263 NotebookApp] 
    
    To access the notebook, open this file in a browser:
        file:///home/chris/.local/share/jupyter/runtime/nbserver-18690-open.html
    Or copy and paste one of these URLs:
        http://localhost:8888/?token=9c7529e19e12cb8121d66ff471e96fdd3056f6acc4480274
     or http://127.0.0.1:8888/?token=9c7529e19e12cb8121d66ff471e96fdd3056f6acc4480274
```

The notebook command supports passing parameters to jupyter, such as host IP
for sharing access with machines on the local network, notebook token, and
notebook password:

```bash
epmt notebook -- --ip 0.0.0.0 --NotebookApp.token='thisisatoken' --NotebookApp.password='hereisasupersecurepassword'
```

## Debugging

`epmt` can be passed both `-n` (dry-run) and `-v` (verbosity) to help
with debugging. Add more `-v` flags to increase verbosity level (`-vvv`).

```bash
epmt -v start
```

Or to attempt a submit without touching the database:

```bash
epmt -vv submit -n /dir/to/jobdata
```

Also, one can decode and dump the job_metadata file in a dir or compressed dir.

<!-- markdownlint-disable MD013 -->
```bash
$ epmt dump ~/Downloads/yrs05-25.20190221/CM4_piControl_C_atmos_00050101.papiex.gfdl.19712961.tgz 
exp_component           atmos                                                   
exp_jobname             CM4_piControl_C_atmos_00050101                          
exp_name                CM4_piControl_C                                         
exp_oname               00050101                                                
job_el_env_changes      {}                                                      
job_el_env_changes_len  0                                                       
job_el_from_batch       []                                                      
job_el_status           0                                                       
job_el_stop             2019-02-20 22:13:23.131187                              
job_pl_env              {'LANG': 'en_US', 'PBS_QUEUE': 'batch', 'SHELL': '/bin/csh', 'PBS_ENVIRONMENT': 'PBS_BATCH', 'PAPIEX_TAGS': 'atmos', 'SHLVL': '3', 'PBS_WALLTIME': '216000', 'MOAB_NODELIST': 'pp057.princeton.rdhpcs.noaa.gov', 'PBS_VERSION': 'TORQUE-6.0.2', 'PAPIEX_OUTPUT': '/vftmp/Foo.Bar/pbs20345339/papiex',  'LOADEDMODULES': '', 'LC_TIME': 'C', 'MACHTYPE': 'x86_64', 'PAPIEX_OPTIONS': 'PERF_COUNT_SW_CPU_CLOCK', 'MOAB_GROUP': 'f'}
job_pl_env_len          81                                                      
job_pl_from_batch       []                                                      
job_pl_groupnames       ['f', 'f']                                              
job_pl_hostname         pp057                                                   
job_pl_id               20345339.moab01.princeton.rdhpcs.noaa.gov               
job_pl_jobname          CM4_piControl_C_atmos_00050101                          
job_pl_scriptname       CM4_piControl_C_atmos_00050101                          
job_pl_start            2019-02-20 19:58:41.274267                              
job_pl_submit           2019-02-20 19:58:41.274463                              
job_pl_username         Foo.Bar                                        
```
<!-- markdownlint-enable MD013 -->

## Performance Metrics Data Dictionary

`epmt` collects data both from the job runtime and the applications run in that
environment. See the `src/epmt/models/` directory for fixed data stored related to each
object. Metric data is stored differently; the data collector's data dictionary
can be found in papiex-oss/README.md. At the time of this writing, it looked
like this:

| Key                        | Scope    | Description                                             |
|--------------------------- |--------- |-------------------------------------------------------- |
| 1. tags                    | Process  | User specified tags for this executable                 |
| 2. hostname                | Process  | hostname                                                |
| 3. exename                 | Process  | Name of the application, usually argv[0]                |
| 4. path                    | Process  | Path to the application                                 |
| 5. args                    | Process  | All arguments to exe excluding argv[0]                  |
| 6. exitcode                | Process  | Exit code                                               |
| 7. exitsignal              | Process  | Exited due to a signal                                  |
| 8. pid                     | Process  | Process id                                              |
| 9. generation              | Process  | Incremented after every exec() or PID wrap              |
| 10. ppid                   | Process  | Parent process id                                       |
| 11. pgid                   | Process  | Process group id                                        |
| 12. sid                    | Process  | Process session id                                      |
| 13. numtids                | Process  | Number of threads caught by instrumentation             |
| 14. numranks               | Process  | Number of MPI ranks detected                            |
| 15. tid                    | Process  | Thread id                                               |
| 16. mpirank                | Thread   | MPI rank                                                |
| 17. start                  | Process  | Microsecond timestamp at start                          |
| 18. end                    | Process  | Microsecond timestamp at end                            |
| 19. usertime               | Thread   | Microsecond user time                                   |
| 20. systemtime             | Thread   | Microsecond system time                                 |
| 21. rssmax                 | Thread   | Kb max resident set size                                |
| 22. minflt                 | Thread   | Minor faults (TLB misses/new page frames)               |
| 23. majflt                 | Thread   | Major page faults (requiring I/O)                       |
| 24. inblock                | Thread   | 512B blocks read from I/O                               |
| 25. outblock               | Thread   | 512B blocks written to I/O                              |
| 26. vol_ctxsw              | Thread   | Voluntary context switches (yields)                     |
| 27. invol_ctxsw            | Thread   | Involuntary context switches (preemptions)              |
| 28. cminflt                | Process  | minflt (20) for all wait()ed children                   |
| 29. cmajflt                | Thread   | majflt (21) for all wait()ed children                   |
| 30. cutime                 | Process  | utime (17) for all wait()ed children                    |
| 31. cstime                 | Thread   | stime (18) for all wait()ed children                    |
| 32. num_threads            | Process  | Threads in process at finish                            |
| 33. starttime              | Thread   | Timestamp in jiffies after boot thread was started      |
| 34. processor              | Thread   | CPU this thread last ran on                             |
| 35. delayacct_blkio_time   | Thread   | Jiffies process blocked in D state on I/O device        |
| 36. guest_time             | Thread   | Jiffies running a virtual CPU for a guest OS            |
| 37. rchar                  | Thread   | Bytes read via syscall (maybe from cache not dev I/O)   |
| 38. wchar                  | Thread   | Bytes written via syscall (maybe to cache not dev I/O)  |
| 39. syscr                  | Thread   | Read syscalls                                           |
| 40. syscw                  | Thread   | Write syscalls                                          |
| 41. read_bytes             | Thread   | Bytes read from I/O device                              |
| 42. write_bytes            | Thread   | Bytes written to I/O device                             |
| 43. cancelled_write_bytes  | Thread   | Bytes discarded by truncation                           |
| 44. time_oncpu             | Thread   | Nanoseconds spent running                               |
| 45. time_waiting           | Thread   | Nanoseconds runnable but waiting                        |
| 46. timeslices             | Thread   | Number of run periods on CPU                            |
| 47. rdtsc_duration         | Thread   | If PAPI, real time cycle duration of thread             |
| *                          | Thread   | PAPI metrics                                            |

### Addition of new metrics

Additional metrics can be configured either in two ways:

- The `papiex_options` string in `settings.py` if using `epmt run` or `epmt source`
- The value of the `PAPIEX_OPTIONS` environment variable if using `LD_PRELOAD` directly.

The value of these should be a comma separated string:

```bash
export PAPIEX_OPTIONS="PERF_COUNT_SW_CPU_CLOCK,PAPI_CYCLES"
```

To list available and functioning metrics, use one of the included command line tools:

- `papi_avail` and `papi_native_avail` (via `papi`)
- `check_events` and `showevtinfo` (via `libpfm`)
- `perf list` (`linux`)

The `PERF_COUNT_SW_*` events should work on any system that has the proper
`/proc/sys/kernel/perf_event_paranoid` setting.

One should verify the functionality of the metric using the `papi_command_line` tool:

```bash
papi_command_line PERF_COUNT_SW_CPU_CLOCK
papi_command_line CYCLES
```

## CI/CD Workflows and Caching

`epmt`'s GitHub Actions CI is split into focused workflows that use
`actions/cache` to avoid rebuilding expensive artifacts on every pull request
run.

### Workflows

<!-- markdownlint-disable MD013 MD060 -->
| Workflow | Trigger | Purpose |
|---|---|---|
| `docker_build_test.yml` | `push` to `main`, `pull_request` | Full build + test pipeline; restores cached artifacts before building |
| `slurm_image_build.yml` | Weekly (Mon 06:00 UTC), `workflow_dispatch` | Builds the `slurm-cluster` Docker image from source and saves it to cache |
| `weekly_tarball_build.yml` | Weekly (Mon 06:00 UTC), `workflow_dispatch` | Compiles `papiex` and downloads `epmt-dash`, then saves both to cache |
| `build_and_test_epmt.yml` | `push` to `main`, `pull_request` | Source-tree unit tests (no Docker) |
<!-- markdownlint-enable MD013 MD060 -->

### Caches and Invalidation

Each cache step is keyed on its build prerequisites — analogous to how `make`
uses file modification times to decide whether a target must be rebuilt. Changing
a prerequisite produces a new cache key, causing a cache miss and forcing a fresh
build.

<!-- markdownlint-disable MD013 MD033 MD060 -->
| Cache | Cache key components | Invalidation trigger | Notes |
|---|---|---|---|
| `epmt-build` Docker image | `OS_TARGET` + `PYTHON_VERSION` +<br/>`SQLITE_VERSION` +<br/>`hashFiles(Dockerfile, requirements.txt.py3)` | Edit the Dockerfile or requirements file, or bump any version variable | Fully content-hash based — closest analogy to `make` |
| `papiex` compiled tarball | `PAPIEX_VERSION` +<br/>`OS_TARGET` | Bump `PAPIEX_VERSION` in `docker_build_test.yml`,<br/>`weekly_tarball_build.yml`, and `Makefile` | Version-gated; relies on papiex using<br/>immutable release tags |
| `test-release` Docker image | `OS_TARGET` + `PYTHON_VERSION` +<br/>`SQLITE_VERSION` +<br/>`hashFiles(Dockerfile, requirements.txt.py3)` +<br/>`github.sha` | Always rebuilds (by design) —<br/>`restore-keys` prefix reuses<br/>unchanged early layers via<br/>`--cache-from` | Image content changes<br/>every commit; layer reuse<br/>keeps it fast |
| `slurm-cluster` Docker image | `IMAGE_TAG` +<br/>`SLURM_TAG` +<br/>`SLURM_CLUSTER_TAG` | Bump any of the three version<br/>variables in `docker_build_test.yml`<br/>and `slurm_image_build.yml` | Version-string based; upstream<br/>tag mutations without a version<br/>bump won't invalidate |
| `epmt-dash` UI directory | `EPMT_DASH_SRC_BRANCH` | Change `EPMT_DASH_SRC_BRANCH`<br/>in `docker_build_test.yml`,<br/>`weekly_tarball_build.yml`,<br/>and `Makefile` | Branch-name based; new commits<br/>to same branch don't invalidate —<br/>weekly workflow bounds staleness<br/>to ≤1 week |
<!-- markdownlint-enable MD013 MD033 MD060 -->

### Cache Invalidation Gap vs. `make`

`make` detects prerequisite changes via file modification times regardless of
version numbers. The GitHub Actions caches above use version strings or content
hashes instead, so:

- `epmt-build` is fully `make`-like — changing the Dockerfile or
  requirements file immediately produces a different hash and forces a rebuild.
- `papiex` and `slurm-cluster` require a deliberate version bump in the
  workflow env block to trigger a rebuild. Remote source changes without a
  version bump go undetected until the next weekly build.
- `epmt-dash` requires either a branch rename or waiting for the weekly
  build. The weekly `weekly_tarball_build.yml` workflow always fetches fresh
  content, bounding maximum staleness to one week.

### Forcing a Rebuild

To force any individual cache to rebuild, bump the relevant version variable
in the `env:` section of both the weekly workflow and `docker_build_test.yml`,
and update the `Makefile` to match:

```yaml
# docker_build_test.yml  (and weekly_tarball_build.yml)
env:
  PAPIEX_VERSION: "2.3.16"          # bump to force papiex rebuild
  EPMT_DASH_SRC_BRANCH: "new-branch" # change to force epmt-dash rebuild
  IMAGE_TAG: "25.05.4"              # bump to force slurm-cluster rebuild
```

The `epmt-build` cache is invalidated automatically whenever
`Dockerfiles/Dockerfile.rocky-8-epmt-build` or `requirements.txt.py3` is
modified — no manual version bump is needed.

### Weekly Pre-warming

`slurm_image_build.yml` and `weekly_tarball_build.yml` run every Monday
morning to pre-warm their respective caches before the working week begins.
`docker_build_test.yml` then restores those caches on pull request and push
runs, skipping the expensive Docker compile steps. If the cache is missing (first
run, eviction, or new key), `docker_build_test.yml` falls back to building the
artifact inline so the pipeline never silently skips a required build step.

## Troubleshooting

### Virtual Environments

Note that often in virtual environments, hardware counters are not often available in the VM.

### Common Error Messages

#### `version GLIBC_x.xx not found`

The collector library may not have been built for the current environment or the release
OS version does not match the current environment.
