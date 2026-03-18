# Proposal: EPMT Absorption of DORA Database Functionality

**Date:** March 2026  
**Status:** Draft  

---

## 1. Executive Summary

This proposal outlines a plan for EPMT to absorb the database-layer responsibilities currently handled by DORA. DORA is a Flask/MariaDB web application that tracks climate model experiments, their parameters, project groupings, and users. EPMT is a SQLAlchemy-based performance monitoring tool that tracks jobs, processes, and execution metrics using PostgreSQL or SQLite.

The two systems have complementary but non-overlapping data domains: DORA tracks *what* experiments are and how they're configured; EPMT tracks *how* jobs perform when those experiments run. Merging the database layer would create a single source of truth for the experiment-to-execution pipeline, eliminate redundant user management, and enable powerful cross-domain queries (e.g., "which parameter changes correlated with performance regressions?").

---

## 2. Current State

### 2.1 DORA Database (MariaDB)

| Table | Purpose |
|-------|---------|
| `master` | Experiment registry — names, owners, model types, filesystem paths, status, labels, CMIP MIP associations |
| `parameters` | MOM6/SIS2 model configuration parameters per experiment (keyed by MD5 hash) |
| `projects` | Organizational groupings (mdt, cmip6, cmip7, esm45, etc.) with YAML display config |
| `[project]_map` | Per-project experiment ID mappings (e.g., `cmip6_map`, `esm45_map`) |
| `users` | Google OAuth users with per-project CRUD permissions |
| `tokens` | API authentication tokens with expiration |
| `logs` | Error tracking with tracebacks |

**Access pattern:** Raw SQL via PyMySQL with DictCursor. No ORM.

### 2.2 EPMT Database (PostgreSQL / SQLite)

| Table | Purpose |
|-------|---------|
| `jobs` | Execution records — jobid, timing, CPU metrics, tags (JSON), annotations, analyses |
| `processes` | Per-process metrics within jobs — hierarchy, CPU, threads |
| `users` | System users (name-based, lightweight) |
| `hosts` | Compute nodes |
| `refmodels` | Reference baselines for anomaly detection |
| `unprocessed_jobs` | Post-processing queue |
| 3 association tables | M:M relationships (host↔job, refmodel↔job, process ancestry) |

**Access pattern:** SQLAlchemy ORM with Alembic migrations. JSON fields indexed for tag-based queries.

### 2.3 Key Differences

| Aspect | DORA | EPMT |
|--------|------|------|
| Database engine | MariaDB | PostgreSQL or SQLite |
| ORM | None (raw SQL) | SQLAlchemy + Alembic |
| Primary entity | Experiment (parameter set / model run) | Job (executable run) |
| Experiment concept | First-class table (`master`) | Tag-based (`exp_name` in JSON) |
| Project concept | First-class table (`projects` + `_map`) | None |
| User model | Google OAuth, per-project permissions | Lightweight name-based |
| Parameter storage | Dedicated table with MD5 keys | None |
| Metadata flexibility | Fixed schema columns | Extensible JSON fields |

---

## 3. Proposed New Schema

Add four new SQLAlchemy models to EPMT and extend one existing model to absorb DORA's responsibilities.

### 3.1 New Model: `Experiment`

Replaces DORA's `master` table. Promotes the tag-based experiment concept to a first-class entity.

```python
class Experiment(Base):
    __tablename__ = 'experiments'

    id            = Column(Integer, primary_key=True, autoincrement=True)
    exp_name      = Column(String(100), unique=True, nullable=False, index=True)
    display_name  = Column(String(100))
    owner         = Column(String(50), index=True)
    model_type    = Column(String(30), index=True)     # CM4, OM5, ESM4, etc.
    exp_type      = Column(String(30))                  # control, perturbation, etc.
    exp_mip       = Column(String(100))                 # CMIP MIP association
    exp_labels    = Column(JSONB)                       # searchable keyword list
    exp_length    = Column(Integer)                     # duration in years
    exp_year      = Column(Integer)                     # most recent year
    status        = Column(String(30))                  # Running / Complete
    job_id        = Column(String(30))                  # batch scheduler job ID
    queue         = Column(String(30))                  # job queue
    path_pp       = Column(String(500))                 # post-processing directory
    path_analysis = Column(String(500))
    path_db       = Column(String(500))                 # global sums database dir
    path_script   = Column(String(500))
    path_xml      = Column(String(500))
    path_log      = Column(String(500))
    url_curator   = Column(String(100))                 # curator experiment ID
    metadata      = Column(JSONB, default={})           # extensible metadata
    created_at    = Column(DateTime, server_default=func.now())
    updated_at    = Column(DateTime, onupdate=func.now())

    # Relationships
    parameters    = relationship('ExperimentParameter', back_populates='experiment',
                                 cascade='all, delete-orphan')
    projects      = relationship('Project', secondary='project_experiment_map',
                                 back_populates='experiments')
    jobs          = relationship('Job', secondary='experiment_job_map',
                                 back_populates='experiments')
```

### 3.2 New Model: `ExperimentParameter`

Replaces DORA's `parameters` table. Drops the MD5 hash key in favor of a proper composite unique constraint.

```python
class ExperimentParameter(Base):
    __tablename__ = 'experiment_parameters'

    id            = Column(Integer, primary_key=True, autoincrement=True)
    experiment_id = Column(Integer, ForeignKey('experiments.id', ondelete='CASCADE'),
                           nullable=False, index=True)
    param_name    = Column(String(255), nullable=False)
    param_value   = Column(String(1000))

    experiment    = relationship('Experiment', back_populates='parameters')

    __table_args__ = (
        UniqueConstraint('experiment_id', 'param_name', name='uq_exp_param'),
    )
```

### 3.3 New Model: `Project`

Replaces DORA's `projects` table and all dynamic `[project]_map` tables with a single normalized many-to-many relationship.

```python
class Project(Base):
    __tablename__ = 'projects'

    id            = Column(Integer, primary_key=True, autoincrement=True)
    name          = Column(String(50), unique=True, nullable=False, index=True)
    description   = Column(String(500))
    config        = Column(JSONB, default={})           # display table YAML → JSON
    created_at    = Column(DateTime, server_default=func.now())

    experiments   = relationship('Experiment', secondary='project_experiment_map',
                                 back_populates='projects')

# Association table (replaces all dynamic [project]_map tables)
project_experiment_map = Table(
    'project_experiment_map', Base.metadata,
    Column('project_id', Integer, ForeignKey('projects.id', ondelete='CASCADE'),
           primary_key=True),
    Column('experiment_id', Integer, ForeignKey('experiments.id', ondelete='CASCADE'),
           primary_key=True),
    Column('project_local_id', Integer),  # optional per-project numbering
)
```

### 3.4 Extended Model: `Job` (existing)

Add a many-to-many relationship linking EPMT jobs to experiments. This bridges the performance data (EPMT's domain) with experiment metadata (DORA's domain).

```python
# New association table
experiment_job_map = Table(
    'experiment_job_map', Base.metadata,
    Column('experiment_id', Integer, ForeignKey('experiments.id', ondelete='CASCADE'),
           primary_key=True),
    Column('jobid', String, ForeignKey('jobs.jobid', ondelete='CASCADE'),
           primary_key=True),
)

# Add to existing Job model:
class Job(Base):
    # ... existing fields ...
    experiments = relationship('Experiment', secondary='experiment_job_map',
                               back_populates='jobs')
```

### 3.5 Enhanced User Model

Upgrade EPMT's lightweight `User` to support OAuth and per-project permissions currently in DORA.

```python
class User(Base):
    __tablename__ = 'users'

    # Existing EPMT fields
    name        = Column(String, primary_key=True)    # keep for backward compat
    id          = Column(String, unique=True)          # already exists
    created_at  = Column(DateTime)
    info_dict   = Column(JSONB)

    # New fields from DORA
    email         = Column(String(100), unique=True, index=True)
    profile_pic   = Column(String(200))
    remote_addr   = Column(String(40))
    login_date    = Column(DateTime)
    oauth_id      = Column(String(100), unique=True)  # Google/OAuth provider ID
    is_admin      = Column(Boolean, default=False)

    # Permissions stored as JSON (replaces DORA's CSV columns)
    permissions   = Column(JSONB, default={})
    # Example: {"view": [1,2,3], "add": [2], "modify": [], "delete": []}
```

### 3.6 New Model: `ApiToken`

Replaces DORA's `tokens` table.

```python
class ApiToken(Base):
    __tablename__ = 'api_tokens'

    token       = Column(String(60), primary_key=True)
    active      = Column(Boolean, default=True)
    user_email  = Column(String(100), ForeignKey('users.email', ondelete='CASCADE'),
                         unique=True)
    created_at  = Column(DateTime, server_default=func.now())
    expires_at  = Column(DateTime)
    last_used   = Column(DateTime)
    remote_addr = Column(String(40))

    user        = relationship('User')
```

---

## 4. Entity Relationship Diagram

```
┌──────────────┐       ┌──────────────────────┐       ┌──────────────┐
│   Project    │ M : M │     Experiment        │ M : M │     Job      │
│──────────────│◄─────►│──────────────────────│◄─────►│──────────────│
│ id           │       │ id                    │       │ jobid (PK)   │
│ name         │       │ exp_name              │       │ jobname      │
│ description  │       │ display_name          │       │ tags (JSON)  │
│ config (JSON)│       │ owner                 │       │ duration     │
└──────────────┘       │ model_type            │       │ cpu_time     │
       │               │ exp_type, exp_mip     │       │ proc_sums    │
       │               │ paths (pp, xml, ...)  │       │ annotations  │
       │               │ status, queue         │       │ analyses     │
       │               │ metadata (JSON)       │       │ ...          │
       │               └──────────────────────┘       └──────┬───────┘
       │                        │ 1:M                         │ 1:M
       │               ┌───────┴───────────┐          ┌──────┴───────┐
       │               │ ExperimentParameter│          │   Process    │
       │               │───────────────────│          │──────────────│
       │               │ experiment_id (FK) │          │ jobid (FK)   │
       │               │ param_name         │          │ pid, ppid    │
       │               │ param_value        │          │ cpu_time     │
       │               └───────────────────┘          │ tags (JSON)  │
       │                                              └──────────────┘
       │               ┌──────────────────┐
       │               │      User        │
       │               │──────────────────│
       │               │ name (PK)        │
       │               │ email            │
       └ ─ perms ─ ─ ►│ oauth_id         │
                       │ permissions(JSON)│
                       │ is_admin         │
                       └───────┬──────────┘
                               │ 1:M
                       ┌───────┴──────────┐
                       │    ApiToken      │
                       │──────────────────│
                       │ token (PK)       │
                       │ user_email (FK)  │
                       │ active, expires  │
                       └──────────────────┘
```

---

## 5. Migration Strategy

### Phase 1: Schema Extension (Non-Breaking)

1. **Create Alembic migration** adding the new tables (`experiments`, `experiment_parameters`, `projects`, `project_experiment_map`, `experiment_job_map`, `api_tokens`) and new columns on `users`.
2. All existing EPMT functionality remains unchanged — new tables are additive.
3. Existing tag-based experiment queries (`tags='exp_name:...'`) continue to work.

### Phase 2: Data Migration

1. **Export DORA data** via its existing `/backup` endpoint (MySQL dump) or direct SQL queries.
2. **Transform and load** into EPMT's new tables:
   - `master` → `experiments` (column renaming, path field mapping)
   - `parameters` → `experiment_parameters` (drop MD5 key, use FK + composite unique)
   - `projects` → `projects` (YAML config → JSON)
   - All `[project]_map` tables → `project_experiment_map` rows
   - `users` → merge into existing `users` table (add OAuth fields)
   - `tokens` → `api_tokens`
3. **Link existing EPMT jobs to experiments**: Match `exp_name` tags on EPMT jobs to `experiments.exp_name` and populate `experiment_job_map`.
4. Write a one-time migration script (Python) that:
   - Connects to DORA's MariaDB (read-only)
   - Connects to EPMT's PostgreSQL (write)
   - Performs the ETL with validation and rollback support

### Phase 3: API Layer

Extend EPMT's command/query interface with experiment management operations:

```python
# New EPMT commands
epmt experiment list [--project <name>] [--model-type <type>]
epmt experiment show <exp_name>
epmt experiment add --name <name> --owner <owner> --model-type <type> ...
epmt experiment update <exp_name> --status Complete
epmt experiment params <exp_name> [--diff <other_exp>]
epmt experiment link-jobs <exp_name> [--tags <tag_filter>]

# New EPMT query API functions
epmt_query.get_experiments(project=None, model_type=None, owner=None, ...)
epmt_query.get_experiment_params(exp_name, diff_with=None)
epmt_query.get_experiment_jobs(exp_name, metric=None)

# Project management
epmt project list
epmt project show <name>
epmt project add --name <name> --description <desc>
epmt project add-experiment <project_name> <exp_name>
```

### Phase 4: DORA Web Layer Adaptation

DORA's Flask web UI continues to serve the front-end but switches its database backend:

1. Replace `dora/db.py` (PyMySQL) with SQLAlchemy session pointing to EPMT's database.
2. Replace `dora/Experiment.py` with imports from EPMT's ORM models.
3. Rewrite `dora/projects.py`, `dora/parameters.py`, `dora/user.py` to use SQLAlchemy queries.
4. DORA's API endpoints (`/api/info`, `/api/list`, `/api/search`, `/api/add`) become thin wrappers over EPMT query functions.
5. Authentication (Google OAuth) stays in DORA's Flask app but writes to EPMT's enhanced `users` table.

---

## 6. Benefits

| Benefit | Detail |
|---------|--------|
| **Single source of truth** | One database for experiment metadata + execution performance. No more cross-referencing two systems. |
| **Cross-domain queries** | "Show me all experiments where jobs had >2x CPU time regression" — impossible today without manual joins across databases. |
| **Parameter-performance correlation** | Link parameter changes directly to performance outcomes. |
| **Proper ORM** | Replace DORA's raw SQL (with format-string injection risks) with SQLAlchemy's parameterized queries. |
| **Schema migrations** | Alembic provides versioned, reversible schema changes — DORA currently has no migration tooling. |
| **Flexible metadata** | JSONB fields for both experiments and jobs allow schema evolution without migrations for optional fields. |
| **Unified user model** | One user table with OAuth support, eliminating auth duplication. |
| **Backend flexibility** | PostgreSQL for production, SQLite for development/testing — DORA is locked to MariaDB. |
| **Normalized project mappings** | One `project_experiment_map` table replaces N dynamic `[project]_map` tables. |

---

## 7. Risks and Mitigations

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| Data loss during migration | Medium | Run migration against a copy first. Keep DORA MariaDB read-only as backup for 3 months post-migration. |
| DORA web app breakage | High (short-term) | Phase 4 is the most labor-intensive. Maintain DORA on MariaDB until the full SQLAlchemy rewrite is validated. |
| Performance regression on experiment queries | Low | PostgreSQL JSONB indexing + proper SQLAlchemy indexes. Benchmark DORA's heavy queries (full-text search, parameter diff) against new schema before cutover. |
| Tag-based vs. FK-based experiment lookups | Low | Keep backward compatibility: EPMT jobs can still be queried by `exp_name` tag. The `experiment_job_map` is additive, not a replacement. |
| User permission model mismatch | Medium | DORA's CSV-based permissions are brittle. Migrating to JSON is strictly better but requires testing all permission checks. |
| OAuth dependency | Low | OAuth stays in the DORA web layer. EPMT CLI doesn't need OAuth — it uses the existing user model or API tokens. |

---

## 8. FY2027 Implementation Plan (SENA)

This work falls under the **Software Engineering for Novel Architectures (SENA)** effort at NOAA/GFDL. SENA's mission is to modernize the climate modeling software stack for next-generation computing architectures — GPU-capable DSLs (NDSL/GT4Py), Python model rewrites (pyFV3, pace, pySHiELD), and the runtime/workflow tooling (fre-cli, fre-workflows) that ties them together. EPMT and DORA are the observability and experiment-tracking pillars of this stack. Consolidating their database layers directly supports SENA's goals by:

- Enabling **performance-to-parameter traceability** as models are ported to novel architectures (e.g., correlating GPU-vs-CPU job metrics with specific parameter configurations).
- Providing a **unified query surface** for the experiment lifecycle — from XML configuration through execution profiling — critical for validating architecture-ported model fidelity.
- Eliminating ad-hoc MariaDB infrastructure in favor of EPMT's PostgreSQL/SQLAlchemy stack, aligning with SENA's preference for portable, well-tested Python tooling.

The plan follows **government fiscal quarters** (FY2027: October 2026 – September 2027).

---

### FYQ1 — October 2026 through December 2026: Foundation

**Theme:** Schema design, ORM models, and migration infrastructure. All work is additive — zero disruption to existing EPMT or DORA functionality.

#### Milestone 1.1: Schema Design Review and Approval
> Target: End of October 2026

| # | Deliverable | Description |
|---|-------------|-------------|
| D1.1.1 | **Finalized ERD and schema specification** | Reviewed version of the entity-relationship diagram (Section 4) and column-level schema (Section 3) approved by EPMT and DORA stakeholders. Includes decisions on all Open Questions (Section 9). |
| D1.1.2 | **Data mapping document** | Column-by-column mapping from every DORA table (`master`, `parameters`, `projects`, `*_map`, `users`, `tokens`, `logs`) to the proposed EPMT schema, including type conversions, default values, and edge cases (e.g., DORA's CSV permissions → JSONB). |

#### Milestone 1.2: SQLAlchemy Models and Alembic Migration
> Target: End of November 2026

| # | Deliverable | Description |
|---|-------------|-------------|
| D1.2.1 | **New SQLAlchemy model classes** | Production-ready Python modules for `Experiment`, `ExperimentParameter`, `Project`, `ApiToken`, the `project_experiment_map` and `experiment_job_map` association tables, and the enhanced `User` model — integrated into EPMT's existing `orm/sqlalchemy/models.py`. |
| D1.2.2 | **Alembic migration script** | Versioned, reversible migration that creates all new tables and adds new columns to `users`. Tested against both PostgreSQL and SQLite backends. |
| D1.2.3 | **Unit test suite for new models** | pytest coverage for all CRUD operations on the new entities, relationship traversals (Project ↔ Experiment ↔ Job), cascade deletes, and unique constraint enforcement. |

#### Milestone 1.3: ETL Script (DORA → EPMT) — Development
> Target: End of December 2026

| # | Deliverable | Description |
|---|-------------|-------------|
| D1.3.1 | **Data migration script v1** | Python script that connects to DORA's MariaDB (read-only), extracts all tables, transforms per the mapping document (D1.1.2), and loads into EPMT's PostgreSQL. Includes validation checksums (row counts, referential integrity checks) and dry-run mode. |
| D1.3.2 | **Job-experiment backfill logic** | Module within the ETL script that matches existing EPMT jobs (by `exp_name` tag) to newly imported `experiments` rows and populates `experiment_job_map`. Logs unmatched jobs for manual review. |

---

### FYQ2 — January 2027 through March 2027: Data Migration and Query API

**Theme:** Execute the data migration on staging, build the EPMT query/CLI interface for experiment and project management, and validate with real data.

#### Milestone 2.1: Staging Migration and Validation
> Target: End of January 2027

| # | Deliverable | Description |
|---|-------------|-------------|
| D2.1.1 | **Successful staging migration** | Full ETL run against a copy of production DORA MariaDB → staging EPMT PostgreSQL. All DORA data verified present: experiments, parameters, projects, project mappings, users, tokens. |
| D2.1.2 | **Migration validation report** | Document comparing row counts, spot-check queries (e.g., "experiment X has parameters Y, Z" matches in both systems), and a list of any data anomalies. Includes job-experiment link coverage statistics (% of EPMT jobs successfully matched to experiments). |
| D2.1.3 | **Rollback test** | Demonstrated Alembic downgrade that cleanly removes all new tables/columns, restoring EPMT's database to its pre-migration schema. |

#### Milestone 2.2: Experiment and Project Query API
> Target: End of February 2027

| # | Deliverable | Description |
|---|-------------|-------------|
| D2.2.1 | **`epmt_query` experiment functions** | New functions in `epmt_query.py`: `get_experiments()`, `get_experiment_params()`, `get_experiment_jobs()` with filtering by project, model type, owner, status, and tag intersection. Returns in all existing EPMT formats (ORM, dict, pandas, terse). |
| D2.2.2 | **`epmt_query` project functions** | New functions: `get_projects()`, `get_project_experiments()`, `get_project_config()`. |
| D2.2.3 | **Parameter diff function** | `diff_experiment_params(exp_a, exp_b)` returning added/removed/changed parameters between two experiments — replaces DORA's `paramdiff.py` logic. |

#### Milestone 2.3: EPMT CLI Extensions
> Target: End of March 2027

| # | Deliverable | Description |
|---|-------------|-------------|
| D2.3.1 | **`epmt experiment` subcommand** | CLI commands: `epmt experiment list`, `show`, `add`, `update`, `params`, `link-jobs`. Documented in EPMT man pages and `--help`. |
| D2.3.2 | **`epmt project` subcommand** | CLI commands: `epmt project list`, `show`, `add`, `add-experiment`, `remove-experiment`. |
| D2.3.3 | **Integration tests** | End-to-end tests exercising the CLI against a PostgreSQL test database with migrated DORA data, verifying round-trip correctness (add experiment via CLI → query via API → verify in DB). |

---

### FYQ3 — April 2027 through June 2027: DORA Web Backend Switchover

**Theme:** Rewire DORA's Flask web application to use EPMT's database as its backend. DORA's UI stays intact; only the data access layer changes.

#### Milestone 3.1: DORA Database Layer Replacement
> Target: End of April 2027

| # | Deliverable | Description |
|---|-------------|-------------|
| D3.1.1 | **Rewritten `dora/db.py`** | Replace PyMySQL connection management with SQLAlchemy session factory pointing to EPMT's PostgreSQL. Maintain Flask `g` integration and teardown semantics. |
| D3.1.2 | **Rewritten `dora/Experiment.py`** | `Experiment` class backed by EPMT's `Experiment` ORM model. All existing methods (`insert`, `update`, `to_dict`, `validate_path`) preserved with identical signatures. |
| D3.1.3 | **Rewritten `dora/user.py` and `dora/auth.py`** | `User` and `Token` classes backed by EPMT's enhanced `User` and `ApiToken` models. Google OAuth flow writes to EPMT's database. Flask-Login integration preserved. |

#### Milestone 3.2: DORA Feature Module Rewrites
> Target: End of May 2027

| # | Deliverable | Description |
|---|-------------|-------------|
| D3.2.1 | **Rewritten `dora/projects.py` and `dora/project_util.py`** | All project operations (list, associate, remap) use EPMT's `Project` model and `project_experiment_map`. Eliminates dynamic `CREATE TABLE [project]_map` pattern. |
| D3.2.2 | **Rewritten `dora/parameters.py` and `dora/paramdiff.py`** | Parameter extraction and comparison via EPMT's `ExperimentParameter` model. Parameter scanner (`mom6_parameter_scanner`) writes to EPMT. |
| D3.2.3 | **Rewritten `dora/api.py`** | All REST endpoints (`/api/info`, `/api/list`, `/api/search`, `/api/add`) delegate to `epmt_query` functions. Token authentication uses EPMT's `ApiToken` model. |

#### Milestone 3.3: DORA End-to-End Validation
> Target: End of June 2027

| # | Deliverable | Description |
|---|-------------|-------------|
| D3.3.1 | **DORA regression test suite** | Automated tests covering all DORA web routes, API endpoints, authentication flows, and permission checks — run against EPMT's PostgreSQL with migrated data. Compared output-for-output against the legacy MariaDB-backed DORA. |
| D3.3.2 | **DORA staging deployment** | DORA Flask app deployed in staging environment with EPMT PostgreSQL backend, accessible to stakeholders for manual acceptance testing. MariaDB instance kept read-only as fallback. |

---

### FYQ4 — July 2027 through September 2027: Production Cutover and Decommission

**Theme:** Ship to production, decommission MariaDB, document everything, and deliver cross-domain analytics that justify the integration.

#### Milestone 4.1: Production Migration
> Target: End of July 2027

| # | Deliverable | Description |
|---|-------------|-------------|
| D4.1.1 | **Production data migration** | Final ETL run against production DORA MariaDB → production EPMT PostgreSQL. Executed during a scheduled maintenance window with DORA in read-only mode. |
| D4.1.2 | **Production DORA cutover** | DORA Flask app switched to EPMT PostgreSQL backend in production. DNS and Docker configs updated. MariaDB remains online (read-only) for 90-day fallback period. |
| D4.1.3 | **Post-cutover monitoring dashboard** | Grafana/logging dashboard tracking DORA query latency, error rates, and user activity against the new backend — compared to MariaDB-era baselines. |

#### Milestone 4.2: Cross-Domain Analytics and SENA Integration
> Target: End of August 2027

| # | Deliverable | Description |
|---|-------------|-------------|
| D4.2.1 | **Parameter-performance correlation queries** | Documented query recipes and/or Jupyter notebook demonstrating cross-domain analysis: e.g., "show experiments where a MOM6 parameter change correlated with >10% CPU time increase on GPU-ported runs vs. CPU baselines." Directly supports SENA's architecture comparison mission. |
| D4.2.2 | **Auto-linking on job ingest** | Optional EPMT post-processing hook that, on job ingestion, checks the `exp_name` tag against the `experiments` table and auto-populates `experiment_job_map`. Configurable via `epmt_settings`. |
| D4.2.3 | **fre-cli / fre-workflows integration point** | Documented interface (and optional implementation) for `fre` tooling to register experiments in EPMT's database at submission time, closing the loop from XML configuration → experiment record → job execution → performance data. |

#### Milestone 4.3: Decommission and Documentation
> Target: End of September 2027 (FY2027 close)

| # | Deliverable | Description |
|---|-------------|-------------|
| D4.3.1 | **MariaDB decommission** | DORA's MariaDB instance archived (final dump stored) and shut down. Docker Compose files updated to remove MariaDB service. Fallback period concluded. |
| D4.3.2 | **Updated documentation** | EPMT docs updated with experiment/project management guides. DORA README updated to reflect new backend. Consolidated architecture diagram showing the unified EPMT+DORA data flow within the SENA ecosystem (fre-cli → experiment registration → job submission → EPMT ingest → DORA visualization). |
| D4.3.3 | **FY2027 closeout report** | Summary of work completed, metrics (migration stats, query performance comparisons, user adoption), lessons learned, and recommendations for FY2028 (e.g., absorbing gfdlvitals/om4labs diagnostic data, adding EPMT web dashboard). |

---

### Plan Summary

| Quarter | Milestones | Deliverables | Phase Mapping |
|---------|-----------|-------------|---------------|
| **FYQ1** (Oct–Dec '26) | 3 | 7 | Phase 1 (Schema) + Phase 2 start (ETL development) |
| **FYQ2** (Jan–Mar '27) | 3 | 9 | Phase 2 (Migration) + Phase 3 (API/CLI) |
| **FYQ3** (Apr–Jun '27) | 3 | 8 | Phase 4 (DORA rewrite + validation) |
| **FYQ4** (Jul–Sep '27) | 3 | 9 | Phase 5 (Production cutover + decommission) |
| **Total** | **12** | **33** | |

### Key Dependencies and Risks by Quarter

| Quarter | Critical Path | Risk | Mitigation |
|---------|--------------|------|------------|
| FYQ1 | Schema approval gates all subsequent work | Stakeholder availability during Oct–Nov | Schedule review in first two weeks of October |
| FYQ2 | Migration quality gates DORA rewrite | Dirty/inconsistent DORA data | Dry-run migration in FYQ1; anomaly report included in D2.1.2 |
| FYQ3 | DORA rewrite is the highest-effort quarter | Regression in DORA web functionality | Output-for-output regression tests (D3.3.1); MariaDB fallback stays live |
| FYQ4 | Production cutover requires maintenance window | Scheduling conflicts with model runs | Coordinate with operations 60 days in advance; weekend window preferred |

---

## 9. Open Questions

1. **Should DORA's web UI remain a separate Flask app or be folded into an EPMT web dashboard?** This proposal assumes DORA stays as the web front-end, just switching backends. A full merge is possible but significantly more work.

2. **EPMT's `exp_name` tags vs. the new `experiments` table:** Should auto-linking be enforced (every job ingestion checks for a matching experiment), or remain optional?

3. **Parameter scanner integration:** DORA's `mom6_parameter_scanner` extracts params from PP directories. Should this become an EPMT post-processing step triggered alongside process tree computation?

4. **DORA's gfdlvitals/om4labs/scalar diagnostic integration:** DORA serves CSV data from gfdlvitals SQLite databases embedded in experiment directories. Should EPMT absorb these too, or leave them as filesystem-level access?

5. **Multi-tenancy:** DORA has per-project permissions. EPMT currently has no access control. How granular should permissions be in the merged system?

---

## 10. Appendix: Table Count Comparison

| | DORA (Current) | EPMT (Current) | EPMT (Proposed) |
|-|----------------|-----------------|-----------------|
| Core tables | 4 + N dynamic map tables | 6 | 10 |
| Association tables | 0 (map tables baked in) | 3 | 5 |
| Total | ~10–12 | 9 | 15 |
| ORM | None | SQLAlchemy | SQLAlchemy |
| Migrations | None | Alembic | Alembic |
| Database | MariaDB | PostgreSQL/SQLite | PostgreSQL/SQLite |
