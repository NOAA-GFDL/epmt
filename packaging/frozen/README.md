# Legacy frozen distribution

This directory owns the retained PyInstaller/bundled delivery approach.
It is **separate from Python packaging**, not deprecated or removed by this change.
The normal source distribution, wheel, PyPI workflow, and source-tree test workflow
do not require this directory or its freezer dependencies.

## Ownership

- `targets.mk`: frozen build, test-archive, Docker, release, and cleanup targets.
- `epmt.spec`: the legacy PyInstaller specification.
- `requirements.txt`: freezer-only version pins.
- `epmt-installer`: the legacy full-bundle installer.
- `test_targets.py`: build-boundary and compatibility checks, outside the application package.

Run commands from the repository root. The optional Makefile include owns both
the explicit `frozen-*` targets and the historical compatibility aliases:

| Explicit target | Historical alias |
| --- | --- |
| `frozen-dist` | `dist` |
| `frozen-dist-test` | `dist-test` |
| `frozen-docker-dist` | `docker-dist` |
| `frozen-docker-dist-test` | `docker-dist-test` |
| `frozen-full-release` | `epmt-full-release` |
| `frozen-build-check-release` | `build-check-release` |
| `frozen-check-release` | `check-release` |
| `frozen-release` | `release` |
| `frozen-dockerclean` | `dockerclean` |

Archive names and layouts are unchanged: `epmt-VERSION-OS.tgz`,
`test-epmt-VERSION-OS.tgz`, and `EPMT-release-VERSION-OS.tgz`.
The Python artifacts remain separate files in `dist/`.
Frozen delivery may consume those Python artifacts; Python packaging never
depends on the frozen delivery targets.

## Prerequisites and validation

The legacy approach requires its original docs/notebook/UI dependencies and
staged epmt-dash and papiex sources. Install its extra tooling explicitly:

```bash
python -m pip install -r requirements.txt.py3 -r packaging/frozen/requirements.txt
make epmt-dash papiex-dist
make frozen-dist frozen-dist-test
make frozen-full-release
```

The legacy Docker build images install these additional requirements explicitly.
Their cache keys include the freezer requirements. `docker_build_test.yml`
retains its automatic triggers and database matrix for legacy bundled-delivery
coverage; it is not a dependency of `pip_publish.yml`.

The boundary tests do not invoke PyInstaller, Docker, or a full installation:

```bash
python -m unittest discover -s packaging/frozen -p test_targets.py
```

**Known defects are not repaired by this structural separation.** The spec still
references the historical repository-root `epmt` launcher, which is absent.
The legacy recipes/installer can mask errors or print success without a runnable
frozen executable. The Docker release harness exercises a pip-installed CLI,
not authoritative frozen-binary validation. A green compatibility job therefore
does not certify a working standalone executable.

Do not run the legacy Docker release harness in a shared engine without reviewing
it: it uses fixed container/network names, privileged containers, and kernel
settings. `frozen-clean` removes frozen outputs; `clean-all` also includes the
legacy Docker image cleanup. These operations are not prerequisites of a normal
Python build or validation.

## Eventual deprecation

When removal is approved:

1. Remove the optional frozen include from the root Makefile and this directory.
2. Remove the `utils/epmt-installer` compatibility shim and legacy Docker build/
   release images and workflow after checking downstream users.
3. Remove the frozen-distribution documentation and historical CI/cache references.

No Python entry point, runtime package layout, conda recipe, or PyPI publishing
gate needs to change. Decide separately whether to remove runtime path
compatibility code that was originally added for PyInstaller.
