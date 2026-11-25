"""Validate EPMT distributions and, optionally, their clean installation.

This is build/release tooling, not part of the installed EPMT application. It
lives outside ``src/epmt/`` so archive validation can run before EPMT is installed,
without importing the application or initializing its settings/database. Only
the explicit installed-package check imports EPMT from the environment under
test, rather than relying on an import from the source checkout.

The source distribution includes this tool for maintainers; the runtime wheel
does not. Its regression tests live beside it in
``utils/test_check_python_artifacts.py``, separate from EPMT's installed
application tests in ``src/epmt/test/``.
"""

import argparse
from configparser import ConfigParser
from email import message_from_bytes
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import tarfile
import tempfile
import venv
import zipfile

from packaging.utils import parse_sdist_filename, parse_wheel_filename
from packaging.version import Version


REQUIRED_FILES = {
    "__init__.py",
    "cli.py",
    "epmtlib.py",
    "settings.py",
    "alembic.ini",
    "epmt_migrations/env.py",
    "epmt_migrations/script.py.mako",
    "epmt_migrations/versions/392efb1132ae_baseline.py",
    "test/test_submit.py",
    "test/data/submit/692500.tgz",
    "test/integration/conftest.py",
    "test/integration/test_integration_basic.py",
    "test/shell/epmt-example.bash",
}


def require(condition, message):
    """Raise an actionable validation error, including with Python -O."""
    if not condition:
        raise ValueError(message)


def check_paths(files):
    """Reject native libraries, staged UI code, backups, and unsafe paths."""
    for name in files:
        path = PurePosixPath(name)
        require(not path.is_absolute() and ".." not in path.parts,
                f"Unsafe artifact path: {name}")
        require(not any(part.startswith("#") or part.endswith("~") for part in path.parts),
                f"Editor backup in artifact: {name}")
        require(not re.search(r"\.(so(?:\..*)?|dylib|dll|a|pyc|pyo)$", name),
                f"Non-Python build product in artifact: {name}")
        require(not {"papiex-epmt-install", "ui", "lib"}.intersection(path.parts),
                f"Staged native/UI payload in artifact: {name}")


def check_artifacts(directory, expected_version=None, release_tag=None):
    """Require one self-contained sdist and a matching pure wheel."""
    paths = sorted(directory.iterdir())
    sdists = [path for path in paths if path.name.endswith(".tar.gz")]
    wheels = [path for path in paths if path.suffix == ".whl"]
    require(len(sdists) == len(wheels) == 1 and len(paths) == 2,
            "Expected exactly one sdist and one wheel in the output directory")
    sdist, wheel = sdists[0], wheels[0]
    source_name, source_version = parse_sdist_filename(sdist.name)
    wheel_name, wheel_version, _, tags = parse_wheel_filename(wheel.name)
    require(source_name == wheel_name == "epmt" and source_version == wheel_version,
            "Artifact names or versions disagree")
    require({str(tag) for tag in tags} == {"py3-none-any"}, "Wheel must be py3-none-any")
    if expected_version is not None:
        require(source_version == Version(expected_version), "Source/artifact version mismatch")
    if release_tag is not None:
        require(source_version == Version(release_tag), "Release tag/artifact version mismatch")

    with tarfile.open(sdist) as archive:
        members = [member for member in archive.getmembers() if not member.isdir()]
        require(all(member.isfile() for member in members), "Source archive contains special files")
        check_paths(member.name for member in members)
        roots = {PurePosixPath(member.name).parts[0] for member in members}
        require(len(roots) == 1, "Source archive must have one top-level directory")
        source = {
            member.name.split("/", 1)[1]: archive.extractfile(member).read()
            for member in members
        }
        require(len(source) == len(members), "Duplicate paths in source archive")
    with zipfile.ZipFile(wheel) as archive:
        names = [name for name in archive.namelist() if not name.endswith("/")]
        require(len(names) == len(set(names)), "Duplicate paths in wheel")
        binary = {name: archive.read(name) for name in names}
    check_paths(source)
    check_paths(binary)
    for name in ("README.md", "LICENSE", "pyproject.toml", "PKG-INFO"):
        require(bool(source.get(name)), f"Source archive missing {name}")

    info_dirs = {name.split("/")[0] for name in binary if ".dist-info/" in name}
    require(len(info_dirs) == 1, "Expected one wheel metadata directory")
    info = info_dirs.pop()
    metadata = message_from_bytes(binary[f"{info}/METADATA"])
    source_metadata = message_from_bytes(source["PKG-INFO"])
    for record in (metadata, source_metadata):
        require(record["Name"] == "epmt" and Version(record["Version"]) == source_version,
                "Distribution metadata version mismatch")
        require(record.get_payload().strip() == source["README.md"].decode().strip(),
                "Distribution long description differs from README.md")
    licenses = [value for name, value in binary.items()
                if name.startswith(info + "/") and name.endswith("/LICENSE")]
    require(licenses == [source["LICENSE"]], "Wheel license missing or differs from source")
    wheel_info = message_from_bytes(binary[f"{info}/WHEEL"])
    require(wheel_info["Root-Is-Purelib"] == "true", "Wheel is not pure Python")
    entrypoints = ConfigParser()
    entrypoints.read_string(binary[f"{info}/entry_points.txt"].decode())
    require(entrypoints["console_scripts"]["epmt"] == "epmt.cli:main",
            "Missing EPMT console entry point")

    source_payload = {name.removeprefix("src/epmt/"): value
                      for name, value in source.items() if name.startswith("src/epmt/")}
    wheel_payload = {name.removeprefix("epmt/"): value
                     for name, value in binary.items() if name.startswith("epmt/")}
    require(REQUIRED_FILES <= source_payload.keys(), "Required runtime/test resources missing")
    require(source_payload == wheel_payload, "Wheel payload differs from sdist package payload")
    return source_version, wheel_payload


def check_installed(version, payload):
    """Verify installed resources and CLI without importing the source tree."""
    from importlib.metadata import version as installed_version
    import epmt

    package = Path(epmt.__file__).resolve().parent
    require(package.is_relative_to(Path(sys.prefix).resolve()),
            f"EPMT is not installed inside this interpreter's environment: {package}")
    require(Version(installed_version("epmt")) == version, "Installed metadata version mismatch")
    for name, contents in payload.items():
        require((package / name).read_bytes() == contents, f"Installed payload differs: {name}")
    command = Path(sys.executable).parent / "epmt"
    subprocess.run([str(command), "--help"], check=True, stdout=subprocess.DEVNULL)
    output = subprocess.check_output([str(command), "-V"], text=True).strip()
    require(output.startswith("EPMT ") and Version(output.split()[-1]) == version,
            f"Installed CLI version mismatch: {output}")
    print(f"Installed EPMT {version} verified at {package}")


def check_fresh_install(directory):
    """Install and test the exact wheel in a temporary environment outside the checkout."""
    directory = directory.resolve()
    wheel = next(directory.glob("*.whl"))
    checker = Path(__file__).resolve()
    with tempfile.TemporaryDirectory(prefix="epmt-artifact-") as temporary:
        root = Path(temporary)
        environment = root / "venv"
        venv.EnvBuilder(with_pip=True).create(environment)
        python = environment / "bin" / "python"
        env = os.environ.copy()
        env.pop("PYTHONPATH", None)
        env.update(PATH=f"{environment / 'bin'}:{os.defpath}", TZ="UTC",
                   OPENBLAS_NUM_THREADS="1", OMP_NUM_THREADS="1")

        def run(*arguments):
            subprocess.run([str(python), *arguments], cwd=root, env=env, check=True)

        run("-m", "pip", "install", str(wheel), "pytest", "packaging")
        run("-m", "pip", "check")
        run(str(checker), "--dist-dir", str(directory), "--installed")
        purelib = subprocess.check_output(
            [str(python), "-c", "import sysconfig; print(sysconfig.get_path('purelib'))"],
            cwd=root, env=env, text=True,
        ).strip()
        for suite in ("test_anysh", "test_db_schema", "test_submit", "test_query"):
            run("-m", "pytest", "-x", "-vv", str(Path(purelib) / "epmt/test" / f"{suite}.py"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dist-dir", type=Path, default=Path("dist"))
    parser.add_argument("--expected-version")
    parser.add_argument("--release-tag")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--installed", action="store_true")
    mode.add_argument("--install", action="store_true",
                      help="Install into a fresh environment and run installed-package smoke tests")
    args = parser.parse_args()
    try:
        version, payload = check_artifacts(
            args.dist_dir, args.expected_version, args.release_tag
        )
        if args.installed:
            check_installed(version, payload)
        if args.install:
            check_fresh_install(args.dist_dir)
    except (ValueError, KeyError) as error:
        parser.exit(1, f"Artifact validation failed: {error}\n")
    print(f"EPMT {version}: source and wheel payloads verified ({len(payload)} files)")


if __name__ == "__main__":
    main()
