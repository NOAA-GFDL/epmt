"""Custom build step: compile papiex native libraries during pip install.

Following the same pattern as NOAA-GFDL/pyFMS, this module hooks a shell
script into setuptools' build step so that papiex shared libraries are
compiled and placed in epmt/lib/ before the Python package is assembled.

All package metadata (name, version, dependencies, …) lives in
pyproject.toml.  This file contains only imperative build logic.
"""
import logging
import subprocess
from pathlib import Path

from setuptools import setup
from setuptools.command.build import build

logger = logging.getLogger(__name__)

LOG_FILE = "install_papiex.log"


class BuildPapiex(build):
    """Compile papiex native libraries before the standard setuptools build."""

    def run(self):
        script = Path(__file__).parent / "compile_papiex.sh"
        if script.exists():
            try:
                with open(LOG_FILE, "w", encoding="utf-8") as log_fh:
                    subprocess.run(
                        [str(script)],
                        stdout=log_fh,
                        stderr=subprocess.STDOUT,
                        check=True,
                    )
            except subprocess.CalledProcessError as exc:
                logger.warning(
                    "papiex compilation failed (exit %d). EPMT will still "
                    "install but hardware counter collection will be "
                    "unavailable. Build log follows:\n%s",
                    exc.returncode, _read_log(),
                )
            except FileNotFoundError:
                logger.warning(
                    "compile_papiex.sh not found — skipping papiex build."
                )
            else:
                logger.info("papiex build log:\n%s", _read_log())
        super().run()


def _read_log():
    """Return the contents of the papiex build log, or a fallback message."""
    try:
        return Path(LOG_FILE).read_text(encoding="utf-8", errors="replace")
    except OSError:
        return "(log file not available)"


setup(cmdclass={"build": BuildPapiex})
