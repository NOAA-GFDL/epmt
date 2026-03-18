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


class BuildPapiex(build):
    """Compile papiex native libraries before the standard setuptools build."""

    def run(self):
        script = Path(__file__).parent / "compile_papiex.sh"
        if script.exists():
            try:
                with open("install_papiex.log", "w", encoding="utf-8") as log_fh:
                    subprocess.run(
                        [str(script)],
#                        stdout=subprocess.STDOUT,#log_fh,
#                        stderr=subprocess.STDERR,
                        check=True,
                    )
            except subprocess.CalledProcessError as e:
                logger.warning(
                    "papiex compilation failed. EPMT will still install "
                    "but hardware counter collection will be unavailable. "
                    "See install_papiex.log for details."
                )
                raise e
            except FileNotFoundError as e:
                logger.warning(
                    "compile_papiex.sh not found — skipping papiex build."
                )
                raise e
        super().run()


setup(cmdclass={"build": BuildPapiex})
