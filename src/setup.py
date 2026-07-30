"""Custom build step: compile papiex native libraries during pip install.

Following the same pattern as NOAA-GFDL/pyFMS, this module hooks a shell
script into setuptools' build step so that papiex shared libraries are
compiled and placed in epmt/lib/ before the Python package is assembled.

All package metadata (name, version, dependencies, …) lives in
pyproject.toml.  This file contains only imperative build logic.
"""
import logging
import subprocess
import sys
from pathlib import Path

from setuptools import setup
from setuptools.command.build import build

logger = logging.getLogger(__name__)

LOG_FILE = "install_papiex.log"


def _open_tty():
    """Open /dev/tty for direct user output, bypassing pip's capture."""
    try:
        return open("/dev/tty", "w", encoding="utf-8")
    except OSError:
        return None


class BuildPapiex(build):
    """Compile papiex native libraries before the standard setuptools build."""

    def run(self):
        script = Path(__file__).parent / "compile_papiex.sh"
        if script.exists():
            try:
                self._run_compile(script)
            except subprocess.CalledProcessError as exc:
                logger.warning(
                    "papiex compilation failed (exit %d). EPMT will still "
                    "install but hardware counter collection will be "
                    "unavailable. See %s for details.",
                    exc.returncode, LOG_FILE,
                )
            except FileNotFoundError:
                logger.warning(
                    "compile_papiex.sh not found — skipping papiex build."
                )
        super().run()

    @staticmethod
    def _run_compile(script):
        """Run compile_papiex.sh, teeing output to /dev/tty and a log file.

        pip suppresses build-backend stdout/stderr in non-verbose mode.
        Writing directly to /dev/tty ensures the compilation progress is
        always visible to interactive users.  In non-interactive contexts
        (CI, no TTY) the output still goes to the log file and stderr.
        """
        tty_fh = _open_tty()
        fallback_to_stderr = tty_fh is None

        proc = subprocess.Popen(
            [str(script)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        with open(LOG_FILE, "w", encoding="utf-8") as log_fh:
            for line in proc.stdout:
                log_fh.write(line)
                if tty_fh:
                    tty_fh.write(line)
                    tty_fh.flush()
                elif fallback_to_stderr:
                    sys.stderr.write(line)
                    sys.stderr.flush()

        proc.wait()

        if tty_fh:
            tty_fh.close()

        if proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, str(script))


setup(cmdclass={"build": BuildPapiex})
