"""Verify the optional frozen-build boundary without executing legacy builds.

These are build-tool tests, not installed application tests. They live with the
removable legacy implementation and are excluded from Python distributions.
"""

from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


class FrozenTargets(unittest.TestCase):
    """Keep legacy commands compatible and independently removable."""

    def plan(self, target, root=ROOT, check=True):
        """Ask make for a plan without running a freezer or Docker command."""
        return subprocess.run(
            ["make", "--no-print-directory", "-n", "EPMT_VERSION=0.0.0", target],
            cwd=root, text=True, capture_output=True, check=check,
        )

    def test_python_build_works_without_frozen_include(self):
        with tempfile.TemporaryDirectory(prefix="epmt-without-frozen-") as directory:
            root = Path(directory)
            shutil.copyfile(ROOT / "Makefile", root / "Makefile")
            output = self.plan("python-dist", root).stdout
            self.assertIn("python3 -m build", output)
            self.assertIn("check_python_artifacts.py", output)
            self.assertNotIn("pyinstaller", output)
            self.assertNotIn("epmt.spec", output)
            self.assertEqual(output, self.plan("python-dist").stdout)
            self.assertNotEqual(self.plan("frozen-dist", root, check=False).returncode, 0)

    def test_legacy_aliases_use_the_explicit_frozen_targets(self):
        for alias in ("dist", "dist-test", "docker-dist", "docker-dist-test"):
            with self.subTest(alias=alias):
                self.assertEqual(self.plan(alias).stdout, self.plan("frozen-" + alias).stdout)
        output = self.plan("frozen-dist").stdout
        self.assertIn("packaging/frozen/epmt.spec", output)
        self.assertIn("epmt-0.0.0-rocky-8.tgz", output)

    def test_installer_alias_preserves_missing_archive_failure(self):
        with tempfile.TemporaryDirectory(prefix="epmt-installer-test-") as directory:
            missing = str(Path(directory) / "missing.tgz")
            results = [
                subprocess.run(
                    ["/bin/bash", "-e", str(ROOT / path), missing],
                    cwd=directory, text=True, capture_output=True, check=False,
                )
                for path in ("utils/epmt-installer", "packaging/frozen/epmt-installer")
            ]
        self.assertEqual(results[0].returncode, 1)
        self.assertEqual(results[0].returncode, results[1].returncode)
        self.assertEqual(results[0].stderr, results[1].stderr)


if __name__ == "__main__":
    unittest.main()
