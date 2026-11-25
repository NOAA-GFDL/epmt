"""Focused regressions for the distribution validation gate.

EPMT's application tests generally live in ``src/epmt/test/`` and are shipped
inside the package so they can exercise an installed application. These tests
are an intentional exception: they test the repository's build/release tool
``utils/check_python_artifacts.py``, using synthetic, sometimes deliberately
invalid distributions rather than EPMT application behavior.

They live beside that tool in ``utils/``, rather than in ``src/epmt/test/``,
so the validation gate can be tested before installing EPMT. Both the tool and
these tests are included in the source distribution, not the runtime wheel.
"""

import io
from pathlib import Path
import shutil
import tarfile
import tempfile
import unittest
import zipfile

from utils.check_python_artifacts import REQUIRED_FILES, check_artifacts


class PythonArtifacts(unittest.TestCase):
    """Reject incomplete, contaminated, or inconsistent Python distributions."""

    def setUp(self):
        self.directory = Path(tempfile.mkdtemp(prefix="epmt-artifact-test-"))
        self.addCleanup(shutil.rmtree, self.directory)

    def write_artifacts(self, *, extra_payload=None, missing=None, wheel_changes=None):
        payload = {name: b"fixture\n" for name in REQUIRED_FILES}
        payload.update(extra_payload or {})
        if missing:
            del payload[missing]
        metadata = b"Name: epmt\nVersion: 1.2.3\n\nExample README\n"
        source = {
            "README.md": b"Example README\n",
            "LICENSE": b"Example license\n",
            "pyproject.toml": b"[project]\n",
            "PKG-INFO": metadata,
            **{"src/epmt/" + name: contents for name, contents in payload.items()},
        }
        with tarfile.open(self.directory / "epmt-1.2.3.tar.gz", "w:gz") as archive:
            for name, contents in source.items():
                member = tarfile.TarInfo("epmt-1.2.3/" + name)
                member.size = len(contents)
                archive.addfile(member, io.BytesIO(contents))
        info = "epmt-1.2.3.dist-info/"
        wheel = {
            "epmt/" + name: contents for name, contents in payload.items()
        }
        wheel.update({
            info + "METADATA": metadata,
            info + "LICENSE": source["LICENSE"],
            info + "WHEEL": b"Root-Is-Purelib: true\nTag: py3-none-any\n",
            info + "entry_points.txt": b"[console_scripts]\nepmt = epmt.cli:main\n",
        })
        wheel.update(wheel_changes or {})
        with zipfile.ZipFile(self.directory / "epmt-1.2.3-py3-none-any.whl", "w") as archive:
            for name, contents in wheel.items():
                archive.writestr(name, contents)

    def test_matching_artifacts_and_normalized_release_tag(self):
        self.write_artifacts()
        version, _ = check_artifacts(self.directory, "1.2.3", "v1.2.3")
        self.assertEqual(str(version), "1.2.3")
        with self.assertRaisesRegex(ValueError, "tag/artifact version mismatch"):
            check_artifacts(self.directory, release_tag="1.2.4")

    def test_rejects_staged_native_ui_and_editor_files(self):
        for name in ("lib/libpapiex.so.2", "ui/index.py", "#settings.py#", "../escape.py"):
            with self.subTest(name=name):
                self.write_artifacts(extra_payload={name: b"unwanted"})
                with self.assertRaises(ValueError):
                    check_artifacts(self.directory)

    def test_rejects_missing_resources_and_different_wheel_payload(self):
        self.write_artifacts(missing="alembic.ini")
        with self.assertRaisesRegex(ValueError, "resources missing"):
            check_artifacts(self.directory)
        self.write_artifacts(wheel_changes={"epmt/cli.py": b"unexpected contents"})
        with self.assertRaisesRegex(ValueError, "Wheel payload differs"):
            check_artifacts(self.directory)

    def test_rejects_missing_license_description_and_stale_outputs(self):
        for name, contents in (
            ("LICENSE", b""),
            ("METADATA", b"Name: epmt\nVersion: 1.2.3\n\n"),
        ):
            with self.subTest(name=name):
                self.write_artifacts(wheel_changes={"epmt-1.2.3.dist-info/" + name: contents})
                with self.assertRaises(ValueError):
                    check_artifacts(self.directory)
        self.write_artifacts()
        (self.directory / "stale.whl").touch()
        with self.assertRaisesRegex(ValueError, "exactly one"):
            check_artifacts(self.directory)


if __name__ == "__main__":
    unittest.main()
