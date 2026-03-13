"""Integration tests for epmt_concat.py CSV concatenation utility.

Translated from 005-concat.bats.
"""
import os
import shutil
import pytest

from conftest import run_cmd


@pytest.fixture(autouse=True)
def setup_and_teardown():
    """Clean up concat output files before and after each test."""
    for f in ["pp053-collated-papiex-csv-0.csv", "corrupted_csv.tgz"]:
        if os.path.exists(f):
            os.remove(f)
    yield
    for f in ["pp053-collated-papiex-csv-0.csv", "corrupted_csv.tgz"]:
        if os.path.exists(f):
            os.remove(f)


class TestConcat:
    def test_epmt_concat_help(self):
        """epmt concat -h prints help."""
        r = run_cmd("epmt concat -h")
        assert r.returncode == 0
        assert "Concatenate CSV files" in r.stdout

    def test_epmt_concat_with_valid_input_dir(self):
        """epmt concat with a directory of CSV files."""
        r = run_cmd("epmt concat test/data/csv/")
        assert r.returncode == 0
        assert os.path.isfile("pp053-collated-papiex-csv-0.csv")
        checksum = run_cmd("sum pp053-collated-papiex-csv-0.csv")
        assert "13120" in checksum.stdout

    def test_epmt_concat_with_valid_input_files(self):
        """epmt concat with explicit CSV file arguments."""
        r = run_cmd("epmt concat test/data/csv/*.csv")
        assert r.returncode == 0
        assert os.path.isfile("pp053-collated-papiex-csv-0.csv")
        checksum = run_cmd("sum pp053-collated-papiex-csv-0.csv")
        assert "13120" in checksum.stdout

    def test_epmt_concat_with_nonexistent_directory(self):
        """epmt concat with non-existent directory should fail."""
        r = run_cmd("epmt concat x/")
        assert r.returncode != 0
        assert "x/ does not exist or is not a directory" in r.stdout + r.stderr

    def test_epmt_concat_with_nonexistent_files(self):
        """epmt concat with non-existent files should fail."""
        r = run_cmd("epmt concat x.csv y.csv")
        assert r.returncode != 0
        assert "does not exist or is not a file" in r.stdout + r.stderr

    def test_epmt_concat_with_corrupted_csv(self):
        """epmt concat with corrupted CSV should fail with error message."""
        r = run_cmd("epmt concat -e test/data/corrupted_csv/")
        assert r.returncode != 0
        output = r.stdout + r.stderr
        assert (
            "File: test/data/corrupted_csv/pp053-papiex-615503-0.csv, Header: 40 delimiters, but this row has 39 delimiters"
            in output
        )
