"""Integration tests for kernel compilation workflow.

Translated from 050-kernel-compile.bats.

NOTE: All tests in this module are marked as skipped (matching the original
bats tests which had 'skip' at the top of each test case).
"""
import pytest


class TestKernelCompile:
    @pytest.mark.skip(reason="Skipped in original bats tests")
    def test_kernel_compile_with_csv_v1(self):
        """Kernel compile with CSV_v1 output format."""
        pass

    @pytest.mark.skip(reason="Skipped in original bats tests")
    def test_kernel_compile_with_collated_tsv(self):
        """Kernel compile with COLLATED_TSV output format."""
        pass
