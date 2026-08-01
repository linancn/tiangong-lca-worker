#!/usr/bin/env python3
"""Unit and negative tests for the Worker API cutover scanner."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import check_worker_control_plane_api_cutover as target


class WorkerControlPlaneApiCutoverScannerTest(unittest.TestCase):
    def test_accepts_versioned_api_consumers(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            consumer = root / "consumer.rs"
            consumer.write_text("select api.worker_claim_jobs_v1($1)", encoding="utf-8")
            self.assertEqual(target.scan([consumer], root), [])

    def test_rejects_each_frozen_public_alias(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            consumer = root / "consumer.rs"
            consumer.write_text("\n".join(target.FORBIDDEN), encoding="utf-8")
            violations = target.scan([consumer], root)
            self.assertEqual(len(violations), len(target.FORBIDDEN))
            for forbidden in target.FORBIDDEN:
                self.assertTrue(any(forbidden in violation for violation in violations))


if __name__ == "__main__":
    unittest.main()
