#!/usr/bin/env python3
"""Security tests for the Worker behavioral DB target guard."""

from __future__ import annotations

import unittest

import check_worker_control_plane_database_target as target

PREVIEW_REF = "nlcyzijvoyufjoqgxlku"


class WorkerControlPlaneDatabaseTargetTest(unittest.TestCase):
    def test_accepts_only_literal_loopback_hosts(self) -> None:
        for database_url in (
            "postgresql://postgres:postgres@localhost:5432/postgres",
            "postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable",
            "postgresql://postgres:postgres@[::1]:5432/postgres",
            "postgresql://postgres:postgres@LOCALHOST:5432/postgres",
        ):
            with self.subTest(database_url=database_url):
                self.assertTrue(target.is_literal_loopback_database_url(database_url))

    def test_rejects_production_host_even_when_preview_ref_appears_elsewhere(self) -> None:
        for database_url in (
            f"postgresql://postgres.{PREVIEW_REF}:secret@db.production.supabase.co/postgres?sslmode=require",
            f"postgresql://postgres:{PREVIEW_REF}@db.production.supabase.co/postgres?sslmode=require",
            f"postgresql://postgres:secret@db.production.supabase.co/postgres?application_name={PREVIEW_REF}",
            f"postgresql://postgres:secret@{PREVIEW_REF}.db.production.supabase.co/postgres?sslmode=require",
            f"postgresql://postgres:secret@db.production.supabase.co/postgres?options=-c%20application_name%3D{PREVIEW_REF}",
        ):
            with self.subTest(database_url=database_url):
                self.assertFalse(target.is_literal_loopback_database_url(database_url))

    def test_rejects_loopback_authority_with_connection_target_override(self) -> None:
        for parameter in ("host", "hostaddr", "service", "servicefile"):
            database_url = (
                "postgresql://postgres:postgres@127.0.0.1:5432/postgres"
                f"?{parameter}=db.production.supabase.co&application_name={PREVIEW_REF}"
            )
            with self.subTest(parameter=parameter):
                self.assertFalse(target.is_literal_loopback_database_url(database_url))

    def test_rejects_lookalikes_and_malformed_urls(self) -> None:
        for database_url in (
            "postgresql://postgres:postgres@127.0.0.1.example.com/postgres",
            "postgresql://postgres:postgres@localhost.example.com/postgres",
            "postgresql://postgres:postgres@127.0.0.2/postgres",
            "postgresql:///postgres?host=/var/run/postgresql",
            "https://127.0.0.1/postgres",
            "postgresql://postgres:postgres@127.0.0.1:invalid/postgres",
            "",
        ):
            with self.subTest(database_url=database_url):
                self.assertFalse(target.is_literal_loopback_database_url(database_url))


if __name__ == "__main__":
    unittest.main()
