#!/usr/bin/env python3
"""Fail-closed unit tests for the runner-owned Worker DB harness."""

from __future__ import annotations

import json
import os
import unittest
from pathlib import Path
from unittest import mock

import worker_control_plane_db_harness as harness


def container_inspect(*, port: int = 55432, project_id: str = "worker192-test") -> dict[str, object]:
    return {
        "Id": "a" * 64,
        "Name": f"/supabase_db_{project_id}",
        "Config": {"Labels": {
            harness.PROJECT_LABEL: project_id,
            harness.COMPOSE_LABEL: project_id,
        }},
        "State": {"Running": True},
        "NetworkSettings": {"Ports": {"5432/tcp": [
            {"HostIp": "0.0.0.0", "HostPort": str(port)},
            {"HostIp": "::", "HostPort": str(port)},
        ]}},
    }


class WorkerControlPlaneHarnessTest(unittest.TestCase):
    def test_caller_database_and_libpq_targets_are_replaced_not_forwarded(self) -> None:
        trusted = harness.TrustedDatabase(
            "postgresql://postgres:postgres@127.0.0.1:55432/postgres",
            "a" * 64,
            "123456789",
            "worker_harness_" + "b" * 32,
            "c" * 64,
        )
        hostile = {
            harness.CALLER_DATABASE_ENV: "postgresql://127.0.0.1:60000/postgres",
            "DATABASE_URL": "postgresql://hosted.example/postgres",
            "PGHOST": "relay.example",
            "PGHOSTADDR": "203.0.113.1",
            "PGOPTIONS": "-c application_name=relay",
            "WORKER_CONTROL_PLANE_HARNESS_SENTINEL": "caller-value",
        }
        with mock.patch.dict(os.environ, hostile, clear=True):
            environment = harness.cargo_environment(trusted)
        self.assertEqual(environment[harness.CALLER_DATABASE_ENV], trusted.url)
        for key in ("DATABASE_URL", "PGHOST", "PGHOSTADDR", "PGOPTIONS"):
            self.assertNotIn(key, environment)
        self.assertEqual(
            environment["WORKER_CONTROL_PLANE_HARNESS_SENTINEL"], trusted.sentinel
        )

    def test_caller_local_relay_and_hosted_database_urls_are_rejected(self) -> None:
        for database_url in (
            "postgresql://postgres:postgres@127.0.0.1:60000/postgres",
            "postgresql://postgres:postgres@localhost:60000/postgres",
            "postgresql://postgres:secret@db.production.supabase.co/postgres",
        ):
            with self.subTest(database_url=database_url), self.assertRaisesRegex(
                harness.HarnessError, "caller-supplied"
            ):
                harness.reject_caller_database_target(
                    {harness.CALLER_DATABASE_ENV: database_url}
                )

    def test_rejects_localhost_nss_relay_and_rebound_status_targets(self) -> None:
        for value in (
            "postgresql://postgres:postgres@localhost:55432/postgres",
            "postgresql://postgres:postgres@127.0.0.1:60000/postgres",
            "postgresql://postgres:postgres@127.0.0.1:55432/postgres?host=relay.example",
            "postgresql://postgres:postgres@[::1]:55432/postgres",
            "postgresql://postgres:postgres@127.0.0.2:55432/postgres",
        ):
            with self.subTest(value=value), self.assertRaises(harness.HarnessError):
                harness.validate_status_database_url(value, expected_port=55432)

    def test_container_binding_requires_exact_id_name_labels_and_port(self) -> None:
        project_id = "worker192-test"
        self.assertEqual(
            harness.validate_database_container(
                container_inspect(project_id=project_id),
                project_id=project_id,
                expected_port=55432,
            ),
            "a" * 64,
        )
        invalid = (
            {**container_inspect(), "Id": "short"},
            {**container_inspect(), "Name": "/supabase_db_other"},
            {**container_inspect(), "Config": {"Labels": {harness.PROJECT_LABEL: "other"}}},
            container_inspect(port=60000),
        )
        for inspect in invalid:
            with self.subTest(inspect=inspect), self.assertRaises(harness.HarnessError):
                harness.validate_database_container(
                    inspect, project_id=project_id, expected_port=55432
                )

    def test_exact_database_selection_tolerates_owned_stopped_init_container(self) -> None:
        project_id = "worker192-test"
        database = container_inspect(project_id=project_id)
        init = {
            **container_inspect(project_id=project_id),
            "Id": "b" * 64,
            "Name": f"/supabase_init_{project_id}",
            "State": {"Running": False},
        }
        resources = harness.Resources(containers=("a" * 64, "b" * 64))
        responses = (
            mock.Mock(stdout=json.dumps([database])),
            mock.Mock(stdout=json.dumps([init])),
        )
        with mock.patch.object(harness, "docker", side_effect=responses):
            self.assertEqual(
                harness.inspect_one_database_container(
                    "local", resources, project_id=project_id, expected_port=55432
                ),
                "a" * 64,
            )

    def test_remote_docker_endpoint_and_context_overrides_fail_closed(self) -> None:
        with mock.patch.dict(os.environ, {"DOCKER_HOST": "ssh://builder"}, clear=True):
            with self.assertRaisesRegex(harness.HarnessError, "overrides"):
                harness.require_local_docker_context()
        with mock.patch.dict(os.environ, {}, clear=True), \
                mock.patch.object(harness, "capture", side_effect=("remote", '"ssh://builder"')):
            with self.assertRaisesRegex(harness.HarnessError, "Unix-socket"):
                harness.require_local_docker_context()

    def test_cleanup_rejects_resources_without_exact_project_label(self) -> None:
        resources = harness.Resources(volumes=("owned-volume",))
        inspect = (
            '[{"Labels":{"com.supabase.cli.project":"other",'
            '"com.docker.compose.project":"other"}}]'
        )
        completed = mock.Mock(stdout=inspect)
        with mock.patch.object(harness, "docker", return_value=completed):
            with self.assertRaisesRegex(harness.HarnessError, "unowned volume"):
                harness.cleanup("local", resources, "worker192-test")

    def test_config_override_changes_only_random_identity_and_ports(self) -> None:
        original = (
            'project_id = "database-engine"\n\n[api]\nport = 55321\n\n[db]\n'
            '# Port to use for the local database URL.\nport = 55322\n'
            '# Port used by db diff command to initialize the shadow database.\nshadow_port = 55320\n'
        )
        with self.subTest(), mock.patch.object(Path, "read_text", return_value=original), \
                mock.patch.object(Path, "write_text") as write:
            harness.rewrite_local_config(
                Path("config.toml"), project_id="worker192-test", db_port=55432, shadow_port=55433
            )
        rendered = write.call_args.args[0]
        self.assertIn('project_id = "worker192-test"', rendered)
        self.assertIn("[api]\nport = 55321", rendered)
        self.assertIn("[db]\n# Port to use for the local database URL.\nport = 55432", rendered)
        self.assertIn("shadow_port = 55433", rendered)


if __name__ == "__main__":
    unittest.main()
