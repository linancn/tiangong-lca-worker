#!/usr/bin/env python3
"""Fail-closed unit tests for the runner-owned Worker DB harness."""

from __future__ import annotations

import json
import os
import signal
import socket
import stat
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import worker_control_plane_db_harness as harness


IMAGE_ID = "sha256:" + "d" * 64


def container_inspect(
    *,
    host_ip: str = "127.0.0.1",
    port: str = "55432",
    project_id: str = "worker192-test",
    bindings: list[dict[str, str]] | None = None,
) -> dict[str, object]:
    return {
        "Id": "a" * 64,
        "Name": f"/supabase_db_{project_id}",
        "Image": IMAGE_ID,
        "Config": {"Labels": {
            harness.PROJECT_LABEL: project_id,
            harness.COMPOSE_LABEL: project_id,
        }},
        "State": {"Running": True, "Health": {"Status": "healthy"}},
        "NetworkSettings": {"Ports": {"5432/tcp": bindings or [
            {"HostIp": host_ip, "HostPort": port},
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

    def test_container_binding_is_exactly_one_loopback_publication(self) -> None:
        project_id = "worker192-test"
        self.assertEqual(
            harness.validate_database_container(
                container_inspect(project_id=project_id),
                project_id=project_id,
                expected_image_id=IMAGE_ID,
            ),
            ("a" * 64, 55432),
        )
        invalid = (
            container_inspect(host_ip="0.0.0.0"),
            container_inspect(host_ip="::"),
            container_inspect(host_ip=""),
            container_inspect(bindings=[
                {"HostIp": "127.0.0.1", "HostPort": "55432"},
                {"HostIp": "127.0.0.1", "HostPort": "55432"},
            ]),
            container_inspect(port="60000x"),
        )
        for inspect in invalid:
            with self.subTest(inspect=inspect), self.assertRaises(harness.HarnessError):
                harness.validate_database_container(
                    inspect, project_id=project_id, expected_image_id=IMAGE_ID
                )

    def test_container_requires_exact_id_name_labels_image_and_health(self) -> None:
        invalid = (
            {**container_inspect(), "Id": "short"},
            {**container_inspect(), "Name": "/supabase_db_other"},
            {**container_inspect(), "Image": "sha256:" + "e" * 64},
            {**container_inspect(), "Config": {"Labels": {harness.PROJECT_LABEL: "worker192-test"}}},
            {**container_inspect(), "State": {"Running": True, "Health": {"Status": "starting"}}},
        )
        for inspect in invalid:
            with self.subTest(inspect=inspect), self.assertRaises(harness.HarnessError):
                harness.validate_database_container(
                    inspect,
                    project_id="worker192-test",
                    expected_image_id=IMAGE_ID,
                )

    def test_remote_docker_endpoint_and_all_context_overrides_fail_closed(self) -> None:
        for name in harness.DOCKER_OVERRIDE_ENV:
            with self.subTest(name=name), mock.patch.dict(
                os.environ, {name: "hostile"}, clear=True
            ), self.assertRaisesRegex(harness.HarnessError, "overrides"):
                harness.require_local_docker_endpoint(Path("unused"))
        with mock.patch.dict(os.environ, {}, clear=True), mock.patch.object(
            harness, "capture", side_effect=("remote", '"ssh://builder"')
        ), self.assertRaisesRegex(harness.HarnessError, "Unix-socket"):
            harness.require_local_docker_endpoint(Path("unused"))

    def test_endpoint_pins_real_socket_inode_and_daemon_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            socket_path = Path(directory) / "docker.sock"
            listener = socket.socket(socket.AF_UNIX)
            listener.bind(str(socket_path))
            try:
                answers = (
                    "local",
                    json.dumps(f"unix://{socket_path}"),
                    '"daemon-id" "daemon-name"',
                    '"daemon-id" "daemon-name"',
                )
                with mock.patch.dict(os.environ, {}, clear=True), mock.patch.object(
                    harness, "capture", side_effect=answers
                ):
                    endpoint = harness.require_local_docker_endpoint(
                        Path(directory) / "docker-config"
                    )
            finally:
                listener.close()
        self.assertEqual(endpoint.daemon_id, "daemon-id")
        self.assertEqual(endpoint.daemon_name, "daemon-name")
        self.assertNotIn("DOCKER_CONTEXT", endpoint.environment())
        self.assertEqual(
            endpoint.environment()["DOCKER_HOST"], f"unix://{socket_path.resolve()}"
        )

    def test_endpoint_revalidation_rejects_daemon_identity_change(self) -> None:
        endpoint = harness.DockerEndpoint(
            "unix:///tmp/docker.sock", Path("/tmp/docker.sock"), 1, 2,
            "daemon-id", "daemon-name", Path("/tmp/docker-config"),
        )
        fake_stat = mock.Mock(st_mode=stat.S_IFSOCK, st_dev=1, st_ino=2)
        with mock.patch.object(Path, "resolve", return_value=Path("/tmp/docker.sock")), \
                mock.patch.object(Path, "stat", return_value=fake_stat), \
                mock.patch.object(harness, "_daemon_identity", return_value=("other", "daemon-name")), \
                self.assertRaisesRegex(harness.HarnessError, "daemon identity"):
            endpoint.assert_stable()

    def test_resource_queries_require_both_ownership_labels(self) -> None:
        endpoint = mock.Mock(spec=harness.DockerEndpoint)
        completed = mock.Mock(stdout="")
        with mock.patch.object(harness, "docker", return_value=completed) as docker:
            self.assertEqual(
                harness.resources_for_project(endpoint, "worker192-test"),
                ((), (), ()),
            )
        for call in docker.call_args_list:
            arguments = call.args
            self.assertIn(f"label={harness.PROJECT_LABEL}=worker192-test", arguments)
            self.assertIn(f"label={harness.COMPOSE_LABEL}=worker192-test", arguments)

    def test_cleanup_rejects_exact_resource_without_both_labels(self) -> None:
        endpoint = mock.Mock(spec=harness.DockerEndpoint)
        resources = harness.Resources(volume_name="owned-volume")
        with mock.patch.object(
            harness,
            "_resource_labels",
            return_value={harness.PROJECT_LABEL: "worker192-test"},
        ), self.assertRaisesRegex(harness.HarnessError, "both exact ownership labels"):
            harness.cleanup(endpoint, resources, "worker192-test")

    def test_cancellation_runs_exact_cleanup(self) -> None:
        endpoint = mock.Mock(spec=harness.DockerEndpoint)
        resources = harness.Resources(
            container_id="a" * 64,
            volume_name="supabase_db_worker192-test",
            network_id="b" * 64,
        )

        def cancel() -> None:
            harness.cancellation_signal_handler(signal.SIGTERM, None)

        with mock.patch.object(harness, "cleanup") as cleanup, self.assertRaises(
            harness.HarnessCancelled
        ):
            harness.run_owned_action(endpoint, resources, "worker192-test", cancel)
        cleanup.assert_called_once_with(endpoint, resources, "worker192-test")

    def test_pinned_image_is_immutable_registry_digest(self) -> None:
        self.assertRegex(
            harness.EXPECTED_POSTGRES_IMAGE,
            r"^public\.ecr\.aws/supabase/postgres@sha256:[0-9a-f]{64}$",
        )


if __name__ == "__main__":
    unittest.main()
