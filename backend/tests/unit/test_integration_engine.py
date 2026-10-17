import os

from tests.integration.engine import default_docker_host


def test_default_docker_host_is_the_given_users_podman_socket() -> None:
    assert default_docker_host(4242) == "unix:///run/user/4242/podman/podman.sock"


def test_default_docker_host_follows_the_current_uid() -> None:
    assert default_docker_host() == f"unix:///run/user/{os.getuid()}/podman/podman.sock"
