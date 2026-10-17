"""Where the integration tests look for a container engine when `DOCKER_HOST` is unset.

Kept apart from `conftest.py` (which imports testcontainers) so a unit test can check it.
"""

import os


def default_docker_host(uid: int | None = None) -> str:
    """The rootless podman socket of `uid` (default: the current user)."""
    if uid is None:
        uid = os.getuid()
    return f"unix:///run/user/{uid}/podman/podman.sock"
