from __future__ import annotations

import os

import redis
from redis.sentinel import Sentinel


DEFAULT_REDIS_PORT = 6379
DEFAULT_SENTINEL_PORT = 26379


def _env(prefix: str, name: str, default: str | None = None) -> str | None:
    return os.getenv(f"{prefix}_{name}", default)


def _parse_bool(raw: str | None, default: bool) -> bool:
    if raw is None:
        return default
    return raw.lower() in {"1", "true", "yes", "on"}


def _parse_sentinel_nodes(raw: str) -> list[tuple[str, int]]:
    nodes: list[tuple[str, int]] = []
    for part in raw.split(","):
        node = part.strip()
        if not node:
            continue
        host, sep, port_raw = node.partition(":")
        port = int(port_raw) if sep and port_raw else DEFAULT_SENTINEL_PORT
        nodes.append((host, port))
    if not nodes:
        raise ValueError("Sentinel mode requires at least one sentinel node")
    return nodes


def create_redis_client(
    prefix: str = "REDIS",
    *,
    socket_connect_timeout: float = 2,
    socket_timeout: float = 2,
    health_check_interval: int = 30,
) -> redis.Redis:
    password = _env(prefix, "PASSWORD")
    db = int(_env(prefix, "DB", "0") or "0")
    retry_on_timeout = _parse_bool(_env(prefix, "RETRY_ON_TIMEOUT"), True)
    sentinel_nodes = _env(prefix, "SENTINELS")

    if sentinel_nodes:
        master_name = _env(prefix, "MASTER_NAME")
        if not master_name:
            raise ValueError(f"{prefix}_MASTER_NAME is required when {prefix}_SENTINELS is set")

        sentinel_kwargs: dict[str, object] = {
            "socket_connect_timeout": socket_connect_timeout,
            "socket_timeout": socket_timeout,
        }
        sentinel_password = _env(prefix, "SENTINEL_PASSWORD")
        if sentinel_password:
            sentinel_kwargs["password"] = sentinel_password

        sentinel = Sentinel(
            _parse_sentinel_nodes(sentinel_nodes),
            sentinel_kwargs=sentinel_kwargs,
        )
        return sentinel.master_for(
            master_name,
            password=password,
            db=db,
            socket_connect_timeout=socket_connect_timeout,
            socket_timeout=socket_timeout,
            retry_on_timeout=retry_on_timeout,
            health_check_interval=health_check_interval,
        )

    host = _env(prefix, "HOST")
    if not host:
        raise ValueError(f"{prefix}_HOST is required when {prefix}_SENTINELS is not set")

    return redis.Redis(
        host=host,
        port=int(_env(prefix, "PORT", str(DEFAULT_REDIS_PORT)) or DEFAULT_REDIS_PORT),
        password=password,
        db=db,
        socket_connect_timeout=socket_connect_timeout,
        socket_timeout=socket_timeout,
        retry_on_timeout=retry_on_timeout,
        health_check_interval=health_check_interval,
    )
