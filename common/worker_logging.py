import time


_LAST_LOG_AT: dict[tuple[str, str], float] = {}
_RATE_LIMIT_SECONDS = 5.0


def _should_log(component: str, key: str) -> bool:
    now = time.monotonic()
    lookup = (component, key)
    last = _LAST_LOG_AT.get(lookup)
    if last is not None and now - last < _RATE_LIMIT_SECONDS:
        return False
    _LAST_LOG_AT[lookup] = now
    return True


def log_worker_exception(logger, component: str, worker_name: str, exc: Exception) -> None:
    """
    Classify common transient Redis/Docker restart failures so logs explain
    whether a worker crashed unexpectedly or is simply retrying after a
    dependency restart.
    """
    message = str(exc)

    if "Name or service not known" in message:
        if _should_log(component, "dns"):
            logger.warning(
                f"[{component}] {worker_name}: dependency hostname is temporarily missing from Docker DNS; retrying"
            )
        return

    if "Connection closed by server" in message:
        if _should_log(component, "closed"):
            logger.warning(
                f"[{component}] {worker_name}: Redis closed the connection during a restart/shutdown; retrying"
            )
        return

    if "Connection reset by peer" in message:
        if _should_log(component, "reset"):
            logger.warning(
                f"[{component}] {worker_name}: Redis reset the connection; retrying"
            )
        return

    if "Connection refused" in message:
        if _should_log(component, "refused"):
            logger.warning(
                f"[{component}] {worker_name}: dependency is not accepting connections yet; retrying"
            )
        return

    logger.error(f"[{component}] {worker_name} crashed unexpectedly: {message}")
