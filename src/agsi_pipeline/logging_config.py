from __future__ import annotations

import logging


def configure_logging(level: str | int = "INFO") -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
        force=True,
    )


def log_progress(
    logger: logging.Logger,
    current: int,
    total: int,
    interval: int,
    msg: str,
    *args: object,
) -> None:
    if total == 0:
        return
    if current == 1 or current == total or current % interval == 0:
        logger.info(msg, *args)
