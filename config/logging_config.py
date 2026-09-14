"""
config/logging_config.py  –  Shared logging setup for all scrapers and orchestrators.

Usage:
    from config.logging_config import setup_logging
    logger = setup_logging("my_module")
"""

from __future__ import annotations

import logging
import os
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def setup_logging(
    name: str,
    log_dir: str | None = None,
    filename_prefix: str | None = None,
    level: int = logging.INFO,
) -> logging.Logger:
    """Create a logger that writes to both a timestamped file and stdout.

    Args:
        name: Logger name (typically __name__ from the caller).
        log_dir: Directory for log files (defaults to ROOT/logs).
        filename_prefix: Prefix for the log filename (defaults to name).
        level: Logging level.

    Returns:
        Configured logging.Logger instance.
    """
    log_dir = log_dir or os.path.join(ROOT, "logs")
    prefix = filename_prefix or name.replace("__", "").replace(".", "_").strip("_")
    os.makedirs(log_dir, exist_ok=True)

    log_filename = os.path.join(
        log_dir,
        f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
    )

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid adding duplicate handlers on re-import
    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )
        fh = logging.FileHandler(log_filename, encoding="utf-8")
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    logger.log_file = log_filename
    return logger


def get_logger(name: str | None = None) -> logging.Logger:
    """Return the root logger for the scraper package (no new handlers)."""
    return logging.getLogger(name)
