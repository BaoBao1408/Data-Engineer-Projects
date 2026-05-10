"""
config/logging_conf.py — Structured logging setup.

Call configure_logging() once at pipeline entry point.
AWS: logs ship to CloudWatch via awslogs driver (docker) or Lambda built-in handler.
"""
import logging
import sys
from datetime import date
from pathlib import Path

from temp.config.base import settings


def configure_logging(run_date: date = None) -> None:
    log_dir: Path = settings.log_dir
    log_dir.mkdir(exist_ok=True)

    suffix = str(run_date or date.today())
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_dir / f"pipeline_{suffix}.log"),
    ]

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        handlers=handlers,
        force=True,
    )

    # Silence noisy third-party loggers
    logging.getLogger("prefect").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
