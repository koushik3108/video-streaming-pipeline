"""
Centralized logging configuration using Loguru.
"""

import sys
from pathlib import Path
from loguru import logger


def setup_logger(log_dir: str = "logs", level: str = "INFO") -> None:
    """Configure application-wide logging."""

    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    # Remove default handler
    logger.remove()

    # Console handler - colorized output
    logger.add(
        sys.stdout,
        level=level,
        colorize=True,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | "
               "<cyan>{module}</cyan>:<cyan>{function}</cyan> | <level>{message}</level>",
    )

    # File handler - rotating log files
    logger.add(
        log_path / "pipeline_{time:YYYY-MM-DD}.log",
        level=level,
        rotation="10 MB",
        retention="7 days",
        compression="zip",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {module}:{function}:{line} | {message}",
    )

    # Error-only log
    logger.add(
        log_path / "errors.log",
        level="ERROR",
        rotation="5 MB",
        retention="30 days",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {module}:{function}:{line} | {message}\n{exception}",
    )

    logger.info(f"Logger initialized. Level={level}, LogDir={log_path.resolve()}")


# Re-export logger for use across the app
__all__ = ["logger", "setup_logger"]
