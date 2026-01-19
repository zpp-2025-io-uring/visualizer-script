import logging
from pathlib import Path

from colorama import Fore


class ColoredLogger(logging.Formatter):
    def format(self, record):
        match record.levelno:
            case logging.DEBUG:
                color = Fore.WHITE
            case logging.INFO:
                color = Fore.BLUE
            case logging.WARNING:
                color = Fore.YELLOW
            case logging.ERROR:
                color = Fore.RED
            case logging.CRITICAL:
                color = Fore.RED

        message = super().format(record)
        return f"{color}{message}{Fore.RESET}"


class LoggerLevel:
    level = logging.NOTSET


def set_level(new_level: str):
    match new_level:
        case "debug":
            LoggerLevel.level = logging.DEBUG
        case "info":
            LoggerLevel.level = logging.INFO
        case "warning":
            LoggerLevel.level = logging.WARNING
        case "error":
            LoggerLevel.level = logging.ERROR
        case "critical":
            LoggerLevel.level = logging.CRITICAL


def get_logger(name: str | None) -> logging.Logger:
    handler = logging.StreamHandler()
    handler.setFormatter(ColoredLogger("%(levelname)s: %(message)s"))
    logger = logging.getLogger(name)
    logger.setLevel(LoggerLevel.level)
    logger.addHandler(handler)
    return logger


logger = get_logger(__name__)


def warn_if_not_release(path: Path):
    if "release" not in path.parts:
        logger.warning(f"Path does not contain release: {path}")
