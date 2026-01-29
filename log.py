import logging
from pathlib import Path

from colorama import Fore


class ColoredLogger(logging.Formatter):
    def format(self, record) -> str:
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


def _get_logger(name: str | None) -> logging.Logger:
    handler = logging.StreamHandler()
    handler.setFormatter(ColoredLogger("%(levelname)s: %(message)s"))
    logger = logging.getLogger(name)
    logger.setLevel(logging.NOTSET)
    logger.addHandler(handler)
    return logger


class __Logger:
    logger = _get_logger(None)


def set_level(new_level: str):
    match new_level:
        case "debug":
            __Logger.logger.setLevel(logging.DEBUG)
        case "info":
            __Logger.logger.setLevel(logging.INFO)
        case "warning":
            __Logger.logger.setLevel(logging.WARNING)
        case "error":
            __Logger.logger.setLevel(logging.ERROR)
        case "critical":
            __Logger.logger.setLevel(logging.CRITICAL)


def get_logger() -> logging.Logger:
    return __Logger.logger


logger = get_logger()


def warn_if_not_release(path: Path):
    if "release" not in path.parts:
        logger.warning(f"Path does not contain release: {path}")
