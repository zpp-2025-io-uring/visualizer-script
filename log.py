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


_DEFAULT_LOGGER_NAME = "io_tester_visualizer"


def _get_logger() -> logging.Logger:
    logger = logging.getLogger(_DEFAULT_LOGGER_NAME)

    handler = logging.StreamHandler()
    handler.setFormatter(ColoredLogger("%(levelname)s: %(message)s"))
    logger.addHandler(handler)

    logger.setLevel(logging.NOTSET)

    # Prevent our logger from propagating to ancestor/root loggers
    # — this avoids influencing other libraries such as `kaleido`.
    logger.propagate = False

    return logger


class __Logger:
    logger = _get_logger()


def set_level(new_level: str) -> None:
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


def warn_if_not_release(path: Path) -> None:
    if "release" not in path.parts:
        logger.warning(f"Path does not contain release: {path}")
