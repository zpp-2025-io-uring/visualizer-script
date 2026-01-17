import logging

from colorama import Fore

_level = logging.NOTSET


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


def set_level(new_level: str):
    global _level
    match new_level:
        case "debug":
            _level = logging.DEBUG
        case "info":
            _level = logging.INFO
        case "warning":
            _level = logging.WARNING
        case "error":
            _level = logging.ERROR
        case "critical":
            _level = logging.CRITICAL


def get_logger(name: str | None) -> logging.Logger:
    handler = logging.StreamHandler()
    handler.setFormatter(ColoredLogger("%(levelname)s: %(message)s"))
    logger = logging.getLogger(name)
    logger.setLevel(_level)
    logger.addHandler(handler)
    return logger
