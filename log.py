import logging
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


def get_logger(name) -> logging.Logger:
    handler = logging.StreamHandler()
    handler.setFormatter(ColoredLogger("%(levelname)s: %(message)s"))
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    return logger
