import logging


class ColoredLogger(logging.Formatter):
    def format(self, record):
        message = super().format(record)
        return f"{message}"


def get_logger(name) -> logging.Logger:
    handler = logging.StreamHandler()
    handler.setFormatter(ColoredLogger("%(levelname)s: %(message)s"))
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    return logger
