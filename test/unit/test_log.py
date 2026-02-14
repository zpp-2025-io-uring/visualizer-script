import logging

from log import get_logger


def test_get_logger_does_not_configure_root_and_has_expected_name() -> None:
    root_handlers_before = list(logging.getLogger().handlers)

    logger = get_logger()

    # Our logger should not be the root logger
    assert logger is not logging.getLogger()
    assert logger.name == "io_tester_visualizer"

    # Importing/creating our logger must not add handlers to the root logger
    assert logging.getLogger().handlers == root_handlers_before

    # Ensure we don't propagate to avoid affecting other libraries
    assert logger.propagate is False
