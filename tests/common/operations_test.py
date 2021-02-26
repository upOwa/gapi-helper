import io
import logging

import pytest

from gapi_helper.common.operations import execute


def nominal_callback() -> int:
    return 42


def error_callback(e: Exception) -> None:
    raise e


def test_nominal() -> None:
    assert execute(nominal_callback) == 42


def test_exception_noretry() -> None:
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    log_stream = io.StringIO()
    ch = logging.StreamHandler(log_stream)
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(ch)

    with pytest.raises(RuntimeError) as e:
        execute(lambda: error_callback(RuntimeError("error")), logger=logger)
    assert str(e.value) == "error"
    assert log_stream.getvalue() == ""


def test_exception_retry_failure() -> None:
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    log_stream = io.StringIO()
    ch = logging.StreamHandler(log_stream)
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(ch)

    with pytest.raises(RuntimeWarning) as e:
        execute(lambda: error_callback(RuntimeWarning("warning")), retry_delay=0, logger=logger)
    assert str(e.value) == "warning"
    assert (
        log_stream.getvalue()
        == """Failed 1 times (warning), retrying in 0 seconds...
Retrying...
Failed 2 times (warning), retrying in 0.0 seconds...
Retrying...
Failed 3 times (warning), retrying in 0.0 seconds...
Retrying...
Failed 4 times (warning), retrying in 0.0 seconds...
Retrying...
Failed 5 times (warning), retrying in 0.0 seconds...
Retrying...
Too many failures, abandonning
"""
    )
