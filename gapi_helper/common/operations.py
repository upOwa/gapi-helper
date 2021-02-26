import logging
import time
from typing import Callable, Optional, TypeVar

import googleapiclient

X = TypeVar("X")


def execute(
    callback: Callable[[], X], retry_delay: float = 5.0, logger: Optional[logging.Logger] = None
) -> X:
    failures = 0
    delay = retry_delay
    while True:
        try:
            if failures > 0 and logger is not None:
                logger.info("Retrying...")
            return callback()

        except Exception as e:
            if isinstance(e, googleapiclient.errors.HttpError):
                if e.resp.status in [403, 400]:
                    raise e
            elif isinstance(e, RuntimeError):
                raise e

            failures += 1
            if failures > 5:
                if logger is not None:
                    logger.warning("Too many failures, abandonning")
                raise e

            if logger is not None:
                logger.warning("Failed {} times ({}), retrying in {} seconds...".format(failures, e, delay))
            time.sleep(delay)
            delay *= 1.5
