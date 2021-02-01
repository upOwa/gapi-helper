from typing import List

import click
from simpletasks.cli import CliParams, _click_parameter


def testing() -> _click_parameter:
    """Adds use-testing flag.

    In `TransferTask` objects, the parameter value can directly be retrieved via `self.use_testing`

    Returns:
    - _click_parameter: parameter
    """
    return click.option(
        "--use-testing", is_flag=True, default=False, help="Use testing Google Sheets for writing"
    )


def default_task_options() -> List[_click_parameter]:
    """Sets dryrun, date and testing options.

    Returns:
    - List[_click_parameter]: List of parameters for Tasks
    """
    return [
        CliParams.dryrun(),
        CliParams.date(),
        testing(),
    ]
