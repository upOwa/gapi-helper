from typing import List

import click

from simpletasks.cli import CliParams, _click_parameter


def testing() -> _click_parameter:
    return click.option(
        "--use-testing", is_flag=True, default=False, help="Use testing Google Sheets for writing"
    )


def default_task_options() -> List[_click_parameter]:
    return [
        CliParams.dryrun(),
        CliParams.date(),
        testing(),
    ]
