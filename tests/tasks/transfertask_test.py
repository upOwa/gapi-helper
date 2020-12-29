import io
import logging
from typing import Collection, Iterable

import pytest
from simpletasks import Task

from gapi_helper.sheets import Range, Sheet, SheetsService, Spreadsheet
from gapi_helper.tasks.transfertask import TransferDestination, TransferredRange, TransferTask
from gapi_helper.tasks.types import Row


@pytest.fixture(scope="function")
def configure():
    DEBUGGING_old = Task.DEBUGGING
    TESTING_old = Task.TESTING
    LOGGER_NAMESPACE_old = Task.LOGGER_NAMESPACE

    Task.DEBUGGING = False
    Task.TESTING = False
    Task.LOGGER_NAMESPACE = "gapi_helper."
    logger = logging.getLogger("gapi_helper")
    logger.setLevel(logging.DEBUG)

    _logger_old = SheetsService._logger
    SheetsService._logger = logging.getLogger("gapi_helper")

    yield Task

    Task.DEBUGGING = DEBUGGING_old
    Task.TESTING = TESTING_old
    Task.LOGGER_NAMESPACE = LOGGER_NAMESPACE_old

    SheetsService._logger = _logger_old


class MyTask(TransferTask):
    def getData(self) -> Iterable[Row]:
        return [
            ["A1", "B1"],
            ["A2", "B2"],
            ["A3", "B3"],
        ]

    def skipHeader(self, idx: int, row: Row) -> bool:
        return idx > 0

    def dupCols(self, idx: int, row: Row) -> Row:
        return list(row) + list(row)

    def getDestinations(self) -> Collection[TransferDestination]:
        spreadsheet1 = Spreadsheet("abcdef", "Spreadsheet1")
        sheet1 = Sheet(spreadsheet1, "tab1", 0)
        sheet2 = Sheet(spreadsheet1, "tab2", 1)

        spreadsheet2 = Spreadsheet("0123456789", "Spreadsheet2")
        sheet3 = Sheet(spreadsheet2, "tab3", 2)
        sheet4 = Sheet(spreadsheet2, "tab4", 3)

        return [
            TransferDestination(sheet1, [TransferredRange(Range.fromA1N1("A1:B"), Range.fromA1N1("A1:*"))]),
            TransferDestination(
                sheet2,
                [
                    TransferredRange(Range.fromA1N1("A1:A"), Range.fromA1N1("A2:A")),
                    TransferredRange(Range.fromA1N1("B1:B"), Range.fromA1N1("B2:B")),
                ],
            ),
            TransferDestination(
                sheet3,
                [TransferredRange(Range.fromA1N1("A1:B"), Range.fromA1N1("A1:*"), clean=True)],
                filter=self.skipHeader,
            ),
            TransferDestination(
                sheet4,
                [
                    TransferredRange(
                        Range.fromA1N1("A1:B"), Range.fromA1N1("A1:*"), adapter=self.dupCols, clean=True
                    )
                ],
                filter=self.skipHeader,
            ),
        ]


class StaticRangeErrorTask(TransferTask):
    def getData(self) -> Iterable[Row]:
        return [
            ["A1", "B1"],
            ["A2", "B2"],
            ["A3", "B3"],
        ]

    def getDestinations(self) -> Collection[TransferDestination]:
        spreadsheet1 = Spreadsheet("abcdef", "Spreadsheet1")
        sheet1 = Sheet(spreadsheet1, "tab1", 0)

        return [
            TransferDestination(sheet1, [TransferredRange(Range.fromA1N1("A1:B"), Range.fromA1N1("A1:A"))]),
        ]


class DynamicRangeErrorTask(TransferTask):
    def getData(self) -> Iterable[Row]:
        return [
            ["A1", "B1"],
            ["A2", "B2"],
            ["A3", "B3"],
        ]

    def getDestinations(self) -> Collection[TransferDestination]:
        spreadsheet1 = Spreadsheet("abcdef", "Spreadsheet1")
        sheet1 = Sheet(spreadsheet1, "tab1", 0)

        return [
            TransferDestination(
                sheet1,
                [TransferredRange(Range.fromA1N1("A1:B"), Range.fromA1N1("A1:A"), adapter=lambda i, x: x)],
            ),
        ]


def test_init(configure) -> None:
    o = MyTask(dryrun=True)

    logger = io.StringIO()
    ch = logging.StreamHandler(logger)
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(logging.Formatter("%(name)s - %(levelname)s - %(message)s"))
    logging.getLogger("gapi_helper").addHandler(ch)

    o.run()

    assert (
        logger.getvalue()
        == """gapi_helper - INFO - Removing filter in abcdef (0)
gapi_helper.MyTask - INFO - Writing to abcdef (Spreadsheet1) in 0 (tab1) (size=21)...
gapi_helper.MyTask - DEBUG - Sending: [{'pasteData': {'data': 'A1,B1\\r\\nA2,B2\\r\\nA3,B3\\r\\n', 'type': 'PASTE_NORMAL', 'delimiter': ',', 'coordinate': {'sheetId': 0, 'rowIndex': 0, 'columnIndex': 0}}}]
gapi_helper.MyTask - INFO - Stubbed
gapi_helper.MyTask - INFO - Done writing to abcdef (Spreadsheet1): 1 requests sent
gapi_helper - INFO - Removing filter in abcdef (1)
gapi_helper.MyTask - INFO - Writing to abcdef (Spreadsheet1) in 1 (tab2) (size=24)...
gapi_helper.MyTask - DEBUG - Sending: [{'pasteData': {'data': 'A1\\r\\nA2\\r\\nA3\\r\\n', 'type': 'PASTE_NORMAL', 'delimiter': ',', 'coordinate': {'sheetId': 1, 'rowIndex': 1, 'columnIndex': 0}}}, {'pasteData': {'data': 'B1\\r\\nB2\\r\\nB3\\r\\n', 'type': 'PASTE_NORMAL', 'delimiter': ',', 'coordinate': {'sheetId': 1, 'rowIndex': 1, 'columnIndex': 1}}}]
gapi_helper.MyTask - INFO - Stubbed
gapi_helper.MyTask - INFO - Done writing to abcdef (Spreadsheet1): 1 requests sent
gapi_helper - INFO - Removing filter in 0123456789 (2)
gapi_helper - INFO - Cleaning 'tab3'!A1:B 0123456789 (Spreadsheet2) in 2 (tab3)...
gapi_helper.MyTask - INFO - Writing to 0123456789 (Spreadsheet2) in 2 (tab3) (size=14)...
gapi_helper.MyTask - DEBUG - Sending: [{'pasteData': {'data': 'A2,B2\\r\\nA3,B3\\r\\n', 'type': 'PASTE_NORMAL', 'delimiter': ',', 'coordinate': {'sheetId': 2, 'rowIndex': 0, 'columnIndex': 0}}}]
gapi_helper.MyTask - INFO - Stubbed
gapi_helper.MyTask - INFO - Done writing to 0123456789 (Spreadsheet2): 1 requests sent
gapi_helper - INFO - Removing filter in 0123456789 (3)
gapi_helper - INFO - Cleaning 'tab4'!A1:D 0123456789 (Spreadsheet2) in 3 (tab4)...
gapi_helper.MyTask - INFO - Writing to 0123456789 (Spreadsheet2) in 3 (tab4) (size=26)...
gapi_helper.MyTask - DEBUG - Sending: [{'pasteData': {'data': 'A2,B2,A2,B2\\r\\nA3,B3,A3,B3\\r\\n', 'type': 'PASTE_NORMAL', 'delimiter': ',', 'coordinate': {'sheetId': 3, 'rowIndex': 0, 'columnIndex': 0}}}]
gapi_helper.MyTask - INFO - Stubbed
gapi_helper.MyTask - INFO - Done writing to 0123456789 (Spreadsheet2): 1 requests sent
"""
    )


def test_staticrangeerror(configure) -> None:
    with pytest.raises(ValueError) as e:
        o = StaticRangeErrorTask(dryrun=True)
        o.run()
    assert (
        str(e.value)
        == "destination dimension does not match source dimension for (0, 0, 1, None)/A1:B->(0, 0, 0, None)/A1:A"
    )


def test_dynamicrangeerror(configure) -> None:
    with pytest.raises(ValueError) as e:
        o = DynamicRangeErrorTask(dryrun=True)
        o.run()
    assert str(e.value) == "destination dimension does not match data dimension: expected 1, got 2"
