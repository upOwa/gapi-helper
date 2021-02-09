import googleapiclient
import pytest

from gapi_helper.sheets.operations import bulkappend, bulkclean, bulkupdate, bulkwrite, removefilter

from .conftest import read_datafile


def test_removefilter(request_mock) -> None:
    request_mock._iterable.append(
        ({"status": "200"}, read_datafile("remove_filter.json", "rb")),
    )

    removefilter("myFakeGoogleSpreadsheetId", 0)


def test_bulkupdate(request_mock) -> None:
    request_mock._iterable.append(
        ({"status": "200"}, read_datafile("get_values.json", "rb")),
    )
    request_mock._iterable.append(
        ({"status": "200"}, read_datafile("bulkupdate.json", "rb")),
    )

    index = bulkupdate(
        ["14802", "14945"],
        "myFakeGoogleSpreadsheetId",
        "Test",
        0,
        "MY TAB",
        "C2:C",
        1,
        5,
        {
            "14802": "abcdef",
            "14945": "012345",
        },
        remove_filter=False,
    )
    assert index == [(2, "abcdef"), (7, "012345")]


def test_bulkclean(request_mock) -> None:
    request_mock._iterable.append(
        ({"status": "200"}, read_datafile("bulkclean.json", "rb")),
    )

    bulkclean(
        "myFakeGoogleSpreadsheetId",
        "Test",
        0,
        "MY TAB",
        "F2:F",
        remove_filter=False,
    )


def test_bulkwrite(request_mock) -> None:
    request_mock._iterable.append(
        ({"status": "200"}, read_datafile("bulkwrite.json", "rb")),
    )

    bulkwrite(
        [
            ["ROW1,COL1", "ROW1,COL2", "ROW1,COL3"],
            ["ROW2,COL1", "ROW2,COL2", "ROW2,COL3"],
            ["ROW3,COL1", "ROW3,COL2", "ROW3,COL3"],
            ["ROW4,COL1", "ROW4,COL2", "ROW4,COL3"],
        ],
        "myFakeGoogleSpreadsheetId",
        "Test",
        0,
        "MY TAB",
        0,
        0,
        remove_filter=False,
    )


def test_bulkappend(request_mock) -> None:
    request_mock._iterable.append(
        ({"status": "200"}, read_datafile("bulkappend.json", "rb")),
    )

    bulkappend(
        [
            ["ROW1,COL1", "ROW1,COL2", "ROW1,COL3"],
            ["ROW2,COL1", "ROW2,COL2", "ROW2,COL3"],
            ["ROW3,COL1", "ROW3,COL2", "ROW3,COL3"],
            ["ROW4,COL1", "ROW4,COL2", "ROW4,COL3"],
        ],
        "myFakeGoogleSpreadsheetId",
        "Test",
        0,
        "MY TAB",
        "A1:C",
        remove_filter=False,
    )


def test_removefilter_unauthorized(request_mock) -> None:
    request_mock._iterable.append(
        ({"status": "403"}, read_datafile("unauthorized.json", "rb")),
    )
    with pytest.raises(googleapiclient.errors.HttpError, match="The caller does not have permission"):
        removefilter("unauthorized", 0)


def test_bulkappend_limitreached(request_mock) -> None:
    request_mock._iterable.append(
        ({"status": "400"}, read_datafile("limitreached.json", "rb")),
    )
    with pytest.raises(
        googleapiclient.errors.HttpError,
        match="This action would increase the number of cells in the workbook above the limit of 5000000 cells",
    ):
        bulkappend(
            [
                ["ROW1,COL1", "ROW1,COL2", "ROW1,COL3"],
                ["ROW2,COL1", "ROW2,COL2", "ROW2,COL3"],
                ["ROW3,COL1", "ROW3,COL2", "ROW3,COL3"],
                ["ROW4,COL1", "ROW4,COL2", "ROW4,COL3"],
            ],
            "myFakeGoogleSpreadsheetId",
            "Test",
            0,
            "MY TAB",
            "A1:C",
            remove_filter=False,
        )
