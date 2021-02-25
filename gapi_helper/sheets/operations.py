import csv
import io
import time
from typing import Any, Dict, Iterable, List, Tuple, Union, cast

import googleapiclient

from .client import SheetsService


def _handleException(e: Exception, spreadsheet_id: str) -> None:
    if isinstance(e, googleapiclient.errors.HttpError):
        if e.resp.status == 403:
            SheetsService._logger.critical(
                "Forbidden: cannot access spreadsheet_id={}".format(spreadsheet_id)
            )
            raise e
        elif e.resp.status == 400:
            SheetsService._logger.critical(
                "Cannot perform operation on spreadsheet_id={}".format(spreadsheet_id)
            )
            raise e
    elif isinstance(e, RuntimeError):
        raise e


def removefilter(spreadsheet_id: str, tab_id: int, dryrun: bool = False) -> None:
    """Removes any active filter on the sheet.

    This method should probably not be called directly; use Sheet.removeFilter instead.

    Args:
    - spreadsheet_id (str): Spreadsheet ID
    - tab_id (int): Sheet ID
    - dryrun (bool, optional): If True, does not actually do anything. Defaults to False.

    Raises:
    - Exception: Failure after 5 retries
    """
    if SheetsService._force_test_spreadsheet:
        spreadsheet_id = SheetsService.getTestSpreadsheet().parent.spreadsheet_id
        tab_id = cast(int, SheetsService.getTestSpreadsheet().tab_id)

    remove_filter_spreadsheet_request_body = {"requests": [{"clearBasicFilter": {"sheetId": tab_id}}]}
    failures = 0
    delay = SheetsService._retry_delay
    while True:
        try:
            SheetsService._logger.info("Removing filter in {} ({})".format(spreadsheet_id, tab_id))
            if not dryrun:
                service = SheetsService.getService()
                with SheetsService._lock:
                    service.spreadsheets().batchUpdate(
                        spreadsheetId=spreadsheet_id, body=remove_filter_spreadsheet_request_body
                    ).execute()
            break
        except Exception as e:
            _handleException(e, spreadsheet_id)

            failures += 1
            if failures > 5:
                SheetsService._logger.warning("Too many failures removing filter, abandonning")
                raise e

            # Retry
            SheetsService._logger.warning(
                "Failed removing filter {} times ({}), retrying in {} seconds...".format(failures, e, delay)
            )
            time.sleep(delay)
            SheetsService.reset()
            delay *= 1.5


def bulkupdate(
    keys: Iterable[str],
    spreadsheet_id: str,
    spreadsheet_name: str,
    tab_id: int,
    tab_name: str,
    source_range: str,
    destination_row_offset: int,
    destination_column: int,
    destination_value: Union[str, Dict[str, str]],
    dryrun: bool = False,
    remove_filter: bool = True,
) -> List[Tuple[int, str]]:
    """Updates a column based on the value of another column.

    This method should probably not be called directly; use Sheet.bulkUpdate instead.

    Args:
    - keys (Iterable[str]): Keys to update
    - spreadsheet_id (str): Spreadsheet ID to update
    - spreadsheet_name (str): Spreadsheet name to update
    - tab_id (int): Sheet ID to update
    - tab_name (str): Sheet name to update
    - source_range (str): Column and range which contains keys (e.g. "'CONTRATS SIGNES'!Z3:Z" ou "Z3:Z" - tab name is optional, and filled from tab_name argument if needed)
    - destination_row_offset (int): Offset from header (0-indexed) - typically line of source_range minus 1
    - destination_column (int): Index of the column to update in the sheet (0-indexed)
    - destination_value (Union[str, Dict[str, str]]): Values to put in the sheet:
        - Either a single value (will put the same value for all keys)
        - Or a map key=>value (will put the value corresponding to the key)
    - dryrun (bool, optional): If True, does not actually do anything. Defaults to False.
    - remove_filter (bool, optional): If True, removes any active filter before doing anything. Defaults to True.

    Raises:
    - Exception: Failure after 5 retries

    Returns:
    - List[Tuple[int, str]]: List of tuples (line number update, new value)
    """
    if SheetsService._force_test_spreadsheet:
        s = SheetsService.getTestSpreadsheet()
        spreadsheet_id = s.parent.spreadsheet_id
        spreadsheet_name = cast(str, s.parent.spreadsheet_name)
        tab_id = cast(int, s.tab_id)
        tab_name = s.tab_name

    if remove_filter:
        removefilter(spreadsheet_id, tab_id, dryrun=dryrun)

    if "!" in source_range:
        fullSourceRange = source_range
    else:
        fullSourceRange = "'{}'!{}".format(tab_name.replace("'", "\\'"), source_range)

    failures = 0
    delay = SheetsService._retry_delay
    while True:
        try:
            SheetsService._logger.info(
                "Getting values from {} {} ({}) in {} ({})...".format(
                    fullSourceRange, spreadsheet_id, spreadsheet_name, tab_id, tab_name
                )
            )
            service = SheetsService.getService()
            with SheetsService._lock:
                result = (
                    service.spreadsheets()
                    .values()
                    .get(spreadsheetId=spreadsheet_id, range=fullSourceRange)
                    .execute()
                )

            index = []
            if result and "values" in result:
                for idx, val in enumerate(result["values"]):
                    if len(val) > 0 and val[0] in keys:
                        if isinstance(destination_value, dict):
                            newValue = destination_value[val[0]]
                        else:
                            newValue = destination_value
                        index.append((idx + destination_row_offset, newValue))

                batch_update_spreadsheet_request_body: Dict[str, Any] = {"requests": []}
                for i in index:
                    batch_update_spreadsheet_request_body["requests"].append(
                        {
                            "pasteData": {
                                "data": i[1],
                                "type": "PASTE_NORMAL",
                                "delimiter": ",",
                                "coordinate": {
                                    "sheetId": tab_id,
                                    "rowIndex": i[0],
                                    "columnIndex": destination_column,
                                },
                            },
                        }
                    )

                if batch_update_spreadsheet_request_body["requests"]:
                    SheetsService._logger.info(
                        "Writing to {} ({}) in {} ({})...".format(
                            spreadsheet_id, spreadsheet_name, tab_id, tab_name
                        )
                    )
                    if dryrun:
                        SheetsService._logger.info("Stubbed")
                    else:
                        service = SheetsService.getService()
                        with SheetsService._lock:
                            service.spreadsheets().batchUpdate(
                                spreadsheetId=spreadsheet_id, body=batch_update_spreadsheet_request_body
                            ).execute()
                else:
                    SheetsService._logger.info("Nothing to update")
            else:
                SheetsService._logger.info("No input data to update")
            break
        except Exception as e:
            _handleException(e, spreadsheet_id)

            failures += 1
            if failures > 5:
                SheetsService._logger.warning("Too many failures, abandonning")
                raise e

            # Retry
            SheetsService._logger.warning(
                "Failed {} times ({}), retrying in {} seconds...".format(failures, e, delay)
            )
            time.sleep(delay)
            SheetsService.reset()
            delay *= 1.5

    return index


def bulkclean(
    spreadsheet_id: str,
    spreadsheet_name: str,
    tab_id: int,
    tab_name: str,
    range: str,
    dryrun: bool = False,
    remove_filter: bool = True,
) -> None:
    """Removes all data in a range.

    This method should probably not be called directly; use Sheet.bulkClean instead.

    Args:
    - spreadsheet_id (str): Spreadsheet ID to clean
    - spreadsheet_name (str): Spreadsheet name to clean
    - tab_id (int): Sheet ID to clean
    - tab_name (str): Sheet name to clean
    - range (str): Range to clean (e.g. A2:C)
    - dryrun (bool, optional): If True, does not actually do anything. Defaults to False.
    - remove_filter (bool, optional): If True, removes any active filter before doing anything. Defaults to True.

    Raises:
    - Exception: Failure after 5 retries
    """
    if SheetsService._force_test_spreadsheet:
        s = SheetsService.getTestSpreadsheet()
        spreadsheet_id = s.parent.spreadsheet_id
        spreadsheet_name = cast(str, s.parent.spreadsheet_name)
        tab_id = cast(int, s.tab_id)
        tab_name = s.tab_name

    if remove_filter:
        removefilter(spreadsheet_id, tab_id, dryrun=dryrun)

    if "!" in range:
        fullRangetoClean = range
    else:
        fullRangetoClean = "'{}'!{}".format(tab_name.replace("'", "\\'"), range)

    failures = 0
    delay = SheetsService._retry_delay
    while True:
        try:
            SheetsService._logger.info(
                "Cleaning {} {} ({}) in {} ({})...".format(
                    fullRangetoClean, spreadsheet_id, spreadsheet_name, tab_id, tab_name
                )
            )
            if not dryrun:
                service = SheetsService.getService()
                with SheetsService._lock:
                    service.spreadsheets().values().clear(
                        spreadsheetId=spreadsheet_id, range=fullRangetoClean
                    ).execute()
            break
        except Exception as e:
            _handleException(e, spreadsheet_id)

            failures += 1
            if failures > 5:
                SheetsService._logger.warning("Too many failures cleaning, abandonning")
                raise e

            # Retry
            SheetsService._logger.warning(
                "Failed cleaning {} times ({}), retrying in {} seconds...".format(failures, e, delay)
            )
            time.sleep(delay)
            SheetsService.reset()
            delay *= 1.5


def bulkwrite(
    data: Iterable[Iterable[str]],
    spreadsheet_id: str,
    spreadsheet_name: str,
    tab_id: int,
    tab_name: str,
    row_index: int,
    column_index: int,
    dryrun: bool = False,
    remove_filter: bool = True,
) -> None:
    """Writes data into a range.

    This method should probably not be called directly; use Sheet.bulkWrite instead.

    Args:
    - data (Iterable[Iterable[str]]): Data to write, as a 2-dimension iterable of strings (list of rows, then columns)
    - spreadsheet_id (str): Spreadsheet ID to write to
    - spreadsheet_name (str): Spreadsheet name to write to
    - tab_id (int): Sheet ID to write to
    - tab_name (str): Sheet name to write to
    - row_index (int): Index of the row where to start writing (0-based)
    - column_index (int): Index of the column where to start writing (0-based)
    - dryrun (bool, optional): If True, does not actually do anything. Defaults to False.
    - remove_filter (bool, optional): If True, removes any active filter before doing anything. Defaults to True.

    Raises:
    - Exception: Failure after 5 retries
    """
    if SheetsService._force_test_spreadsheet:
        s = SheetsService.getTestSpreadsheet()
        spreadsheet_id = s.parent.spreadsheet_id
        spreadsheet_name = cast(str, s.parent.spreadsheet_name)
        tab_id = cast(int, s.tab_id)
        tab_name = s.tab_name

    output = io.StringIO()
    csvwriter = csv.writer(output, delimiter=",", quotechar='"')

    for row in data:
        csvwriter.writerow(row)

    csvdata = output.getvalue()

    if remove_filter:
        removefilter(spreadsheet_id, tab_id, dryrun=dryrun)

    batch_update_spreadsheet_request_body = {
        "requests": [
            {
                "pasteData": {
                    "data": csvdata,
                    "type": "PASTE_NORMAL",
                    "delimiter": ",",
                    "coordinate": {"sheetId": tab_id, "rowIndex": row_index, "columnIndex": column_index},
                },
            }
        ]
    }
    failures = 0
    delay = SheetsService._retry_delay
    while True:
        try:
            SheetsService._logger.info(
                "Writing to {} ({}) in {} ({})...".format(spreadsheet_id, spreadsheet_name, tab_id, tab_name)
            )
            if not dryrun:
                service = SheetsService.getService()
                with SheetsService._lock:
                    service.spreadsheets().batchUpdate(
                        spreadsheetId=spreadsheet_id, body=batch_update_spreadsheet_request_body
                    ).execute()
            break
        except Exception as e:
            _handleException(e, spreadsheet_id)

            failures += 1
            if failures > 5:
                SheetsService._logger.warning("Too many failures writing, abandonning")
                raise e

            # Retry
            SheetsService._logger.warning(
                "Failed writing {} times ({}), retrying in {} seconds...".format(failures, e, delay)
            )
            time.sleep(delay)
            SheetsService.reset()
            delay *= 1.5


def bulkappend(
    data: Iterable[Iterable[str]],
    spreadsheet_id: str,
    spreadsheet_name: str,
    tab_id: int,
    tab_name: str,
    range: str,
    dryrun: bool = False,
    remove_filter: bool = False,
) -> None:
    """Appends data to a range.

    This method should probably not be called directly; use Sheet.bulkAppend instead.

    Args:
    - data (Iterable[Iterable[str]]): Data to write, as a 2-dimension iterable of strings (list of rows, then columns)
    - spreadsheet_id (str): Spreadsheet ID to write to
    - spreadsheet_name (str): Spreadsheet name to write to
    - tab_id (int): Sheet ID to write to
    - tab_name (str): Sheet name to write to
    - range (str): Range to append data to (e.g. A1:R)
    - dryrun (bool, optional): If True, does not actually do anything. Defaults to False.
    - remove_filter (bool, optional): If True, removes any active filter before doing anything. Defaults to True.

    Raises:
    - Exception: Failure after 5 retries
    """
    if SheetsService._force_test_spreadsheet:
        s = SheetsService.getTestSpreadsheet()
        spreadsheet_id = s.parent.spreadsheet_id
        spreadsheet_name = cast(str, s.parent.spreadsheet_name)
        tab_id = cast(int, s.tab_id)
        tab_name = s.tab_name

    if remove_filter:
        removefilter(spreadsheet_id, tab_id, dryrun=dryrun)

    if "!" in range:
        fullRange = range
    else:
        fullRange = "'{}'!{}".format(tab_name.replace("'", "\\'"), range)

    request_body = {
        "range": fullRange,
        "majorDimension": "ROWS",
        "values": data,
    }

    failures = 0
    delay = SheetsService._retry_delay
    while True:
        try:
            SheetsService._logger.info(
                "Writing to {} ({}) in {} ({})...".format(spreadsheet_id, spreadsheet_name, tab_id, tab_name)
            )
            if not dryrun:
                service = SheetsService.getService()
                with SheetsService._lock:
                    service.spreadsheets().values().append(
                        spreadsheetId=spreadsheet_id,
                        range=fullRange,
                        valueInputOption="USER_ENTERED",
                        insertDataOption="INSERT_ROWS",
                        body=request_body,
                    ).execute()
            break
        except Exception as e:
            _handleException(e, spreadsheet_id)

            failures += 1
            if failures > 5:
                SheetsService._logger.warning("Too many failures writing, abandonning")
                raise e

            # Retry
            SheetsService._logger.warning(
                "Failed writing {} times ({}), retrying in {} seconds...".format(failures, e, delay)
            )
            time.sleep(delay)
            SheetsService.reset()
            delay *= 1.5
