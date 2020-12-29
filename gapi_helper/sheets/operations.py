import csv
import io
import time
from typing import Any, Dict, Iterable, List, Tuple, Union, cast

from .client import SheetsService


def removefilter(spreadsheet_id: str, tab_id: int, dryrun=False) -> None:
    if SheetsService._force_test_spreadsheet:
        spreadsheet_id = SheetsService.getTestSpreadsheet().parent.spreadsheet_id
        tab_id = cast(int, SheetsService.getTestSpreadsheet().tab_id)

    remove_filter_spreadsheet_request_body = {"requests": [{"clearBasicFilter": {"sheetId": tab_id}}]}
    failures = 0
    delay: float = 30
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
    numerosinstallation: Iterable[str],
    spreadsheet_id: str,
    spreadsheet_name: str,
    tab_id: int,
    tab_name: str,
    sourceRange: str,
    destinationRowOffset: int,
    destinationCol: int,
    destinationValue: Union[str, Dict[str, str]],
    dryrun: bool = False,
    removeFilter: bool = True,
) -> List[Tuple[int, str]]:
    """Met à jour toute une colonne en fonction d'une clé

    Args:
        numerosinstallation (Iterable[str]): Liste de clés à mettre à jour
        destination (GsSheet): Destination à mettre à jour
        sourceRange (str): Colonne des clés (e.g. "'CONTRATS SIGNES'!Z3:Z" ou "Z3:Z")
        destinationRowOffset (int): Offset par rapport à l'entête à mettre à jour (0-indexed)
            typiquement ligne de `sourcerange` - 1
        destinationCol (int): Index de colonne à mettre à jour (0-indexed)
        destinationValue (Union[str, Dict[str, str]]): Valeurs à mettre à jour
            Soit une valeur (mettra la même valeur pour toutes les clés)
            Soit une map clé=>valeur (mettra la valeur correspondant à la clé)
        dryrun (bool, optional): Dry-run. Defaults to False.

    Raises:
        e: Impossible de mettre à jour après 5 tentatives

    Returns:
        List[Tuple[int, str]]: Liste de tuples (numéro de ligne modifiée, nouvelle valeur)
    """
    if SheetsService._force_test_spreadsheet:
        s = SheetsService.getTestSpreadsheet()
        spreadsheet_id = s.parent.spreadsheet_id
        spreadsheet_name = cast(str, s.parent.spreadsheet_name)
        tab_id = cast(int, s.tab_id)
        tab_name = s.tab_name

    if removeFilter:
        removefilter(spreadsheet_id, tab_id, dryrun=dryrun)

    if "!" in sourceRange:
        fullSourceRange = sourceRange
    else:
        fullSourceRange = "'{}'!{}".format(tab_name.replace("'", "\\'"), sourceRange)

    failures = 0
    delay: float = 30
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
                    if len(val) > 0 and val[0] in numerosinstallation:
                        if isinstance(destinationValue, dict):
                            newValue = destinationValue[val[0]]
                        else:
                            newValue = destinationValue
                        index.append((idx + destinationRowOffset, newValue))

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
                                    "columnIndex": destinationCol,
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
    removeFilter: bool = True,
) -> None:
    if SheetsService._force_test_spreadsheet:
        s = SheetsService.getTestSpreadsheet()
        spreadsheet_id = s.parent.spreadsheet_id
        spreadsheet_name = cast(str, s.parent.spreadsheet_name)
        tab_id = cast(int, s.tab_id)
        tab_name = s.tab_name

    if removeFilter:
        removefilter(spreadsheet_id, tab_id, dryrun=dryrun)

    if "!" in range:
        fullRangetoClean = range
    else:
        fullRangetoClean = "'{}'!{}".format(tab_name.replace("'", "\\'"), range)

    failures = 0
    delay: float = 30
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
    rowIndex: int,
    columnIndex: int,
    dryrun: bool = False,
    removeFilter: bool = True,
) -> None:
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

    if removeFilter:
        removefilter(spreadsheet_id, tab_id, dryrun=dryrun)

    batch_update_spreadsheet_request_body = {
        "requests": [
            {
                "pasteData": {
                    "data": csvdata,
                    "type": "PASTE_NORMAL",
                    "delimiter": ",",
                    "coordinate": {"sheetId": tab_id, "rowIndex": rowIndex, "columnIndex": columnIndex},
                },
            }
        ]
    }
    failures = 0
    delay: float = 30
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
    removeFilter: bool = False,
) -> None:
    if SheetsService._force_test_spreadsheet:
        s = SheetsService.getTestSpreadsheet()
        spreadsheet_id = s.parent.spreadsheet_id
        spreadsheet_name = cast(str, s.parent.spreadsheet_name)
        tab_id = cast(int, s.tab_id)
        tab_name = s.tab_name

    if removeFilter:
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
    delay: float = 30
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
