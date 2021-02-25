import copy
import csv
import os
import pickle
import tempfile
import time
from typing import Any, Dict, Optional

from simpletasks_data import Mapping

from gapi_helper.drive import File, Folder
from gapi_helper.sheets.operations import bulkwrite

from .client import SheetsService
from .sheet import Sheet


class Spreadsheet:
    """Abstraction for SpreadSheets"""

    def __init__(self, spreadsheet_id: str, spreadsheet_name: str = None) -> None:
        """Constructor

        Providing a spreadsheet_name can be useful if you have special characters in the spreadsheet name and want to keep nice
        filenames onces downloaded.

        Sheets are not discovered automatically, and requires calls to `addSheet` or `addKnownSheet`.

        Args:
        - spreadsheet_id (str): Spreadsheet ID (as given by https://docs.google.com/spreadsheets/d/<spreadsheet_id>/edit)
        - spreadsheet_name (str, optional): Name of the spreadsheet. Defaults to None (discovered, based on the ID).
        """
        self.spreadsheet_name = spreadsheet_name
        self.spreadsheet_id = spreadsheet_id
        self.registeredSheets: Dict[str, Sheet] = {}  # Map Name -> Sheet
        self.isLoaded = False

    def loadInfos(self, force: bool = False) -> "Spreadsheet":
        """Loads spreadsheet informations.

        You should not need to call this explicitely.

        Returns:
        - Spreadsheet: self
        """
        if self.isLoaded and not force:
            return self

        cachePath = os.path.join(
            SheetsService.getCacheLocation(), "gs_infos-{}.pkl".format(self.spreadsheet_id)
        )
        if os.path.exists(cachePath) and not force:
            with open(cachePath, "rb") as f:
                result = pickle.load(f)
        else:
            failures = 0
            delay = SheetsService._retry_delay
            while True:
                try:
                    SheetsService._logger.info(
                        "Downloading cache info for {} ({})...".format(
                            self.spreadsheet_name, self.spreadsheet_id
                        )
                    )
                    service = SheetsService.getService()
                    with SheetsService._lock:
                        result = service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()

                    with open(cachePath, "wb") as f:
                        pickle.dump(result, f, pickle.HIGHEST_PROTOCOL)
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

        if self.spreadsheet_name is None:
            self.spreadsheet_name = result["properties"]["title"]

        for sheet in result["sheets"]:
            sheet_name = sheet["properties"]["title"]
            if sheet_name in self.registeredSheets:
                self.registeredSheets[sheet_name].tab_id = sheet["properties"]["sheetId"]
            else:
                self.registeredSheets[sheet_name] = Sheet(self, sheet_name, sheet["properties"]["sheetId"])

        self.isLoaded = True
        return self

    def clearInfos(self) -> None:
        cachePath = os.path.join(
            SheetsService.getCacheLocation(), "gs_infos-{}.pkl".format(self.spreadsheet_id)
        )
        if os.path.exists(cachePath):
            os.remove(cachePath)

    def addSheet(self, tab_name: str, tab_id: int = None, mapping: Mapping = None) -> Sheet:
        """Maps a Sheet to this Spreadsheet.

        Args:
        - tab_name (str): Name of the Sheet (can be different from the "real" tab name if tab_id is provided)
        - tab_id (int, optional): Id of the Sheet (as given by https://docs.google.com/spreadsheets/d/<spreadsheet_id>/edit#gid=<tab_id>). Defaults to None (discovered, based on the name).
        - mapping (Mapping, optional): Mapping. Defaults to None.

        Returns:
        - Sheet: Sheet
        """
        if tab_name not in self.registeredSheets:
            sheet = Sheet(self, tab_name, tab_id, mapping)
            self.registeredSheets[tab_name] = sheet

        return self.registeredSheets[tab_name]

    def addKnownSheet(self, sheet: Sheet) -> Sheet:
        """Maps a Sheet to this Spreadsheet.

        Args:
        - sheet (Sheet): Sheet

        Returns:
        - Sheet: Sheet
        """
        if sheet.tab_name not in self.registeredSheets:
            self.registeredSheets[sheet.tab_name] = sheet

        return self.registeredSheets[sheet.tab_name]

    def createSheet(
        self,
        tab_name: str,
        tab_id: Optional[int] = None,
        properties: Dict[str, Any] = {},
        dryrun: bool = False,
    ) -> Sheet:
        """Creates a new Sheet in this spreadsheet

        Args:
        - tab_name (str): Name of the sheet
        - tab_id (int, optional): ID of the tab. Defaults to None (auto-generated by Google Sheets). Fails if ID already exists.
        - properties (Dict[str, Any]): additional properties for sheet creation (see https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/sheets#SheetProperties)
        - dryrun (bool, optional): If True, does not actually do anything. Defaults to False.

        Raises:
        - Exception: Failed.

        Returns:
        - Sheet: created sheet
        """
        self.loadInfos()
        if tab_name in self.registeredSheets:
            return self.registeredSheets[tab_name]

        failures = 0
        delay = SheetsService._retry_delay

        props = copy.deepcopy(properties)
        props["title"] = tab_name
        if tab_id is not None:
            props["sheetId"] = tab_id

        while True:
            try:
                SheetsService._logger.info(
                    "Creating sheet {} in {} ({})...".format(
                        tab_name, self.spreadsheet_name, self.spreadsheet_id
                    )
                )
                if not dryrun:
                    service = SheetsService.getService()
                    with SheetsService._lock:
                        res = (
                            service.spreadsheets()
                            .batchUpdate(
                                spreadsheetId=self.spreadsheet_id,
                                body={"requests": [{"addSheet": {"properties": props}}]},
                            )
                            .execute()
                        )
                    sheet_name = res["replies"][0]["addSheet"]["properties"]["title"]
                    sheet_id = res["replies"][0]["addSheet"]["properties"]["sheetId"]

                    self.registeredSheets[sheet_name] = Sheet(self, sheet_name, sheet_id)
                    return self.registeredSheets[tab_name]
                else:
                    return Sheet(self, "Stubbed")

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

    def dumpTo(self, spreadsheet: "Spreadsheet", dryrun: bool = False) -> "Spreadsheet":
        """Copy/pastes a spreadsheet into another one.

        This differs from copyTo: with this method, any formulas are exported as values, making the dumped spreadsheet
        an export of the values, and not a clone of the initial spreadsheet.

        Args:
        - spreadsheet: destination spreadsheet
        - dryrun (bool, optional): If True, does not actually do anything. Defaults to False.

        Returns:
        - Spreadsheet: dumped spreadsheet (same as `spreadsheet` parameter)
        """
        self.loadInfos()

        for sheet_name, sheet in self.registeredSheets.items():
            assert sheet.tab_id is not None

            with tempfile.NamedTemporaryFile("r", encoding="utf-8") as csvfile:
                sheet.downloadTo(csvfile.name)
                new_sheet = spreadsheet.createSheet(
                    sheet_name,
                    properties={"gridProperties": {"rowCount": 1, "columnCount": 1}},
                    dryrun=dryrun,
                )

                assert spreadsheet.spreadsheet_name is not None
                assert new_sheet.tab_id is not None

                csvreader = csv.reader(csvfile, delimiter=",", quotechar='"')
                bulkwrite(
                    csvreader,
                    spreadsheet.spreadsheet_id,
                    spreadsheet.spreadsheet_name,
                    new_sheet.tab_id,
                    new_sheet.tab_name,
                    0,
                    0,
                    remove_filter=False,
                    dryrun=dryrun,
                )
        return spreadsheet

    def dumpIn(self, folder: Folder, name: str, dryrun: bool = False) -> "Spreadsheet":
        """Copy/pastes a spreadsheet into another one.

        This differs from copyIn: with this method, any formulas are exported as values, making the dumped spreadsheet
        an export of the values, and not a clone of the initial spreadsheet.

        Args:
        - folder: destination folder that will contain the dumped spreadsheet
        - name: name of the file to create
        - dryrun (bool, optional): If True, does not actually do anything. Defaults to False.

        Returns:
        - Spreadsheet: dumped spreadsheet
        """
        if not dryrun:
            file = folder.insertFile(name, "application/vnd.google-apps.spreadsheet")
            assert file is not None

            SheetsService.getService()
            if SheetsService.credentials.service_account_email:
                file.share(
                    SheetsService.credentials.service_account_email,
                    role="writer",
                    sendNotificationEmail=False,
                )
        else:
            file = File("stubbed", "abcdef", folder.client)

        spreadsheet = Spreadsheet(file.file_id, name)
        self.dumpTo(spreadsheet, dryrun=dryrun)
        return spreadsheet

    def copyIn(self, folder: Folder, name: str, dryrun: bool = False) -> "Spreadsheet":
        """Copy/pastes a spreadsheet into another one.

        This creates a clone of this spreadsheet (keeping formulas, duplicating attached Forms, etc.).
        See also `dumpIn` to remove formulas.

        **Note:** if any Google Form is attached to the Spreadsheet, it will also be copied. But due to
        limitations in the Google Spreadsheet/Drive APIs, the new Form will:
        - be in the same folder as the old form (NOT in the folder provided to this function)
        - be named "Copy of ..." (NOT following the name provided to this function)

        Args:
        - folder: destination folder that will contain the dumped spreadsheet
        - name: name of the file to create
        - dryrun (bool, optional): If True, does not actually do anything. Defaults to False.

        Returns:
        - Spreadsheet: dumped spreadsheet
        """
        self.loadInfos()
        assert self.spreadsheet_name is not None

        file = File(self.spreadsheet_name, self.spreadsheet_id, folder.client)

        if not dryrun:
            f = file.copyTo(folder, new_name=name)
            assert f is not None

            SheetsService.getService()
            if SheetsService.credentials.service_account_email:
                f.share(
                    SheetsService.credentials.service_account_email,
                    role="writer",
                    sendNotificationEmail=False,
                )
        else:
            f = File(self.spreadsheet_name, name, folder.client)

        return Spreadsheet(f.file_id, name)
