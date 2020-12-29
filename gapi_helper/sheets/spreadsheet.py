import os
import pickle
import time
from typing import Dict

from simpletasks_data import Mapping

from .client import SheetsService
from .sheet import Sheet


class Spreadsheet:
    def __init__(self, spreadsheet_id: str, spreadsheet_name: str = None) -> None:
        self.spreadsheet_name = spreadsheet_name
        self.spreadsheet_id = spreadsheet_id
        self.registeredSheets: Dict[str, Sheet] = {}  # Map Name -> Sheet
        self.isLoaded = False

    def loadInfos(self, force: bool = False) -> "Spreadsheet":
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
            delay: float = 30
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

    def addSheet(self, tab_name: str, tab_id: int = None, mapping: Mapping = None) -> Sheet:
        if tab_name not in self.registeredSheets:
            sheet = Sheet(self, tab_name, tab_id, mapping)
            self.registeredSheets[tab_name] = sheet

        return self.registeredSheets[tab_name]

    def addKnownSheet(self, sheet: Sheet) -> Sheet:
        if sheet.tab_name not in self.registeredSheets:
            self.registeredSheets[sheet.tab_name] = sheet

        return self.registeredSheets[sheet.tab_name]
