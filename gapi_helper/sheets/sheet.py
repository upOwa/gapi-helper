import contextlib
import csv
import datetime
import os
import tempfile
import time
from typing import IO, TYPE_CHECKING, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple, Union

import requests

from simpletasks_data import Mapping

from .client import SheetsService
from .operations import bulkappend, bulkclean, bulkupdate, bulkwrite, removefilter

if TYPE_CHECKING:
    from .spreadsheet import Spreadsheet


class Sheet:
    def __init__(
        self, parent: "Spreadsheet", tab_name: str, tab_id: int = None, mapping: Mapping = None
    ) -> None:
        self.tab_name = tab_name
        self.tab_id = tab_id
        self.parent = parent
        self.mapping = mapping

    def loadInfos(self, force=False) -> "Sheet":
        if self.tab_id is None:
            self.parent.loadInfos(force)
            if self.tab_id is None:
                self.parent.loadInfos(True)
        return self

    def getFilepath(self, timestamp: Union[datetime.datetime, datetime.date, str]) -> str:
        if self.parent.spreadsheet_name is None:
            self.parent.loadInfos()

        if isinstance(timestamp, datetime.datetime) or isinstance(timestamp, datetime.date):
            t = timestamp.strftime("%Y-%m-%d")
        else:
            t = timestamp

        return os.path.join(
            SheetsService.getBackupLocation(),
            "{} - {}-{}.csv".format(self.parent.spreadsheet_name, self.tab_name, t),
        )

    def download(self, filePath: str) -> None:
        if self.tab_id is None:
            self.parent.loadInfos()
            if self.tab_id is None:
                self.parent.loadInfos(True)

        url = "https://docs.google.com/spreadsheets/d/{}/export?format=csv&gid={}".format(
            self.parent.spreadsheet_id, self.tab_id
        )

        failures = 0
        delay: float = 30
        while True:
            try:
                SheetsService._logger.info("Downloading {}...".format(filePath))
                response = requests.get(url, headers=SheetsService.getHeaders())
                response.raise_for_status()
                if response.headers.get("Content-Type") != "text/csv":
                    raise Exception("Bad format received")
                with open(filePath, "wb") as csvFile:
                    csvFile.write(response.content)
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

    def removeFilter(self, dryrun=False) -> None:
        self.loadInfos()
        assert self.tab_id is not None
        removefilter(self.parent.spreadsheet_id, self.tab_id, dryrun=dryrun)

    def bulkUpdate(
        self,
        keys: Iterable[str],
        sourceRange: str,
        destinationRowOffset: int,
        destinationCol: int,
        destinationValue: Union[str, Dict[str, str]],
        dryrun: bool = False,
    ) -> List[Tuple[int, str]]:
        self.loadInfos()
        self.parent.loadInfos()
        assert self.parent.spreadsheet_name is not None
        assert self.tab_id is not None
        return bulkupdate(
            keys,
            self.parent.spreadsheet_id,
            self.parent.spreadsheet_name,
            self.tab_id,
            self.tab_name,
            sourceRange,
            destinationRowOffset,
            destinationCol,
            destinationValue,
            dryrun,
        )

    def bulkClean(self, range: str, dryrun=False) -> None:
        self.loadInfos()
        self.parent.loadInfos()
        assert self.parent.spreadsheet_name is not None
        assert self.tab_id is not None
        return bulkclean(
            self.parent.spreadsheet_id,
            self.parent.spreadsheet_name,
            self.tab_id,
            self.tab_name,
            range,
            dryrun=dryrun,
        )

    def bulkWrite(
        self, data: Iterable[Iterable[str]], rowIndex: int, columnIndex: int, dryrun: bool = False
    ) -> None:
        self.loadInfos()
        self.parent.loadInfos()
        assert self.parent.spreadsheet_name is not None
        assert self.tab_id is not None
        return bulkwrite(
            data,
            self.parent.spreadsheet_id,
            self.parent.spreadsheet_name,
            self.tab_id,
            self.tab_name,
            rowIndex,
            columnIndex,
            dryrun=dryrun,
        )

    def bulkAppend(self, data: Iterable[Iterable[str]], range: str, dryrun: bool = False) -> None:
        self.loadInfos()
        self.parent.loadInfos()
        assert self.parent.spreadsheet_name is not None
        assert self.tab_id is not None
        return bulkappend(
            data,
            self.parent.spreadsheet_id,
            self.parent.spreadsheet_name,
            self.tab_id,
            self.tab_name,
            range,
            dryrun=dryrun,
        )

    @contextlib.contextmanager
    def csvreader(
        self, date: Optional[Union[datetime.date, datetime.datetime]] = None
    ) -> Iterator[Iterable[Sequence[str]]]:
        if date is None:
            with tempfile.NamedTemporaryFile("r", encoding="utf-8") as csvfile:
                self.download(csvfile.name)
                yield csv.reader(csvfile, delimiter=",", quotechar='"')  # type: ignore
        else:
            with open(self.getFilepath(date), "r", encoding="utf-8") as csvfile:
                yield csv.reader(csvfile, delimiter=",", quotechar='"')  # type: ignore

    @contextlib.contextmanager
    def open(self, date: Optional[Union[datetime.date, datetime.datetime]] = None) -> Iterator[IO]:
        if date is None:
            with tempfile.NamedTemporaryFile("r", encoding="utf-8") as csvfile:
                self.download(csvfile.name)
                yield csvfile
        else:
            with open(self.getFilepath(date), "r", encoding="utf-8") as csvfile:
                yield csvfile
