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
    from .spreadsheet import Spreadsheet  # pragma: no cover


class Sheet:
    """Abstraction for Sheets (i.e. tabs)"""

    def __init__(
        self, parent: "Spreadsheet", tab_name: str, tab_id: int = None, mapping: Mapping = None
    ) -> None:
        """Constructor.

        Providing a tab_id can be useful if you have special characters in the tab name and want to keep nice
        filenames onces downloaded.

        Args:
        - parent (Spreadsheet): Parent SpreadSheet
        - tab_name (str): Name of the Sheet (can be different from the "real" tab name if tab_id is provided)
        - tab_id (int, optional): Id of the Sheet (as given by https://docs.google.com/spreadsheets/d/<spreadsheet_id>/edit#gid=<tab_id>). Defaults to None (discovered, based on the name).
        - mapping (Mapping, optional): Mapping. Defaults to None.
        """
        self.tab_name = tab_name
        self.tab_id = tab_id
        self.parent = parent
        self.mapping = mapping

    def loadInfos(self, force: bool = False) -> "Sheet":
        """Loads sheet informations.

        You should not need to call this explicitely.

        Returns:
        - Sheet: self
        """
        if self.tab_id is None:
            self.parent.loadInfos(force)
            if self.tab_id is None:
                self.parent.loadInfos(True)
        return self

    def getFilepath(self, timestamp: Union[datetime.datetime, datetime.date, str]) -> str:
        """Gets the path to the downloaded file for a specific date.

        Args:
        - timestamp (Union[datetime.datetime, datetime.date, str]): Date

        Returns:
        - str: Full path to the file
        """
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

    def download(self, force: bool = False) -> str:
        """Downloads the Sheet as a CSV file.

        Stored in the backup location configured with SheetsService.configure. Filename is formatted as: <spreadsheet name> - <tab name>-<date>.csv

        Args:
        - force (bool, optional): Forces download of the file. Defaults to False - skips download if the file already exists.

        Returns:
        - str: Full path to the downloaded file.
        """
        now = datetime.datetime.now()
        path = self.getFilepath(now)
        if not os.path.exists(path) or force:
            self.downloadTo(path)
        return path

    def downloadTo(self, filePath: str) -> None:
        """Downloads the Sheet as a CSV file at a specific path.

        Args:
        - filePath (str): Destination path of the file (where it should be downloaded)

        Raises:
        - Exception: Failed
        """
        if self.tab_id is None:
            self.parent.loadInfos()
            if self.tab_id is None:
                self.parent.loadInfos(True)

        url = "https://docs.google.com/spreadsheets/d/{}/export?format=csv&gid={}".format(
            self.parent.spreadsheet_id, self.tab_id
        )

        failures = 0
        delay = SheetsService._retry_delay
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

    def removeFilter(self, dryrun: bool = False) -> None:
        """Removes any active filter on the sheet.

        Args:
        - dryrun (bool, optional): If True, does not actually do anything. Defaults to False.

        Raises:
        - Exception: Failure after 5 retries
        """
        self.loadInfos()
        assert self.tab_id is not None
        removefilter(self.parent.spreadsheet_id, self.tab_id, dryrun=dryrun)

    def bulkUpdate(
        self,
        keys: Iterable[str],
        source_range: str,
        destination_row_offset: int,
        destination_column: int,
        destination_value: Union[str, Dict[str, str]],
        dryrun: bool = False,
    ) -> List[Tuple[int, str]]:
        """Updates a column based on the value of another column

        Args:
        - keys (Iterable[str]): Keys to update
        - source_range (str): Column and range which contains keys (e.g. "'CONTRATS SIGNES'!Z3:Z" ou "Z3:Z" - tab name is optional, and filled from tab_name argument if needed)
        - destination_row_offset (int): Offset from header (0-indexed) - typically line of source_range minus 1
        - destination_column (int): Index of the column to update in the sheet (0-indexed)
        - destination_value (Union[str, Dict[str, str]]): Values to put in the sheet:
            - Either a single value (will put the same value for all keys)
            - Or a map key=>value (will put the value corresponding to the key)
        - dryrun (bool, optional): If True, does not actually do anything. Defaults to False.

        Raises:
        - Exception: Failure after 5 retries

        Returns:
        - List[Tuple[int, str]]: List of tuples (line number update, new value)
        """
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
            source_range,
            destination_row_offset,
            destination_column,
            destination_value,
            dryrun,
        )

    def bulkClean(self, range: str, dryrun: bool = False) -> None:
        """Removes all data in a range.

        Args:
        - range (str): Range to clean (e.g. A2:C)
        - dryrun (bool, optional): If True, does not actually do anything. Defaults to False.

        Raises:
        - Exception: Failure after 5 retries
        """
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
        self, data: Iterable[Iterable[str]], row_index: int, column_index: int, dryrun: bool = False
    ) -> None:
        """Writes data into a range

        Args:
        - data (Iterable[Iterable[str]]): Data to write, as a 2-dimension iterable of strings (list of rows, then columns)
        - row_index (int): Index of the row where to start writing (0-based)
        - column_index (int): Index of the column where to start writing (0-based)
        - dryrun (bool, optional): If True, does not actually do anything. Defaults to False.

        Raises:
        - Exception: Failure after 5 retries
        """
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
            row_index,
            column_index,
            dryrun=dryrun,
        )

    def bulkAppend(self, data: Iterable[Iterable[str]], range: str, dryrun: bool = False) -> None:
        """Appends data to a range

        Args:
        - data (Iterable[Iterable[str]]): Data to write, as a 2-dimension iterable of strings (list of rows, then columns)
        - range (str): Range to append data to (e.g. A1:R)
        - dryrun (bool, optional): If True, does not actually do anything. Defaults to False.

        Raises:
        - Exception: Failure after 5 retries
        """
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
        """Generator on CSV data from a Sheet.

        This can be used like:
        ```
        # Download current data from sheet:
        with mysheet.csvreader() as csvreader:
            for row in csvreader:
                # CSV row as given by csv.reader
                print(row[0])

        # Use cached data:
        with mysheet.csvreader(datetime(2020, 1, 1)) as csvreader:
            for row in csvreader:
                # CSV row as given by csv.reader
                print(row[0])
        ```

        Args:
        - date (Optional[Union[datetime.date, datetime.datetime]], optional): Date for cached data. Defaults to None, downloading current data from the sheet.

        Yields:
        - Iterator[Iterable[Sequence[str]]]: Iterable on the CSV rows
        """
        if date is None:
            with tempfile.NamedTemporaryFile("r", encoding="utf-8") as csvfile:
                self.downloadTo(csvfile.name)
                yield csv.reader(csvfile, delimiter=",", quotechar='"')  # type: ignore
        else:
            with open(self.getFilepath(date), "r", encoding="utf-8") as csvfile:
                yield csv.reader(csvfile, delimiter=",", quotechar='"')  # type: ignore

    @contextlib.contextmanager
    def open(self, date: Optional[Union[datetime.date, datetime.datetime]] = None) -> Iterator[IO]:
        """Opens a Sheet as a file

        Args:
        - date (Optional[Union[datetime.date, datetime.datetime]], optional): Date for cached data. Defaults to None, downloading current data from the sheet.

        Yields:
        - Iterator[IO]: file-like object in read-only mod
        """
        if date is None:
            with tempfile.NamedTemporaryFile("r", encoding="utf-8") as csvfile:
                self.downloadTo(csvfile.name)
                yield csvfile
        else:
            with open(self.getFilepath(date), "r", encoding="utf-8") as csvfile:
                yield csvfile
