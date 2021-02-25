import abc
import copy
import csv
import io
import logging
import random
from typing import Any, Collection, Dict, Iterable, List, Optional, Tuple, Union, cast

from simpletasks import Task

from ..sheets import Range, Sheet, SheetsService
from ..sheets.operations import bulkclean
from .types import Adapter, Filter, Row


class TransferredRange:
    """Represents a range from the Source data that is copied into a destination range in the sheet."""

    def __init__(
        self,
        source: Union[Range, str],
        destination: Union[Range, str],
        adapter: Adapter = None,
        adapter_partial: Adapter = None,
        clean: bool = False,
    ) -> None:
        """Constructor

        Args:
        - source (Union[Range, str]): Range in the source data (e.g. "C2:D")
        - destination (Union[Range, str]): Range in the destination data (e.g. "A1:B")
        - adapter (Adapter, optional): Adapter to change data on the fly (uses the whole row as input). Defaults to None.
        - adapter_partial (Adapter, optional): Adapter to change data on the fly (uses the source range as input). Defaults to None.
        - clean (bool, optional): If True, cleans the destination range before transferring data. Defaults to False.

        Raises:
        - ValueError: Source and destination ranges do not match (e.g. not same width or height)
        """
        self._source = source if isinstance(source, Range) else Range.fromA1N1(source)
        self._destination = destination if isinstance(destination, Range) else Range.fromA1N1(destination)
        self._adapter = adapter
        self._adapter_partial = adapter_partial
        self._clean = clean

        if self._adapter is None and self._adapter_partial is None:
            if not self._source.matches(self._destination):
                raise ValueError(
                    "destination dimension does not match source dimension for {}->{}".format(
                        self._source, self._destination
                    )
                )


class TransferDestination:
    """Represents a destination for a transfer.

    A destination is a single Sheet, but with potentially multiple ranges"""

    def __init__(
        self,
        sheet_to: Sheet,
        ranges: Iterable[Union[TransferredRange, Tuple[str, str]]],
        filter: Filter = None,
        clean: bool = False,
    ) -> None:
        """Constructor

        Args:
        - sheet_to (Sheet): Destination Sheet
        - ranges (Iterable[Union[TransferredRange, Tuple[str, str]]]): List of ranges to transfer, either:
            - instance of TransferredRange
            - Tuple `(source_range, destination_range)` using A1N1 notations (e.g. `("C2:D", "A1:B")`)
        - filter (Filter, optional): Filter to filter-out data on the fly. Defaults to None.
        - clean (bool, optional): If True, cleans the destination ranges before transferring data. Defaults to False.
        """
        self._sheet_to = sheet_to
        self._ranges: List[TransferredRange] = []
        for r in ranges:
            if isinstance(r, TransferredRange):
                self._ranges.append(r)
            else:
                self._ranges.append(TransferredRange(r[0], r[1]))
        self._filter = filter
        self._clean = clean

    def _replace(self, new_sheet_to: Sheet) -> "TransferDestination":
        return TransferDestination(new_sheet_to, ranges=self._ranges, filter=self._filter, clean=self._clean)


class TransferTask(Task, metaclass=abc.ABCMeta):
    """Task to transfer data into a Sheet"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.use_testing = self.options.get("use_testing", False) or SheetsService._force_test_spreadsheet

    @abc.abstractmethod
    def getData(self) -> Iterable[Row]:
        """Method to implement to generate data

        Returns:
        - Iterable[Row]: List of rows to write
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def getDestinations(self) -> Collection[TransferDestination]:
        """Method to implement to define the destinations

        Returns:
        - Collection[TransferDestination]: List of destinations
        """
        raise NotImplementedError  # pragma: no cover

    def _computeData(self, destination: TransferDestination) -> List[Row]:
        generated_data: List[Row] = []

        outputs: List[io.StringIO] = []
        csvwriters: List[Any] = []

        for idx, r in enumerate(destination._ranges):
            outputs.append(io.StringIO())
            csvwriters.append(csv.writer(outputs[idx], delimiter=",", quotechar='"'))

        for i, row in enumerate(self._data):
            if destination._filter:
                if not destination._filter(i, row):
                    continue

            for idx, r in enumerate(destination._ranges):
                if i < r._source.start_row or (r._source.end_row is not None and i > r._source.end_row):
                    continue

                if r._adapter:
                    selectRow = r._adapter(i, row)
                elif r._adapter_partial:
                    selectRow = r._adapter_partial(
                        i, row[r._source.start_col : cast(int, r._source.end_col) + 1]
                    )
                else:
                    selectRow = row[r._source.start_col : cast(int, r._source.end_col) + 1]

                if r._destination.end_col is None:
                    r._destination.end_col = r._destination.start_col + len(selectRow) - 1
                    r._destination.width = len(selectRow)
                else:
                    if r._destination.end_col != r._destination.start_col + len(selectRow) - 1:
                        raise ValueError(
                            "destination dimension does not match data dimension: expected {}, got {}".format(
                                r._destination.width, len(selectRow)
                            )
                        )

                csvwriters[idx].writerow(selectRow)

        for idx, r in enumerate(destination._ranges):
            generated_data.append(outputs[idx].getvalue())

        return generated_data

    @staticmethod
    def _write(spreadsheetId, body) -> Any:
        service = SheetsService.getService()
        with SheetsService._lock:
            return service.spreadsheets().batchUpdate(spreadsheetId=spreadsheetId, body=body).execute()

    def _write_to_sheet(self, destination: TransferDestination, generated_data: List[Row]) -> int:
        requests: List[Any] = []
        requests_size = 0
        requests_sent = 0

        destination._sheet_to.loadInfos()
        if destination._sheet_to.parent.spreadsheet_name is None or destination._sheet_to.tab_id is None:
            raise ValueError("Could not find spreadsheet")

        for idx, r in enumerate(destination._ranges):
            if generated_data[idx] == "":
                continue

            if destination._clean or r._clean:
                bulkclean(
                    destination._sheet_to.parent.spreadsheet_id,
                    cast(str, destination._sheet_to.parent.spreadsheet_name),
                    cast(int, destination._sheet_to.tab_id),
                    destination._sheet_to.tab_name,
                    r._destination.toA1N1(),
                    dryrun=self.dryrun,
                    remove_filter=False,
                )

            requests.append(
                {
                    "pasteData": {
                        "data": generated_data[idx],
                        "type": "PASTE_NORMAL",
                        "delimiter": ",",
                        "coordinate": {
                            "sheetId": destination._sheet_to.tab_id,
                            "rowIndex": r._destination.start_row,
                            "columnIndex": r._destination.start_col,
                        },
                    },
                }
            )
            requests_size += len(generated_data[idx])

        if requests:
            if requests_size > 20000000:
                self.logger.info(
                    "Size of data is {}, larger than 20M, splitting into {} requests...".format(
                        requests_size, len(requests)
                    )
                )
                for request in requests:
                    batch_update_spreadsheet_request_body = {"requests": [request]}
                    self.logger.info(
                        "Writing to {} ({}) in {} ({}) (size={})...".format(
                            destination._sheet_to.parent.spreadsheet_id,
                            destination._sheet_to.parent.spreadsheet_name,
                            destination._sheet_to.tab_id,
                            destination._sheet_to.tab_name,
                            len(request["pasteData"]["data"]),
                        )
                    )
                    if self.logger.isEnabledFor(logging.DEBUG):
                        self.logger.debug("Sending: {}".format(request))

                    self.executeOrRetry(
                        lambda: (
                            TransferTask._write(
                                destination._sheet_to.parent.spreadsheet_id,
                                batch_update_spreadsheet_request_body,
                            )
                        ),
                        initialdelay=random.uniform(20, 30),
                    )
                    requests_sent += 1
            else:
                batch_update_spreadsheet_request_body = {"requests": requests}

                self.logger.info(
                    "Writing to {} ({}) in {} ({}) (size={})...".format(
                        destination._sheet_to.parent.spreadsheet_id,
                        destination._sheet_to.parent.spreadsheet_name,
                        destination._sheet_to.tab_id,
                        destination._sheet_to.tab_name,
                        requests_size,
                    )
                )
                if self.logger.isEnabledFor(logging.DEBUG):
                    self.logger.debug("Sending: {}".format(requests))

                self.executeOrRetry(
                    lambda: (
                        TransferTask._write(
                            destination._sheet_to.parent.spreadsheet_id, batch_update_spreadsheet_request_body
                        )
                    ),
                    initialdelay=max(
                        2, random.uniform(SheetsService._retry_delay - 5, SheetsService._retry_delay + 5)
                    ),
                )
                requests_sent += 1
        else:
            self.logger.info(
                "Nothing to write to {} ({}) in {} ({})".format(
                    destination._sheet_to.parent.spreadsheet_id,
                    destination._sheet_to.parent.spreadsheet_name,
                    destination._sheet_to.tab_id,
                    destination._sheet_to.tab_name,
                )
            )
        return requests_sent

    def _work(self, dest, generatedData) -> int:
        dest._sheet_to.removeFilter(self.dryrun)
        return self._write_to_sheet(dest, generatedData)

    def do(self) -> bool:
        if Task.TESTING:
            self.dryrun = True

        self._data: Iterable[Row] = self.getData()
        destinations = self.getDestinations()

        self.exceptions: List[Tuple[TransferDestination, Exception]] = []
        success = True
        for dest in destinations:
            if self.use_testing:
                self.logger.info("Using test spreadsheet instead")
                dest = dest._replace(SheetsService.getTestSpreadsheet())

            generatedData = self._computeData(dest)
            try:
                dest._sheet_to.removeFilter(self.dryrun)
                requests_sent = self._write_to_sheet(dest, generatedData)
                self.logger.info(
                    "Done writing to {} ({}): {} requests sent".format(
                        dest._sheet_to.parent.spreadsheet_id,
                        dest._sheet_to.parent.spreadsheet_name,
                        requests_sent,
                    )
                )
            except Exception as e:
                self.logger.critical(
                    "Could not export to {} ({}): {}".format(
                        dest._sheet_to.parent.spreadsheet_id,
                        dest._sheet_to.parent.spreadsheet_name,
                        e,
                    )
                )
                success = False
        return success


class TransferCsvTask(TransferTask):
    """Task to transfer a CSV file into a Sheet"""

    def __init__(self, filepath: str, csvargs: Optional[Dict[str, Any]] = {}, *args, **kwargs):
        """Constructor

        Args:
        - filepath (str): full path to the CSV file to transfer
        - csvargs (Optional[Dict[str, Any]], optional): Arguments to pass to csv.reader method. Default arguments are {"delimiter": ",", "quotechar": '"'}.
        """
        super().__init__(*args, **kwargs)
        self._filepath = filepath
        self._csvargs = {"delimiter": ",", "quotechar": '"'}
        if csvargs:
            self._csvargs.update(copy.deepcopy(csvargs))

    def getData(self) -> Iterable[Row]:
        data: List[Row] = []
        with open(self._filepath, "r", encoding="utf-8") as csvfile:
            csvreader = csv.reader(csvfile, **self._csvargs)
            for row in csvreader:
                data.append(row)
        return data


class TransferSheetTask(TransferTask):
    """Task to transfer a Sheet into another Sheet"""

    def __init__(self, sheet: Sheet, use_cache=True, *args, **kwargs):
        """Constructor

        Args:
        - sheet (Sheet): Source sheet
        - use_cache (bool, optional): True to use cached version, False to download the content of the Sheet on the fly. Defaults to True (date is taken from the Task parameters).
        """
        super().__init__(*args, **kwargs)
        self._sheet_from = sheet
        self._use_cache = use_cache

    def getData(self) -> Iterable[Row]:
        data: List[Row] = []
        with self._sheet_from.csvreader(self.date if self._use_cache else None) as csvreader:
            for row in csvreader:
                data.append(row)
        return data
