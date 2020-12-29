import contextlib
import datetime
from typing import Iterable, Iterator, Sequence

from simpletasks_data import ImportSource, Mapping

from ..sheets import Sheet


class ImportSheet(ImportSource):
    def __init__(
        self,
        sheet: Sheet,
        mapping: Mapping = None,
        is_offline: bool = False,
        date: datetime.date = None,
    ) -> None:
        self._sheet = sheet
        self._is_offline = is_offline
        self._date = date

        if mapping is None:
            if sheet.mapping is None:
                raise ValueError("Sheet mapping cannot be None")
            mapping = sheet.mapping
        super().__init__(mapping)

    @contextlib.contextmanager
    def getGeneratorData(self) -> Iterator[Iterable[Sequence[str]]]:
        with self._sheet.csvreader(self._date if self._is_offline else None) as csvreader:
            yield csvreader
