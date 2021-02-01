import contextlib
import datetime
from typing import Iterable, Iterator, Sequence

from simpletasks_data import ImportSource, Mapping

from ..sheets import Sheet


class ImportSheet(ImportSource):
    """Source for ImportTask to import Sheets"""

    def __init__(
        self,
        sheet: Sheet,
        mapping: Mapping = None,
        is_offline: bool = False,
        date: datetime.date = None,
    ) -> None:
        """Constructor

        Args:
        - sheet (Sheet): Sheet to import
        - mapping (Mapping, optional): Mapping used for import. This parameter must be provided if the Sheet object does not have a mapping defined.
        - is_offline (bool, optional): True to use cached version, False to download the content of the Sheet on the fly. Defaults to False.
        - date (datetime.date, optional): Date to use if using cached version. Defaults to None.

        Raises:
        - ValueError: Mapping not defined
        """
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
