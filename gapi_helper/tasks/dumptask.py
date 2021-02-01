import abc
from typing import Dict, Generic, List, Tuple, TypeVar, cast

from flask_sqlalchemy.model import Model as BaseModel
from simpletasks import Task
from simpletasks_data import Mapping
from simpletasks_data.helpers import num2col
from simpletasks_data.mapping import Column
from sqlalchemy.orm import Query

from gapi_helper.sheets import Sheet

SourceModel = TypeVar("SourceModel", bound=BaseModel)


class DumpTask(Task, Generic[SourceModel], metaclass=abc.ABCMeta):
    """Task for dumping a model into a Sheet"""

    @abc.abstractmethod
    def getModel(self) -> SourceModel:
        """Method to implement to define the model to dump

        Returns:
        - SourceModel: Instance of a model
        """
        raise NotImplementedError  # pragma: no cover

    def getQuery(self) -> Query:
        """Method that can be re-implemented to define the query to execute for dumping.

        By default, selects all items in the model.

        Returns:
        - Query: Query to execute - all() method will be called to execute the query
        """
        return self.getModel().query

    @abc.abstractmethod
    def getSheet(self) -> Sheet:
        """Method to implement to define the Sheet to dump the data into.

        Data will be dumped from the first row and first column.

        Returns:
        - Sheet: Sheet to dump data into.
        """
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    def getMapping(self) -> Mapping:
        """Method to implement to define the mapping used for dumping.

        This is mandatory to know the order of the columns and the formatting.

        Returns:
        - Mapping: Mapping to use for dumping
        """
        raise NotImplementedError  # pragma: no cover

    def _dumpToSheet(self) -> Dict[str, int]:
        written = 0
        columns: List[Tuple[str, Column]] = []
        for name, column in self._mapping.get_columns():
            if isinstance(column, Column):
                columns.append((name, column))
            else:
                raise ValueError("Column type for {} is not supported".format(name))

        columns.sort(key=lambda x: x[1].column_number)

        assert columns[0][1].column_number == 0

        range_clear = "A1:{}".format(num2col(max([x[1].column_number for x in columns])))

        header = []
        for name, column in columns:
            header.append(cast(str, column.header))
        values: List[List[str]] = [header]

        for x in self.progress(self._query.all(), desc="Generating data for sheet"):
            row: List[str] = []
            for name, column in columns:
                val = column.formatter(getattr(x, name))
                if len(val) > 0 and val[0] == "0":
                    val = "'" + val
                row.append(val)
            values.append(row)
            written += 1

        # TODO: clean and write in same operation
        self.executeOrRetry(lambda: self._sheet.bulkClean(range_clear))
        self.executeOrRetry(lambda: self._sheet.bulkWrite(values, 0, 0))
        return {"written": written}

    def do(self) -> Dict[str, Dict[str, int]]:
        self._model = self.getModel()
        self._sheet = self.getSheet()
        self._mapping = self.getMapping()
        self._query = self.getQuery()

        self._mapping._complete_from_model(self._model)

        return {"dump": self._dumpToSheet()}
