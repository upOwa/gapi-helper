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
    @abc.abstractmethod
    def getModel(self) -> SourceModel:
        raise NotImplementedError

    @abc.abstractmethod
    def getQuery(self) -> Query:
        raise NotImplementedError

    @abc.abstractmethod
    def getSheet(self) -> Sheet:
        raise NotImplementedError

    @abc.abstractmethod
    def getMapping(self) -> Mapping:
        raise NotImplementedError

    def get_keycolumn_name(self) -> str:
        return "id"

    def dumpToSheet(self) -> Dict[str, int]:
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

        return {"dump": self.dumpToSheet()}
