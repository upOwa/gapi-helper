from typing import Optional

from simpletasks_data.helpers import num2col, range2tab


class Range:
    def __init__(
        self, start_col: int, start_row: int, end_col: Optional[int] = None, end_row: Optional[int] = None
    ) -> None:
        self.start_col = start_col
        self.start_row = start_row
        self.end_col = end_col
        self.end_row = end_row

        self.width = (end_col - start_col + 1) if end_col is not None else None
        self.height = (end_row - start_row + 1) if end_row is not None else None

    def matches(self, other: "Range") -> bool:
        if self.width is not None and other.width is not None:
            if self.width != other.width:
                return False
        if self.height is not None and other.height is not None:
            if self.height != other.height:
                return False
        return True

    @staticmethod
    def fromA1N1(range: str) -> "Range":
        (col_start, row_start, col_end, row_end) = range2tab(range)
        return Range(col_start, row_start, col_end, row_end)

    def toA1N1(self) -> str:
        a1n1 = f"{num2col(self.start_col+1)}{self.start_row+1}:"
        if self.end_col is not None and self.end_row is not None:
            a1n1 += f"{num2col(self.end_col+1)}{self.end_row+1}"
        elif self.end_col is not None:
            a1n1 += f"{num2col(self.end_col+1)}"
        elif self.end_row is not None:
            a1n1 += f"{self.end_row+1}"
        else:
            a1n1 += "*"
        return a1n1

    def __repr__(self) -> str:
        tab = f"({self.start_col}, {self.start_row}, {self.end_col}, {self.end_row})"

        return tab + "/" + self.toA1N1()
