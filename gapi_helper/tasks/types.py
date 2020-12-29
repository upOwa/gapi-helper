from typing import Callable, Sequence

Row = Sequence[str]
Filter = Callable[[int, Row], bool]
Adapter = Callable[[int, Row], Row]
