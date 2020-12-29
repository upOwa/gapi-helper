import pytest

from gapi_helper.sheets.range import Range


def test_init() -> None:
    o = Range(0, 0, 0, None)
    assert o.start_col == 0
    assert o.end_col == 0
    assert o.start_row == 0
    assert o.end_row is None
    assert o.height is None
    assert o.width == 1
    assert o.toA1N1() == "A1:A"
    assert repr(o) == "(0, 0, 0, None)/A1:A"

    o = Range(0, 0, 1, 2)
    assert o.start_col == 0
    assert o.end_col == 1
    assert o.start_row == 0
    assert o.end_row == 2
    assert o.height == 3
    assert o.width == 2
    assert o.toA1N1() == "A1:B3"
    assert repr(o) == "(0, 0, 1, 2)/A1:B3"

    o = Range(0, 0, None, None)
    assert o.height is None
    assert o.width is None
    assert o.toA1N1() == "A1:*"
    assert repr(o) == "(0, 0, None, None)/A1:*"

    o = Range(0, 0, 1, None)
    assert o.height is None
    assert o.width == 2
    assert o.toA1N1() == "A1:B"
    assert repr(o) == "(0, 0, 1, None)/A1:B"


def test_a1n1() -> None:
    o = Range.fromA1N1("A1:B3")
    assert o.start_col == 0
    assert o.end_col == 1
    assert o.start_row == 0
    assert o.end_row == 2
    assert o.height == 3
    assert o.width == 2
    assert o.toA1N1() == "A1:B3"
    assert repr(o) == "(0, 0, 1, 2)/A1:B3"

    o = Range.fromA1N1("A1:B")
    assert o.start_col == 0
    assert o.end_col == 1
    assert o.start_row == 0
    assert o.end_row is None
    assert o.height is None
    assert o.width == 2
    assert o.toA1N1() == "A1:B"
    assert repr(o) == "(0, 0, 1, None)/A1:B"

    o = Range.fromA1N1("A1:*")
    assert o.start_col == 0
    assert o.end_col is None
    assert o.start_row == 0
    assert o.end_row is None
    assert o.height is None
    assert o.width is None
    assert o.toA1N1() == "A1:*"
    assert repr(o) == "(0, 0, None, None)/A1:*"

    with pytest.raises(ValueError) as e:
        o = Range.fromA1N1("A1")
    assert str(e.value) == "Could not parse range A1"

    with pytest.raises(ValueError) as e:
        o = Range.fromA1N1("A")
    assert str(e.value) == "Could not parse range A"
