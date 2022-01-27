from .utils import parse_table


def test_parse_table_horizontal():
    table = parse_table(
        """+---------+--------------+
| Id      | Name            |
+---------+--------------+
| a | b |
| c   | d |
+---------+--------------+
"""
    )
    assert table.header == ["Id", "Name"]
    assert table.rows == [
        ["a", "b"],
        ["c", "d"],
    ]


def test_parse_table_horizontal_empty():
    table = parse_table(
        """+---------+--------------+
| Id      | Name            |
+---------+--------------+
"""
    )
    assert table.header == ["Id", "Name"]
    assert table.rows == []


def test_parse_table_vertical():
    table = parse_table(
        """+---------+--------------+
| Id      | 1            |
| Name | 2 |
| Value   | c |
+---------+--------------+
"""
    )
    assert table.header is None
    assert table.rows == [
        ["Id", "1"],
        ["Name", "2"],
        ["Value", "c"],
    ]


def test_parse_table_multiline_value():
    table = parse_table(
        """+---------+--------------+
| Id      | 1            |
| Name | line1 |
| | line2 |
| | line3 |
| Value   | c |
+---------+--------------+
"""
    )
    assert table.header is None
    assert table.rows == [
        ["Id", "1"],
        ["Name", "line1\nline2\nline3"],
        ["Value", "c"],
    ]


def test_parse_table_empty():
    table = parse_table(
        """
+---------+--------------+
+---------+--------------+
    """
    )
    assert table.header is None
    assert table.rows == []


def test_parse_table_ignore_suffix():
    table = parse_table(
        """
+---------+--------------+
|a|b|
+---------+--------------+
|c|d|
+---------+--------------+
hello world
    """
    )
    assert table.header == ["a", "b"]
    assert table.rows == [["c", "d"]]
