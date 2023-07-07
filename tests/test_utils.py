from .utils import parse_table, parse_tables


def test_parse_table_horizontal():
    table = parse_table("""+---------+--------------+
| Id      | Name            |
+---------+--------------+
| a | b |
| c   | d |
+---------+--------------+
""")
    assert table.header == ["Id", "Name"]
    assert table.rows == [
        ["a", "b"],
        ["c", "d"],
    ]


def test_parse_table_horizontal_empty():
    table = parse_table("""+---------+--------------+
| Id      | Name            |
+---------+--------------+
""")
    assert table.header == ["Id", "Name"]
    assert table.rows == []


def test_parse_table_vertical():
    table = parse_table("""+---------+--------------+
| Id      | 1            |
| Name | 2 |
| Value   | c |
+---------+--------------+
""")
    assert table.header is None
    assert table.rows == [
        ["Id", "1"],
        ["Name", "2"],
        ["Value", "c"],
    ]


def test_parse_table_multiline_value():
    table = parse_table("""+---------+--------------+
| Id      | 1            |
| Name | line1 |
| | line2 |
| | line3 |
| Value   | c |
+---------+--------------+
""")
    assert table.header is None
    assert table.rows == [
        ["Id", "1"],
        ["Name", "line1\nline2\nline3"],
        ["Value", "c"],
    ]


def test_parse_table_empty():
    table = parse_table("""
+---------+--------------+
+---------+--------------+
    """)
    assert table is None


def test_parse_table_ignore_suffix():
    table, remainder = parse_table("""
+---------+--------------+
|a|b|
+---------+--------------+
|c|d|
+---------+--------------+
hello world
    """)
    assert table.header == ["a", "b"]
    assert table.rows == [["c", "d"]]


def test_parse_tables_horizontal():
    tables = parse_tables("""+---------+--------------+
| Id      | Name            |
+---------+--------------+
| a | b |
| c   | d |
+---------+--------------+
+---------+--------------+
| Id      | Name            |
+---------+--------------+
| e | f |
| g   | h |
+---------+--------------+
""")
    print("Found tables:", len(tables))
    assert tables[0].header == ["Id", "Name"]
    assert tables[0].rows == [
        ["a", "b"],
        ["c", "d"],
    ]
    assert tables[1].header == ["Id", "Name"]
    assert tables[1].rows == [
        ["e", "f"],
        ["g", "h"],
    ]


def test_parse_tables_vertical():
    tables = parse_tables("""+---------+--------------+
| Id      | 1            |
| Name | 2 |
| Value   | a |
+---------+--------------+
+---------+--------------+
| Id      | 3            |
| Name | 4 |
| Value   | b |
+---------+--------------+
""")
    assert tables[0].header is None
    assert tables[0].rows == [
        ["Id", "1"],
        ["Name", "2"],
        ["Value", "a"],
    ]
    assert tables[1].header is None
    assert tables[1].rows == [
        ["Id", "3"],
        ["Name", "4"],
        ["Value", "b"],
    ]
