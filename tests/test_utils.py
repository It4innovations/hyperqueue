from .utils import parse_table


def test_table_parser():
    table = parse_table(
        """+---------+--------------+
| Id      | 1            |
+---------+--------------+
|         | WAITING [2]  |
+---------+--------------+
| Tasks   | 10           |
+---------+--------------+
| Command | xxx          |
|         | 1            |
|         | 2            |
+---------+--------------+
| Stdout  | N/A          |
+---------+--------------+"""
    )
    assert table == [
        ["Id", "1"],
        ["", "WAITING [2]"],
        ["Tasks", "10"],
        ["Command", "xxx\n1\n2"],
        ["Stdout", "N/A"],
    ]
