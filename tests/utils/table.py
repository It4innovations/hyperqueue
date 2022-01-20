from typing import Dict, List, Optional

JOB_TABLE_ROWS = 16


class Table:
    def __init__(self, rows: List[List[str]], header: Optional[List[str]]):
        self.rows = rows
        self.header = header

    def __getitem__(self, item):
        if isinstance(item, slice):
            return Table(self.rows[item], header=self.header)
        return self.rows[item]

    def __iter__(self):
        yield from self.rows

    def as_horizontal(self) -> "Table":
        if self.header is not None:
            return self
        assert self.rows
        return Table(self.rows[1:], self.rows[0])

    def get_row_value(self, key: str) -> Optional[str]:
        """
        Assumes vertical table (each value has a separate row).
        """
        row = [r for r in self.rows if r and r[0] == key]
        if not row:
            return None
        assert len(row) == 1
        return row[0][1]

    def check_row_value(self, key: str, value: str):
        row = self.get_row_value(key)
        if row is None:
            raise Exception(f"Key `{key!r}` not found in\n{self}")
        assert_equals(row, value)

    def get_column_value(self, key: str) -> Optional[List[str]]:
        """
        Assumes horizontal table (each value has a separate column).
        """
        if self.header is None:
            raise Exception(f"This table is not horizontal!\n{self}")
        if key not in self.header:
            return None

        index = self.header.index(key)
        return [row[index] for row in self.rows]

    def check_column_value(self, key: str, index: int, value: str):
        column = self.get_column_value(key)
        if not column:
            raise Exception(f"Value for key `{key!r}` not found in\n{self}")
        row = column[index]
        assert_equals(row, value)

    def check_columns_value(self, keys: List[str], index: int, values: List[str]):
        assert len(keys) == len(values)
        for (key, val) in zip(keys, values):
            self.check_column_value(key, index, val)

    def print(self):
        if self.header:
            print("Header", self.header)
        for i, row in enumerate(self):
            print(i, row)

    def __len__(self):
        return len(self.rows)

    def __repr__(self):
        rows = [self.header] if self.header else []
        rows.extend(self.rows)
        return "\n".join(" | ".join(val) for val in rows)


def assert_equals(a, b):
    """
    Workaround for pytest.
    Without this it won't show the differing values if an assert error happens inside some method.
    """
    if a != b:
        raise Exception(f"{a!r} does not equal {b!r}")


def parse_table(table_string: str) -> Table:
    lines = table_string.strip().split("\n")
    rows = []
    new_row = True
    header = None

    for line in lines:
        # Log output
        if line.startswith("["):
            continue
        if line.startswith("+-"):
            if len(rows) == 1 and not header:
                # Second +- has appeared, the previous row was a header
                header = rows[0]
                rows.clear()
            new_row = True
            continue
        items = [x.strip() for x in line.split("|")[1:-1]]

        # Empty first column = previous row continues
        if items and items[0]:
            new_row = True
        if new_row:
            rows.append(items)
            new_row = False
        else:
            for i, item in enumerate(items):
                if item:
                    rows[-1][i] += "\n" + item
    return Table(rows, header=header)


def parse_multiline_cell(cell: str) -> Dict[str, str]:
    lines = cell.splitlines(keepends=False)
    return dict(line.split(": ") for line in lines)
