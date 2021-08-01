import time
from typing import List, Optional, Union

DEFAULT_TIMEOUT = 5
JOB_TABLE_ROWS = 12


# TODO: create a pandas dataframe instead?
class Table:
    def __init__(self, rows):
        self.rows = rows

    def __getitem__(self, item):
        if isinstance(item, slice):
            return Table(self.rows[item])
        return self.rows[item]

    def __iter__(self):
        yield from self.rows

    def get_row_value(self, key: str) -> Optional[str]:
        """
        Assumes vertical table (each value has a separate row).
        """
        row = [r for r in self.rows if r and r[0] == key]
        if not row:
            return None
        assert len(row) == 1
        return row[0][1]

    def check_value_row(self, key: str, value: str):
        row = self.get_row_value(key)
        if not row:
            raise Exception(f"Key {key} not found in\n{self}")
        assert_equals(row, value)

    def get_column_value(self, key: str) -> Optional[List[str]]:
        """
        Assumes horizontal table (each value has a separate column).
        """
        header = self.rows[0]
        if key not in header:
            return None

        index = header.index(key)
        return [row[index] for row in self.rows[1:]]

    def check_value_column(self, key: str, index: int, value: str):
        column = self.get_column_value(key)
        if not column:
            raise Exception(f"Key {key} not found in\n{self}")
        row = column[index]
        assert_equals(row, value)

    def check_value_columns(self, keys: List[str], index: int, values: List[str]):
        assert len(keys) == len(values)
        for (key, val) in zip(keys, values):
            self.check_value_column(key, index, val)

    def __len__(self):
        return len(self.rows)

    def __repr__(self):
        return "\n".join(" ".join(val) for val in self.rows)


def assert_equals(a, b):
    """
    Workaround for pytest.
    Without this it won't show the differing values if an assert error happens inside some method.
    """
    if a != b:
        raise Exception(f"{a} does not equal {b}")


def parse_table(table_string: str) -> Table:
    lines = table_string.strip().split("\n")
    result = []
    new_row = True
    for line in lines:
        if line.startswith("+-"):
            new_row = True
            continue
        items = [x.strip() for x in line.split("|")[1:-1]]
        if new_row:
            result.append(items)
            new_row = False
        else:
            for i, item in enumerate(items):
                if item:
                    result[-1][i] += "\n" + item
    return Table(result)


def wait_until(fn, sleep_s=0.2, timeout_s=DEFAULT_TIMEOUT):
    end = time.time() + timeout_s

    while time.time() < end:
        value = fn()
        if value is not None and value is not False:
            return value
        time.sleep(sleep_s)
    raise Exception(f"Wait timeouted after {timeout_s} seconds")


def wait_for_state(
    env,
    ids: Union[int, List[int]],
    target_states: Union[str, List[str]],
    commands: List[str],
    state_index: int,
    **kwargs,
):
    if isinstance(ids, int):
        ids = {str(ids)}
    else:
        ids = set(str(id) for id in ids)

    if isinstance(target_states, str):
        target_states = {target_states.lower()}
    else:
        target_states = set(state.lower() for state in target_states)

    def check():
        table = env.command(commands, as_table=True)
        jobs = [row for row in table[1:] if row[0] in ids]
        return len(jobs) >= len(ids) and all(
            j[state_index].lower() in target_states for j in jobs
        )

    wait_until(check, **kwargs)


def wait_for_job_state(
    env, ids: Union[int, List[int]], target_states: Union[str, List[str]], **kwargs
):
    wait_for_state(env, ids, target_states, ["jobs"], 2, **kwargs)


def wait_for_worker_state(
    env, ids: Union[int, List[int]], target_states: Union[str, List[str]], **kwargs
):
    wait_for_state(env, ids, target_states, ["worker", "list"], 1, **kwargs)
