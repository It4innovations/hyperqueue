from typing import List

import time


def parse_table(table_string: str) -> List[str]:
    lines = table_string.strip().split("\n")
    result = []
    frame = set("+-")
    for i, line in enumerate(lines):
        if i % 2 == 1:
            result.append([x.strip() for x in line.split("|")[1:-1]])
        else:
            if not set(line).issubset(frame):
                raise Exception("Invalid table line: {}".format(set(line)))
    return result


def wait_until(fn, sleep_s=0.2, timeout_s=5):
    end = time.time() + timeout_s

    while time.time() < end:
        value = fn()
        if value is not None and value is not False:
            return value
        time.sleep(sleep_s)
    raise Exception(f"Wait timeouted after {timeout_s} seconds")
