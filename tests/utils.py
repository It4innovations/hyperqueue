from typing import List

import time


def parse_table(table_string: str) -> List[str]:
    lines = table_string.strip().split("\n")
    result = []
    frame = set("+-")
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
    return result


def wait_until(fn, sleep_s=0.2, timeout_s=5):
    end = time.time() + timeout_s

    while time.time() < end:
        value = fn()
        if value is not None and value is not False:
            return value
        time.sleep(sleep_s)
    raise Exception(f"Wait timeouted after {timeout_s} seconds")