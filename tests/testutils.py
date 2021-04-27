from typing import List


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
