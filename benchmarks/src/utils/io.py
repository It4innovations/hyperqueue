import typing
from typing import TypeVar

Type = TypeVar("Type")


def from_json(cls: type[Type], input: typing.Union[typing.TextIO, str]) -> Type:
    from serde import json

    if not isinstance(input, str):
        input = input.read()
    return json.from_json(cls, input)


def to_json(object: typing.Any, file: typing.TextIO):
    from serde import json

    serialized = json.to_json(object)
    file.write(serialized)
